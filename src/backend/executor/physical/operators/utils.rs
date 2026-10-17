use crate::types::datatype::DataType;
use crate::types::value::{DataValue, NumericValue, OrderedF64};

/// The types within which [`canonical_key`] agrees with comparison: two
/// values of one class compare equal exactly when their keys are equal.
/// Across classes comparison can find values equal whose keys differ (an
/// integer and a float, CHAR and blank-padded VARCHAR, a DATE and text) or
/// tell apart values whose keys are equal (REAL and DOUBLE), so such values
/// must be compared rather than hashed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyClass {
    /// Integers and NUMERIC: compared exactly.
    Exact,
    Real,
    Double,
    Bool,
    Char,
    Varchar,
    /// DATE and TIMESTAMP: a DATE is the TIMESTAMP at its midnight.
    DateTime,
    Time,
    Bit,
}

impl KeyClass {
    pub fn of_value(value: &DataValue) -> Self {
        match value {
            DataValue::SmallInt(_)
            | DataValue::Int(_)
            | DataValue::BigInt(_)
            | DataValue::Numeric(_) => KeyClass::Exact,
            DataValue::Real(_) => KeyClass::Real,
            DataValue::DoublePrecision(_) => KeyClass::Double,
            DataValue::Bool(_) => KeyClass::Bool,
            DataValue::Char(_) => KeyClass::Char,
            DataValue::Varchar(_) => KeyClass::Varchar,
            DataValue::Date(_) | DataValue::Timestamp(_) => KeyClass::DateTime,
            DataValue::Time(_) => KeyClass::Time,
            DataValue::Bit(_) => KeyClass::Bit,
        }
    }

    pub fn of_type(data_type: &DataType) -> Self {
        match data_type {
            DataType::SmallInt
            | DataType::Int
            | DataType::BigInt
            | DataType::Numeric { .. }
            | DataType::Decimal { .. } => KeyClass::Exact,
            DataType::Real => KeyClass::Real,
            DataType::DoublePrecision => KeyClass::Double,
            DataType::Bool => KeyClass::Bool,
            DataType::Char(_) | DataType::Character(_) => KeyClass::Char,
            DataType::Varchar(_) => KeyClass::Varchar,
            DataType::Date | DataType::Timestamp => KeyClass::DateTime,
            DataType::Time => KeyClass::Time,
            DataType::Bit(_) => KeyClass::Bit,
        }
    }
}

/// Map a value to the form used for hashing in joins and set operations, so
/// that values which compare equal hash equal across types: every integer
/// width, NUMERIC, and floats holding the same number share one key; a DATE
/// equals the TIMESTAMP at its midnight; CHAR padding is ignored.
///
/// A whole number that fits 64 bits keys as a BIGINT, so the common integer
/// key needs no allocation; other numbers key as a normalized NUMERIC.
pub fn canonical_key(dv: DataValue) -> DataValue {
    match dv {
        DataValue::SmallInt(v) => DataValue::BigInt(v as i64),
        DataValue::Int(v) => DataValue::BigInt(v as i64),
        DataValue::BigInt(v) => DataValue::BigInt(v),
        DataValue::Numeric(num) => numeric_key(*num),
        DataValue::Real(v) => float_key(v.0 as f64, &v.0.to_string()),
        DataValue::DoublePrecision(v) => float_key(v.0, &v.0.to_string()),
        DataValue::Date(d) => match d.and_hms_opt(0, 0, 0) {
            Some(dt) => DataValue::Timestamp(dt),
            None => DataValue::Date(d),
        },
        DataValue::Char(s) => DataValue::Varchar(s.trim_end().to_string()),
        other => other,
    }
}

/// [`canonical_key`] of a borrowed value, borrowing it when it is its own
/// key, so a lookup of a VARCHAR or BIGINT allocates nothing.
pub fn canonical_key_ref(dv: &DataValue) -> std::borrow::Cow<'_, DataValue> {
    use std::borrow::Cow;
    match dv {
        DataValue::SmallInt(v) => Cow::Owned(DataValue::BigInt(*v as i64)),
        DataValue::Int(v) => Cow::Owned(DataValue::BigInt(*v as i64)),
        DataValue::BigInt(_)
        | DataValue::Bool(_)
        | DataValue::Varchar(_)
        | DataValue::Time(_)
        | DataValue::Timestamp(_)
        | DataValue::Bit(_) => Cow::Borrowed(dv),
        other => Cow::Owned(canonical_key(other.clone())),
    }
}

/// Key for a whole row.
pub fn canonical_row_key(values: &[Option<DataValue>]) -> Vec<Option<DataValue>> {
    values
        .iter()
        .map(|v| v.clone().map(canonical_key))
        .collect()
}

/// A float keys as the decimal it prints as (`1.0` and `1` meet, `1.5` meets
/// NUMERIC `1.50`). Non-finite values and ones too long for a NUMERIC keep
/// their float form.
fn float_key(value: f64, shortest: &str) -> DataValue {
    let fallback = DataValue::DoublePrecision(OrderedF64(value));
    if !value.is_finite() {
        return fallback;
    }
    let (negative, digits) = match shortest.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, shortest),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if frac.len() > 30
        || !whole
            .bytes()
            .chain(frac.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return fallback;
    }
    match format!("{whole}{frac}").parse::<i128>() {
        Ok(unscaled) => numeric_key(NumericValue {
            unscaled: if negative { -unscaled } else { unscaled },
            scale: frac.len() as u8,
        }),
        Err(_) => fallback,
    }
}

/// A NUMERIC's key: a BIGINT when it is a whole number that fits one.
fn numeric_key(num: NumericValue) -> DataValue {
    let num = normalize_numeric(num);
    match i64::try_from(num.unscaled) {
        Ok(v) if num.scale == 0 => DataValue::BigInt(v),
        _ => DataValue::numeric(num),
    }
}

pub(super) fn normalize_numeric(mut num: NumericValue) -> NumericValue {
    if num.unscaled == 0 {
        return NumericValue {
            unscaled: 0,
            scale: 0,
        };
    }
    while num.scale > 0 && num.unscaled % 10 == 0 {
        num.unscaled /= 10;
        num.scale -= 1;
    }
    num
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::OrderedF32;

    #[test]
    fn whole_numbers_key_as_bigint_across_types() {
        let one = DataValue::BigInt(1);
        for v in [
            DataValue::SmallInt(1),
            DataValue::Int(1),
            DataValue::BigInt(1),
            DataValue::numeric(NumericValue {
                unscaled: 100,
                scale: 2,
            }),
            DataValue::Real(OrderedF32(1.0)),
            DataValue::DoublePrecision(OrderedF64(1.0)),
        ] {
            assert_eq!(canonical_key(v), one);
        }
    }

    #[test]
    fn borrowed_keys_match_owned_keys() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        for v in [
            DataValue::SmallInt(-3),
            DataValue::Int(7),
            DataValue::BigInt(9),
            DataValue::numeric(NumericValue {
                unscaled: 150,
                scale: 2,
            }),
            DataValue::Real(OrderedF32(2.0)),
            DataValue::DoublePrecision(OrderedF64(0.1)),
            DataValue::Bool(true),
            DataValue::Char("ab  ".into()),
            DataValue::Varchar("ab  ".into()),
            DataValue::Date(date),
            DataValue::Timestamp(date.and_hms_opt(1, 2, 3).unwrap()),
            DataValue::Time(chrono::NaiveTime::from_hms_opt(1, 2, 3).unwrap()),
            DataValue::Bit("101".into()),
        ] {
            assert_eq!(*canonical_key_ref(&v), canonical_key(v.clone()), "{:?}", v);
        }
    }

    #[test]
    fn fractions_and_wide_numbers_key_as_numeric() {
        let half = canonical_key(DataValue::numeric(NumericValue {
            unscaled: 150,
            scale: 2,
        }));
        assert_eq!(
            canonical_key(DataValue::DoublePrecision(OrderedF64(1.5))),
            half
        );
        assert!(matches!(half, DataValue::Numeric(_)));

        let wide = canonical_key(DataValue::numeric(NumericValue {
            unscaled: i64::MAX as i128 + 1,
            scale: 0,
        }));
        assert!(matches!(wide, DataValue::Numeric(_)));
        assert_ne!(wide, DataValue::BigInt(i64::MAX));
    }
}
