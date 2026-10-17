use crate::types::value::{DataValue, NumericValue, OrderedF64};

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
