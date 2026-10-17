use crate::types::value::{DataValue, NumericValue, OrderedF64};

/// Map a value to the form used for hashing in joins and set operations, so
/// that values which compare equal hash equal across types: every integer
/// width, NUMERIC, and floats holding the same number share one key; a DATE
/// equals the TIMESTAMP at its midnight; CHAR padding is ignored.
pub fn canonical_key(dv: DataValue) -> DataValue {
    let integer = |v: i128| {
        DataValue::numeric(NumericValue {
            unscaled: v,
            scale: 0,
        })
    };
    match dv {
        DataValue::SmallInt(v) => integer(v as i128),
        DataValue::Int(v) => integer(v as i128),
        DataValue::BigInt(v) => integer(v as i128),
        DataValue::Numeric(num) => DataValue::numeric(normalize_numeric(*num)),
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
        Ok(unscaled) => DataValue::numeric(normalize_numeric(NumericValue {
            unscaled: if negative { -unscaled } else { unscaled },
            scale: frac.len() as u8,
        })),
        Err(_) => fallback,
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
