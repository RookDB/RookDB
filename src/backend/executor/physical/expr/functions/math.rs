//! Numeric scalar functions: ABS, FLOOR, CEILING, MOD, POWER, SQRT, ROUND.

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

use super::super::{arithmetic_op, numeric_rem};

/// `ABS(x)`.
pub(super) fn eval_abs(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("ABS requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::abs(&dv)
            .map(Some)
            .context("ABS error"),
    }
}

/// `FLOOR(x)`, or the temporal `FLOOR(date TO part)`.
pub(super) fn eval_floor(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let val = iter.next().flatten();
    let field_arg = iter.next().flatten();
    match (val, field_arg) {
        (Some(dv), None) => {
            // Numeric FLOOR: FLOOR(3.7) → 3
            crate::types::functions::floor(&dv)
                .map(Some)
                .context("FLOOR error")
        }
        (Some(dv), Some(part_str)) => {
            // Temporal FLOOR: FLOOR(date TO MONTH) → truncate date
            let part = crate::types::functions::parse_date_part(&part_str)?;
            crate::types::functions::date_trunc_floor(&dv, part)
                .map(Some)
                .context("FLOOR temporal truncation error")
        }
        (None, _) => Ok(None),
    }
}

/// `CEIL(x)` / `CEILING(x)`, or the temporal `CEILING(date TO part)`.
pub(super) fn eval_ceiling(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let val = iter.next().flatten();
    let field_arg = iter.next().flatten();
    match (val, field_arg) {
        (Some(dv), None) => {
            // Numeric CEILING: CEILING(3.7) → 4
            crate::types::functions::ceiling(&dv)
                .map(Some)
                .context("CEILING error")
        }
        (Some(dv), Some(part_str)) => {
            // Temporal CEILING: CEILING(date TO MONTH) → round up
            let part = crate::types::functions::parse_date_part(&part_str)?;
            crate::types::functions::date_trunc_ceil(&dv, part)
                .map(Some)
                .context("CEILING temporal truncation error")
        }
        (None, _) => Ok(None),
    }
}

/// `MOD(a, b)`.
pub(super) fn eval_mod(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let left = iter.next().flatten();
    let right = iter.next().flatten();
    match (left, right) {
        (None, _) | (_, None) => Ok(None),
        (Some(lv), Some(rv)) => arithmetic_op(
            Some(lv),
            Some(rv),
            |a, b| {
                if b == 0 {
                    Err(RookError::Evaluation("Division by zero".to_string()))
                } else {
                    a.checked_rem(b)
                        .ok_or_else(|| RookError::Evaluation("Integer modulo overflow".to_string()))
                }
            },
            |a, b| {
                if b == 0 {
                    Err(RookError::Evaluation("Division by zero".to_string()))
                } else {
                    a.checked_rem(b)
                        .ok_or_else(|| RookError::Evaluation("BigInt modulo overflow".to_string()))
                }
            },
            |a, b| {
                if b == 0 {
                    Err(RookError::Evaluation("Division by zero".to_string()))
                } else {
                    a.checked_rem(b).ok_or_else(|| {
                        RookError::Evaluation("SmallInt modulo overflow".to_string())
                    })
                }
            },
            |a, b| {
                if b == 0.0 {
                    Err(RookError::Evaluation("Division by zero".to_string()))
                } else {
                    Ok(a % b)
                }
            },
            |a, b| {
                if b == 0.0 {
                    Err(RookError::Evaluation("Division by zero".to_string()))
                } else {
                    Ok(a % b)
                }
            },
            numeric_rem,
        ),
    }
}

/// `POWER(base, exponent)` / `POW`.
pub(super) fn eval_power(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let base = iter.next().flatten();
    let exponent = iter.next().flatten();
    match (base, exponent) {
        (None, _) | (_, None) => Ok(None),
        (Some(b), Some(e)) => {
            let b_f64 = match b {
                DataValue::SmallInt(v) => v as f64,
                DataValue::Int(v) => v as f64,
                DataValue::BigInt(v) => v as f64,
                DataValue::Real(v) => v.0 as f64,
                DataValue::DoublePrecision(v) => v.0,
                DataValue::Numeric(v) => v.unscaled as f64 / 10_f64.powi(v.scale as i32),
                _ => {
                    return Err(RookError::Evaluation(
                        "POWER requires numeric arguments".to_string(),
                    ));
                }
            };
            let e_f64 = match e {
                DataValue::SmallInt(v) => v as f64,
                DataValue::Int(v) => v as f64,
                DataValue::BigInt(v) => v as f64,
                DataValue::Real(v) => v.0 as f64,
                DataValue::DoublePrecision(v) => v.0,
                DataValue::Numeric(v) => v.unscaled as f64 / 10_f64.powi(v.scale as i32),
                _ => {
                    return Err(RookError::Evaluation(
                        "POWER requires numeric arguments".to_string(),
                    ));
                }
            };
            let res = b_f64.powf(e_f64);
            if !res.is_finite() {
                // Negative base with a fractional exponent, zero to
                // a negative power, or overflow.
                return Err(RookError::Evaluation(format!(
                    "POWER({}, {}) is out of range or undefined",
                    b_f64, e_f64
                )));
            }
            Ok(Some(DataValue::DoublePrecision(
                crate::types::value::OrderedF64(res),
            )))
        }
    }
}

/// `SQRT(x)`.
pub(super) fn eval_sqrt(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("SQRT requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => {
            let num = match dv {
                DataValue::SmallInt(v) => v as f64,
                DataValue::Int(v) => v as f64,
                DataValue::BigInt(v) => v as f64,
                DataValue::Real(v) => v.0 as f64,
                DataValue::DoublePrecision(v) => v.0,
                DataValue::Numeric(v) => v.unscaled as f64 / 10_f64.powi(v.scale as i32),
                _ => {
                    return Err(RookError::Evaluation(
                        "SQRT requires a numeric argument".to_string(),
                    ));
                }
            };
            if num < 0.0 {
                return Err(RookError::Evaluation(
                    "cannot take square root of a negative number".to_string(),
                ));
            }
            let res = num.sqrt();
            Ok(Some(DataValue::DoublePrecision(
                crate::types::value::OrderedF64(res),
            )))
        }
    }
}

/// `ROUND(x [, places])`.
pub(super) fn eval_round(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let val = match iter.next() {
        Some(Some(dv)) => dv,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "ROUND requires a numeric argument".to_string(),
            ));
        }
    };
    let places = match iter.next() {
        Some(Some(DataValue::Int(p))) => p,
        Some(Some(DataValue::BigInt(p))) => p as i32,
        Some(None) => return Ok(None),
        None => 0,
        _ => {
            return Err(RookError::Evaluation(
                "ROUND requires integer places".to_string(),
            ));
        }
    };
    crate::types::functions::round(&val, places)
        .map(Some)
        .context("ROUND error")
}
