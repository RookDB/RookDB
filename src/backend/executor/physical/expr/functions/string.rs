//! String scalar functions: case, length, search, trim, substring, concatenation.

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

/// Helper to convert a DataValue to its raw string representation for functions like CONCAT.
pub(super) fn value_to_raw_string(value: &DataValue) -> String {
    match value {
        DataValue::Char(s) => s.trim_end_matches(' ').to_string(),
        DataValue::Varchar(s) => s.clone(),
        DataValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        DataValue::Time(t) => t.format("%H:%M:%S%.6f").to_string(),
        DataValue::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        DataValue::Bit(bits) => bits.clone(),
        _ => value.to_string(),
    }
}

/// `UPPER(s)` / `UCASE(s)`.
pub(super) fn eval_upper(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("UPPER requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::upper(&dv)
            .map(Some)
            .context("UPPER error"),
    }
}

/// `LOWER(s)` / `LCASE(s)`.
pub(super) fn eval_lower(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("LOWER requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::lower(&dv)
            .map(Some)
            .context("LOWER error"),
    }
}

/// `LENGTH(s)` and its aliases; returns the character count.
pub(super) fn eval_length(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("LENGTH requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => {
            let len = crate::types::functions::length(&dv).context("LENGTH error")?;
            Ok(Some(DataValue::Int(len as i32)))
        }
    }
}

/// `POSITION(substring, value)` / `CHARINDEX`.
pub(super) fn eval_position(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let substring = match iter.next() {
        Some(Some(dv)) => dv,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "POSITION requires at least 1 argument".to_string(),
            ));
        }
    };
    let value = match iter.next() {
        Some(Some(dv)) => dv,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "POSITION requires 2 arguments".to_string(),
            ));
        }
    };
    let pos = crate::types::functions::position(&value, &substring).context("POSITION error")?;
    Ok(Some(DataValue::Int(pos)))
}

/// `TRIM(s)`.
pub(super) fn eval_trim(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("TRIM requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::trim(&dv)
            .map(Some)
            .context("TRIM error"),
    }
}

/// `LTRIM(s)`.
pub(super) fn eval_ltrim(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("LTRIM requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::ltrim(&dv)
            .map(Some)
            .context("LTRIM error"),
    }
}

/// `RTRIM(s)`.
pub(super) fn eval_rtrim(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let val = evaluated
        .into_iter()
        .next()
        .ok_or_else(|| RookError::Evaluation("RTRIM requires 1 argument".to_string()))?;
    match val {
        None => Ok(None),
        Some(dv) => crate::types::functions::rtrim(&dv)
            .map(Some)
            .context("RTRIM error"),
    }
}

/// `SUBSTRING(s, start [, len])` / `SUBSTR`.
pub(super) fn eval_substring(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let val = match iter.next() {
        Some(Some(dv)) => dv,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "SUBSTRING requires a string argument".to_string(),
            ));
        }
    };
    let int_arg = |arg: Option<Option<DataValue>>, what: &str| match arg {
        Some(Some(DataValue::SmallInt(v))) => Ok(Some(Some(v as i64))),
        Some(Some(DataValue::Int(v))) => Ok(Some(Some(v as i64))),
        Some(Some(DataValue::BigInt(v))) => Ok(Some(Some(v))),
        Some(None) => Ok(Some(None)),
        None => Ok(None),
        Some(Some(_)) => Err(RookError::Evaluation(format!(
            "SUBSTRING requires integer {}",
            what
        ))),
    };
    let start = match int_arg(iter.next(), "start position")? {
        Some(Some(s)) => s,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "SUBSTRING requires a start position".to_string(),
            ));
        }
    };
    // As in PostgreSQL, the result is the characters at positions
    // start .. start + len - 1 that exist: a start before 1 counts toward
    // the length, so SUBSTRING('hello', 0, 2) is 'h'.
    let end = match int_arg(iter.next(), "length")? {
        Some(Some(l)) if l < 0 => return Ok(None),
        Some(Some(l)) => Some(start.saturating_add(l)),
        Some(None) => return Ok(None),
        None => None,
    };
    let from = start.max(1);
    let len = match end {
        Some(end) => end.saturating_sub(from).max(0) as usize,
        None => usize::MAX,
    };
    let start = usize::try_from(from).unwrap_or(usize::MAX);
    crate::types::functions::substring(&val, start, len)
        .map(Some)
        .context("SUBSTRING error")
}

/// The `||` operator: NULL if either operand is NULL (SQL), unlike the
/// CONCAT() function, which skips NULLs.
pub(super) fn eval_concat_operator(
    evaluated: Vec<Option<DataValue>>,
) -> RookResult<Option<DataValue>> {
    let mut res = String::new();
    for dv in evaluated {
        match dv {
            Some(dv) => res.push_str(&value_to_raw_string(&dv)),
            None => return Ok(None),
        }
    }
    Ok(Some(DataValue::Varchar(res)))
}

/// `CONCAT(a, b, ...)`: skips NULL arguments.
pub(super) fn eval_concat(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut res = String::new();
    for dv in evaluated.into_iter().flatten() {
        res.push_str(&value_to_raw_string(&dv));
    }
    Ok(Some(DataValue::Varchar(res)))
}
