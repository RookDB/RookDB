//! Date/time scalar functions: EXTRACT / DATE_PART.

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

/// `EXTRACT(part, value)` / `DATE_PART`; `arg_count` is the number of call arguments.
pub(super) fn eval_extract(
    arg_count: usize,
    evaluated: Vec<Option<DataValue>>,
) -> RookResult<Option<DataValue>> {
    if arg_count != 2 {
        return Err(RookError::Evaluation(
            "EXTRACT requires 2 arguments: (part, value)".to_string(),
        ));
    }
    let mut iter = evaluated.into_iter();
    let part_str = match iter.next() {
        Some(Some(DataValue::Varchar(s))) | Some(Some(DataValue::Char(s))) => s.to_uppercase(),
        Some(None) => return Ok(None),
        _ => {
            return Err(RookError::Evaluation(
                "EXTRACT first argument must be a string (YEAR/MONTH/DAY/etc.)".to_string(),
            ));
        }
    };
    let val = match iter.next() {
        Some(Some(dv)) => dv,
        Some(None) => return Ok(None),
        None => {
            return Err(RookError::Evaluation(
                "EXTRACT requires 2 arguments".to_string(),
            ));
        }
    };

    let part = match part_str.as_str() {
        "YEAR" => crate::types::functions::DatePart::Year,
        "MONTH" => crate::types::functions::DatePart::Month,
        "DAY" => crate::types::functions::DatePart::Day,
        "HOUR" => crate::types::functions::DatePart::Hour,
        "MINUTE" => crate::types::functions::DatePart::Minute,
        "SECOND" => crate::types::functions::DatePart::Second,
        _ => {
            return Err(RookError::Evaluation(format!(
                "Unknown EXTRACT part: {}",
                part_str
            )));
        }
    };
    let result = crate::types::functions::extract(part, &val).context("EXTRACT error")?;
    Ok(Some(DataValue::Int(result)))
}
