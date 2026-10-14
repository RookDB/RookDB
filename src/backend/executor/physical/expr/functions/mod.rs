//! Scalar function calls (`UPPER(x)`, `ROUND(x)`, `EXTRACT(...)`, ...).
//!
//! `evaluate_scalar_function` evaluates the arguments and dispatches by function
//! name to the per-family helpers in `string`, `math` and `datetime`.

mod datetime;
mod math;
mod string;

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

use super::Expr;
use crate::backend::executor::physical::tuple::{ColumnInfo, Tuple};

/// Evaluate a scalar function call by dispatching to the implementations in
/// `types/functions.rs`. Returns an error for unknown functions or type mismatches.
pub(super) fn evaluate_scalar_function(
    name: &str,
    args: &[Expr],
    tuple: &Tuple,
    schema: &[ColumnInfo],
) -> RookResult<Option<DataValue>> {
    // Evaluate all argument expressions against the tuple
    let evaluated: RookResult<Vec<Option<DataValue>>> =
        args.iter().map(|arg| arg.evaluate(tuple, schema)).collect();
    let evaluated = evaluated?;

    let upper = name.to_ascii_uppercase();

    match upper.as_str() {
        // ── 0-argument functions ────────────────────────────────────────
        "CURRENT_DATE" => Ok(Some(crate::types::functions::current_date())),
        "CURRENT_TIME" => Ok(Some(crate::types::functions::current_time())),
        "CURRENT_TIMESTAMP" | "NOW" => Ok(Some(crate::types::functions::current_timestamp())),

        // ── String functions ────────────────────────────────────────────
        "UPPER" | "UCASE" => string::eval_upper(evaluated),
        "LOWER" | "LCASE" => string::eval_lower(evaluated),
        "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => string::eval_length(evaluated),
        "POSITION" | "CHARINDEX" => string::eval_position(evaluated),
        "TRIM" => string::eval_trim(evaluated),
        "LTRIM" => string::eval_ltrim(evaluated),
        "RTRIM" => string::eval_rtrim(evaluated),
        "SUBSTRING" | "SUBSTR" => string::eval_substring(evaluated),
        "||" => string::eval_concat_operator(evaluated),
        "CONCAT" => string::eval_concat(evaluated),

        // ── Numeric functions ───────────────────────────────────────────
        "ABS" => math::eval_abs(evaluated),
        "FLOOR" => math::eval_floor(evaluated),
        "CEIL" | "CEILING" => math::eval_ceiling(evaluated),
        "MOD" => math::eval_mod(evaluated),
        "POWER" | "POW" => math::eval_power(evaluated),
        "SQRT" => math::eval_sqrt(evaluated),
        "ROUND" => math::eval_round(evaluated),

        // ── DATE/TIME extract ───────────────────────────────────────────
        "EXTRACT" | "DATE_PART" => datetime::eval_extract(args.len(), evaluated),

        // ── NULL handling ───────────────────────────────────────────────
        "COALESCE" => {
            let vals: Vec<Option<DataValue>> = evaluated;
            Ok(crate::types::functions::coalesce(&vals))
        }
        "NULLIF" => eval_nullif(evaluated),

        _ => Err(RookError::Evaluation(format!(
            "Unknown scalar function: '{}'",
            name
        ))),
    }
}

/// `NULLIF(a, b)`.
fn eval_nullif(evaluated: Vec<Option<DataValue>>) -> RookResult<Option<DataValue>> {
    let mut iter = evaluated.into_iter();
    let left = iter.next().flatten();
    let right = iter.next().flatten();
    // SQL NULLIF(expr1, expr2) is equivalent to:
    //   CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END
    // If expr1 is NULL, then expr1 = expr2 is UNKNOWN, so the ELSE
    // branch fires and returns expr1 (which is NULL).
    match (left, right) {
        (None, _) => Ok(None),          // NULLIF(NULL, anything) → NULL
        (Some(l), None) => Ok(Some(l)), // NULLIF(val, NULL) → val (val = NULL is UNKNOWN → ELSE val)
        (Some(l), Some(r)) => crate::types::functions::nullif(l, r).context("NULLIF error"),
    }
}
