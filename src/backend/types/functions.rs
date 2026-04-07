//! Built-in SQL scalar functions operating on [`DataValue`].
//!
//! All functions follow these conventions:
//! - They accept **typed** [`DataValue`] references (not raw strings).
//! - They return [`FunctionError::TypeMismatch`] if the input type is not
//!   supported by that function.
//! - String functions (`upper`, `lower`, `trim`, etc.) always return
//!   `DataValue::Varchar` even when given a `CHAR` input, matching SQL
//!   standard behaviour.
//!
//! # Function index
//!
//! | Function | SQL | Supported types |
//! |---|---|---|
//! | [`length`] | `LENGTH(s)` | CHAR, VARCHAR |
//! | [`substring`] | `SUBSTRING(s FROM n FOR len)` | CHAR, VARCHAR |
//! | [`upper`] / [`lower`] | `UPPER(s)` / `LOWER(s)` | CHAR, VARCHAR |
//! | [`trim`] / [`ltrim`] / [`rtrim`] | `TRIM(s)` etc. | CHAR, VARCHAR |
//! | [`extract`] | `EXTRACT(part FROM val)` | DATE, TIME, TIMESTAMP |
//! | [`abs`] | `ABS(n)` | all numeric types |
//! | [`round`] | `ROUND(n, places)` | all numeric types |
//! | [`floor`] / [`ceiling`] | `FLOOR(n)` / `CEIL(n)` | all numeric types |
//! | [`cast`] | `CAST(val AS type)` | any compatible pair |
//! | [`coalesce`] | `COALESCE(...)` | any nullable sequence |
//! | [`nullif`] | `NULLIF(a, b)` | any comparable pair |
//! | [`current_date`] / [`current_time`] / [`current_timestamp`] | session time | — |

use chrono::{Datelike, Local, Timelike};
use std::fmt;

use crate::types::comparison::Comparable;
use crate::types::comparison::value_type_name;
use crate::types::datatype::DataType;
use crate::types::value::DataValue;

// ── Error types ───────────────────────────────────────────────────────────────

/// Error returned by built-in scalar functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionError {
    /// The input value's type is not accepted by this function.
    TypeMismatch { expected: String, found: String },
    /// The argument value is syntactically correct but semantically invalid
    /// (e.g. a cast that cannot be performed, or an out-of-range index).
    InvalidArgument(String),
}

impl fmt::Display for FunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionError::TypeMismatch { expected, found } => {
                write!(f, "Type mismatch: expected {}, found {}", expected, found)
            }
            FunctionError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl std::error::Error for FunctionError {}

// ── DatePart enum ─────────────────────────────────────────────────────────────

/// A calendar or clock field that can be extracted from a temporal value.
///
/// Used by [`extract`] to select which component to return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

// ── String functions ──────────────────────────────────────────────────────────

/// Return the character length of a string value.
///
/// Strips trailing spaces from `CHAR` values before counting (SQL semantics).
/// Returns the number of Unicode scalar values, not bytes.
pub fn length(value: &DataValue) -> Result<usize, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(s.chars().count()),
        // CHAR: strip trailing padding before measuring
        DataValue::Char(s) => Ok(s.trim_end_matches(' ').chars().count()),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Return a substring of a string value.
///
/// `start` is **1-based** (SQL convention); passing `0` is an error.
/// If `start` exceeds the string length, returns an empty VARCHAR.
/// The result is always `DataValue::Varchar`.
pub fn substring(value: &DataValue, start: usize, len: usize) -> Result<DataValue, FunctionError> {
    let s = match value {
        DataValue::Varchar(s) => s.clone(),
        DataValue::Char(s) => s.trim_end_matches(' ').to_string(),
        _ => {
            return Err(FunctionError::TypeMismatch {
                expected: "VARCHAR/CHAR".to_string(),
                found: value_type_name(value).to_string(),
            });
        }
    };

    if start == 0 {
        return Err(FunctionError::InvalidArgument(
            "start is 1-based and must be >= 1".to_string(),
        ));
    }

    let chars: Vec<char> = s.chars().collect();
    if start > chars.len() {
        return Ok(DataValue::Varchar(String::new()));
    }

    // Convert 1-based SQL index to 0-based Rust index
    let from = start - 1;
    let to = (from + len).min(chars.len());
    let out: String = chars[from..to].iter().collect();
    Ok(DataValue::Varchar(out))
}

/// Convert a string value to uppercase. Result is always `Varchar`.
pub fn upper(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.to_uppercase())),
        DataValue::Char(s) => Ok(DataValue::Varchar(s.trim_end_matches(' ').to_uppercase())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Convert a string value to lowercase. Result is always `Varchar`.
pub fn lower(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.to_lowercase())),
        DataValue::Char(s) => Ok(DataValue::Varchar(s.trim_end_matches(' ').to_lowercase())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Strip leading and trailing whitespace from a string value.
pub fn trim(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.trim().to_string())),
        DataValue::Char(s) => Ok(DataValue::Varchar(s.trim().to_string())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Strip leading whitespace from a string value.
pub fn ltrim(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.trim_start().to_string())),
        DataValue::Char(s) => Ok(DataValue::Varchar(s.trim_start().to_string())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Strip trailing whitespace from a string value.
pub fn rtrim(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.trim_end().to_string())),
        DataValue::Char(s) => Ok(DataValue::Varchar(s.trim_end().to_string())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

// ── Temporal functions ────────────────────────────────────────────────────────

/// Extract an integer component from a date/time/timestamp value.
///
/// Supported combinations:
/// - `DATE`: `Year`, `Month`, `Day`
/// - `TIME`: `Hour`, `Minute`, `Second`
/// - `TIMESTAMP`: all six parts
///
/// Returns [`FunctionError::TypeMismatch`] if the part is not valid for the
/// input type (e.g. extracting `Hour` from a `DATE`).
pub fn extract(part: DatePart, value: &DataValue) -> Result<i32, FunctionError> {
    match value {
        DataValue::Date(d) => match part {
            DatePart::Year => Ok(d.year()),
            DatePart::Month => Ok(d.month() as i32),
            DatePart::Day => Ok(d.day() as i32),
            _ => Err(FunctionError::TypeMismatch {
                expected: "DATE part (year/month/day)".to_string(),
                found: format!("DATE with {:?}", part),
            }),
        },
        DataValue::Time(t) => match part {
            DatePart::Hour => Ok(t.hour() as i32),
            DatePart::Minute => Ok(t.minute() as i32),
            DatePart::Second => Ok(t.second() as i32),
            _ => Err(FunctionError::TypeMismatch {
                expected: "TIME part (hour/minute/second)".to_string(),
                found: format!("TIME with {:?}", part),
            }),
        },
        DataValue::Timestamp(ts) => match part {
            DatePart::Year => Ok(ts.year()),
            DatePart::Month => Ok(ts.month() as i32),
            DatePart::Day => Ok(ts.day() as i32),
            DatePart::Hour => Ok(ts.hour() as i32),
            DatePart::Minute => Ok(ts.minute() as i32),
            DatePart::Second => Ok(ts.second() as i32),
        },
        _ => Err(FunctionError::TypeMismatch {
            expected: "DATE/TIME/TIMESTAMP".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

// ── Numeric functions ─────────────────────────────────────────────────────────

/// Helper: compute 10^exp as i128.
fn pow10(exp: u32) -> i128 {
    10_i128.pow(exp)
}

/// Return the absolute value of a numeric value.
/// Supported for all numeric types: SMALLINT, INT, BIGINT, REAL, DOUBLE, NUMERIC.
pub fn abs(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::SmallInt(v) => Ok(DataValue::SmallInt(v.abs())),
        DataValue::Int(v) => Ok(DataValue::Int(v.abs())),
        DataValue::BigInt(v) => Ok(DataValue::BigInt(v.abs())),
        DataValue::Real(v) => Ok(DataValue::Real(crate::types::value::OrderedF32(v.0.abs()))),
        DataValue::DoublePrecision(v) => Ok(DataValue::DoublePrecision(
            crate::types::value::OrderedF64(v.0.abs()),
        )),
        DataValue::Numeric(v) => Ok(DataValue::Numeric(crate::types::value::NumericValue {
            unscaled: v.unscaled.abs(),
            scale: v.scale,
        })),
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Round a numeric value to `places` decimal points.
///
/// For integer types (`SMALLINT`, `INT`, `BIGINT`) the value is returned unchanged.
/// For `REAL` / `DOUBLE PRECISION`, standard IEEE 754 rounding is used.
/// For `NUMERIC`, half-up rounding is applied to the unscaled integer representation
/// and the result scale is reduced to `max(places, 0)`.
pub fn round(value: &DataValue, places: i32) -> Result<DataValue, FunctionError> {
    match value {
        // Integers are already whole numbers — no rounding needed
        DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_) => Ok(value.clone()),
        DataValue::Real(v) => {
            let factor = 10_f32.powi(places);
            Ok(DataValue::Real(crate::types::value::OrderedF32(
                (v.0 * factor).round() / factor,
            )))
        }
        DataValue::DoublePrecision(v) => {
            let factor = 10_f64.powi(places);
            Ok(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                (v.0 * factor).round() / factor,
            )))
        }
        DataValue::Numeric(v) => {
            let target_scale = places.max(0) as u8;
            if target_scale >= v.scale {
                // No rounding needed — target has more or equal precision
                return Ok(DataValue::Numeric(v.clone()));
            }
            let delta = (v.scale - target_scale) as u32;
            let div = pow10(delta);
            let q = v.unscaled / div;
            let r = v.unscaled.abs() % div;
            // Half-up rounding: round up if remainder >= half the divider
            let rounded = if r * 2 >= div {
                q + v.unscaled.signum()
            } else {
                q
            };
            Ok(DataValue::Numeric(crate::types::value::NumericValue {
                unscaled: rounded,
                scale: target_scale,
            }))
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Return the largest integer value not greater than the input (floor).
///
/// For `NUMERIC`, the result has scale 0. For negative values with a fractional
/// part, the result is decremented by one (e.g. `FLOOR(-1.3)` = `-2`).
pub fn floor(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_) => Ok(value.clone()),
        DataValue::Real(v) => Ok(DataValue::Real(crate::types::value::OrderedF32(v.0.floor()))),
        DataValue::DoublePrecision(v) => Ok(DataValue::DoublePrecision(
            crate::types::value::OrderedF64(v.0.floor()),
        )),
        DataValue::Numeric(v) => {
            if v.scale == 0 {
                return Ok(DataValue::Numeric(v.clone()));
            }
            let div = pow10(v.scale as u32);
            let mut q = v.unscaled / div;
            // For negative values with a non-zero remainder, decrement
            if v.unscaled < 0 && v.unscaled % div != 0 {
                q -= 1;
            }
            Ok(DataValue::Numeric(crate::types::value::NumericValue {
                unscaled: q,
                scale: 0,
            }))
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Return the smallest integer value not less than the input (ceiling).
///
/// For `NUMERIC`, the result has scale 0. For positive values with a fractional
/// part, the result is incremented by one (e.g. `CEILING(1.3)` = `2`).
pub fn ceiling(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_) => Ok(value.clone()),
        DataValue::Real(v) => Ok(DataValue::Real(crate::types::value::OrderedF32(v.0.ceil()))),
        DataValue::DoublePrecision(v) => Ok(DataValue::DoublePrecision(
            crate::types::value::OrderedF64(v.0.ceil()),
        )),
        DataValue::Numeric(v) => {
            if v.scale == 0 {
                return Ok(DataValue::Numeric(v.clone()));
            }
            let div = pow10(v.scale as u32);
            let mut q = v.unscaled / div;
            // For positive values with a non-zero remainder, increment
            if v.unscaled > 0 && v.unscaled % div != 0 {
                q += 1;
            }
            Ok(DataValue::Numeric(crate::types::value::NumericValue {
                unscaled: q,
                scale: 0,
            }))
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

// ── Type conversion helper ────────────────────────────────────────────────────

/// Convert a `DataValue` to the canonical string literal used by the parser.
///
/// Used internally by [`cast`] to re-parse and re-encode the value into the
/// target type via `DataValue::parse_and_encode`.
fn value_to_literal(value: &DataValue) -> String {
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

// ── Type-conversion and conditional functions ─────────────────────────────────

/// Cast a value to a different SQL type.
///
/// Conversion is done by converting the source value to its canonical literal
/// representation and re-encoding it for the target type. Returns
/// [`FunctionError::InvalidArgument`] if the conversion is not possible (e.g.
/// casting a VARCHAR that doesn't parse as an integer to INT).
pub fn cast(value: &DataValue, target: &DataType) -> Result<DataValue, FunctionError> {
    let literal = value_to_literal(value);
    let encoded = DataValue::parse_and_encode(target, &literal)
        .map_err(FunctionError::InvalidArgument)?;
    DataValue::from_bytes(target, &encoded).map_err(FunctionError::InvalidArgument)
}

/// Return the first non-NULL value in the slice, or `None` if all are NULL.
///
/// Implements SQL `COALESCE(expr1, expr2, ...)` semantics.
pub fn coalesce(values: &[Option<DataValue>]) -> Option<DataValue> {
    values.iter().find_map(|v| v.clone())
}

/// Return `NULL` if `left = right`, otherwise return `left`.
///
/// Implements SQL `NULLIF(left, right)` semantics. Returns
/// [`FunctionError::InvalidArgument`] if the two values have incompatible types.
pub fn nullif(left: DataValue, right: DataValue) -> Result<Option<DataValue>, FunctionError> {
    if left
        .compare(&right)
        .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?
        == std::cmp::Ordering::Equal
    {
        Ok(None)
    } else {
        Ok(Some(left))
    }
}

// ── Session time functions ────────────────────────────────────────────────────

/// Return the current local date (`DATE`).
pub fn current_date() -> DataValue {
    DataValue::Date(Local::now().date_naive())
}

/// Return the current local time (`TIME`).
pub fn current_time() -> DataValue {
    DataValue::Time(Local::now().time())
}

/// Return the current local date and time (`TIMESTAMP`).
pub fn current_timestamp() -> DataValue {
    DataValue::Timestamp(Local::now().naive_local())
}
