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

use crate::backend::error::{RookError, RookResult};
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
/// Used by [`extract`] to select which component to return,
/// and by [`date_trunc_floor`]/[`date_trunc_ceil`] for temporal truncation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// Parse a date-part string (e.g. "YEAR", "MONTH", "DAY") into a `DatePart`.
/// Returns `Err` if the string is not a recognized date part.
pub fn parse_date_part(s: &DataValue) -> RookResult<DatePart> {
    let upper = match s {
        DataValue::Varchar(s) => s.to_uppercase(),
        DataValue::Char(s) => s.to_uppercase(),
        _ => {
            return Err(RookError::TypeMismatch(format!(
                "Invalid date part argument: expected string, got {:?}",
                s
            )));
        }
    };
    match upper.as_str() {
        "YEAR" => Ok(DatePart::Year),
        "MONTH" => Ok(DatePart::Month),
        "DAY" => Ok(DatePart::Day),
        "HOUR" => Ok(DatePart::Hour),
        "MINUTE" => Ok(DatePart::Minute),
        "SECOND" => Ok(DatePart::Second),
        _ => Err(RookError::TypeMismatch(format!(
            "Unknown date part: '{}'",
            upper
        ))),
    }
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
    let to = from
        .checked_add(len)
        .map(|t| t.min(chars.len()))
        .unwrap_or(chars.len());
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

/// Return the 1-based position of `substring` within `value`.
///
/// Implements SQL `POSITION(substring IN string)` semantics.
/// Returns 0 if `substring` is not found (SQL returns 0, not NULL).
/// Returns [`FunctionError::TypeMismatch`] for non-string values.
pub fn position(value: &DataValue, substring: &DataValue) -> Result<i32, FunctionError> {
    let s = match value {
        DataValue::Varchar(s) => s.as_str(),
        DataValue::Char(s) => s.trim_end_matches(' '),
        _ => {
            return Err(FunctionError::TypeMismatch {
                expected: "VARCHAR/CHAR".to_string(),
                found: value_type_name(value).to_string(),
            });
        }
    };
    let sub = match substring {
        DataValue::Varchar(s) => s.as_str(),
        DataValue::Char(s) => s.trim_end_matches(' '),
        _ => {
            return Err(FunctionError::TypeMismatch {
                expected: "VARCHAR/CHAR".to_string(),
                found: value_type_name(value).to_string(),
            });
        }
    };
    match s.find(sub) {
        Some(byte_idx) => Ok(s[..byte_idx].chars().count() as i32 + 1), // 1-based SQL character position
        None => Ok(0),
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
    // The result keeps the argument's type, as the planner declares it, so
    // the one value whose magnitude does not fit (the type's minimum) is an
    // overflow, as for `-x`.
    let overflow = |ty: &str| FunctionError::InvalidArgument(format!("{} out of range in ABS", ty));
    match value {
        DataValue::SmallInt(v) => v
            .checked_abs()
            .map(DataValue::SmallInt)
            .ok_or_else(|| overflow("SMALLINT")),
        DataValue::Int(v) => v
            .checked_abs()
            .map(DataValue::Int)
            .ok_or_else(|| overflow("INT")),
        DataValue::BigInt(v) => v
            .checked_abs()
            .map(DataValue::BigInt)
            .ok_or_else(|| overflow("BIGINT")),
        DataValue::Real(v) => Ok(DataValue::Real(crate::types::value::OrderedF32(v.0.abs()))),
        DataValue::DoublePrecision(v) => Ok(DataValue::DoublePrecision(
            crate::types::value::OrderedF64(v.0.abs()),
        )),
        DataValue::Numeric(v) => match v.unscaled.checked_abs() {
            Some(x) => Ok(DataValue::numeric(crate::types::value::NumericValue {
                unscaled: x,
                scale: v.scale,
            })),
            None => Err(FunctionError::InvalidArgument(
                "NUMERIC value overflow in ABS".to_string(),
            )),
        },
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

fn round_int(val: i64, places: i32) -> i128 {
    if places >= 0 {
        return val as i128;
    }
    let exp = (-places) as u32;
    if exp >= 19 {
        return 0;
    }
    let div = 10_i128.pow(exp);
    let v = val as i128;
    let q = v / div;
    let r = (v % div).abs();
    let rounded = if r * 2 >= div { q + v.signum() } else { q };
    rounded * div
}

/// Round a numeric value to `places` decimal points.
///
/// For integer types (`SMALLINT`, `INT`, `BIGINT`) the value is returned unchanged.
/// For `REAL` / `DOUBLE PRECISION`, standard IEEE 754 rounding is used.
/// For `NUMERIC`, half-up rounding is applied to the unscaled integer representation
/// and the result scale is reduced to `max(places, 0)`.
pub fn round(value: &DataValue, places: i32) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::SmallInt(v) => {
            let r = round_int(*v as i64, places);
            if r < i16::MIN as i128 || r > i16::MAX as i128 {
                Err(FunctionError::InvalidArgument(
                    "SMALLINT value overflow in ROUND".to_string(),
                ))
            } else {
                Ok(DataValue::SmallInt(r as i16))
            }
        }
        DataValue::Int(v) => {
            let r = round_int(*v as i64, places);
            if r < i32::MIN as i128 || r > i32::MAX as i128 {
                Err(FunctionError::InvalidArgument(
                    "INT value overflow in ROUND".to_string(),
                ))
            } else {
                Ok(DataValue::Int(r as i32))
            }
        }
        DataValue::BigInt(v) => {
            let r = round_int(*v, places);
            if r < i64::MIN as i128 || r > i64::MAX as i128 {
                Err(FunctionError::InvalidArgument(
                    "BIGINT value overflow in ROUND".to_string(),
                ))
            } else {
                Ok(DataValue::BigInt(r as i64))
            }
        }
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
            if places < 0 {
                let exp = (-places) as u32;
                let delta = v.scale as u32 + exp;
                if delta >= 38 {
                    return Ok(DataValue::numeric(crate::types::value::NumericValue {
                        unscaled: 0,
                        scale: 0,
                    }));
                }
                let div = pow10(delta);
                let q = v.unscaled / div;
                let r = v.unscaled.abs() % div;
                let rounded = if r * 2 >= div {
                    q + v.unscaled.signum()
                } else {
                    q
                };
                let mult = pow10(exp);
                Ok(DataValue::numeric(crate::types::value::NumericValue {
                    unscaled: rounded * mult,
                    scale: 0,
                }))
            } else {
                let target_scale = places as u8;
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
                Ok(DataValue::numeric(crate::types::value::NumericValue {
                    unscaled: rounded,
                    scale: target_scale,
                }))
            }
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "numeric type".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

// ── Temporal truncation ───────────────────────────────────────────────────────

/// Truncate a temporal value to the start of the specified `DatePart` unit.
///
/// This implements `FLOOR(date TO <unit>)` and `CEILING(date TO <unit>)` semantics.
/// For `Floor` mode, the value is truncated down to the start of the unit.
/// For `Ceil` mode, the value is rounded up to the start of the NEXT unit.
///
/// Supported units for DATE: Year, Month, Day
/// Supported units for TIMESTAMP: Year, Month, Day, Hour, Minute, Second
///
/// Examples:
/// - `date_trunc_floor(Date(2024-07-15), Month)` → Date(2024-07-01)
/// - `date_trunc_floor(Timestamp(2024-07-15 14:30:00), Day)` → Timestamp(2024-07-15 00:00:00)
pub fn date_trunc_floor(value: &DataValue, part: DatePart) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Date(d) => {
            let (y, m, d_part) = (d.year(), d.month(), d.day());
            let truncated = match part {
                DatePart::Year => chrono::NaiveDate::from_ymd_opt(y, 1, 1),
                DatePart::Month => chrono::NaiveDate::from_ymd_opt(y, m, 1),
                DatePart::Day => chrono::NaiveDate::from_ymd_opt(y, m, d_part),
                _ => {
                    return Err(FunctionError::TypeMismatch {
                        expected: "DATE with year/month/day".to_string(),
                        found: format!("DATE with {:?}", part),
                    });
                }
            };
            Ok(DataValue::Date(truncated.ok_or_else(|| {
                FunctionError::InvalidArgument("Invalid date after truncation".to_string())
            })?))
        }
        DataValue::Timestamp(ts) => {
            let (y, m, d_part, h, min, s) = (
                ts.year(),
                ts.month(),
                ts.day(),
                ts.hour(),
                ts.minute(),
                ts.second(),
            );
            let naivedate = chrono::NaiveDate::from_ymd_opt(y, m, d_part).ok_or_else(|| {
                FunctionError::InvalidArgument("Invalid date in timestamp".to_string())
            })?;
            let truncated = match part {
                DatePart::Year => {
                    let d = chrono::NaiveDate::from_ymd_opt(y, 1, 1).ok_or_else(|| {
                        FunctionError::InvalidArgument("Invalid year".to_string())
                    })?;
                    d.and_hms_opt(0, 0, 0)
                }
                DatePart::Month => {
                    let d = chrono::NaiveDate::from_ymd_opt(y, m, 1).ok_or_else(|| {
                        FunctionError::InvalidArgument("Invalid month".to_string())
                    })?;
                    d.and_hms_opt(0, 0, 0)
                }
                DatePart::Day => naivedate.and_hms_opt(0, 0, 0),
                DatePart::Hour => naivedate.and_hms_opt(h, 0, 0),
                DatePart::Minute => naivedate.and_hms_opt(h, min, 0),
                DatePart::Second => naivedate.and_hms_opt(h, min, s),
            };
            Ok(DataValue::Timestamp(truncated.ok_or_else(|| {
                FunctionError::InvalidArgument("Invalid timestamp after truncation".to_string())
            })?))
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "DATE/TIMESTAMP".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

/// Ceiling (round up) a temporal value to the start of the specified `DatePart` unit.
///
/// This implements `CEILING(date TO <unit>)` semantics.
/// If the value is already at a unit boundary, it stays the same.
/// Otherwise, it advances to the START of the next unit.
pub fn date_trunc_ceil(value: &DataValue, part: DatePart) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Date(d) => {
            // Check if already truncated to the given unit
            let is_already = match part {
                DatePart::Year => d.month() == 1 && d.day() == 1,
                DatePart::Month => d.day() == 1,
                DatePart::Day => true,
                _ => {
                    return Err(FunctionError::TypeMismatch {
                        expected: "DATE with year/month/day".to_string(),
                        found: format!("DATE with {:?}", part),
                    });
                }
            };
            if is_already {
                return Ok(DataValue::Date(*d));
            }
            // Truncate to floor first, then advance
            let floored = match date_trunc_floor(value, part) {
                Ok(DataValue::Date(df)) => df,
                _ => {
                    return Err(FunctionError::InvalidArgument(
                        "Cannot compute CEILING for date".to_string(),
                    ));
                }
            };
            // Add the duration of one unit
            let advanced = match part {
                DatePart::Year => chrono::NaiveDate::from_ymd_opt(floored.year() + 1, 1, 1),
                DatePart::Month => {
                    if floored.month() == 12 {
                        chrono::NaiveDate::from_ymd_opt(floored.year() + 1, 1, 1)
                    } else {
                        chrono::NaiveDate::from_ymd_opt(floored.year(), floored.month() + 1, 1)
                    }
                }
                DatePart::Day => floored.succ_opt(),
                _ => unreachable!(),
            };
            Ok(DataValue::Date(advanced.ok_or_else(|| {
                FunctionError::InvalidArgument("Invalid date after ceiling".to_string())
            })?))
        }
        DataValue::Timestamp(ts) => {
            // Check if already truncated
            let is_already = match part {
                DatePart::Year => {
                    ts.month() == 1
                        && ts.day() == 1
                        && ts.hour() == 0
                        && ts.minute() == 0
                        && ts.second() == 0
                        && ts.nanosecond() == 0
                }
                DatePart::Month => {
                    ts.day() == 1
                        && ts.hour() == 0
                        && ts.minute() == 0
                        && ts.second() == 0
                        && ts.nanosecond() == 0
                }
                DatePart::Day => {
                    ts.hour() == 0 && ts.minute() == 0 && ts.second() == 0 && ts.nanosecond() == 0
                }
                DatePart::Hour => ts.minute() == 0 && ts.second() == 0 && ts.nanosecond() == 0,
                DatePart::Minute => ts.second() == 0 && ts.nanosecond() == 0,
                DatePart::Second => ts.nanosecond() == 0,
            };
            if is_already {
                return Ok(DataValue::Timestamp(*ts));
            }
            // Floor and advance
            match date_trunc_floor(value, part) {
                Ok(DataValue::Timestamp(floored)) => {
                    let advanced = match part {
                        DatePart::Year => chrono::NaiveDate::from_ymd_opt(floored.year() + 1, 1, 1)
                            .and_then(|d| d.and_hms_opt(0, 0, 0)),
                        DatePart::Month => {
                            let next_date = if floored.month() == 12 {
                                chrono::NaiveDate::from_ymd_opt(floored.year() + 1, 1, 1)
                            } else {
                                chrono::NaiveDate::from_ymd_opt(
                                    floored.year(),
                                    floored.month() + 1,
                                    1,
                                )
                            };
                            next_date.and_then(|d| d.and_hms_opt(0, 0, 0))
                        }
                        DatePart::Day => floored
                            .date()
                            .succ_opt()
                            .and_then(|d| d.and_hms_opt(0, 0, 0)),
                        DatePart::Hour => floored.checked_add_signed(chrono::Duration::hours(1)),
                        DatePart::Minute => {
                            floored.checked_add_signed(chrono::Duration::minutes(1))
                        }
                        DatePart::Second => {
                            floored.checked_add_signed(chrono::Duration::seconds(1))
                        }
                    };
                    Ok(DataValue::Timestamp(advanced.ok_or_else(|| {
                        FunctionError::InvalidArgument(
                            "Invalid timestamp after ceiling".to_string(),
                        )
                    })?))
                }
                _ => Err(FunctionError::InvalidArgument(
                    "Cannot compute CEILING for timestamp".to_string(),
                )),
            }
        }
        _ => Err(FunctionError::TypeMismatch {
            expected: "DATE/TIMESTAMP".to_string(),
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
        DataValue::Real(v) => Ok(DataValue::Real(crate::types::value::OrderedF32(
            v.0.floor(),
        ))),
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
            Ok(DataValue::numeric(crate::types::value::NumericValue {
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
            Ok(DataValue::numeric(crate::types::value::NumericValue {
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
///
/// For `BIT(n)` targets, supports:
/// - Integer types → BIT(n): two's complement binary representation, truncated/padded
/// - Boolean → BIT(1): `true` → `"1"`, `false` → `"0"`
/// - BIT(m) → BIT(n): truncates or zero-pads
/// - String → BIT(n): uses `parse_and_encode` (requires `'0'`/`'1'` chars only)
pub fn cast(value: &DataValue, target: &DataType) -> Result<DataValue, FunctionError> {
    // ── Special handling for BIT(n) target ───────────────────────────────
    if let DataType::Bit(n) = target {
        return cast_to_bit(value, *n);
    }

    if let Some(res) = cast_numeric_to_integer(value, target) {
        return res;
    }
    // Temporal conversions that have no text form in common.
    match (value, target) {
        (DataValue::Timestamp(ts), DataType::Date) => return Ok(DataValue::Date(ts.date())),
        (DataValue::Timestamp(ts), DataType::Time) => return Ok(DataValue::Time(ts.time())),
        (DataValue::Date(d), DataType::Timestamp) => {
            return d
                .and_hms_opt(0, 0, 0)
                .map(DataValue::Timestamp)
                .ok_or_else(|| FunctionError::InvalidArgument("Invalid date".to_string()));
        }
        _ => {}
    }
    // A numeric value cast to NUMERIC(p, s) is rounded to s places first; the
    // text parser below rejects extra fractional digits.
    if let DataType::Numeric { scale, .. } | DataType::Decimal { scale, .. } = target
        && matches!(
            value,
            DataValue::Numeric(_) | DataValue::Real(_) | DataValue::DoublePrecision(_)
        )
    {
        // A float is rounded as the decimal it prints as, not as its binary
        // value: `1.005` is stored as 1.0049999…, which rounds to 1.00 when
        // the float itself is scaled, where the literal says 1.01.
        let exact = match value {
            DataValue::Real(v) => float_text_to_numeric(&v.0.to_string())?,
            DataValue::DoublePrecision(v) => float_text_to_numeric(&v.0.to_string())?,
            other => other.clone(),
        };
        let rounded = round(&exact, *scale as i32)?;
        let encoded = DataValue::parse_and_encode(target, &value_to_literal(&rounded))
            .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
        return DataValue::from_bytes(target, &encoded)
            .map_err(|e| FunctionError::InvalidArgument(e.to_string()));
    }

    let literal = value_to_literal(value);
    let encoded = DataValue::parse_and_encode(target, &literal)
        .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
    DataValue::from_bytes(target, &encoded)
        .map_err(|e| FunctionError::InvalidArgument(e.to_string()))
}

/// The NUMERIC with exactly the digits of a float's shortest text form
/// (Rust prints floats without an exponent).
fn float_text_to_numeric(text: &str) -> Result<DataValue, FunctionError> {
    let scale = text.split('.').nth(1).map_or(0, str::len);
    let scale = u8::try_from(scale)
        .ok()
        .filter(|s| *s <= 38)
        .ok_or_else(|| {
            FunctionError::InvalidArgument(format!("{} is out of range for NUMERIC", text))
        })?;
    crate::types::value::parse_numeric_literal(text, 38, scale)
        .map(DataValue::numeric)
        .map_err(|_| {
            FunctionError::InvalidArgument(format!("{} is out of range for NUMERIC", text))
        })
}

/// Convert `value` for storage in a column of type `target`, as INSERT and
/// UPDATE do.
///
/// Text going into a text column is kept exactly, only checked against the
/// column's length (and space-padded for CHAR). [`cast`] would trim it and
/// strip enclosing quotes, since it re-reads the value as a literal. A
/// fractional number stored in an integer column is rounded half away from
/// zero, as PostgreSQL does; an explicit [`cast`] truncates. Every other
/// conversion is a [`cast`].
pub fn assign_cast(value: &DataValue, target: &DataType) -> Result<DataValue, FunctionError> {
    let text = match value {
        DataValue::Varchar(s) => s.as_str(),
        DataValue::Char(s) => s.trim_end_matches(' '),
        DataValue::Real(_) | DataValue::DoublePrecision(_) | DataValue::Numeric(_)
            if matches!(
                target,
                DataType::SmallInt | DataType::Int | DataType::BigInt
            ) =>
        {
            return cast(&round(value, 0)?, target);
        }
        _ => return cast(value, target),
    };
    let too_long = |ty: String, max: u16| {
        FunctionError::InvalidArgument(format!(
            "{} value '{}' is out of range: maximum length is {} bytes",
            ty, text, max
        ))
    };
    match target {
        DataType::Varchar(max) if text.len() > *max as usize => {
            Err(too_long(format!("VARCHAR({})", max), *max))
        }
        DataType::Varchar(_) => Ok(DataValue::Varchar(text.to_string())),
        DataType::Char(n) | DataType::Character(n) if text.len() > *n as usize => {
            Err(too_long(format!("CHAR({})", n), *n))
        }
        DataType::Char(n) | DataType::Character(n) => Ok(DataValue::Char(format!(
            "{:<width$}",
            text,
            width = *n as usize
        ))),
        _ => cast(value, target),
    }
}

/// Truncate numeric types toward zero when casting to exact integer types (SQL:1999 standard).
fn cast_numeric_to_integer(
    value: &DataValue,
    target: &DataType,
) -> Option<Result<DataValue, FunctionError>> {
    let (bits, name) = match target {
        DataType::SmallInt => (16, "SMALLINT"),
        DataType::Int => (32, "INT"),
        DataType::BigInt => (64, "BIGINT"),
        _ => return None,
    };
    // The value as a whole number (None when a float is out of range), and
    // how an error names it.
    let (whole, what) = match value {
        DataValue::DoublePrecision(v) => {
            (float_whole(v.0, bits), format!("DOUBLE value '{}'", v.0))
        }
        DataValue::Real(v) => (
            float_whole(f64::from(v.0), bits),
            format!("REAL value '{}'", v.0),
        ),
        DataValue::Numeric(v) => (
            Some(v.unscaled / 10_i128.pow(v.scale as u32)),
            "NUMERIC value".to_string(),
        ),
        DataValue::SmallInt(v) => (Some(i128::from(*v)), format!("SMALLINT value '{}'", v)),
        DataValue::Int(v) => (Some(i128::from(*v)), format!("INT value '{}'", v)),
        DataValue::BigInt(v) => (Some(i128::from(*v)), format!("BIGINT value '{}'", v)),
        _ => return None,
    };
    let limit = 1_i128 << (bits - 1);
    Some(match whole {
        Some(n) if (-limit..limit).contains(&n) => Ok(match target {
            DataType::SmallInt => DataValue::SmallInt(n as i16),
            DataType::Int => DataValue::Int(n as i32),
            _ => DataValue::BigInt(n as i64),
        }),
        _ => Err(FunctionError::InvalidArgument(format!(
            "{} is out of range for {}",
            what, name
        ))),
    })
}

/// `x` truncated toward zero, when that fits a signed `bits`-bit integer.
/// The bounds are powers of two, so they are exact as floats.
fn float_whole(x: f64, bits: u32) -> Option<i128> {
    let t = x.trunc();
    let limit = 2_f64.powi(bits as i32 - 1);
    (t >= -limit && t < limit).then_some(t as i128)
}

/// Convert a value to a BIT(n) string.
fn cast_to_bit(value: &DataValue, n: u16) -> Result<DataValue, FunctionError> {
    let bits = match value {
        // Integer types → two's complement: simple bitwise masking handles negatives
        DataValue::SmallInt(v) => to_binary_string(*v as i64, n),
        DataValue::Int(v) => to_binary_string(*v as i64, n),
        DataValue::BigInt(v) => to_binary_string(*v, n),
        // Boolean → BIT(n): single bit, zero-padded to n bits
        DataValue::Bool(true) => format!("{:0>width$}", "1", width = n as usize),
        DataValue::Bool(false) => format!("{:0>width$}", "0", width = n as usize),
        // BIT(m) → BIT(n): truncate or zero-pad
        DataValue::Bit(bits) => {
            if bits.len() as u16 == n {
                bits.clone()
            } else if bits.len() as u16 > n {
                bits[..n as usize].to_string()
            } else {
                format!("{:0>width$}", bits, width = n as usize)
            }
        }
        // String types → use parse_and_encode (validates '0'/'1' chars)
        DataValue::Varchar(s) => {
            let encoded = DataValue::parse_and_encode(&DataType::Bit(n), s.trim())
                .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
            let dv = DataValue::from_bytes(&DataType::Bit(n), &encoded)
                .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
            if let DataValue::Bit(bits) = dv {
                bits
            } else {
                unreachable!()
            }
        }
        DataValue::Char(s) => {
            let encoded = DataValue::parse_and_encode(&DataType::Bit(n), s.trim_end_matches(' '))
                .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
            let dv = DataValue::from_bytes(&DataType::Bit(n), &encoded)
                .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
            if let DataValue::Bit(bits) = dv {
                bits
            } else {
                unreachable!()
            }
        }
        // Fallback for temporal/other types
        _ => {
            let literal = value_to_literal(value);
            let encoded =
                DataValue::parse_and_encode(&DataType::Bit(n), &literal).map_err(|e| {
                    FunctionError::InvalidArgument(format!(
                        "Cannot cast {:?} to BIT({}): {}",
                        value, n, e
                    ))
                })?;
            let dv = DataValue::from_bytes(&DataType::Bit(n), &encoded)
                .map_err(|e| FunctionError::InvalidArgument(e.to_string()))?;
            if let DataValue::Bit(bits) = dv {
                bits
            } else {
                unreachable!()
            }
        }
    };

    Ok(DataValue::Bit(bits))
}

/// Convert an i64 value to a binary string of exactly `n` bits.
/// The result is right-padded (least significant bits) when `n` exceeds
/// the position of the highest set bit.
fn to_binary_string(val: i64, n: u16) -> String {
    if n == 0 {
        return String::new();
    }
    // Use only the lowest n bits
    let mask = if n >= 64 { !0i64 } else { (1i64 << n) - 1 };
    let masked = val & mask;
    // Format as binary, padded to n bits
    format!("{:0>width$b}", masked as u64, width = n as usize)
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
