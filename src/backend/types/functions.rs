use chrono::{Datelike, Local, Timelike};
use std::fmt;

use crate::types::comparison::Comparable;
use crate::types::comparison::value_type_name;
use crate::types::datatype::DataType;
use crate::types::value::DataValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionError {
    TypeMismatch { expected: String, found: String },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

pub fn length(value: &DataValue) -> Result<usize, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(s.chars().count()),
        DataValue::Char(s) => Ok(s.trim_end_matches(' ').chars().count()),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR/CHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

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

    let from = start - 1;
    let to = (from + len).min(chars.len());
    let out: String = chars[from..to].iter().collect();
    Ok(DataValue::Varchar(out))
}

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

fn pow10(exp: u32) -> i128 {
    10_i128.pow(exp)
}

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

pub fn round(value: &DataValue, places: i32) -> Result<DataValue, FunctionError> {
    match value {
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
                return Ok(DataValue::Numeric(v.clone()));
            }
            let delta = (v.scale - target_scale) as u32;
            let div = pow10(delta);
            let q = v.unscaled / div;
            let r = v.unscaled.abs() % div;
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

pub fn cast(value: &DataValue, target: &DataType) -> Result<DataValue, FunctionError> {
    let literal = value_to_literal(value);
    let encoded = DataValue::parse_and_encode(target, &literal)
        .map_err(FunctionError::InvalidArgument)?;
    DataValue::from_bytes(target, &encoded).map_err(FunctionError::InvalidArgument)
}

pub fn coalesce(values: &[Option<DataValue>]) -> Option<DataValue> {
    values.iter().find_map(|v| v.clone())
}

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

pub fn current_date() -> DataValue {
    DataValue::Date(Local::now().date_naive())
}

pub fn current_time() -> DataValue {
    DataValue::Time(Local::now().time())
}

pub fn current_timestamp() -> DataValue {
    DataValue::Timestamp(Local::now().naive_local())
}
