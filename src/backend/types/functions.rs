use chrono::Datelike;
use std::fmt;

use crate::types::comparison::value_type_name;
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
}

pub fn length(value: &DataValue) -> Result<usize, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(s.chars().count()),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

pub fn substring(value: &DataValue, start: usize, len: usize) -> Result<DataValue, FunctionError> {
    let s = match value {
        DataValue::Varchar(s) => s,
        _ => {
            return Err(FunctionError::TypeMismatch {
                expected: "VARCHAR".to_string(),
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
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

pub fn lower(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.to_lowercase())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

pub fn trim(value: &DataValue) -> Result<DataValue, FunctionError> {
    match value {
        DataValue::Varchar(s) => Ok(DataValue::Varchar(s.trim().to_string())),
        _ => Err(FunctionError::TypeMismatch {
            expected: "VARCHAR".to_string(),
            found: value_type_name(value).to_string(),
        }),
    }
}

pub fn extract(part: DatePart, value: &DataValue) -> Result<i32, FunctionError> {
    let d = match value {
        DataValue::Date(d) => d,
        _ => {
            return Err(FunctionError::TypeMismatch {
                expected: "DATE".to_string(),
                found: value_type_name(value).to_string(),
            });
        }
    };

    Ok(match part {
        DatePart::Year => d.year(),
        DatePart::Month => d.month() as i32,
        DatePart::Day => d.day() as i32,
    })
}
