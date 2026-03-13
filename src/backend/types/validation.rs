use chrono::NaiveDate;
use std::fmt;

use crate::types::bit_utils::normalize_bit_literal;
use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeValidationError {
    OutOfRange {
        ty: String,
        value: String,
        details: String,
    },
    InvalidFormat {
        ty: String,
        value: String,
        details: String,
    },
}

impl fmt::Display for TypeValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeValidationError::OutOfRange { ty, value, details } => {
                write!(f, "{} value '{}' is out of range: {}", ty, value, details)
            }
            TypeValidationError::InvalidFormat { ty, value, details } => {
                write!(f, "{} value '{}' has invalid format: {}", ty, value, details)
            }
        }
    }
}

impl std::error::Error for TypeValidationError {}

pub fn validate_smallint(input: &str) -> Result<(), TypeValidationError> {
    input
        .trim()
        .parse::<i16>()
        .map(|_| ())
        .map_err(|_| TypeValidationError::OutOfRange {
            ty: "SMALLINT".to_string(),
            value: input.trim().to_string(),
            details: "expected signed 16-bit integer [-32768, 32767]".to_string(),
        })
}

pub fn validate_int(input: &str) -> Result<(), TypeValidationError> {
    input
        .trim()
        .parse::<i32>()
        .map(|_| ())
        .map_err(|_| TypeValidationError::OutOfRange {
            ty: "INT".to_string(),
            value: input.trim().to_string(),
            details: "expected signed 32-bit integer".to_string(),
        })
}

pub fn validate_varchar(input: &str, max_len: u16) -> Result<(), TypeValidationError> {
    let value = input.trim().trim_matches('"').trim_matches('\'');
    if value.len() > max_len as usize {
        return Err(TypeValidationError::OutOfRange {
            ty: format!("VARCHAR({})", max_len),
            value: value.to_string(),
            details: format!("maximum length is {} bytes", max_len),
        });
    }
    Ok(())
}

pub fn validate_date(input: &str) -> Result<(), TypeValidationError> {
    let raw = input.trim().trim_matches('\'');
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|e| TypeValidationError::InvalidFormat {
            ty: "DATE".to_string(),
            value: raw.to_string(),
            details: format!("expected YYYY-MM-DD ({})", e),
        })
}

pub fn validate_bit(input: &str, bit_len: u16) -> Result<(), TypeValidationError> {
    let value = normalize_bit_literal(input);
    if value.len() != bit_len as usize {
        return Err(TypeValidationError::OutOfRange {
            ty: format!("BIT({})", bit_len),
            value,
            details: format!("requires exactly {} bits", bit_len),
        });
    }
    if !value.chars().all(|c| c == '0' || c == '1') {
        return Err(TypeValidationError::InvalidFormat {
            ty: format!("BIT({})", bit_len),
            value,
            details: "allowed symbols are only '0' and '1'".to_string(),
        });
    }
    Ok(())
}

pub fn validate_value(ty: &DataType, input: &str) -> Result<(), TypeValidationError> {
    match ty {
        DataType::SmallInt => validate_smallint(input),
        DataType::Int => validate_int(input),
        DataType::Varchar(max_len) => validate_varchar(input, *max_len),
        DataType::Date => validate_date(input),
        DataType::Bit(bit_len) => validate_bit(input, *bit_len),
    }
}
