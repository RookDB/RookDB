use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
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

pub fn validate_bigint(input: &str) -> Result<(), TypeValidationError> {
    input
        .trim()
        .parse::<i64>()
        .map(|_| ())
        .map_err(|_| TypeValidationError::OutOfRange {
            ty: "BIGINT".to_string(),
            value: input.trim().to_string(),
            details: "expected signed 64-bit integer [-9223372036854775808, 9223372036854775807]"
                .to_string(),
        })
}

pub fn validate_real(input: &str) -> Result<(), TypeValidationError> {
    input
        .trim()
        .parse::<f32>()
        .map(|_| ())
        .map_err(|_| TypeValidationError::InvalidFormat {
            ty: "REAL".to_string(),
            value: input.trim().to_string(),
            details: "expected IEEE 754 single-precision float".to_string(),
        })
}

pub fn validate_double(input: &str) -> Result<(), TypeValidationError> {
    input
        .trim()
        .parse::<f64>()
        .map(|_| ())
        .map_err(|_| TypeValidationError::InvalidFormat {
            ty: "DOUBLE PRECISION".to_string(),
            value: input.trim().to_string(),
            details: "expected IEEE 754 double-precision float".to_string(),
        })
}

pub fn validate_numeric(input: &str, precision: u8, scale: u8) -> Result<(), TypeValidationError> {
    let raw = input.trim().trim_matches('"').trim_matches('\'');
    if raw.is_empty() {
        return Err(TypeValidationError::InvalidFormat {
            ty: format!("NUMERIC({},{})", precision, scale),
            value: raw.to_string(),
            details: "value cannot be empty".to_string(),
        });
    }
    let body = raw.strip_prefix(['+', '-']).unwrap_or(raw);
    let mut parts = body.split('.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next().unwrap_or("");
    if parts.next().is_some()
        || !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(TypeValidationError::InvalidFormat {
            ty: format!("NUMERIC({},{})", precision, scale),
            value: raw.to_string(),
            details: "expected numeric literal [+-]?digits[.digits]".to_string(),
        });
    }
    if frac_part.len() > scale as usize {
        return Err(TypeValidationError::OutOfRange {
            ty: format!("NUMERIC({},{})", precision, scale),
            value: raw.to_string(),
            details: format!("fractional digits must be <= {}", scale),
        });
    }

    let mut combined = String::new();
    if int_part.is_empty() {
        combined.push('0');
    } else {
        combined.push_str(int_part);
    }
    combined.push_str(frac_part);
    combined.push_str(&"0".repeat(scale as usize - frac_part.len()));
    let significant = combined.trim_start_matches('0');
    let digits = if significant.is_empty() { 1 } else { significant.len() };
    if digits > precision as usize {
        return Err(TypeValidationError::OutOfRange {
            ty: format!("NUMERIC({},{})", precision, scale),
            value: raw.to_string(),
            details: format!("precision exceeded; max {} significant digits", precision),
        });
    }
    Ok(())
}

pub fn validate_bool(input: &str) -> Result<(), TypeValidationError> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" | "false" | "t" | "f" | "1" | "0" => Ok(()),
        _ => Err(TypeValidationError::InvalidFormat {
            ty: "BOOLEAN".to_string(),
            value: input.trim().to_string(),
            details: "expected one of: true, false, t, f, 1, 0".to_string(),
        }),
    }
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

pub fn validate_char(input: &str, fixed_len: u16) -> Result<(), TypeValidationError> {
    let value = input.trim().trim_matches('"').trim_matches('\'');
    if value.len() > fixed_len as usize {
        return Err(TypeValidationError::OutOfRange {
            ty: format!("CHAR({})", fixed_len),
            value: value.to_string(),
            details: format!("maximum length is {} bytes (shorter values are space-padded)", fixed_len),
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

pub fn validate_time(input: &str) -> Result<(), TypeValidationError> {
    let raw = input.trim().trim_matches('\'');

    if let Some(fraction) = raw.split('.').nth(1) {
        if fraction.len() > 6 {
            return Err(TypeValidationError::InvalidFormat {
                ty: "TIME".to_string(),
                value: raw.to_string(),
                details: "Time precision exceeds microsecond limit (maximum 6 fractional digits).".to_string(),
            });
        }
    }
    NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(raw, "%H:%M:%S"))
        .map(|_| ())
        .map_err(|e| TypeValidationError::InvalidFormat {
            ty: "TIME".to_string(),
            value: raw.to_string(),
            details: format!("expected HH:MM:SS[.ffffff] ({})", e),
        })
}

pub fn validate_timestamp(input: &str) -> Result<(), TypeValidationError> {
    let raw = input.trim().trim_matches('\'');
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .map(|_| ())
        .map_err(|e| TypeValidationError::InvalidFormat {
            ty: "TIMESTAMP".to_string(),
            value: raw.to_string(),
            details: format!("expected 'YYYY-MM-DD HH:MM:SS[.ffffff]' ({})", e),
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
        DataType::BigInt => validate_bigint(input),
        DataType::Real => validate_real(input),
        DataType::DoublePrecision => validate_double(input),
        DataType::Numeric { precision, scale } => validate_numeric(input, *precision, *scale),
        DataType::Decimal { precision, scale } => validate_numeric(input, *precision, *scale),
        DataType::Bool => validate_bool(input),
        DataType::Char(fixed_len) => validate_char(input, *fixed_len),
        DataType::Character(fixed_len) => validate_char(input, *fixed_len),
        DataType::Varchar(max_len) => validate_varchar(input, *max_len),
        DataType::Date => validate_date(input),
        DataType::Time => validate_time(input),
        DataType::Bit(bit_len) => validate_bit(input, *bit_len),
        DataType::Timestamp => validate_timestamp(input),
    }
}
