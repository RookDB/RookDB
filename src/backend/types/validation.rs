//! Input validation for SQL literal strings before encoding.
//!
//! Each `validate_*` function checks whether a raw string literal is a legal
//! value for the corresponding SQL type, returning a descriptive
//! [`TypeValidationError`] if not. These functions are called by
//! [`DataValue::parse_and_encode`](crate::types::value::DataValue::parse_and_encode)
//! before any encoding work is attempted.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::fmt;

use crate::types::bit_utils::normalize_bit_literal;
use crate::types::datatype::DataType;

/// Error returned when a string literal is not a valid value for a SQL type.
///
/// Two variants cover the two most common rejection reasons:
/// - [`OutOfRange`](Self::OutOfRange) — the literal is syntactically valid but
///   outside the type's permitted range (e.g. precision overflow, too many bytes).
/// - [`InvalidFormat`](Self::InvalidFormat) — the literal does not parse at all
///   (e.g. non-numeric characters in a REAL literal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeValidationError {
    /// Value is syntactically valid but outside the type's permitted range.
    OutOfRange {
        ty: String,
        value: String,
        details: String,
    },
    /// Value cannot be parsed as the target type at all.
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

/// Validate a `SMALLINT` literal (`-32768` … `32767`).
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

/// Validate a 32-bit `INT` literal.
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

/// Validate a 64-bit `BIGINT` literal.
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

/// Validate an IEEE 754 single-precision `REAL` literal.
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

/// Validate a double-precision `DOUBLE PRECISION` literal.
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

/// Validate that a numeric literal fits within `NUMERIC(precision, scale)`.
///
/// Checks both the fractional digit count (≤ `scale`) and the total significant
/// digit count (≤ `precision`). Strips optional leading `+`/`-` signs.
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

/// Validate a `BOOLEAN` literal.
///
/// Accepts: `true`, `false`, `t`, `f`, `1`, `0` (case-insensitive).
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

/// Validate that a `VARCHAR(max_len)` literal does not exceed `max_len` bytes.
/// Surrounding quotes are stripped before the length check.
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

/// Validate that a `CHAR(fixed_len)` literal does not exceed `fixed_len` bytes.
/// Values shorter than `fixed_len` are space-padded on insert; they are not
/// rejected here.
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

/// Validate a `DATE` literal in `YYYY-MM-DD` format.
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

/// Validate a `TIME` literal in `HH:MM:SS` or `HH:MM:SS.ffffff` format.
/// Fractional seconds are limited to 6 digits (microsecond precision).
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

/// Validate a `TIMESTAMP` literal in `YYYY-MM-DD HH:MM:SS[.ffffff]` format.
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

/// Validate a `BIT(n)` literal.
///
/// The normalised value (after stripping `B'...'` delimiters) must contain
/// exactly `bit_len` characters and consist only of `'0'` and `'1'`.
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

/// Dispatch validation to the appropriate type-specific function.
///
/// Acts as a single entry point for validating any SQL literal string against
/// its declared column type. Used by
/// [`parse_and_encode`](crate::types::value::DataValue::parse_and_encode)
/// before encoding work begins.
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
