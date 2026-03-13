//! This module introduces the schema-level `DataType` enum and the
//! runtime `DataValue` enum for the types assigned in the proposal:
//! `SMALLINT`, `INTEGER`, `VARCHAR(n)`, and `DATE`.

use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    SmallInt,
    Int,
    Varchar(u16),
    Date,
}

impl DataType {
    /// Returns the alignment rule described in the proposal.
    pub fn alignment(&self) -> u32 {
        match self {
            DataType::SmallInt => 2,
            DataType::Int | DataType::Date => 4,
            DataType::Varchar(_) => 1,
        }
    }

    /// Returns the exact on-disk size for fixed-width types.
    pub fn fixed_size(&self) -> Option<u32> {
        match self {
            DataType::SmallInt => Some(2),
            DataType::Int => Some(4),
            DataType::Date => Some(4),
            DataType::Varchar(_) => None,
        }
    }

    /// Returns the minimum number of bytes required to store this type.
    ///
    /// `VARCHAR(n)` uses a 2-byte length prefix followed by payload bytes.
    pub fn min_storage_size(&self) -> u32 {
        match self {
            DataType::Varchar(_) => 2,
            _ => self.fixed_size().expect("fixed-width type"),
        }
    }

    pub fn is_variable_length(&self) -> bool {
        matches!(self, DataType::Varchar(_))
    }

    pub fn encoded_len(&self, bytes: &[u8]) -> Result<usize, String> {
        match self {
            DataType::Varchar(_) => {
                if bytes.len() < 2 {
                    return Err("VARCHAR field is missing its 2-byte length prefix".to_string());
                }
                let payload_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                let total_len = 2 + payload_len;
                if bytes.len() < total_len {
                    return Err("VARCHAR field is truncated".to_string());
                }
                Ok(total_len)
            }
            _ => Ok(self.fixed_size().expect("fixed-width type") as usize),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Date => write!(f, "DATE"),
        }
    }
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();
        match upper.as_str() {
            "SMALLINT" => Ok(DataType::SmallInt),
            "INT" | "INTEGER" => Ok(DataType::Int),
            "DATE" => Ok(DataType::Date),
            _ => {
                if upper.starts_with("VARCHAR(") && upper.ends_with(')') {
                    let inner = &upper[8..upper.len() - 1];
                    inner
                        .parse::<u16>()
                        .map(DataType::Varchar)
                        .map_err(|_| format!("Invalid VARCHAR size: '{}'", inner))
                } else {
                    Err(format!("Unknown data type: '{}'", s))
                }
            }
        }
    }
}

impl Serialize for DataType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse::<DataType>().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataValue {
    SmallInt(i16),
    Int(i32),
    Varchar(String),
    Date(NaiveDate),
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::SmallInt(v) => write!(f, "{}", v),
            DataValue::Int(v) => write!(f, "{}", v),
            DataValue::Varchar(v) => write!(f, "'{}'", v),
            DataValue::Date(v) => write!(f, "{}", v.format("%Y-%m-%d")),
        }
    }
}

impl DataValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            DataValue::SmallInt(v) => v.to_le_bytes().to_vec(),
            DataValue::Int(v) => v.to_le_bytes().to_vec(),
            DataValue::Varchar(v) => {
                let bytes = v.as_bytes();
                let mut out = Vec::with_capacity(2 + bytes.len());
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                out.extend_from_slice(bytes);
                out
            }
            DataValue::Date(v) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
                let days = v.signed_duration_since(epoch).num_days() as i32;
                days.to_le_bytes().to_vec()
            }
        }
    }

    pub fn from_bytes(ty: &DataType, bytes: &[u8]) -> Result<Self, String> {
        match ty {
            DataType::SmallInt => {
                if bytes.len() < 2 {
                    return Err("SMALLINT requires 2 bytes".to_string());
                }
                Ok(DataValue::SmallInt(i16::from_le_bytes([
                    bytes[0], bytes[1],
                ])))
            }
            DataType::Int => {
                if bytes.len() < 4 {
                    return Err("INT requires 4 bytes".to_string());
                }
                Ok(DataValue::Int(i32::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                ])))
            }
            DataType::Varchar(max_len) => {
                let encoded_len = ty.encoded_len(bytes)?;
                let payload_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                if payload_len > *max_len as usize {
                    return Err(format!(
                        "VARCHAR payload length {} exceeds declared limit {}",
                        payload_len, max_len
                    ));
                }
                let payload = &bytes[2..encoded_len];
                let value = String::from_utf8(payload.to_vec())
                    .map_err(|_| "VARCHAR payload is not valid UTF-8".to_string())?;
                Ok(DataValue::Varchar(value))
            }
            DataType::Date => {
                if bytes.len() < 4 {
                    return Err("DATE requires 4 bytes".to_string());
                }
                let days = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
                let value = epoch
                    .checked_add_signed(Duration::days(days))
                    .ok_or_else(|| "DATE is outside supported chrono range".to_string())?;
                Ok(DataValue::Date(value))
            }
        }
    }

    pub fn parse_and_encode(ty: &DataType, input: &str) -> Result<Vec<u8>, String> {
        let input = input.trim();
        validate_value(ty, input).map_err(|e| e.to_string())?;

        match ty {
            DataType::SmallInt => input
                .parse::<i16>()
                .map(DataValue::SmallInt)
                .map(|v| v.to_bytes())
                .map_err(|e| e.to_string()),
            DataType::Int => input
                .parse::<i32>()
                .map(DataValue::Int)
                .map(|v| v.to_bytes())
                .map_err(|e| e.to_string()),
            DataType::Varchar(_) => {
                let value = input.trim_matches('"').trim_matches('\'');
                Ok(DataValue::Varchar(value.to_string()).to_bytes())
            }
            DataType::Date => {
                let date = NaiveDate::parse_from_str(input.trim_matches('\''), "%Y-%m-%d")
                    .map_err(|e| e.to_string())?;
                Ok(DataValue::Date(date).to_bytes())
            }
        }
    }
}

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

pub fn validate_value(ty: &DataType, input: &str) -> Result<(), TypeValidationError> {
    match ty {
        DataType::SmallInt => validate_smallint(input),
        DataType::Int => validate_int(input),
        DataType::Varchar(max_len) => validate_varchar(input, *max_len),
        DataType::Date => validate_date(input),
    }
}

/// Row-level NULL bitmap as described in the proposal.
///
/// One bit per column: bit=1 means the column value is NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    column_count: usize,
    data: Vec<u8>,
}

impl NullBitmap {
    pub fn new(column_count: usize) -> Self {
        let byte_len = column_count.div_ceil(8);
        Self {
            column_count,
            data: vec![0u8; byte_len],
        }
    }

    pub fn from_bytes(column_count: usize, raw: &[u8]) -> Result<Self, String> {
        let expected = column_count.div_ceil(8);
        if raw.len() != expected {
            return Err(format!(
                "Invalid NULL bitmap length: expected {}, found {}",
                expected,
                raw.len()
            ));
        }
        Ok(Self {
            column_count,
            data: raw.to_vec(),
        })
    }

    pub fn set_null(&mut self, column_index: usize) {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] |= 1 << bit_idx;
    }

    pub fn clear_null(&mut self, column_index: usize) {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] &= !(1 << bit_idx);
    }

    pub fn is_null(&self, column_index: usize) -> bool {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Serialize one row of optional values using:
/// [NULL_BITMAP][COLUMN_DATA...]
///
/// NULL values are represented only in the bitmap and are omitted
/// from column data bytes.
pub fn serialize_nullable_row(
    schema: &[DataType],
    values: &[Option<&str>],
) -> Result<Vec<u8>, String> {
    if schema.len() != values.len() {
        return Err(format!(
            "Schema/value length mismatch: schema={}, values={}",
            schema.len(),
            values.len()
        ));
    }

    let mut bitmap = NullBitmap::new(schema.len());
    let bitmap_len = schema.len().div_ceil(8);
    let mut row = vec![0u8; bitmap_len];

    for (i, (ty, maybe_value)) in schema.iter().zip(values.iter()).enumerate() {
        match maybe_value {
            Some(raw) => {
                let encoded = DataValue::parse_and_encode(ty, raw)?;
                row.extend_from_slice(&encoded);
            }
            None => bitmap.set_null(i),
        }
    }

    row[..bitmap_len].copy_from_slice(bitmap.as_bytes());
    Ok(row)
}

/// Deserialize one row encoded by `serialize_nullable_row`.
pub fn deserialize_nullable_row(
    schema: &[DataType],
    row_bytes: &[u8],
) -> Result<Vec<Option<DataValue>>, String> {
    let bitmap_len = schema.len().div_ceil(8);
    if row_bytes.len() < bitmap_len {
        return Err("Row shorter than NULL bitmap".to_string());
    }

    let bitmap = NullBitmap::from_bytes(schema.len(), &row_bytes[..bitmap_len])?;
    let mut cursor = bitmap_len;
    let mut out = Vec::with_capacity(schema.len());

    for (i, ty) in schema.iter().enumerate() {
        if bitmap.is_null(i) {
            out.push(None);
            continue;
        }

        let remaining = &row_bytes[cursor..];
        let encoded_len = ty.encoded_len(remaining)?;
        let value = DataValue::from_bytes(ty, &remaining[..encoded_len])?;
        cursor += encoded_len;
        out.push(Some(value));
    }

    if cursor != row_bytes.len() {
        return Err(format!(
            "Row has {} trailing byte(s) after decode",
            row_bytes.len() - cursor
        ));
    }

    Ok(out)
}

// ──────────────────────────────────────────────
//  Unit tests
// ──────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_phase_one_types() {
        assert_eq!("SMALLINT".parse::<DataType>().unwrap(), DataType::SmallInt);
        assert_eq!("INT".parse::<DataType>().unwrap(), DataType::Int);
        assert_eq!("VARCHAR(64)".parse::<DataType>().unwrap(), DataType::Varchar(64));
        assert_eq!("DATE".parse::<DataType>().unwrap(), DataType::Date);
    }

    #[test]
    fn parse_unknown_type_is_error() {
        assert!("BIGINT".parse::<DataType>().is_err());
        assert!("BLOB".parse::<DataType>().is_err());
    }

    #[test]
    fn serde_roundtrip() {
        let types = vec![
            DataType::SmallInt,
            DataType::Int,
            DataType::Varchar(32),
            DataType::Date,
        ];
        for dt in &types {
            let json = serde_json::to_string(dt).unwrap();
            let back: DataType = serde_json::from_str(&json).unwrap();
            assert_eq!(dt, &back);
        }
    }

    #[test]
    fn display_matches_parse() {
        let types = vec![
            DataType::SmallInt,
            DataType::Int,
            DataType::Varchar(8),
            DataType::Date,
        ];
        for dt in &types {
            let s = dt.to_string();
            let back: DataType = s.parse().unwrap();
            assert_eq!(dt, &back);
        }
    }

    #[test]
    fn phase_two_layout_rules() {
        assert_eq!(DataType::SmallInt.alignment(), 2);
        assert_eq!(DataType::Int.alignment(), 4);
        assert_eq!(DataType::Date.alignment(), 4);
        assert_eq!(DataType::Varchar(64).alignment(), 1);

        assert_eq!(DataType::SmallInt.fixed_size(), Some(2));
        assert_eq!(DataType::Int.fixed_size(), Some(4));
        assert_eq!(DataType::Date.fixed_size(), Some(4));
        assert_eq!(DataType::Varchar(64).fixed_size(), None);

        assert_eq!(DataType::Varchar(64).min_storage_size(), 2);
        assert!(DataType::Varchar(64).is_variable_length());
        assert!(!DataType::Date.is_variable_length());
    }

    #[test]
    fn roundtrip_smallint() {
        let encoded = DataValue::parse_and_encode(&DataType::SmallInt, "-12").unwrap();
        assert_eq!(DataValue::from_bytes(&DataType::SmallInt, &encoded).unwrap(), DataValue::SmallInt(-12));
    }

    #[test]
    fn roundtrip_int() {
        let encoded = DataValue::parse_and_encode(&DataType::Int, "42").unwrap();
        assert_eq!(DataValue::from_bytes(&DataType::Int, &encoded).unwrap(), DataValue::Int(42));
    }

    #[test]
    fn roundtrip_varchar() {
        let encoded = DataValue::parse_and_encode(&DataType::Varchar(32), "Alice").unwrap();
        assert_eq!(u16::from_le_bytes([encoded[0], encoded[1]]), 5);
        assert_eq!(DataValue::from_bytes(&DataType::Varchar(32), &encoded).unwrap(), DataValue::Varchar("Alice".to_string()));
    }

    #[test]
    fn roundtrip_date() {
        let encoded = DataValue::parse_and_encode(&DataType::Date, "2026-03-13").unwrap();
        assert_eq!(DataValue::from_bytes(&DataType::Date, &encoded).unwrap(), DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()));
    }

    #[test]
    fn varchar_length_violation_is_error() {
        let err = DataValue::parse_and_encode(&DataType::Varchar(3), "Alice").unwrap_err();
        assert!(err.contains("VARCHAR(3)"));
    }

    #[test]
    fn validate_smallint_bounds() {
        assert!(validate_smallint("-32768").is_ok());
        assert!(validate_smallint("32767").is_ok());
        assert!(validate_smallint("32768").is_err());
    }

    #[test]
    fn validate_int_bounds() {
        assert!(validate_int("2147483647").is_ok());
        assert!(validate_int("2147483648").is_err());
    }

    #[test]
    fn validate_varchar_length() {
        assert!(validate_varchar("abc", 3).is_ok());
        assert!(validate_varchar("abcd", 3).is_err());
    }

    #[test]
    fn validate_date_format() {
        assert!(validate_date("2026-03-13").is_ok());
        assert!(validate_date("2026-13-40").is_err());
        assert!(validate_date("13-03-2026").is_err());
    }

    #[test]
    fn null_bitmap_set_clear_and_probe() {
        let mut bitmap = NullBitmap::new(10);
        bitmap.set_null(0);
        bitmap.set_null(3);
        bitmap.set_null(9);

        assert!(bitmap.is_null(0));
        assert!(!bitmap.is_null(1));
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_null(9));

        bitmap.clear_null(3);
        assert!(!bitmap.is_null(3));
    }

    #[test]
    fn nullable_row_roundtrip() {
        let schema = vec![
            DataType::Int,
            DataType::Varchar(16),
            DataType::Date,
            DataType::SmallInt,
        ];

        let encoded = serialize_nullable_row(
            &schema,
            &[Some("42"), None, Some("2026-03-13"), Some("-7")],
        )
        .unwrap();

        // Column 1 is NULL.
        assert_eq!(encoded[0] & (1 << 1), 1 << 1);

        let decoded = deserialize_nullable_row(&schema, &encoded).unwrap();
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0], Some(DataValue::Int(42)));
        assert_eq!(decoded[1], None);
        assert_eq!(
            decoded[2],
            Some(DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()))
        );
        assert_eq!(decoded[3], Some(DataValue::SmallInt(-7)));
    }
}
