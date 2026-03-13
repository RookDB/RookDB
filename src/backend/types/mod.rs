//! Phase 1 foundation for Shubhadeep's fixed-length type work.
//!
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
            DataType::Varchar(max_len) => {
                let value = input.trim_matches('"').trim_matches('\'');
                if value.len() > *max_len as usize {
                    return Err(format!(
                        "VARCHAR({}) cannot store {} bytes",
                        max_len,
                        value.len()
                    ));
                }
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
}
