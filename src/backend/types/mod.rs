//! Phase 1 foundation for Shubhadeep's fixed-length type work.
//!
//! This module introduces the schema-level `DataType` enum and the
//! runtime `DataValue` enum for the types assigned in the proposal:
//! `SMALLINT`, `INTEGER`, `VARCHAR(n)`, and `DATE`.

use chrono::NaiveDate;
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
}
