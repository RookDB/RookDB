use chrono::{Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::types::bit_utils::{normalize_bit_literal, pack_bit_string, unpack_bit_string};
use crate::types::datatype::DataType;
use crate::types::validation::validate_value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataValue {
    SmallInt(i16),
    Int(i32),
    Varchar(String),
    Date(NaiveDate),
    Bit(String),
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::SmallInt(v) => write!(f, "{}", v),
            DataValue::Int(v) => write!(f, "{}", v),
            DataValue::Varchar(v) => write!(f, "'{}'", v),
            DataValue::Date(v) => write!(f, "{}", v.format("%Y-%m-%d")),
            DataValue::Bit(v) => write!(f, "B'{}'", v),
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
            DataValue::Bit(v) => pack_bit_string(v),
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
            DataType::Bit(n) => {
                let needed = (*n as usize).div_ceil(8);
                if bytes.len() < needed {
                    return Err(format!("BIT({}) requires {} bytes", n, needed));
                }
                Ok(DataValue::Bit(unpack_bit_string(&bytes[..needed], *n as usize)))
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
            DataType::Bit(_) => {
                let bits = normalize_bit_literal(input);
                Ok(DataValue::Bit(bits).to_bytes())
            }
        }
    }
}
