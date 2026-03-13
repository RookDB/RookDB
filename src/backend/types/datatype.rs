use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    SmallInt,
    Int,
    BigInt,
    Real,
    DoublePrecision,
    Numeric { precision: u8, scale: u8 },
    Decimal { precision: u8, scale: u8 },
    Bool,
    Char(u16),
    Varchar(u16),
    Date,
    Time,
    Bit(u16),
    Timestamp,
}

impl DataType {
    /// Returns the alignment rule described in the proposal.
    pub fn alignment(&self) -> u32 {
        match self {
            DataType::SmallInt => 2,
            DataType::Int | DataType::Date | DataType::Real => 4,
            DataType::BigInt | DataType::DoublePrecision => 8,
            DataType::Numeric { .. } => 1,
            DataType::Decimal { .. } => 1,
            DataType::Bool => 1,
            DataType::Char(_) => 1,
            DataType::Varchar(_) => 1,
            DataType::Time => 8,
            DataType::Bit(_) => 1,
            DataType::Timestamp => 8,
        }
    }

    /// Returns the exact on-disk size for fixed-width types.
    pub fn fixed_size(&self) -> Option<u32> {
        match self {
            DataType::SmallInt => Some(2),
            DataType::Int | DataType::Real => Some(4),
            DataType::BigInt | DataType::DoublePrecision => Some(8),
            DataType::Numeric { precision, .. } => Some(((*precision as u32) + 1).div_ceil(2)),
            DataType::Decimal { precision, .. } => Some(((*precision as u32) + 1).div_ceil(2)),
            DataType::Date => Some(4),
            DataType::Bool => Some(1),
            DataType::Char(n) => Some(*n as u32),
            DataType::Time => Some(8),
            DataType::Bit(n) => Some((*n as u32).div_ceil(8)),
            DataType::Timestamp => Some(8),
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
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Real => write!(f, "REAL"),
            DataType::DoublePrecision => write!(f, "DOUBLE PRECISION"),
            DataType::Numeric { precision, scale } => {
                write!(f, "NUMERIC({},{})", precision, scale)
            }
            DataType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({},{})", precision, scale)
            }
            DataType::Bool => write!(f, "BOOLEAN"),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Bit(n) => write!(f, "BIT({})", n),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}

fn parse_precision_scale(raw: &str) -> Result<(u8, u8), String> {
    let parts: Vec<&str> = raw.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid precision/scale '{}': expected format (p,s)",
            raw
        ));
    }
    let precision = parts[0]
        .parse::<u8>()
        .map_err(|_| format!("Invalid precision '{}'", parts[0]))?;
    let scale = parts[1]
        .parse::<u8>()
        .map_err(|_| format!("Invalid scale '{}'", parts[1]))?;
    if precision == 0 || precision > 38 {
        return Err("NUMERIC precision must be in [1, 38]".to_string());
    }
    if scale > precision {
        return Err("NUMERIC scale must be <= precision".to_string());
    }
    Ok((precision, scale))
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();
        match upper.as_str() {
            "SMALLINT" => Ok(DataType::SmallInt),
            "INT" | "INTEGER" => Ok(DataType::Int),
            "BIGINT" => Ok(DataType::BigInt),
            "REAL" => Ok(DataType::Real),
            "DOUBLE PRECISION" | "DOUBLEPRECISION" | "FLOAT8" => Ok(DataType::DoublePrecision),
            "BOOL" | "BOOLEAN" => Ok(DataType::Bool),
            "DATE" => Ok(DataType::Date),
            "TIME" => Ok(DataType::Time),
            "TIMESTAMP" => Ok(DataType::Timestamp),
            _ => {
                if upper.starts_with("NUMERIC(") && upper.ends_with(')') {
                    let inner = &upper[8..upper.len() - 1];
                    let (precision, scale) = parse_precision_scale(inner)?;
                    Ok(DataType::Numeric { precision, scale })
                } else if upper.starts_with("DECIMAL(") && upper.ends_with(')') {
                    let inner = &upper[8..upper.len() - 1];
                    let (precision, scale) = parse_precision_scale(inner)?;
                    Ok(DataType::Decimal { precision, scale })
                } else if upper.starts_with("CHAR(") && upper.ends_with(')') && !upper.starts_with("CHARACTER(") {
                    let inner = &upper[5..upper.len() - 1];
                    inner
                        .parse::<u16>()
                        .map(DataType::Char)
                        .map_err(|_| format!("Invalid CHAR size: '{}'", inner))
                } else if upper.starts_with("CHARACTER(") && upper.ends_with(')') {
                    let inner = &upper[10..upper.len() - 1];
                    inner
                        .parse::<u16>()
                        .map(DataType::Char)
                        .map_err(|_| format!("Invalid CHARACTER size: '{}'", inner))
                } else if upper.starts_with("VARCHAR(") && upper.ends_with(')') {
                    let inner = &upper[8..upper.len() - 1];
                    inner
                        .parse::<u16>()
                        .map(DataType::Varchar)
                        .map_err(|_| format!("Invalid VARCHAR size: '{}'", inner))
                } else if upper.starts_with("BIT(") && upper.ends_with(')') {
                    let inner = &upper[4..upper.len() - 1];
                    inner
                        .parse::<u16>()
                        .map(DataType::Bit)
                        .map_err(|_| format!("Invalid BIT size: '{}'", inner))
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
