use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::types::bit_utils::{normalize_bit_literal, pack_bit_string, unpack_bit_string};
use crate::types::datatype::DataType;
use crate::types::validation::validate_value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumericValue {
    pub unscaled: i128,
    pub scale: u8,
}

/// IEEE 754 `f32` wrapper that satisfies `Eq` by comparing bit patterns.
/// NaN values with identical bit patterns compare equal.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderedF32(pub f32);

impl PartialEq for OrderedF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for OrderedF32 {}

impl PartialOrd for OrderedF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or_else(|| {
            match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Less,
            }
        })
    }
}

/// IEEE 754 `f64` wrapper that satisfies `Eq` by comparing bit patterns.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderedF64(pub f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or_else(|| {
            match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Less,
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataValue {
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(OrderedF32),
    DoublePrecision(OrderedF64),
    Numeric(NumericValue),
    Bool(bool),
    Char(String),
    Varchar(String),
    Date(NaiveDate),
    Time(NaiveTime),
    Bit(String),
    Timestamp(NaiveDateTime),
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::SmallInt(v) => write!(f, "{}", v),
            DataValue::Int(v) => write!(f, "{}", v),
            DataValue::BigInt(v) => write!(f, "{}", v),
            DataValue::Real(v) => write!(f, "{}", v.0),
            DataValue::DoublePrecision(v) => write!(f, "{}", v.0),
            DataValue::Numeric(v) => {
                let sign = if v.unscaled < 0 { "-" } else { "" };
                let mut digits = v.unscaled.abs().to_string();
                let scale = v.scale as usize;
                if scale == 0 {
                    write!(f, "{}{}", sign, digits)
                } else {
                    if digits.len() <= scale {
                        digits = format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits);
                    }
                    let split = digits.len() - scale;
                    write!(f, "{}{}.{}", sign, &digits[..split], &digits[split..])
                }
            }
            DataValue::Bool(v) => write!(f, "{}", v),
            DataValue::Char(v) => write!(f, "'{}'", v.trim_end_matches(' ')),
            DataValue::Varchar(v) => write!(f, "'{}'", v),
            DataValue::Date(v) => write!(f, "{}", v.format("%Y-%m-%d")),
            DataValue::Time(v) => write!(f, "{}", v.format("%H:%M:%S%.6f")),
            DataValue::Bit(v) => write!(f, "B'{}'", v),
            DataValue::Timestamp(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S%.6f")),
        }
    }
}

fn parse_numeric_literal(input: &str, precision: u8, scale: u8) -> Result<NumericValue, String> {
    let raw = input.trim().trim_matches('"').trim_matches('\'');
    if raw.is_empty() {
        return Err("NUMERIC value cannot be empty".to_string());
    }

    let (sign, body) = if let Some(rest) = raw.strip_prefix('-') {
        (-1_i128, rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        (1_i128, rest)
    } else {
        (1_i128, raw)
    };

    let mut parts = body.split('.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    if parts.next().is_some() {
        return Err(format!("Invalid NUMERIC value '{}': too many decimal points", raw));
    }
    if !int_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("Invalid NUMERIC value '{}': invalid integer digits", raw));
    }
    let frac = frac_part.unwrap_or("");
    if !frac.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("Invalid NUMERIC value '{}': invalid fractional digits", raw));
    }
    if frac.len() > scale as usize {
        return Err(format!(
            "NUMERIC({}, {}) value '{}' has too many fractional digits",
            precision, scale, raw
        ));
    }

    let mut combined = String::new();
    if int_part.is_empty() {
        combined.push('0');
    } else {
        combined.push_str(int_part);
    }
    combined.push_str(frac);
    combined.push_str(&"0".repeat(scale as usize - frac.len()));

    let normalized = combined.trim_start_matches('0');
    let effective_digits = if normalized.is_empty() { 1 } else { normalized.len() };
    if effective_digits > precision as usize {
        return Err(format!(
            "NUMERIC({}, {}) value '{}' exceeds precision {}",
            precision, scale, raw, precision
        ));
    }

    let unscaled_abs = combined
        .parse::<i128>()
        .map_err(|_| format!("NUMERIC value '{}' is out of supported range", raw))?;
    Ok(NumericValue {
        unscaled: sign * unscaled_abs,
        scale,
    })
}

fn encode_numeric_bcd(value: &NumericValue, precision: u8) -> Result<Vec<u8>, String> {
    let mut digits = value.unscaled.abs().to_string();
    if digits.len() > precision as usize {
        return Err(format!(
            "NUMERIC value '{}' exceeds precision {}",
            value.unscaled, precision
        ));
    }
    if digits.len() < precision as usize {
        digits = format!("{}{}", "0".repeat(precision as usize - digits.len()), digits);
    }

    let mut nibbles: Vec<u8> = Vec::with_capacity(precision as usize + 2);
    if precision % 2 == 0 {
        nibbles.push(0);
    }
    for ch in digits.chars() {
        nibbles.push((ch as u8) - b'0');
    }
    nibbles.push(if value.unscaled < 0 { 0x0D } else { 0x0C });

    let mut out = Vec::with_capacity(nibbles.len().div_ceil(2));
    for pair in nibbles.chunks(2) {
        let hi = pair[0] & 0x0F;
        let lo = pair.get(1).copied().unwrap_or(0) & 0x0F;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn decode_numeric_bcd(bytes: &[u8], precision: u8, scale: u8) -> Result<NumericValue, String> {
    let expected = ((precision as usize) + 1).div_ceil(2);
    if bytes.len() < expected {
        return Err(format!("NUMERIC({}, {}) requires {} bytes", precision, scale, expected));
    }

    let mut nibbles = Vec::with_capacity(expected * 2);
    for b in &bytes[..expected] {
        nibbles.push((b >> 4) & 0x0F);
        nibbles.push(b & 0x0F);
    }

    let start = if precision % 2 == 0 { 1 } else { 0 };
    let digits_slice = &nibbles[start..start + precision as usize];
    let sign_nibble = nibbles[start + precision as usize];
    if !digits_slice.iter().all(|d| *d <= 9) {
        return Err("NUMERIC payload contains invalid BCD digit".to_string());
    }
    let mut digits = String::with_capacity(precision as usize);
    for d in digits_slice {
        digits.push((b'0' + *d) as char);
    }
    let abs_val = digits
        .parse::<i128>()
        .map_err(|_| "NUMERIC decoded value is out of range".to_string())?;
    let sign = match sign_nibble {
        0x0C => 1_i128,
        0x0D => -1_i128,
        _ => return Err("NUMERIC payload has invalid sign nibble".to_string()),
    };
    Ok(NumericValue {
        unscaled: sign * abs_val,
        scale,
    })
}

impl DataValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            DataValue::SmallInt(v) => v.to_le_bytes().to_vec(),
            DataValue::Int(v) => v.to_le_bytes().to_vec(),
            DataValue::BigInt(v) => v.to_le_bytes().to_vec(),
            DataValue::Real(v) => v.0.to_le_bytes().to_vec(),
            DataValue::DoublePrecision(v) => v.0.to_le_bytes().to_vec(),
            DataValue::Numeric(v) => {
                // This variant needs precision from DataType; callers should prefer to_bytes_for_type.
                let mut out = Vec::with_capacity(18);
                out.extend_from_slice(&v.unscaled.to_le_bytes());
                out.push(v.scale);
                out
            }
            DataValue::Bool(v) => vec![u8::from(*v)],
            DataValue::Char(v) => v.as_bytes().to_vec(),
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
            DataValue::Time(v) => {
                // microseconds since midnight
                let micros = v.num_seconds_from_midnight() as i64 * 1_000_000
                    + v.nanosecond() as i64 / 1_000;
                micros.to_le_bytes().to_vec()
            }
            DataValue::Bit(v) => pack_bit_string(v),
                    DataValue::Timestamp(v) => {
                        // microseconds since Unix epoch
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap();
                        let micros = v.signed_duration_since(epoch).num_microseconds()
                            .unwrap_or(i64::MAX);
                        micros.to_le_bytes().to_vec()
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
            DataType::BigInt => {
                if bytes.len() < 8 {
                    return Err("BIGINT requires 8 bytes".to_string());
                }
                Ok(DataValue::BigInt(i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ])))
            }
            DataType::Real => {
                if bytes.len() < 4 {
                    return Err("REAL requires 4 bytes".to_string());
                }
                Ok(DataValue::Real(OrderedF32(f32::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                ]))))
            }
            DataType::DoublePrecision => {
                if bytes.len() < 8 {
                    return Err("DOUBLE PRECISION requires 8 bytes".to_string());
                }
                Ok(DataValue::DoublePrecision(OrderedF64(f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]))))
            }
            DataType::Numeric { precision, scale } => {
                let decoded = decode_numeric_bcd(bytes, *precision, *scale)?;
                Ok(DataValue::Numeric(decoded))
            }
            DataType::Decimal { precision, scale } => {
                let decoded = decode_numeric_bcd(bytes, *precision, *scale)?;
                Ok(DataValue::Numeric(decoded))
            }
            DataType::Bool => {
                if bytes.is_empty() {
                    return Err("BOOLEAN requires 1 byte".to_string());
                }
                Ok(DataValue::Bool(bytes[0] != 0))
            }
            DataType::Char(n) => {
                let len = *n as usize;
                if bytes.len() < len {
                    return Err(format!("CHAR({}) requires {} bytes", n, len));
                }
                let value = String::from_utf8(bytes[..len].to_vec())
                    .map_err(|_| "CHAR payload is not valid UTF-8".to_string())?;
                Ok(DataValue::Char(value))
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
            DataType::Time => {
                if bytes.len() < 8 {
                    return Err("TIME requires 8 bytes".to_string());
                }
                let micros = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let secs = (micros / 1_000_000) as u32;
                let nanos = ((micros % 1_000_000) * 1_000) as u32;
                NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                    .map(DataValue::Time)
                    .ok_or_else(|| "TIME value out of range".to_string())
            }
            DataType::Timestamp => {
                if bytes.len() < 8 {
                    return Err("TIMESTAMP requires 8 bytes".to_string());
                }
                let micros = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
                epoch
                    .checked_add_signed(Duration::microseconds(micros))
                    .map(DataValue::Timestamp)
                    .ok_or_else(|| "TIMESTAMP is outside supported chrono range".to_string())
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
            DataType::BigInt => input
                .parse::<i64>()
                .map(DataValue::BigInt)
                .map(|v| v.to_bytes())
                .map_err(|e| e.to_string()),
            DataType::Real => input
                .parse::<f32>()
                .map(|v| DataValue::Real(OrderedF32(v)))
                .map(|v| v.to_bytes())
                .map_err(|e| e.to_string()),
            DataType::DoublePrecision => input
                .parse::<f64>()
                .map(|v| DataValue::DoublePrecision(OrderedF64(v)))
                .map(|v| v.to_bytes())
                .map_err(|e| e.to_string()),
            DataType::Numeric { precision, scale } => {
                let parsed = parse_numeric_literal(input, *precision, *scale)?;
                encode_numeric_bcd(&parsed, *precision)
            }
            DataType::Decimal { precision, scale } => {
                let parsed = parse_numeric_literal(input, *precision, *scale)?;
                encode_numeric_bcd(&parsed, *precision)
            }
            DataType::Bool => match input.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Ok(DataValue::Bool(true).to_bytes()),
                "false" | "f" | "0" => Ok(DataValue::Bool(false).to_bytes()),
                _ => Err(format!("Invalid BOOLEAN value '{}': expected true/false", input)),
            },
            DataType::Char(n) => {
                let value = input.trim_matches('"').trim_matches('\'');
                let mut bytes = value.as_bytes().to_vec();
                if bytes.len() > *n as usize {
                    return Err(format!(
                        "CHAR({}) value exceeds maximum length {}",
                        n, n
                    ));
                }
                bytes.resize(*n as usize, b' ');
                Ok(bytes)
            }
            DataType::Varchar(_) => {
                let value = input.trim_matches('"').trim_matches('\'');
                Ok(DataValue::Varchar(value.to_string()).to_bytes())
            }
            DataType::Date => {
                let date = NaiveDate::parse_from_str(input.trim_matches('\''), "%Y-%m-%d")
                    .map_err(|e| e.to_string())?;
                Ok(DataValue::Date(date).to_bytes())
            }
            DataType::Time => {
                let raw = input.trim_matches('\'');
                // Accept HH:MM:SS, HH:MM:SS.ffffff
                NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
                    .or_else(|_| NaiveTime::parse_from_str(raw, "%H:%M:%S"))
                    .map(|v| DataValue::Time(v).to_bytes())
                    .map_err(|e| e.to_string())
            }
            DataType::Timestamp => {
                let raw = input.trim_matches('\'');
                NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                    .map(|v| DataValue::Timestamp(v).to_bytes())
                    .map_err(|e| e.to_string())
            }
            DataType::Bit(_) => {
                let bits = normalize_bit_literal(input);
                Ok(DataValue::Bit(bits).to_bytes())
            }
        }
    }

    pub fn to_bytes_for_type(&self, ty: &DataType) -> Result<Vec<u8>, String> {
        match (ty, self) {
            (DataType::Numeric { precision, scale }, DataValue::Numeric(v)) => {
                if v.scale != *scale {
                    return Err(format!(
                        "NUMERIC scale mismatch: value has scale {}, type requires {}",
                        v.scale, scale
                    ));
                }
                encode_numeric_bcd(v, *precision)
            }
            (DataType::Decimal { precision, scale }, DataValue::Numeric(v)) => {
                if v.scale != *scale {
                    return Err(format!(
                        "DECIMAL scale mismatch: value has scale {}, type requires {}",
                        v.scale, scale
                    ));
                }
                encode_numeric_bcd(v, *precision)
            }
            (DataType::Char(n), DataValue::Char(v)) => {
                let mut bytes = v.as_bytes().to_vec();
                if bytes.len() > *n as usize {
                    return Err(format!("CHAR({}) value exceeds maximum length {}", n, n));
                }
                bytes.resize(*n as usize, b' ');
                Ok(bytes)
            }
            _ => Ok(self.to_bytes()),
        }
    }
}
