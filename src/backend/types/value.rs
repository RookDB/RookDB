//! Runtime typed values and their binary encoding/decoding.
//!
//! [`DataValue`] is the in-memory representation of any SQL column value.
//! The two primary operations are:
//! - [`DataValue::parse_and_encode`] — parse a raw string literal and return
//!   the binary encoding ready for on-disk storage.
//! - [`DataValue::from_bytes`] — decode the binary form back into a typed value.
//!
//! **Wire format summary:**
//!
//! | Type | Bytes | Notes |
//! |---|---|---|
//! | SMALLINT | 2 | signed little-endian i16 |
//! | INT | 4 | signed little-endian i32 |
//! | BIGINT | 8 | signed little-endian i64 |
//! | REAL | 4 | IEEE 754 f32, little-endian |
//! | DOUBLE | 8 | IEEE 754 f64, little-endian |
//! | NUMERIC/DECIMAL | ceil((p+1)/2) | packed BCD + sign nibble |
//! | BOOL | 1 | 0x00=false 0x01=true |
//! | CHAR(n) | n | UTF-8, space-padded |
//! | VARCHAR(n) | 2 + len | [u16 len prefix][UTF-8] in `to_bytes()`; stored without prefix in row |
//! | DATE | 4 | days since 1970-01-01, i32 LE |
//! | TIME | 8 | µs since midnight, i64 LE |
//! | TIMESTAMP | 8 | µs since Unix epoch, i64 LE |
//! | BIT(n) | ceil(n/8) | packed MSB-first |

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::types::bit_utils::{normalize_bit_literal, pack_bit_string, unpack_bit_string};
use crate::types::datatype::DataType;
use crate::types::validation::validate_value;

/// An exact fixed-point number stored as a scaled integer.
///
/// The mathematical value is `unscaled / 10^scale`. For example,
/// `NumericValue { unscaled: 12345, scale: 2 }` represents `123.45`.
///
/// On disk the value is encoded as packed BCD (Binary Coded Decimal) with
/// a trailing sign nibble (`0x0C` = positive, `0x0D` = negative).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumericValue {
    /// The integer coefficient: actual value = `unscaled / 10^scale`.
    pub unscaled: i128,
    /// Number of decimal places (fractional digits).
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

/// The in-memory representation of a SQL column value.
///
/// Each variant corresponds to one or more [`DataType`] variants. Encoding and
/// decoding rules are documented in the module-level table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataValue {
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    /// Single-precision float wrapped in [`OrderedF32`] to satisfy `Eq`.
    Real(OrderedF32),
    /// Double-precision float wrapped in [`OrderedF64`] to satisfy `Eq`.
    DoublePrecision(OrderedF64),
    /// Exact decimal; see [`NumericValue`] for the encoding contract.
    Numeric(NumericValue),
    Bool(bool),
    /// Fixed-length character string (may be shorter than declared width;
    /// space-padding is applied during encoding).
    Char(String),
    /// Variable-length UTF-8 string.
    Varchar(String),
    /// Calendar date.
    Date(NaiveDate),
    /// Time of day, sub-second precision up to microseconds.
    Time(NaiveTime),
    /// A fixed-width bit string as a `'0'`/`'1'` character sequence.
    Bit(String),
    /// Date + time (no timezone).
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
    /// Encode a value to bytes using the type's canonical wire format.
    ///
    /// The row serializer uses this output directly. Variable-length (Varchar)
    /// payloads are returned as raw UTF-8 bytes with no length prefix.
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
            DataValue::Varchar(v) => v.as_bytes().to_vec(),
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

    /// Decode a typed value from its on-disk byte representation.
    ///
    /// For variable-length data (`Varchar`), the byte slice explicitly
    /// delimits the exact payload, as extracted from the row's offset geometry.
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
            DataType::Character(n) => {
                let len = *n as usize;
                if bytes.len() < len {
                    return Err(format!("CHARACTER({}) requires {} bytes", n, len));
                }
                let value = String::from_utf8(bytes[..len].to_vec())
                    .map_err(|_| "CHARACTER payload is not valid UTF-8".to_string())?;
                Ok(DataValue::Char(value))
            }
            DataType::Varchar(max_len) => {
                let payload_len = bytes.len();
                if payload_len > *max_len as usize {
                    return Err(format!(
                        "VARCHAR payload length {} exceeds declared limit {}",
                        payload_len, max_len
                    ));
                }
                let value = String::from_utf8(bytes.to_vec())
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

    /// Parse a raw SQL literal string, validate it, and return the encoded bytes.
    ///
    /// Combines validation ([`validate_value`]) and encoding ([`to_bytes`](Self::to_bytes))
    /// into a single step. Used by the INSERT path to convert user-supplied string
    /// literals directly into storage bytes.
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
            DataType::Character(n) => {
                let value = input.trim_matches('"').trim_matches('\'');
                let mut bytes = value.as_bytes().to_vec();
                if bytes.len() > *n as usize {
                    return Err(format!(
                        "CHARACTER({}) value exceeds maximum length {}",
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

    /// Type-aware variant of [`to_bytes`](Self::to_bytes) for types whose
    /// encoding depends on schema metadata (`NUMERIC` precision, `CHAR` length).
    ///
    /// For most types this delegates to [`to_bytes`]. For `NUMERIC`/`DECIMAL`
    /// it validates the scale matches before BCD-encoding. For `CHAR`/`CHARACTER`
    /// it applies space-padding to the declared fixed length.
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
            (DataType::Character(n), DataValue::Char(v)) => {
                let mut bytes = v.as_bytes().to_vec();
                if bytes.len() > *n as usize {
                    return Err(format!("CHARACTER({}) value exceeds maximum length {}", n, n));
                }
                bytes.resize(*n as usize, b' ');
                Ok(bytes)
            }
            _ => Ok(self.to_bytes()),
        }
    }
}
