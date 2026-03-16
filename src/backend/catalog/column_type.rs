//! Column type helpers for serialization and deserialization of tuple data.
//! Supports fixed-length INT and variable-length TEXT and VARCHAR (with optional max length).

use crate::catalog::types::Column;

/// Length prefix size in bytes for variable-length string types (TEXT, VARCHAR).
pub const VAR_STRING_LEN_PREFIX: usize = 2;

/// Maximum length for a single variable-length string (u16::MAX would be 65535).
pub const VAR_STRING_MAX_LEN: usize = u16::MAX as usize;

/// Returns true if the data type is variable-length (TEXT or VARCHAR).
pub fn is_variable_length(data_type: &str) -> bool {
    let t = data_type.trim().to_uppercase();
    t == "TEXT" || t.starts_with("VARCHAR")
}

/// Parses optional max length from VARCHAR(n). Returns None for TEXT or plain VARCHAR (no limit).
pub fn varchar_max_length(data_type: &str) -> Option<u16> {
    let t = data_type.trim();
    if !t.to_uppercase().starts_with("VARCHAR") {
        return None;
    }
    if let Some(start) = t.find('(') {
        if let Some(end) = t.find(')') {
            let inner = t[start + 1..end].trim();
            if let Ok(n) = inner.parse::<u16>() {
                return Some(n);
            }
        }
    }
    None
}

/// Serializes a single column value to bytes for storage.
/// - INT: 4 bytes little-endian.
/// - TEXT / VARCHAR: 2 bytes length (u16 LE) + raw UTF-8 bytes. VARCHAR(n) truncates to n bytes.
pub fn serialize_value(col: &Column, val: &str) -> Vec<u8> {
    let dt = col.data_type.trim();
    if dt.eq_ignore_ascii_case("INT") {
        let num: i32 = val.parse().unwrap_or_default();
        return num.to_le_bytes().to_vec();
    }
    if dt.eq_ignore_ascii_case("TEXT") || dt.to_uppercase().starts_with("VARCHAR") {
        let mut bytes = val.as_bytes().to_vec();
        let max = varchar_max_length(dt).unwrap_or(u16::MAX);
        if bytes.len() > max as usize {
            bytes.truncate(max as usize);
        }
        let len = bytes.len() as u16;
        let mut out = len.to_le_bytes().to_vec();
        out.extend_from_slice(&bytes);
        return out;
    }
    Vec::new()
}

/// Deserializes a single column value from tuple data starting at `cursor`.
/// Advances `cursor` past the consumed bytes. Returns a string suitable for display.
/// Returns None if there is not enough data (corrupt or end of tuple).
pub fn deserialize_value(col: &Column, data: &[u8], cursor: &mut usize) -> Option<String> {
    let dt = col.data_type.trim();
    if dt.eq_ignore_ascii_case("INT") {
        if *cursor + 4 > data.len() {
            return None;
        }
        let val = i32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
        *cursor += 4;
        return Some(val.to_string());
    }
    if dt.eq_ignore_ascii_case("TEXT") || dt.to_uppercase().starts_with("VARCHAR") {
        if *cursor + VAR_STRING_LEN_PREFIX > data.len() {
            return None;
        }
        let len = u16::from_le_bytes(data[*cursor..*cursor + 2].try_into().unwrap()) as usize;
        *cursor += VAR_STRING_LEN_PREFIX;
        if *cursor + len > data.len() {
            return None;
        }
        let s = String::from_utf8_lossy(&data[*cursor..*cursor + len]).to_string();
        *cursor += len;
        return Some(format!("'{}'", s));
    }
    None
}
