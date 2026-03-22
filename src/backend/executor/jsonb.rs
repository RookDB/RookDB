use serde_json::Value;

/// Represents a parsed JSONB value in memory for binary encoding/decoding.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonbValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonbValue>),
    Object(Vec<(String, JsonbValue)>), // sorted by key
}

// Tag bytes for JSONB binary format
const TAG_NULL: u8 = 0x00;
const TAG_FALSE: u8 = 0x01;
const TAG_TRUE: u8 = 0x02;
const TAG_NUMBER: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_ARRAY: u8 = 0x05;
const TAG_OBJECT: u8 = 0x06;

/// Handles conversion between JSON text and JSONB binary format.
pub struct JsonbSerializer;

impl JsonbSerializer {
    /// Parse JSON text into a JsonbValue tree, sorting object keys lexicographically.
    pub fn parse(json_text: &str) -> Result<JsonbValue, String> {
        let value: Value =
            serde_json::from_str(json_text).map_err(|e| format!("Invalid JSON: {}", e))?;
        Ok(Self::convert_value(value))
    }

    /// Recursively convert serde_json::Value into JsonbValue.
    fn convert_value(value: Value) -> JsonbValue {
        match value {
            Value::Null => JsonbValue::Null,
            Value::Bool(b) => JsonbValue::Bool(b),
            Value::Number(n) => JsonbValue::Number(n.as_f64().unwrap_or(0.0)),
            Value::String(s) => JsonbValue::String(s),
            Value::Array(arr) => {
                let elements = arr.into_iter().map(Self::convert_value).collect();
                JsonbValue::Array(elements)
            }
            Value::Object(map) => {
                let mut pairs: Vec<(String, JsonbValue)> = map
                    .into_iter()
                    .map(|(k, v)| (k, Self::convert_value(v)))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                JsonbValue::Object(pairs)
            }
        }
    }

    /// Serialize a JsonbValue tree into JSONB binary bytes.
    pub fn to_binary(value: &JsonbValue) -> Vec<u8> {
        let mut buf = Vec::new();
        Self::write_value(&mut buf, value);
        buf
    }

    fn write_value(buf: &mut Vec<u8>, value: &JsonbValue) {
        match value {
            JsonbValue::Null => {
                buf.push(TAG_NULL);
            }
            JsonbValue::Bool(false) => {
                buf.push(TAG_FALSE);
            }
            JsonbValue::Bool(true) => {
                buf.push(TAG_TRUE);
            }
            JsonbValue::Number(n) => {
                buf.push(TAG_NUMBER);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            JsonbValue::String(s) => {
                buf.push(TAG_STRING);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            JsonbValue::Array(elems) => {
                buf.push(TAG_ARRAY);
                buf.extend_from_slice(&(elems.len() as u32).to_le_bytes());
                for elem in elems {
                    Self::write_value(buf, elem);
                }
            }
            JsonbValue::Object(pairs) => {
                buf.push(TAG_OBJECT);
                buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
                for (key, val) in pairs {
                    // Keys are stored as TAG_STRING entries
                    buf.push(TAG_STRING);
                    let key_bytes = key.as_bytes();
                    buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key_bytes);
                    Self::write_value(buf, val);
                }
            }
        }
    }

    /// Deserialize JSONB binary bytes back into a JsonbValue tree.
    /// Returns the parsed value and number of bytes consumed.
    pub fn from_binary(data: &[u8]) -> Result<(JsonbValue, usize), String> {
        if data.is_empty() {
            return Err("Empty JSONB data".to_string());
        }

        let tag = data[0];
        match tag {
            TAG_NULL => Ok((JsonbValue::Null, 1)),
            TAG_FALSE => Ok((JsonbValue::Bool(false), 1)),
            TAG_TRUE => Ok((JsonbValue::Bool(true), 1)),
            TAG_NUMBER => {
                if data.len() < 9 {
                    return Err("Truncated JSONB number".to_string());
                }
                let n = f64::from_le_bytes(data[1..9].try_into().unwrap());
                Ok((JsonbValue::Number(n), 9))
            }
            TAG_STRING => {
                if data.len() < 5 {
                    return Err("Truncated JSONB string length".to_string());
                }
                let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                if data.len() < 5 + len {
                    return Err("Truncated JSONB string data".to_string());
                }
                let s = String::from_utf8(data[5..5 + len].to_vec())
                    .map_err(|e| format!("Invalid UTF-8 in JSONB string: {}", e))?;
                Ok((JsonbValue::String(s), 5 + len))
            }
            TAG_ARRAY => {
                if data.len() < 5 {
                    return Err("Truncated JSONB array count".to_string());
                }
                let count = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                let mut offset = 5;
                let mut elements = Vec::with_capacity(count);
                for _ in 0..count {
                    let (val, consumed) = Self::from_binary(&data[offset..])?;
                    elements.push(val);
                    offset += consumed;
                }
                Ok((JsonbValue::Array(elements), offset))
            }
            TAG_OBJECT => {
                if data.len() < 5 {
                    return Err("Truncated JSONB object count".to_string());
                }
                let count = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                let mut offset = 5;
                let mut pairs = Vec::with_capacity(count);
                for _ in 0..count {
                    // Read key (must be a string)
                    let (key_val, key_consumed) = Self::from_binary(&data[offset..])?;
                    let key = match key_val {
                        JsonbValue::String(s) => s,
                        _ => return Err("JSONB object key is not a string".to_string()),
                    };
                    offset += key_consumed;

                    // Read value
                    let (val, val_consumed) = Self::from_binary(&data[offset..])?;
                    pairs.push((key, val));
                    offset += val_consumed;
                }
                Ok((JsonbValue::Object(pairs), offset))
            }
            _ => Err(format!("Unknown JSONB tag: 0x{:02x}", tag)),
        }
    }

    /// Convert a JsonbValue tree to a human-readable JSON string for display.
    pub fn to_display_string(value: &JsonbValue) -> String {
        match value {
            JsonbValue::Null => "null".to_string(),
            JsonbValue::Bool(b) => b.to_string(),
            JsonbValue::Number(n) => {
                if *n == (*n as i64) as f64 && n.is_finite() {
                    format!("{}", *n as i64)
                } else {
                    format!("{}", n)
                }
            }
            JsonbValue::String(s) => {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            }
            JsonbValue::Array(elems) => {
                let items: Vec<String> = elems.iter().map(Self::to_display_string).collect();
                format!("[{}]", items.join(","))
            }
            JsonbValue::Object(pairs) => {
                let items: Vec<String> = pairs
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "\"{}\":{}",
                            k.replace('\\', "\\\\").replace('"', "\\\""),
                            Self::to_display_string(v)
                        )
                    })
                    .collect();
                format!("{{{}}}", items.join(","))
            }
        }
    }
}
