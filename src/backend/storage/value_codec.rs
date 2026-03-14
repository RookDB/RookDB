//! Codec for encoding and decoding typed values to/from bytes
//! Provides serialization logic for all supported data types

use crate::catalog::data_type::{DataType, Value};

/// Handles encoding and decoding of Values to/from binary format
pub struct ValueCodec;

impl ValueCodec {
    /// Encode a typed value to bytes
    pub fn encode(value: &Value, ty: &DataType) -> Result<Vec<u8>, String> {
        if value.is_null() {
            return Ok(vec![]);
        }

        match (value, ty) {
            (Value::Int32(v), DataType::Int32) => Ok(v.to_le_bytes().to_vec()),
            (Value::Boolean(v), DataType::Boolean) => Ok(vec![if *v { 1 } else { 0 }]),
            (Value::Text(v), DataType::Text) => Self::encode_text(v),
            (Value::Blob(v), DataType::Blob) => Self::encode_blob(v),
            (Value::Array(items), DataType::Array { element_type }) => {
                Self::encode_array(items, element_type)
            }
            _ => Err(format!(
                "Type mismatch: expected {:?}, got {:?}",
                ty, value.data_type()
            )),
        }
    }

    /// Decode bytes into a typed value
    pub fn decode(bytes: &[u8], ty: &DataType) -> Result<Value, String> {
        if bytes.is_empty() {
            return Ok(Value::Null);
        }

        match ty {
            DataType::Int32 => Self::decode_int32(bytes),
            DataType::Boolean => Self::decode_boolean(bytes),
            DataType::Text => Self::decode_text(bytes),
            DataType::Blob => Self::decode_blob(bytes),
            DataType::Array { element_type } => Self::decode_array(bytes, element_type),
        }
    }

    // === Fixed-type encoders/decoders ===

    fn decode_int32(bytes: &[u8]) -> Result<Value, String> {
        if bytes.len() < 4 {
            return Err("Not enough bytes for INT32".to_string());
        }
        let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Ok(Value::Int32(val))
    }

    fn decode_boolean(bytes: &[u8]) -> Result<Value, String> {
        if bytes.is_empty() {
            return Err("Not enough bytes for BOOLEAN".to_string());
        }
        Ok(Value::Boolean(bytes[0] != 0))
    }

    // === Variable-type encoders/decoders ===

    fn encode_text(text: &str) -> Result<Vec<u8>, String> {
        let bytes = text.as_bytes();
        let mut result = Vec::with_capacity(4 + bytes.len());
        // Prefix with 4-byte length
        result.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        result.extend_from_slice(bytes);
        Ok(result)
    }

    fn decode_text(bytes: &[u8]) -> Result<Value, String> {
        if bytes.len() < 4 {
            return Err("Not enough bytes for TEXT length".to_string());
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + len {
            return Err("Not enough bytes for TEXT data".to_string());
        }
        let text = String::from_utf8_lossy(&bytes[4..4 + len]).to_string();
        Ok(Value::Text(text))
    }

    fn encode_blob(blob: &[u8]) -> Result<Vec<u8>, String> {
        let mut result = Vec::with_capacity(4 + blob.len());
        // Prefix with 4-byte length
        result.extend_from_slice(&(blob.len() as u32).to_le_bytes());
        result.extend_from_slice(blob);
        Ok(result)
    }

    fn decode_blob(bytes: &[u8]) -> Result<Value, String> {
        if bytes.len() < 4 {
            return Err("Not enough bytes for BLOB length".to_string());
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + len {
            return Err("Not enough bytes for BLOB data".to_string());
        }
        Ok(Value::Blob(bytes[4..4 + len].to_vec()))
    }

    // === Array encoders/decoders ===

    fn encode_array(values: &[Value], element_type: &DataType) -> Result<Vec<u8>, String> {
        let mut result = Vec::new();
        // Prefix with number of elements (4 bytes)
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        for value in values {
            let encoded = Self::encode(value, element_type)?;
            // For variable-length elements, store length prefix
            if element_type.is_variable_length() {
                result.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            }
            result.extend_from_slice(&encoded);
        }

        Ok(result)
    }

    fn decode_array(bytes: &[u8], element_type: &DataType) -> Result<Value, String> {
        if bytes.len() < 4 {
            return Err("Not enough bytes for array element count".to_string());
        }

        let element_count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let mut items = Vec::new();
        let mut cursor = 4usize;

        for _ in 0..element_count {
            if element_type.is_variable_length() {
                // Read length prefix for variable-length element
                if cursor + 4 > bytes.len() {
                    return Err("Not enough bytes for element length".to_string());
                }
                let elem_len =
                    u32::from_le_bytes([bytes[cursor], bytes[cursor + 1], bytes[cursor + 2], bytes[cursor + 3]])
                        as usize;
                cursor += 4;

                if cursor + elem_len > bytes.len() {
                    return Err("Not enough bytes for element data".to_string());
                }

                let elem_bytes = &bytes[cursor..cursor + elem_len];
                let value = Self::decode(elem_bytes, element_type)?;
                items.push(value);
                cursor += elem_len;
            } else {
                // For fixed-length elements, read fixed size
                if let Some(fixed_size) = element_type.fixed_size() {
                    if cursor + fixed_size > bytes.len() {
                        return Err("Not enough bytes for element data".to_string());
                    }

                    let elem_bytes = &bytes[cursor..cursor + fixed_size];
                    let value = Self::decode(elem_bytes, element_type)?;
                    items.push(value);
                    cursor += fixed_size;
                } else {
                    return Err("Invalid array element type".to_string());
                }
            }
        }

        Ok(Value::Array(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_int32() {
        let value = Value::Int32(42);
        let encoded = ValueCodec::encode(&value, &DataType::Int32).unwrap();
        let decoded = ValueCodec::decode(&encoded, &DataType::Int32).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_boolean() {
        let value = Value::Boolean(true);
        let encoded = ValueCodec::encode(&value, &DataType::Boolean).unwrap();
        let decoded = ValueCodec::decode(&encoded, &DataType::Boolean).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_text() {
        let value = Value::Text("Hello, World!".to_string());
        let encoded = ValueCodec::encode(&value, &DataType::Text).unwrap();
        let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_blob() {
        let value = Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let encoded = ValueCodec::encode(&value, &DataType::Blob).unwrap();
        let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_int_array() {
        let value = Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);
        let ty = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };
        let encoded = ValueCodec::encode(&value, &ty).unwrap();
        let decoded = ValueCodec::decode(&encoded, &ty).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_text_array() {
        let value = Value::Array(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]);
        let ty = DataType::Array {
            element_type: Box::new(DataType::Text),
        };
        let encoded = ValueCodec::encode(&value, &ty).unwrap();
        let decoded = ValueCodec::decode(&encoded, &ty).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_null_value() {
        let value = Value::Null;
        let encoded = ValueCodec::encode(&value, &DataType::Text).unwrap();
        assert!(encoded.is_empty());
    }
}
