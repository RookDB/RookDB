//! Tuple encoding and decoding for mixed fixed/variable column layouts
//! Handles the complete tuple serialization with header, null bitmap, and variable fields

use crate::backend::catalog::data_type::{DataType, Value};
use crate::backend::storage::row_layout::{ToastPointer, TupleHeader, VarFieldEntry};
use crate::backend::storage::value_codec::ValueCodec;
use crate::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};

/// Encodes a row of values into tuple bytes with proper header and variable-field directory
pub struct TupleCodec;

impl TupleCodec {
    /// Encode a row of values into tuple bytes
    pub fn encode_tuple(
        values: &[Value],
        schema: &[(String, DataType)],
        toast_manager: &mut ToastManager,
    ) -> Result<Vec<u8>, String> {
        if values.len() != schema.len() {
            return Err(format!(
                "Value count mismatch: expected {}, got {}",
                schema.len(),
                values.len()
            ));
        }

        let column_count = values.len() as u16;
        let null_bitmap_bytes = (column_count + 7) / 8;
        let mut var_field_count = 0u16;

        // Count variable fields
        for (_, data_type) in schema {
            if data_type.is_variable_length() {
                var_field_count += 1;
            }
        }

        // Build null bitmap
        let mut null_bitmap = vec![0u8; null_bitmap_bytes as usize];
        for (i, value) in values.iter().enumerate() {
            if value.is_null() {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                null_bitmap[byte_idx] |= 1u8 << bit_idx;
            }
        }

        // Encode fixed and variable regions
        let mut fixed_region = Vec::new();
        let mut var_entries = Vec::new();
        let mut var_payload = Vec::new();

        for (i, (_, data_type)) in schema.iter().enumerate() {
            let value = &values[i];

            if data_type.is_variable_length() {
                let encoded = ValueCodec::encode(value, data_type)?;

                // Check if this should go to TOAST
                if encoded.len() > TOAST_THRESHOLD {
                    let toast_ptr = toast_manager.store_large_value(&encoded)?;
                    let toast_bytes = toast_ptr.to_bytes();
                    let offset = var_payload.len() as u32;
                    let length = toast_bytes.len() as u32;
                    var_payload.extend_from_slice(&toast_bytes);
                    var_entries.push(VarFieldEntry::new(offset, length, true));
                } else {
                    let offset = var_payload.len() as u32;
                    let length = encoded.len() as u32;
                    var_payload.extend_from_slice(&encoded);
                    var_entries.push(VarFieldEntry::new(offset, length, false));
                }
            } else {
                // Fixed-length field
                if let Some(fixed_size) = data_type.fixed_size() {
                    if value.is_null() {
                        fixed_region.extend_from_slice(&vec![0u8; fixed_size]);
                    } else {
                        let encoded = ValueCodec::encode(value, data_type)?;
                        fixed_region.extend_from_slice(&encoded);
                    }
                }
            }
        }

        // Assemble tuple
        let header = TupleHeader::new(column_count, null_bitmap_bytes, var_field_count);
        let mut tuple = Vec::new();

        // Add header
        tuple.extend_from_slice(&header.to_bytes());

        // Add null bitmap
        tuple.extend_from_slice(&null_bitmap);

        // Add variable-field directory
        for entry in &var_entries {
            tuple.extend_from_slice(&entry.to_bytes());
        }

        // Add fixed region
        tuple.extend_from_slice(&fixed_region);

        // Add variable payload
        tuple.extend_from_slice(&var_payload);

        Ok(tuple)
    }

    /// Decode tuple bytes into a row of typed values
    pub fn decode_tuple(
        tuple_bytes: &[u8],
        schema: &[(String, DataType)],
    ) -> Result<Vec<Value>, String> {
        Self::decode_tuple_internal(tuple_bytes, schema, None)
    }

    /// Decode tuple bytes into a row of typed values, resolving TOAST-backed values
    pub fn decode_tuple_with_toast(
        tuple_bytes: &[u8],
        schema: &[(String, DataType)],
        toast_manager: &ToastManager,
    ) -> Result<Vec<Value>, String> {
        Self::decode_tuple_internal(tuple_bytes, schema, Some(toast_manager))
    }

    fn decode_tuple_internal(
        tuple_bytes: &[u8],
        schema: &[(String, DataType)],
        toast_manager: Option<&ToastManager>,
    ) -> Result<Vec<Value>, String> {
        if tuple_bytes.len() < TupleHeader::size() {
            return Err("Tuple too short for header".to_string());
        }

        // Parse header
        let header = TupleHeader::from_bytes(&tuple_bytes[0..TupleHeader::size()])?;

        let mut cursor = TupleHeader::size();

        // Parse null bitmap
        let null_bitmap_bytes = header.null_bitmap_bytes as usize;
        if cursor + null_bitmap_bytes > tuple_bytes.len() {
            return Err("Tuple too short for null bitmap".to_string());
        }
        let null_bitmap = &tuple_bytes[cursor..cursor + null_bitmap_bytes];
        cursor += null_bitmap_bytes;

        // Parse variable-field directory
        let var_dir_size = (header.var_field_count as usize) * VarFieldEntry::size();
        if cursor + var_dir_size > tuple_bytes.len() {
            return Err("Tuple too short for var field directory".to_string());
        }

        let mut var_entries = Vec::new();
        for i in 0..header.var_field_count as usize {
            let entry_bytes = &tuple_bytes[cursor + i * VarFieldEntry::size()
                ..cursor + (i + 1) * VarFieldEntry::size()];
            var_entries.push(VarFieldEntry::from_bytes(entry_bytes)?);
        }
        cursor += var_dir_size;

        // Find fixed region size
        let mut fixed_size = 0usize;
        for (_, data_type) in schema {
            if !data_type.is_variable_length() {
                if let Some(size) = data_type.fixed_size() {
                    fixed_size += size;
                }
            }
        }

        // Parse fixed region
        if cursor + fixed_size > tuple_bytes.len() {
            return Err("Tuple too short for fixed region".to_string());
        }
        let fixed_region = &tuple_bytes[cursor..cursor + fixed_size];
        cursor += fixed_size;

        // Variable payload starts after fixed region
        let var_payload = &tuple_bytes[cursor..];

        // Reconstruct values
        let mut values = Vec::new();
        let mut fixed_offset = 0usize;
        let mut var_entry_idx = 0;

        for (_, data_type) in schema {
            let is_null = {
                let col_idx = values.len();
                let byte_idx = col_idx / 8;
                let bit_idx = col_idx % 8;
                if byte_idx < null_bitmap.len() {
                    (null_bitmap[byte_idx] & (1u8 << bit_idx)) != 0
                } else {
                    false
                }
            };

            if is_null {
                values.push(Value::Null);
                if data_type.is_variable_length() {
                    var_entry_idx += 1;
                } else if let Some(size) = data_type.fixed_size() {
                    fixed_offset += size;
                }
            } else if data_type.is_variable_length() {
                let entry = &var_entries[var_entry_idx];
                let field_bytes = &var_payload[entry.offset as usize
                    ..entry.offset as usize + entry.length as usize];

                let value = Self::decode_variable_field(
                    field_bytes,
                    data_type,
                    entry.is_toast(),
                    toast_manager,
                )?;

                values.push(value);
                var_entry_idx += 1;
            } else {
                if let Some(size) = data_type.fixed_size() {
                    let field_bytes = &fixed_region[fixed_offset..fixed_offset + size];
                    let value = ValueCodec::decode(field_bytes, data_type)?;
                    values.push(value);
                    fixed_offset += size;
                }
            }
        }

        Ok(values)
    }

    fn decode_variable_field(
        field_bytes: &[u8],
        data_type: &DataType,
        is_toast: bool,
        toast_manager: Option<&ToastManager>,
    ) -> Result<Value, String> {
        if is_toast {
            let ptr = ToastPointer::from_bytes(field_bytes)?;
            let toast_manager = toast_manager
                .ok_or_else(|| "TOAST-backed tuple decode requires a ToastManager".to_string())?;
            let payload = toast_manager.fetch_large_value(&ptr)?;
            ValueCodec::decode(&payload, data_type)
        } else {
            ValueCodec::decode(field_bytes, data_type)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_simple_tuple() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::Text),
        ];
        let values = vec![Value::Int32(1), Value::Text("Alice".to_string())];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded.len(), values.len());
    }

    #[test]
    fn test_encode_decode_with_null() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("comment".to_string(), DataType::Text),
        ];
        let values = vec![Value::Int32(42), Value::Null];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded[0], Value::Int32(42));
        assert_eq!(decoded[1], Value::Null);
    }

    #[test]
    fn test_encode_decode_with_array() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            (
                "scores".to_string(),
                DataType::Array {
                    element_type: Box::new(DataType::Int32),
                },
            ),
        ];
        let values = vec![
            Value::Int32(1),
            Value::Array(vec![Value::Int32(100), Value::Int32(200)]),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded[0], Value::Int32(1));
    }

    #[test]
    fn test_encode_decode_with_nested_array() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            (
                "matrix".to_string(),
                DataType::Array {
                    element_type: Box::new(DataType::Array {
                        element_type: Box::new(DataType::Int32),
                    }),
                },
            ),
        ];
        let values = vec![
            Value::Int32(1),
            Value::Array(vec![
                Value::Array(vec![Value::Int32(10), Value::Int32(20)]),
                Value::Array(vec![]),
                Value::Array(vec![Value::Int32(30)]),
            ]),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_decode_with_blob() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("data".to_string(), DataType::Blob),
        ];
        let values = vec![Value::Int32(1), Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded[0], Value::Int32(1));
    }

    #[test]
    fn test_decode_tuple_with_toast_large_blob() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("data".to_string(), DataType::Blob),
        ];
        let values = vec![Value::Int32(1), Value::Blob(vec![0xAB; TOAST_THRESHOLD + 1000])];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded =
            TupleCodec::decode_tuple_with_toast(&encoded, &schema, &toast_manager).unwrap();

        assert_eq!(decoded, values);
    }
}
