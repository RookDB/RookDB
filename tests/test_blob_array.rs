//! Comprehensive tests for BLOB and ARRAY support in RookDB

#[cfg(test)]
mod tests {
    use storage_manager::backend::catalog::data_type::{DataType, Value};
    use storage_manager::backend::storage::row_layout::{ToastChunk, ToastPointer, TupleHeader, VarFieldEntry};
    use storage_manager::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};
    use storage_manager::backend::storage::tuple_codec::TupleCodec;
    use storage_manager::backend::storage::value_codec::ValueCodec;

    // ============= Data Type Tests =============

    #[test]
    fn test_datatype_parse_primitives() {
        assert_eq!(DataType::parse("INT").unwrap(), DataType::Int32);
        assert_eq!(DataType::parse("BOOLEAN").unwrap(), DataType::Boolean);
        assert_eq!(DataType::parse("TEXT").unwrap(), DataType::Text);
        assert_eq!(DataType::parse("BLOB").unwrap(), DataType::Blob);
    }

    #[test]
    fn test_datatype_parse_arrays() {
        let int_array = DataType::parse("ARRAY<INT>").unwrap();
        assert!(matches!(int_array, DataType::Array { .. }));

        let text_array = DataType::parse("ARRAY<TEXT>").unwrap();
        assert!(matches!(text_array, DataType::Array { .. }));

        let bool_array = DataType::parse("ARRAY<BOOLEAN>").unwrap();
        assert!(matches!(bool_array, DataType::Array { .. }));
    }

    #[test]
    fn test_datatype_is_variable_length() {
        assert!(!DataType::Int32.is_variable_length());
        assert!(!DataType::Boolean.is_variable_length());
        assert!(DataType::Text.is_variable_length());
        assert!(DataType::Blob.is_variable_length());
        assert!(DataType::Array {
            element_type: Box::new(DataType::Int32)
        }
        .is_variable_length());
    }

    #[test]
    fn test_value_null() {
        let null_val = Value::Null;
        assert!(null_val.is_null());
    }

    // ============= Value Codec Tests =============

    #[test]
    fn test_encode_decode_int32() {
        let value = Value::Int32(42);
        let encoded = ValueCodec::encode(&value, &DataType::Int32).unwrap();
        let decoded = ValueCodec::decode(&encoded, &DataType::Int32).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_boolean() {
        let value_true = Value::Boolean(true);
        let encoded_true = ValueCodec::encode(&value_true, &DataType::Boolean).unwrap();
        let decoded_true = ValueCodec::decode(&encoded_true, &DataType::Boolean).unwrap();
        assert_eq!(value_true, decoded_true);

        let value_false = Value::Boolean(false);
        let encoded_false = ValueCodec::encode(&value_false, &DataType::Boolean).unwrap();
        let decoded_false = ValueCodec::decode(&encoded_false, &DataType::Boolean).unwrap();
        assert_eq!(value_false, decoded_false);
    }

    #[test]
    fn test_encode_decode_text() {
        let texts = vec![
            "",
            "Hello",
            "Hello, World!",
            "A very long text with special chars: @#$%^&*()",
        ];

        for text in texts {
            let value = Value::Text(text.to_string());
            let encoded = ValueCodec::encode(&value, &DataType::Text).unwrap();
            let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_encode_decode_blob() {
        let blobs = vec![
            vec![],
            vec![0xDE, 0xAD, 0xBE, 0xEF],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            (0..255).collect::<Vec<u8>>(),
        ];

        for blob in blobs {
            let value = Value::Blob(blob.clone());
            let encoded = ValueCodec::encode(&value, &DataType::Blob).unwrap();
            let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_encode_decode_int_array() {
        let value = Value::Array(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
            Value::Int32(100),
        ]);
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
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("cherry".to_string()),
        ]);
        let ty = DataType::Array {
            element_type: Box::new(DataType::Text),
        };
        let encoded = ValueCodec::encode(&value, &ty).unwrap();
        let decoded = ValueCodec::decode(&encoded, &ty).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_nested_int_array() {
        let value = Value::Array(vec![
            Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
            Value::Array(vec![]),
            Value::Array(vec![Value::Int32(3), Value::Int32(4), Value::Int32(5)]),
        ]);
        let ty = DataType::Array {
            element_type: Box::new(DataType::Array {
                element_type: Box::new(DataType::Int32),
            }),
        };
        let encoded = ValueCodec::encode(&value, &ty).unwrap();
        let decoded = ValueCodec::decode(&encoded, &ty).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_empty_array() {
        let value = Value::Array(vec![]);
        let ty = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };
        let encoded = ValueCodec::encode(&value, &ty).unwrap();
        let decoded = ValueCodec::decode(&encoded, &ty).unwrap();
        assert_eq!(value, decoded);
    }

    // ============= Row Layout Tests =============

    #[test]
    fn test_tuple_header_roundtrip() {
        let header = TupleHeader::new(10, 2, 3);
        let bytes = header.to_bytes();
        let restored = TupleHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.column_count, restored.column_count);
        assert_eq!(header.null_bitmap_bytes, restored.null_bitmap_bytes);
        assert_eq!(header.var_field_count, restored.var_field_count);
    }

    #[test]
    fn test_var_field_entry_roundtrip() {
        let entry = VarFieldEntry::new(1000, 500, false);
        let bytes = entry.to_bytes();
        let restored = VarFieldEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry.offset, restored.offset);
        assert_eq!(entry.length, restored.length);
        assert_eq!(entry.is_toast(), restored.is_toast());
    }

    #[test]
    fn test_toast_pointer_roundtrip() {
        let ptr = ToastPointer::new(12345, 50000, 13);
        let bytes = ptr.to_bytes();
        let restored = ToastPointer::from_bytes(&bytes).unwrap();

        assert_eq!(ptr.value_id, restored.value_id);
        assert_eq!(ptr.total_bytes, restored.total_bytes);
        assert_eq!(ptr.chunk_count, restored.chunk_count);
    }

    #[test]
    fn test_toast_chunk_roundtrip() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let chunk = ToastChunk::new(12345, 0, data.clone());
        let bytes = chunk.to_bytes();
        let restored = ToastChunk::from_bytes(&bytes).unwrap();

        assert_eq!(chunk.value_id, restored.value_id);
        assert_eq!(chunk.chunk_no, restored.chunk_no);
        assert_eq!(chunk.data, restored.data);
    }

    // ============= TOAST Manager Tests =============

    #[test]
    fn test_toast_manager_creation() {
        let manager = ToastManager::new();
        assert_eq!(manager.next_value_id, 1);
        assert_eq!(manager.toast_page_count, 0);
    }

    #[test]
    fn test_store_large_value() {
        let mut manager = ToastManager::new();
        let payload = vec![42u8; 10000];
        let ptr = manager.store_large_value(&payload).unwrap();

        assert_eq!(ptr.value_id, 1);
        assert_eq!(ptr.total_bytes, 10000);
        assert!(ptr.chunk_count > 0);
        assert_eq!(manager.next_value_id, 2);
    }

    #[test]
    fn test_should_use_toast() {
        assert!(!ToastManager::should_use_toast(1000));
        assert!(!ToastManager::should_use_toast(TOAST_THRESHOLD - 100));
        assert!(ToastManager::should_use_toast(TOAST_THRESHOLD + 1));
        assert!(ToastManager::should_use_toast(100000));
    }

    // ============= Tuple Codec Tests =============

    #[test]
    fn test_encode_decode_simple_int_tuple() {
        let schema = vec![("id".to_string(), DataType::Int32)];
        let values = vec![Value::Int32(42)];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], Value::Int32(42));
    }

    #[test]
    fn test_encode_decode_mixed_tuple() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("active".to_string(), DataType::Boolean),
            ("name".to_string(), DataType::Text),
        ];
        let values = vec![
            Value::Int32(1),
            Value::Boolean(true),
            Value::Text("Alice".to_string()),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], Value::Int32(1));
        assert_eq!(decoded[1], Value::Boolean(true));
        // Note: Exact match might fail due to encoding/decoding roundtrip, check contains instead
        match &decoded[2] {
            Value::Text(s) => assert_eq!(s, "Alice"),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_encode_decode_tuple_with_null() {
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
    fn test_encode_decode_tuple_with_blob() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("data".to_string(), DataType::Blob),
        ];
        let blob_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let values = vec![Value::Int32(1), Value::Blob(blob_data.clone())];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded[0], Value::Int32(1));
        match &decoded[1] {
            Value::Blob(b) => assert_eq!(b, &blob_data),
            _ => panic!("Expected Blob value"),
        }
    }

    #[test]
    fn test_encode_decode_tuple_with_array() {
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
            Value::Array(vec![Value::Int32(100), Value::Int32(200), Value::Int32(300)]),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded[0], Value::Int32(1));
        assert_eq!(decoded[1], values[1]);
    }

    #[test]
    fn test_encode_decode_complex_tuple() {
        let schema = vec![
            ("user_id".to_string(), DataType::Int32),
            ("username".to_string(), DataType::Text),
            ("is_active".to_string(), DataType::Boolean),
            ("profile_pic".to_string(), DataType::Blob),
            (
                "tags".to_string(),
                DataType::Array {
                    element_type: Box::new(DataType::Text),
                },
            ),
        ];

        let values = vec![
            Value::Int32(1),
            Value::Text("alice_wonder".to_string()),
            Value::Boolean(true),
            Value::Blob(vec![0xFF, 0xD8, 0xFF, 0xE0]),
            Value::Array(vec![
                Value::Text("admin".to_string()),
                Value::Text("moderator".to_string()),
            ]),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();

        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded[0], values[0]);
        assert_eq!(decoded[1], values[1]);
        assert_eq!(decoded[2], values[2]);
        assert_eq!(decoded[3], values[3]);
        assert_eq!(decoded[4], values[4]);
    }

    #[test]
    fn test_encode_decode_large_blob() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("file_data".to_string(), DataType::Blob),
        ];

        let large_blob = vec![42u8; 20000]; // Larger than TOAST threshold
        let values = vec![Value::Int32(1), Value::Blob(large_blob.clone())];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple_with_toast(&encoded, &schema, &toast_manager).unwrap();

        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_decode_10kb_blob_with_toast() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("file_data".to_string(), DataType::Blob),
        ];

        let blob_10kb = vec![0x7Fu8; 10_240];
        let values = vec![Value::Int32(7), Value::Blob(blob_10kb)];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple_with_toast(&encoded, &schema, &toast_manager).unwrap();

        assert_eq!(decoded, values);
    }
}
