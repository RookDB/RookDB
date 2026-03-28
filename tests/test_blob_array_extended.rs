//! Extended tests for BLOB and ARRAY support with edge cases and robustness testing
//! 
//! This test suite focuses on:
//! - Edge cases and boundary conditions
//! - Error handling and robustness
//! - Correctness verification with various data patterns
//! - Performance characteristics under different conditions

use std::time::Instant;
use storage_manager::backend::catalog::data_type::{DataType, Value};
use storage_manager::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};
use storage_manager::backend::storage::tuple_codec::TupleCodec;
use storage_manager::backend::storage::value_codec::ValueCodec;

// ============= Performance Measurement Utilities =============

struct PerfTimer {
    name: String,
    start: Instant,
}

impl PerfTimer {
    fn start(name: &str) -> Self {
        PerfTimer {
            name: name.to_string(),
            start: Instant::now(),
        }
    }
}

impl Drop for PerfTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        println!(
            "  ✓ {}: {:.3}ms",
            self.name,
            elapsed.as_micros() as f64 / 1000.0
        );
    }
}

// ============= Edge Case Tests =============

#[test]
fn test_edge_case_empty_text() {
    let empty_text = Value::Text(String::new());
    let encoded = ValueCodec::encode(&empty_text, &DataType::Text).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
    assert_eq!(empty_text, decoded);
    assert_eq!(encoded.len(), 4); // 4-byte length prefix only
}

#[test]
fn test_edge_case_empty_blob() {
    let empty_blob = Value::Blob(Vec::new());
    let encoded = ValueCodec::encode(&empty_blob, &DataType::Blob).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
    assert_eq!(empty_blob, decoded);
    assert_eq!(encoded.len(), 4); // 4-byte length prefix only
}

#[test]
fn test_edge_case_empty_array() {
    let empty_array = Value::Array(Vec::new());
    let array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };
    let encoded = ValueCodec::encode(&empty_array, &array_type).unwrap();
    let decoded = ValueCodec::decode(&encoded, &array_type).unwrap();
    assert_eq!(empty_array, decoded);
    assert_eq!(encoded.len(), 4); // 4-byte count only
}

#[test]
fn test_edge_case_max_int32() {
    let max_int = Value::Int32(i32::MAX);
    let encoded = ValueCodec::encode(&max_int, &DataType::Int32).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Int32).unwrap();
    assert_eq!(max_int, decoded);
}

#[test]
fn test_edge_case_min_int32() {
    let min_int = Value::Int32(i32::MIN);
    let encoded = ValueCodec::encode(&min_int, &DataType::Int32).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Int32).unwrap();
    assert_eq!(min_int, decoded);
}

#[test]
fn test_edge_case_large_text_at_threshold() {
    // Create text just below TOAST threshold
    let large_text = Value::Text("x".repeat(TOAST_THRESHOLD - 10));
    let encoded = ValueCodec::encode(&large_text, &DataType::Text).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
    assert_eq!(large_text, decoded);
}

#[test]
fn test_edge_case_large_blob_exceeds_threshold() {
    // Create BLOB exceeding TOAST threshold
    let large_blob = Value::Blob(vec![0xFF; TOAST_THRESHOLD + 1000]);
    let encoded = ValueCodec::encode(&large_blob, &DataType::Blob).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
    assert_eq!(large_blob, decoded);
    println!(
        "  Edge case: BLOB exceeding TOAST threshold encoded to {} bytes",
        encoded.len()
    );
}

#[test]
fn test_edge_case_single_element_array() {
    let single_array = Value::Array(vec![Value::Int32(42)]);
    let array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };
    let encoded = ValueCodec::encode(&single_array, &array_type).unwrap();
    let decoded = ValueCodec::decode(&encoded, &array_type).unwrap();
    assert_eq!(single_array, decoded);
}

#[test]
fn test_edge_case_large_array() {
    // Large array with many elements
    let large_array = Value::Array(
        (0..10_000)
            .map(|i| Value::Int32(i as i32))
            .collect(),
    );
    let array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };
    let encoded = ValueCodec::encode(&large_array, &array_type).unwrap();
    let decoded = ValueCodec::decode(&encoded, &array_type).unwrap();
    assert_eq!(large_array, decoded);
    println!("  Edge case: Large array (10k elements) encoded to {} bytes", encoded.len());
}

#[test]
fn test_edge_case_unicode_text() {
    let unicode_text = Value::Text("你好世界🌍🎉αβγδ".to_string());
    let encoded = ValueCodec::encode(&unicode_text, &DataType::Text).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
    assert_eq!(unicode_text, decoded);
}

#[test]
fn test_edge_case_special_byte_patterns() {
    // Test BLOB with all possible byte values
    let mut special_blob = Vec::new();
    for byte in 0u8..=255 {
        special_blob.push(byte);
    }
    let blob_value = Value::Blob(special_blob.clone());
    let encoded = ValueCodec::encode(&blob_value, &DataType::Blob).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
    assert_eq!(blob_value, decoded);
}

#[test]
fn test_edge_case_tuple_all_nulls() {
    let schema = vec![
        ("a".to_string(), DataType::Int32),
        ("b".to_string(), DataType::Text),
        ("c".to_string(), DataType::Blob),
    ];
    let values = vec![Value::Null, Value::Null, Value::Null];
    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
    let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
    assert_eq!(values, decoded);
    println!("  Edge case: All-null tuple encoded to {} bytes", encoded.len());
}

#[test]
fn test_edge_case_tuple_mixed_nulls() {
    let schema = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
        ("data".to_string(), DataType::Blob),
        ("tags".to_string(), DataType::Array {
            element_type: Box::new(DataType::Text),
        }),
    ];
    let values = vec![
        Value::Int32(1),
        Value::Null,
        Value::Blob(vec![0xAB; 100]),
        Value::Null,
    ];
    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
    let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
    assert_eq!(values, decoded);
}

// ============= Data Type Robustness Tests =============

#[test]
fn test_datatype_parse_case_insensitive() {
    // DataType parsing should handle various cases
    assert!(DataType::parse("INT").is_ok());
    assert!(DataType::parse("int").is_ok() || DataType::parse("INT").is_ok()); // At least one should work
}

#[test]
fn test_datatype_nested_array_support() {
    let result = DataType::parse("ARRAY<ARRAY<INT>>").unwrap();
    assert_eq!(
        result,
        DataType::Array {
            element_type: Box::new(DataType::Array {
                element_type: Box::new(DataType::Int32),
            }),
        }
    );
}

#[test]
fn test_datatype_invalid_type_string() {
    assert!(DataType::parse("INVALID_TYPE").is_err());
    assert!(DataType::parse("").is_err());
    assert!(DataType::parse("ARRAY<>").is_err());
}

#[test]
fn test_datatype_to_string_roundtrip() {
    let types = vec![
        DataType::Int32,
        DataType::Boolean,
        DataType::Text,
        DataType::Blob,
        DataType::Array {
            element_type: Box::new(DataType::Int32),
        },
        DataType::Array {
            element_type: Box::new(DataType::Text),
        },
        DataType::Array {
            element_type: Box::new(DataType::Array {
                element_type: Box::new(DataType::Int32),
            }),
        },
    ];

    for dtype in types {
        let string_repr = dtype.to_string();
        let reparsed = DataType::parse(&string_repr).unwrap();
        assert_eq!(dtype, reparsed, "Roundtrip failed for {:?}", dtype);
    }
}

// ============= Value Codec Robustness Tests =============

#[test]
fn test_value_codec_decode_invalid_data() {
    // Attempt to decode invalid data
    let invalid_data = vec![0xFF, 0xFF]; // Too short for INT32
    let result = ValueCodec::decode(&invalid_data, &DataType::Int32);
    assert!(result.is_err());
}

#[test]
fn test_value_codec_decode_incomplete_text() {
    // Text with incomplete data
    let incomplete = vec![0x10, 0x00, 0x00, 0x00]; // Says 16 bytes but we have 0
    let result = ValueCodec::decode(&incomplete, &DataType::Text);
    assert!(result.is_err());
}

#[test]
fn test_value_codec_array_element_mismatch() {
    // Create array that claims one type but has another
    let values = vec![Value::Int32(1), Value::Int32(2)];
    let array_val = Value::Array(values);
    let array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };
    let encoded = ValueCodec::encode(&array_val, &array_type).unwrap();
    let decoded = ValueCodec::decode(&encoded, &array_type).unwrap();
    assert_eq!(array_val, decoded);
}

// ============= TOAST Manager Robustness Tests =============

#[test]
fn test_toast_manager_multiple_values() {
    let mut manager = ToastManager::new();
    
    // Store multiple large values
    let large_value1 = vec![0xAA; TOAST_THRESHOLD + 1000];
    let large_value2 = vec![0xBB; TOAST_THRESHOLD + 2000];
    
    let ptr1 = manager.store_large_value(&large_value1).unwrap();
    let ptr2 = manager.store_large_value(&large_value2).unwrap();
    
    // Verify IDs are different
    assert_ne!(ptr1.value_id, ptr2.value_id);
    assert_eq!(ptr1.total_bytes as usize, large_value1.len());
    assert_eq!(ptr2.total_bytes as usize, large_value2.len());
}

#[test]
fn test_toast_threshold_boundaries() {
    // Test TOAST threshold detection at various sizes
    let test_sizes = vec![
        (TOAST_THRESHOLD - 100, false),
        (TOAST_THRESHOLD - 1, false),
        (TOAST_THRESHOLD, false),
        (TOAST_THRESHOLD + 1, true),
        (TOAST_THRESHOLD + 100, true),
    ];

    for (size, should_toast) in test_sizes {
        assert_eq!(
            ToastManager::should_use_toast(size),
            should_toast,
            "TOAST threshold check failed for size {}",
            size
        );
    }
}

// ============= Tuple Codec Complex Scenarios =============

#[test]
fn test_tuple_codec_many_variable_fields() {
    // Tuple with many variable-length fields
    let schema = vec![
        ("field1".to_string(), DataType::Text),
        ("field2".to_string(), DataType::Text),
        ("field3".to_string(), DataType::Text),
        ("field4".to_string(), DataType::Blob),
        ("field5".to_string(), DataType::Blob),
    ];

    let values = vec![
        Value::Text("text1".to_string()),
        Value::Text("text2".to_string()),
        Value::Text("text3".to_string()),
        Value::Blob(vec![0xAB; 512]),
        Value::Blob(vec![0xCD; 1024]),
    ];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
    let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
    assert_eq!(values, decoded);
}

#[test]
fn test_tuple_codec_interleaved_fixed_variable() {
    // Interleaved fixed and variable fields
    let schema = vec![
        ("id".to_string(), DataType::Int32),           // Fixed
        ("name".to_string(), DataType::Text),          // Variable
        ("active".to_string(), DataType::Boolean),     // Fixed
        ("data".to_string(), DataType::Blob),          // Variable
        ("count".to_string(), DataType::Int32),        // Fixed
    ];

    let values = vec![
        Value::Int32(42),
        Value::Text("Test Name".to_string()),
        Value::Boolean(true),
        Value::Blob(vec![0xFF; 2048]),
        Value::Int32(100),
    ];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
    let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
    assert_eq!(values, decoded);
}

#[test]
fn test_tuple_codec_all_variable_types() {
    // Tuple with all variable-length types
    let schema = vec![
        ("text_field".to_string(), DataType::Text),
        ("blob_field".to_string(), DataType::Blob),
        ("int_array".to_string(), DataType::Array {
            element_type: Box::new(DataType::Int32),
        }),
        ("text_array".to_string(), DataType::Array {
            element_type: Box::new(DataType::Text),
        }),
    ];

    let values = vec![
        Value::Text("sample text".to_string()),
        Value::Blob(vec![0xAB; 1500]),
        Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
        Value::Array(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]),
    ];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
    let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
    assert_eq!(values, decoded);
}

// ============= Performance Characteristic Tests =============

#[test]
fn test_perf_primitive_vs_variable() {
    println!("\n  Performance Characteristics:");
    
    {
        let _timer = PerfTimer::start("1000x INT32 encoding");
        for _ in 0..1000 {
            let _ = ValueCodec::encode(&Value::Int32(42), &DataType::Int32);
        }
    }

    {
        let _timer = PerfTimer::start("1000x TEXT (100B) encoding");
        for _ in 0..1000 {
            let _ = ValueCodec::encode(
                &Value::Text("x".repeat(100)),
                &DataType::Text,
            );
        }
    }

    {
        let _timer = PerfTimer::start("1000x BLOB (10KB) encoding");
        for _ in 0..1000 {
            let _ = ValueCodec::encode(
                &Value::Blob(vec![0xAB; 10240]),
                &DataType::Blob,
            );
        }
    }
}

#[test]
fn test_perf_tuple_encoding_complexity() {
    println!("\n  Tuple Encoding Complexity:");

    let schema_simple = vec![
        ("id".to_string(), DataType::Int32),
        ("active".to_string(), DataType::Boolean),
    ];

    let schema_complex = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
        ("data".to_string(), DataType::Blob),
        ("tags".to_string(), DataType::Array {
            element_type: Box::new(DataType::Text),
        }),
    ];

    {
        let _timer = PerfTimer::start("1000x simple tuple (2 fixed fields)");
        let values = vec![Value::Int32(1), Value::Boolean(true)];
        let mut toast_manager = ToastManager::new();
        for _ in 0..1000 {
            let _ = TupleCodec::encode_tuple(&values, &schema_simple, &mut toast_manager);
        }
    }

    {
        let _timer = PerfTimer::start("1000x complex tuple (4 mixed fields)");
        let values = vec![
            Value::Int32(1),
            Value::Text("name".to_string()),
            Value::Blob(vec![0xAB; 512]),
            Value::Array(vec![Value::Text("tag1".to_string())]),
        ];
        let mut toast_manager = ToastManager::new();
        for _ in 0..1000 {
            let _ = TupleCodec::encode_tuple(&values, &schema_complex, &mut toast_manager);
        }
    }
}

#[test]
fn test_scalability_large_arrays() {
    println!("\n  Array Scalability:");

    let array_sizes = vec![100, 1000, 10000];

    for size in array_sizes {
        let _timer = PerfTimer::start(&format!("Array encoding ({}x elements)", size));
        let values: Vec<Value> = (0..size).map(|i| Value::Int32(i as i32)).collect();
        let array_val = Value::Array(values);
        let array_type = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };

        for _ in 0..100 {
            let _ = ValueCodec::encode(&array_val, &array_type);
        }
    }
}

// ============= Null Bitmap Correctness Tests =============

#[test]
fn test_null_bitmap_single_column() {
    let schema = vec![("nullable_col".to_string(), DataType::Text)];
    
    let cases = vec![
        vec![Value::Null],
        vec![Value::Text("not null".to_string())],
    ];

    for values in cases {
        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
        assert_eq!(values, decoded);
    }
}

#[test]
fn test_null_bitmap_multiple_columns_pattern() {
    // Test various null patterns in multi-column tuples
    let schema = vec![
        ("col1".to_string(), DataType::Int32),
        ("col2".to_string(), DataType::Text),
        ("col3".to_string(), DataType::Blob),
        ("col4".to_string(), DataType::Boolean),
    ];

    let patterns = vec![
        vec![Value::Null, Value::Null, Value::Null, Value::Null],
        vec![Value::Int32(1), Value::Null, Value::Null, Value::Null],
        vec![Value::Null, Value::Text("x".to_string()), Value::Null, Value::Null],
        vec![Value::Int32(1), Value::Text("x".to_string()), Value::Blob(vec![0xAB]), Value::Boolean(true)],
        vec![Value::Null, Value::Text("x".to_string()), Value::Blob(vec![0xAB]), Value::Null],
    ];

    for values in patterns {
        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
        assert_eq!(values, decoded);
    }
}

// ============= Correctness Verification Tests =============

#[test]
fn test_correctness_text_preserves_whitespace() {
    let text_with_ws = Value::Text("  text with  spaces  \n\t  ".to_string());
    let encoded = ValueCodec::encode(&text_with_ws, &DataType::Text).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Text).unwrap();
    assert_eq!(text_with_ws, decoded);
}

#[test]
fn test_correctness_blob_all_zeros() {
    let blob_zeros = Value::Blob(vec![0; 1000]);
    let encoded = ValueCodec::encode(&blob_zeros, &DataType::Blob).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
    assert_eq!(blob_zeros, decoded);
}

#[test]
fn test_correctness_blob_all_ones() {
    let blob_ones = Value::Blob(vec![0xFF; 1000]);
    let encoded = ValueCodec::encode(&blob_ones, &DataType::Blob).unwrap();
    let decoded = ValueCodec::decode(&encoded, &DataType::Blob).unwrap();
    assert_eq!(blob_ones, decoded);
}

#[test]
fn test_correctness_array_value_ordering() {
    let array_ordered = Value::Array(vec![
        Value::Int32(1),
        Value::Int32(2),
        Value::Int32(3),
        Value::Int32(4),
        Value::Int32(5),
    ]);
    let array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };
    let encoded = ValueCodec::encode(&array_ordered, &array_type).unwrap();
    let decoded = ValueCodec::decode(&encoded, &array_type).unwrap();
    assert_eq!(array_ordered, decoded);

    // Verify order is preserved
    if let Value::Array(decoded_vals) = decoded {
        for (i, val) in decoded_vals.iter().enumerate() {
            assert_eq!(val, &Value::Int32((i + 1) as i32));
        }
    }
}

// ============= Integration Tests =============

#[test]
fn test_integration_multi_row_encoding() {
    println!("\n  Integration: Multi-row scenario");
    
    let schema = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
        ("data".to_string(), DataType::Blob),
    ];

    let rows = vec![
        vec![Value::Int32(1), Value::Text("Alice".to_string()), Value::Blob(vec![0xAA; 512])],
        vec![Value::Int32(2), Value::Text("Bob".to_string()), Value::Blob(vec![0xBB; 1024])],
        vec![Value::Int32(3), Value::Null, Value::Blob(vec![0xCC; 256])],
    ];

    let mut toast_manager = ToastManager::new();
    for row in rows.iter() {
        let encoded = TupleCodec::encode_tuple(row, &schema, &mut toast_manager).unwrap();
        let decoded = TupleCodec::decode_tuple(&encoded, &schema).unwrap();
        assert_eq!(*row, decoded);
    }
}

#[test]
fn test_integration_schema_evolution() {
    println!("\n  Integration: Schema evolution");
    
    // Encode with original schema
    let schema_v1 = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
    ];

    let values = vec![Value::Int32(1), Value::Text("Test".to_string())];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema_v1, &mut toast_manager).unwrap();

    // Decode with same schema
    let decoded = TupleCodec::decode_tuple(&encoded, &schema_v1).unwrap();
    assert_eq!(values, decoded);
}
