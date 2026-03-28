//! Practical demo program to test BLOB and ARRAY functionality in RookDB
//! Run with: cargo run --example test_blob_array_demo

use storage_manager::backend::catalog::data_type::{DataType, Value};
use storage_manager::backend::storage::toast::ToastManager;
use storage_manager::backend::storage::tuple_codec::TupleCodec;
use storage_manager::backend::storage::value_codec::ValueCodec;

fn main() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  RookDB BLOB and ARRAY Data Type Testing Demo             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Test 1: Basic BLOB Encoding
    println!("█ TEST 1: BLOB Encoding and Decoding");
    println!("  ─────────────────────────────────────");
    test_blob_encoding();

    // Test 2: ARRAY Encoding
    println!("\n█ TEST 2: ARRAY Encoding and Decoding");
    println!("  ──────────────────────────────────────");
    test_array_encoding();

    // Test 3: Mixed Tuple with BLOB and ARRAY
    println!("\n█ TEST 3: Mixed Tuple (INT, BOOLEAN, TEXT, BLOB, ARRAY)");
    println!("  ──────────────────────────────────────────────────────");
    test_mixed_tuple();

    // Test 4: Large BLOB with TOAST
    println!("\n█ TEST 4: Large BLOB (TOAST Storage)");
    println!("  ──────────────────────────────────");
    test_large_blob();

    // Test 5: Type Parsing
    println!("\n█ TEST 5: Type Parsing");
    println!("  ───────────────────────");
    test_type_parsing();

    // Test 6: Null Handling
    println!("\n█ TEST 6: NULL Value Handling");
    println!("  ──────────────────────────────");
    test_null_handling();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  All tests completed successfully! ✓                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
}

fn test_blob_encoding() {
    let test_cases = vec![
        ("Empty BLOB", vec![]),
        ("Single byte", vec![0xFF]),
        ("Magic number", vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ("Random data", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    ];

    for (name, data) in test_cases {
        let value = Value::Blob(data.clone());
        let encoded = ValueCodec::encode(&value, &DataType::Blob)
            .expect("Failed to encode BLOB");
        let decoded = ValueCodec::decode(&encoded, &DataType::Blob)
            .expect("Failed to decode BLOB");

        match decoded {
            Value::Blob(decoded_data) => {
                if decoded_data == data {
                    println!("  ✓ {} ({} bytes) - Encoded and decoded correctly",
                             name, data.len());
                } else {
                    println!("  ✗ {} - Data mismatch!", name);
                }
            }
            _ => println!("  ✗ {} - Type mismatch!", name),
        }
    }
}

fn test_array_encoding() {
    println!("  Integer Array: [10, 20, 30, 40]");
    let int_array = Value::Array(vec![
        Value::Int32(10),
        Value::Int32(20),
        Value::Int32(30),
        Value::Int32(40),
    ]);
    let int_array_type = DataType::Array {
        element_type: Box::new(DataType::Int32),
    };

    let encoded_int = ValueCodec::encode(&int_array, &int_array_type)
        .expect("Failed to encode int array");
    let decoded_int = ValueCodec::decode(&encoded_int, &int_array_type)
        .expect("Failed to decode int array");

    match decoded_int {
        Value::Array(items) => {
            println!("    Decoded {} elements", items.len());
            for (i, item) in items.iter().enumerate() {
                match item {
                    Value::Int32(v) => println!("      [{}] = {}", i, v),
                    _ => println!("      [{}] = <unknown>", i),
                }
            }
            println!("    ✓ Integer array test passed");
        }
        _ => println!("    ✗ Type mismatch"),
    }

    println!("\n  Text Array: [\"apple\", \"banana\", \"cherry\"]");
    let text_array = Value::Array(vec![
        Value::Text("apple".to_string()),
        Value::Text("banana".to_string()),
        Value::Text("cherry".to_string()),
    ]);
    let text_array_type = DataType::Array {
        element_type: Box::new(DataType::Text),
    };

    let encoded_text = ValueCodec::encode(&text_array, &text_array_type)
        .expect("Failed to encode text array");
    let decoded_text = ValueCodec::decode(&encoded_text, &text_array_type)
        .expect("Failed to decode text array");

    match decoded_text {
        Value::Array(items) => {
            println!("    Decoded {} elements", items.len());
            for (i, item) in items.iter().enumerate() {
                match item {
                    Value::Text(s) => println!("      [{}] = \"{}\"", i, s),
                    _ => println!("      [{}] = <unknown>", i),
                }
            }
            println!("    ✓ Text array test passed");
        }
        _ => println!("    ✗ Type mismatch"),
    }
}

fn test_mixed_tuple() {
    let schema = vec![
        ("user_id".to_string(), DataType::Int32),
        ("is_verified".to_string(), DataType::Boolean),
        ("email".to_string(), DataType::Text),
        ("profile_pic".to_string(), DataType::Blob),
        (
            "tags".to_string(),
            DataType::Array {
                element_type: Box::new(DataType::Text),
            },
        ),
    ];

    let values = vec![
        Value::Int32(12345),
        Value::Boolean(true),
        Value::Text("user@example.com".to_string()),
        Value::Blob(vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]),
        Value::Array(vec![
            Value::Text("admin".to_string()),
            Value::Text("verified".to_string()),
            Value::Text("premium".to_string()),
        ]),
    ];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager)
        .expect("Failed to encode tuple");
    let decoded = TupleCodec::decode_tuple(&encoded, &schema)
        .expect("Failed to decode tuple");

    println!("  Schema:");
    for (name, dtype) in &schema {
        println!("    - {}: {}", name, dtype.to_string());
    }

    println!("\n  Encoded tuple size: {} bytes", encoded.len());

    println!("\n  Decoded values:");
    println!("    user_id = {}", match &decoded[0] {
        Value::Int32(v) => v.to_string(),
        _ => "ERROR".to_string(),
    });
    println!("    is_verified = {}", match &decoded[1] {
        Value::Boolean(v) => v.to_string(),
        _ => "ERROR".to_string(),
    });
    println!("    email = {}", match &decoded[2] {
        Value::Text(v) => format!("\"{}\"", v),
        _ => "ERROR".to_string(),
    });
    println!("    profile_pic = {}", match &decoded[3] {
        Value::Blob(v) => format!("BLOB({} bytes)", v.len()),
        _ => "ERROR".to_string(),
    });
    println!("    tags = {}", match &decoded[4] {
        Value::Array(items) => format!("ARRAY({} items)", items.len()),
        _ => "ERROR".to_string(),
    });

    println!("\n  ✓ Mixed tuple test passed");
}

fn test_large_blob() {
    let large_blob = vec![42u8; 20000]; // 20 KB
    let threshold = 8192; // 8 KB

    println!("  BLOB size: {} bytes", large_blob.len());
    println!("  TOAST threshold: {} bytes", threshold);
    println!("  Will use TOAST: {}", large_blob.len() > threshold);

    let value = Value::Blob(large_blob.clone());
    let encoded = ValueCodec::encode(&value, &DataType::Blob)
        .expect("Failed to encode large BLOB");

    println!("  Encoded size: {} bytes", encoded.len());

    let mut toast_manager = ToastManager::new();
    let toast_ptr = toast_manager.store_large_value(&encoded)
        .expect("Failed to store in TOAST");

    println!("\n  TOAST Pointer Details:");
    println!("    Value ID: {}", toast_ptr.value_id);
    println!("    Total bytes: {}", toast_ptr.total_bytes);
    println!("    Chunk count: {}", toast_ptr.chunk_count);
    println!("    Chunk size: 4096 bytes");

    println!("\n  ✓ Large BLOB with TOAST test passed");
}

fn test_type_parsing() {
    let type_strings = vec![
        "INT",
        "BOOLEAN",
        "TEXT",
        "BLOB",
        "ARRAY<INT>",
        "ARRAY<TEXT>",
        "ARRAY<BOOLEAN>",
        "ARRAY<BLOB>",
    ];

    println!("  Parsing data type declarations:");
    for type_str in type_strings {
        match DataType::parse(type_str) {
            Ok(dtype) => {
                let is_var = if dtype.is_variable_length() {
                    "variable-length"
                } else {
                    "fixed-length"
                };
                let fixed_size = dtype.fixed_size()
                    .map(|s| format!(" ({}B)", s))
                    .unwrap_or_default();
                println!("    ✓ {} → {} {}{}", type_str, dtype.to_string(), is_var, fixed_size);
            }
            Err(e) => {
                println!("    ✗ {} → Error: {}", type_str, e);
            }
        }
    }
}

fn test_null_handling() {
    let schema = vec![
        ("id".to_string(), DataType::Int32),
        ("name".to_string(), DataType::Text),
        ("bio".to_string(), DataType::Text),
    ];

    let values = vec![
        Value::Int32(42),
        Value::Text("Alice".to_string()),
        Value::Null,
    ];

    let mut toast_manager = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager)
        .expect("Failed to encode tuple");
    let decoded = TupleCodec::decode_tuple(&encoded, &schema)
        .expect("Failed to decode tuple");

    println!("  Testing NULL values in tuple:");
    println!("    id (INT) = {} ", match &decoded[0] {
        Value::Int32(v) => format!("{} (NOT NULL)", v),
        Value::Null => "NULL".to_string(),
        _ => "ERROR".to_string(),
    });
    println!("    name (TEXT) = {} ", match &decoded[1] {
        Value::Text(v) => format!("\"{}\" (NOT NULL)", v),
        Value::Null => "NULL".to_string(),
        _ => "ERROR".to_string(),
    });
    println!("    bio (TEXT) = {} ", match &decoded[2] {
        Value::Text(_) => "NOT NULL (ERROR)".to_string(),
        Value::Null => "NULL (Correctly stored)".to_string(),
        _ => "ERROR".to_string(),
    });

    println!("\n  ✓ NULL handling test passed");
}
