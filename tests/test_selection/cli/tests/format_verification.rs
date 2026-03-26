// Make sure our corrected tuple format actually works with TupleAccessor

use storage_manager::backend::executor::selection::{TupleAccessor, Value};

#[test]
fn test_corrected_tuple_format_single_int() {
    println!("\n=== Test: Single INT Column ===");
    
    // Build a simple tuple with one int
    let tuple = build_corrected_int_tuple(42);
    
    // Can TupleAccessor parse it?
    let accessor = TupleAccessor::new(&tuple, 1)
        .expect("TupleAccessor should parse corrected tuple format");
    
    println!("PASS: TupleAccessor::new() succeeded");
    
    // Now check if the value is actually 42
    let value = accessor.get_value(0, "INT")
        .expect("Should extract INT value");
    
    match value {
        Value::Int(v) => {
            assert_eq!(v, 42, "INT value should be 42");
            println!("PASS: INT value decoded correctly: {}", v);
        }
        _ => panic!("Expected Int value, got {:?}", value),
    }
}

#[test]
fn test_corrected_tuple_format_multi_column() {
    println!("\n=== Test: Multi-Column (INT, FLOAT) ===");
    
    let tuple = build_corrected_multi_tuple(100, 3.14);
    
    let accessor = TupleAccessor::new(&tuple, 2)
        .expect("TupleAccessor should parse multi-column tuple");
    
    println!("PASS: TupleAccessor::new() succeeded");
    
    // Check first column (INT)
    let val0 = accessor.get_value(0, "INT").expect("Should get INT");
    match val0 {
        Value::Int(v) => {
            assert_eq!(v, 100);
            println!("PASS: Column 0 (INT): {}", v);
        }
        _ => panic!("Expected Int"),
    }
    
    // Check second column (FLOAT)
    let val1 = accessor.get_value(1, "FLOAT").expect("Should get FLOAT");
    match val1 {
        Value::Float(v) => {
            assert!((v - 3.14).abs() < 0.001);
            println!("PASS: Column 1 (FLOAT): {}", v);
        }
        _ => panic!("Expected Float"),
    }
}

#[test]
fn test_corrected_tuple_format_with_null() {
    println!("\n=== Test: Tuple with NULL ===");
    
    let tuple = build_corrected_tuple_with_null();
    
    let accessor = TupleAccessor::new(&tuple, 2)
        .expect("TupleAccessor should parse tuple with NULL");
    
    println!("PASS: TupleAccessor::new() succeeded");
    
    // First column should be NULL
    assert!(accessor.is_null(0).unwrap(), "Column 0 should be NULL");
    println!("PASS: Column 0 correctly marked as NULL");
    
    // Second column should not be NULL
    assert!(!accessor.is_null(1).unwrap(), "Column 1 should not be NULL");
    println!("PASS: Column 1 correctly marked as NOT NULL");
    
    // Verify second column value
    let val1 = accessor.get_value(1, "INT").expect("Should get INT");
    match val1 {
        Value::Int(v) => {
            assert_eq!(v, 99);
            println!("PASS: Column 1 (INT): {}", v);
        }
        _ => panic!("Expected Int"),
    }
}

// Helper functions matching the corrected format

fn build_corrected_int_tuple(value: i32) -> Vec<u8> {
    let num_columns = 1;
    let null_bitmap_size = 1;
    let offset_array_size = (num_columns + 1) * 4; // need space for sentinel
    let header_size = 8;
    let field_data_start = header_size + null_bitmap_size + offset_array_size;
    let field_data_size = 4; // i32 is 4 bytes
    
    let total_length = field_data_start + field_data_size;
    let mut tuple = vec![0u8; total_length];
    
    // Header with proper format
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes()); // total length
    tuple[4] = 1;                                                       // version
    tuple[5] = 0;                                                       // flags
    tuple[6..8].copy_from_slice(&(num_columns as u16).to_le_bytes()); // column count is u16
    
    // No NULL values here
    tuple[8] = 0;
    
    // Offsets are relative, plus we need the sentinel at the end
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());   // starts at 0
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&4u32.to_le_bytes()); // ends at 4
    
    // Field data
    tuple[field_data_start..field_data_start + 4].copy_from_slice(&value.to_le_bytes());
    
    tuple
}

fn build_corrected_multi_tuple(int_val: i32, float_val: f64) -> Vec<u8> {
    let num_columns = 2;
    let null_bitmap_size = 1;
    let offset_array_size = (num_columns + 1) * 4;
    let header_size = 8;
    let field_data_start = header_size + null_bitmap_size + offset_array_size;
    let field_data_size = 4 + 8; // int is 4, float is 8
    
    let total_length = field_data_start + field_data_size;
    let mut tuple = vec![0u8; total_length];
    
    // Same header format
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1;
    tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_columns as u16).to_le_bytes());
    
    // No NULLs
    tuple[8] = 0;
    
    // Offsets for two columns plus sentinel
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());    // first field at 0
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&4u32.to_le_bytes()); // second field at 4
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&12u32.to_le_bytes()); // sentinel at 12
    
    // Field data
    tuple[field_data_start..field_data_start + 4].copy_from_slice(&int_val.to_le_bytes());
    tuple[field_data_start + 4..field_data_start + 12].copy_from_slice(&float_val.to_le_bytes());
    
    tuple
}

fn build_corrected_tuple_with_null() -> Vec<u8> {
    let num_columns = 2;
    let null_bitmap_size = 1;
    let offset_array_size = (num_columns + 1) * 4;
    let header_size = 8;
    let field_data_start = header_size + null_bitmap_size + offset_array_size;
    let field_data_size = 4 + 4;
    
    let total_length = field_data_start + field_data_size;
    let mut tuple = vec![0u8; total_length];
    
    // CORRECTED HEADER
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1;
    tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_columns as u16).to_le_bytes());
    
    // NULL bitmap (first column is NULL)
    tuple[8] = 0b00000001; // bit 0 set for column 0
    
    // RELATIVE OFFSETS
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&4u32.to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&8u32.to_le_bytes()); // sentinel
    
    // Field data (second column = 99)
    tuple[field_data_start + 4..field_data_start + 8].copy_from_slice(&99i32.to_le_bytes());
    
    tuple
}
