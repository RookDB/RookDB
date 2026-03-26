// How to run these tests:
//   cargo test --test test_selection_edge
//   cargo test --test test_selection_edge -- --nocapture

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn print_separator() {
    println!("\n{}", "-".repeat(60));
}

fn print_schema(schema: &Table) {
    println!("Schema:");
    for col in &schema.columns {
        println!("  Column: {} (Type: {})", col.name, col.data_type);
    }
}

fn create_test_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(), data_type: "INT".to_string() },
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
        ],
    }
}

fn create_test_tuple(id: i32, name: &str) -> Vec<u8> {
    let num_cols = 2;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let id_size = 4;
    let name_bytes = name.as_bytes();
    let name_size = name_bytes.len();
    let total_length = data_start + id_size + name_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&(id_size as u32).to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&((id_size + name_size) as u32).to_le_bytes());
    tuple[data_start..data_start + 4].copy_from_slice(&id.to_le_bytes());
    let name_start = data_start + id_size;
    tuple[name_start..name_start + name_bytes.len()].copy_from_slice(name_bytes);
    tuple
}

fn create_valid_tuple_with_header(num_cols: usize, values: Vec<i32>) -> Vec<u8> {
    assert_eq!(num_cols, values.len());
    let header_size = 8;
    let null_bitmap_len = (num_cols + 7) / 8;
    let offset_array_len = (num_cols + 1) * 4;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let data_size = values.len() * 4;
    let total_length = data_start + data_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    for i in 0..null_bitmap_len { tuple[header_size + i] = 0; }
    let offset_start = header_size + null_bitmap_len;
    for i in 0..num_cols {
        let offset = (i * 4) as u32;
        tuple[offset_start + i * 4..offset_start + (i + 1) * 4].copy_from_slice(&offset.to_le_bytes());
    }
    let sentinel = data_size as u32;
    tuple[offset_start + num_cols * 4..offset_start + (num_cols + 1) * 4].copy_from_slice(&sentinel.to_le_bytes());
    for (i, value) in values.iter().enumerate() {
        let pos = data_start + i * 4;
        tuple[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
    }
    tuple
}

fn create_tuple_with_null(id: Option<i32>, name: Option<&str>) -> Vec<u8> {
    let num_cols = 2;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let id_size = 4;
    let name_size = if let Some(n) = name { n.as_bytes().len() } else { 0 };
    let total_length = data_start + id_size + name_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    let mut null_bitmap = 0u8;
    if id.is_none() { null_bitmap |= 0b00000001; }
    if name.is_none() { null_bitmap |= 0b00000010; }
    tuple[8] = null_bitmap;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&(id_size as u32).to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&((id_size + name_size) as u32).to_le_bytes());
    if let Some(id_val) = id { tuple[data_start..data_start + 4].copy_from_slice(&id_val.to_le_bytes()); }
    if let Some(name_val) = name {
        let nb = name_val.as_bytes();
        tuple[data_start + id_size..data_start + id_size + nb.len()].copy_from_slice(nb);
    }
    tuple
}

fn create_text_tuple(text_value: &str) -> Vec<u8> {
    let num_cols = 1;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let text_bytes = text_value.as_bytes();
    let text_size = text_bytes.len();
    let total_length = data_start + text_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&(text_size as u32).to_le_bytes());
    tuple[data_start..data_start + text_bytes.len()].copy_from_slice(text_bytes);
    tuple
}

fn create_date_tuple(date_str: &str) -> Vec<u8> {
    let num_cols = 1;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let date_bytes = date_str.as_bytes();
    let date_size = date_bytes.len();
    let total_length = data_start + date_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&(date_size as u32).to_le_bytes());
    tuple[data_start..data_start + date_bytes.len()].copy_from_slice(date_bytes);
    tuple
}

fn create_tuple_non_monotonic_offsets() -> Vec<u8> {
    let num_cols = 2;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let data_size = 8;
    let total_length = data_start + data_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&8u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&4u32.to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&8u32.to_le_bytes());
    tuple
}

fn create_tuple_offset_out_of_bounds() -> Vec<u8> {
    let num_cols = 2;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let data_size = 8;
    let total_length = data_start + data_size;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1; tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&4u32.to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&9999u32.to_le_bytes());
    tuple
}

fn create_text_schema() -> Table {
    Table { columns: vec![Column { name: "name".to_string(), data_type: "TEXT".to_string() }] }
}

fn create_date_schema() -> Table {
    Table { columns: vec![Column { name: "created_date".to_string(), data_type: "DATE".to_string() }] }
}

fn create_float_schema() -> Table {
    Table { columns: vec![Column { name: "price".to_string(), data_type: "FLOAT".to_string() }] }
}

// ---- TESTS ----

// TupleAccessor tests

#[test]
fn test_tuple_accessor_valid_tuple() {
    let tuple = create_valid_tuple_with_header(2, vec![42i32, 100i32]);
    let accessor = TupleAccessor::new(&tuple, 2);
    assert!(accessor.is_ok(), "Valid tuple should parse successfully");
    let accessor = accessor.unwrap();
    assert_eq!(accessor.num_columns(), 2);
    println!("\nTest completed successfully.");
}

#[test]
fn test_tuple_accessor_too_short() {
    let tuple = vec![1, 2, 3];
    let result = TupleAccessor::new(&tuple, 2);
    assert!(result.is_err(), "Too short tuple should fail validation");
    match result {
        Err(TupleError::TupleTooShort) => {},
        _ => panic!("Expected TupleTooShort error"),
    }
    println!("\nTest completed successfully.");
}

#[test]
fn test_tuple_accessor_length_mismatch() {
    let mut tuple = create_valid_tuple_with_header(2, vec![42i32, 100i32]);
    tuple[0..4].copy_from_slice(&999u32.to_le_bytes());
    let result = TupleAccessor::new(&tuple, 2);
    assert!(result.is_err(), "Length mismatch should fail validation");
    println!("\nTest completed successfully.");
}

#[test]
fn test_tuple_accessor_is_null() {
    let mut tuple = create_valid_tuple_with_header(2, vec![42i32, 100i32]);
    tuple[8] = 0b00000001;
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    assert!(accessor.is_null(0).unwrap(), "First column should be NULL");
    assert!(!accessor.is_null(1).unwrap(), "Second column should not be NULL");
    println!("\nTest completed successfully.");
}

#[test]
fn test_tuple_accessor_get_value() {
    let tuple = create_valid_tuple_with_header(2, vec![42i32, 100i32]);
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    let value = accessor.get_value(0, "INT").unwrap();
    assert_eq!(value, Value::Int(42), "First column should be 42");
    let value2 = accessor.get_value(1, "INT").unwrap();
    assert_eq!(value2, Value::Int(100), "Second column should be 100");
    println!("\nTest completed successfully.");
}

// Schema mismatch tests

#[test]
fn test_column_not_found() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("nonexistent".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let result = SelectionExecutor::new(predicate, schema);
    assert!(result.is_err(), "Non-existent column should cause error");
    println!("\nTest completed successfully.");
}

#[test]
fn test_tuple_column_count_mismatch() {
    print_separator();
    println!("TEST: Tuple Column Count Mismatch");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("Schema expects: 2 columns");
    println!("\nTuple: Creating tuple with 3 columns:");
    let tuple = create_valid_tuple_with_header(3, vec![42, 100, 200]);
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nResult: Evaluating tuple with mismatched column count...");
    let result = executor.evaluate_tuple(&tuple);
    println!("Result: {:?}", result);
    println!("Expected: Expected: Should handle mismatch (error or adapted evaluation)");
    println!("\nTest completed successfully. (No panic)");
}

#[test]
fn test_incomplete_offset_array() {
    print_separator();
    println!("TEST: Incomplete Offset Array");
    print_separator();
    println!("Tuple: Creating tuple with incomplete offset array:");
    let header_size = 8;
    let null_bitmap_len = 1;
    let truncated_length = header_size + null_bitmap_len + 4;
    let mut tuple = vec![0u8; truncated_length];
    tuple[0..4].copy_from_slice(&(truncated_length as u32).to_le_bytes());
    tuple[4] = 1;
    tuple[5] = 0;
    tuple[6..8].copy_from_slice(&2u16.to_le_bytes());
    tuple[8] = 0;
    tuple[9..13].copy_from_slice(&0u32.to_le_bytes());
    println!("  Header claims: 2 columns");
    println!("  Offset array size: Only 1 offset (should be 3 including sentinel)");
    let result = TupleAccessor::new(&tuple, 2);
    if result.is_ok() {
        println!("\nResult: Result: Unexpectedly succeeded");
    } else {
        println!("\nResult: Result: Err (Validation failed as expected)");
    }
    println!("Expected: Expected: TupleError (incomplete offset array)");
    assert!(result.is_err(), "Incomplete offset array should fail");
    println!("\nTest completed successfully.");
}

#[test]
fn test_schema_expects_wrong_column() {
    print_separator();
    println!("TEST: Schema References Non-Existent Column");
    print_separator();
    let schema = create_test_schema();
    println!("📋 Schema columns: id, name");
    println!("🔍 Predicate references: age (doesn't exist!)");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("age".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(25))),
    );
    let result = SelectionExecutor::new(predicate, schema);
    if result.is_ok() {
        println!("\nResult: Result: Unexpectedly Ok");
    } else {
        println!("\nResult: Result: Err (Column not found)");
    }
    println!("Expected: Expected: Error (column not found)");
    assert!(result.is_err(), "Non-existent column should cause error");
    println!("\nTest completed successfully.");
}

// Mixed-type tests
// STEP 3: Type-mismatch tests updated — new API does NOT reject at construction time.
// INT vs FLOAT cross-comparisons are promoted; TEXT vs INT returns Unknown.

#[test]
fn test_int_column_float_constant() {
    print_separator();
    println!("TEST: INT Column with FLOAT Constant");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id = 42.5 (FLOAT constant)");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Float(42.5))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Result: {:?}", result);
    println!("Expected: False (42 as float != 42.5)");
    assert_eq!(result, TriValue::False);
    println!("\nTest completed successfully.");
}

#[test]
fn test_float_column_int_constant() {
    print_separator();
    println!("TEST: FLOAT Column with INT Constant");
    print_separator();
    let schema = create_float_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  price > 50 (INT constant)");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let _executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nResult: Result: Ok (Executor created successfully, cross-type comparison is promoted)");
    println!("Test completed successfully.");
}

#[test]
fn test_text_column_int_constant() {
    print_separator();
    println!("TEST: TEXT Column with INT Constant (Type Mismatch)");
    print_separator();
    let schema = create_text_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  name = 42 (INT constant on TEXT column)");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let result = SelectionExecutor::new(predicate, schema);
    println!("\nExpected: Err (Type checking rejects TEXT vs INT at planning time)");
    assert!(result.is_err(), "Should reject TEXT vs INT type mismatch");
    if let Err(error_msg) = result {
        println!("Result: Err({})", error_msg);
        assert!(error_msg.contains("Type mismatch"), "Error should mention type mismatch");
    }
    println!("\nTest completed successfully.");
}

// Offset tests

#[test]
fn test_non_monotonic_offsets() {
    print_separator();
    println!("TEST: Non-Monotonic Offsets");
    print_separator();
    println!("Tuple: Creating tuple with non-monotonic offsets:");
    println!("  offset[0] = 8");
    println!("  offset[1] = 4   ERROR: offset[1] < offset[0]");
    let tuple = create_tuple_non_monotonic_offsets();
    let result = TupleAccessor::new(&tuple, 2);
    if result.is_ok() {
        println!("\nResult: Result: Unexpectedly Ok");
    } else {
        println!("\nResult: Result: Err (Validation failed as expected)");
    }
    println!("Expected: Expected: TupleError (offsets not monotonic)");
    assert!(result.is_err(), "Non-monotonic offsets should fail");
    if let Err(err) = result {
        println!("✅ Got expected error: {:?}", err);
    }
    println!("\nTest completed successfully.");
}

#[test]
fn test_offset_out_of_bounds() {
    print_separator();
    println!("TEST: Offset Out of Bounds");
    print_separator();
    println!("Tuple: Creating tuple with offset pointing outside tuple:");
    println!("  Tuple length: ~30 bytes");
    println!("  Sentinel offset: 9999  ERROR: exceeds tuple boundary");
    let tuple = create_tuple_offset_out_of_bounds();
    let result = TupleAccessor::new(&tuple, 2);
    if result.is_ok() {
        println!("\nResult: Result: Unexpectedly Ok");
    } else {
        println!("\nResult: Result: Err (Validation failed as expected)");
    }
    println!("Expected: Expected: TupleError (offset out of bounds)");
    assert!(result.is_err(), "Out of bounds offset should fail");
    if let Err(err) = result {
        println!("✅ Got expected error: {:?}", err);
    }
    println!("\nTest completed successfully.");
}

#[test]
fn test_zero_length_offset_span() {
    print_separator();
    println!("TEST: Zero-Length Offset Span");
    print_separator();
    let num_cols = 2;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 8;
    let mut tuple = vec![0u8; total_length];
    tuple[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    tuple[4] = 1;
    tuple[5] = 0;
    tuple[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    tuple[8] = 0;
    let offset_start = 9;
    tuple[offset_start..offset_start + 4].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 4..offset_start + 8].copy_from_slice(&0u32.to_le_bytes());
    tuple[offset_start + 8..offset_start + 12].copy_from_slice(&8u32.to_le_bytes());
    println!("Tuple: Tuple with zero-length field (offset[0] == offset[1])");
    let accessor = TupleAccessor::new(&tuple, 2);
    assert!(accessor.is_ok(), "Zero-length offset spans are valid for empty strings or NULL representations");
    println!("\nTest completed successfully.");
}

// TEXT / DATE tests

#[test]
fn test_text_field_equality() {
    print_separator();
    println!("TEST: TEXT Field Equality");
    print_separator();
    let schema = create_text_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  name = 'Alice'");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Text("Alice".to_string()))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Test Case 1: name = 'Alice'");
    let tuple1 = create_text_tuple("Alice");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?}", result1);
    println!("  Expected: True (Alice = Alice)");
    assert_eq!(result1, TriValue::True, "Alice should equal Alice");
    println!("\nTuple: Test Case 2: name = 'Bob'");
    let tuple2 = create_text_tuple("Bob");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?}", result2);
    println!("  Expected: False (Bob != Alice)");
    assert_eq!(result2, TriValue::False, "Bob should not equal Alice");
    println!("\nTest completed successfully.");
}

#[test]
fn test_text_field_not_equals() {
    print_separator();
    println!("TEST: TEXT Field Not Equals");
    print_separator();
    let schema = create_text_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  name != 'Bob'");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Text("Bob".to_string()))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Test Case 1: name = 'Alice'");
    let tuple1 = create_text_tuple("Alice");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?}", result1);
    println!("  Expected: True (Alice != Bob)");
    assert_eq!(result1, TriValue::True, "Alice should not equal Bob");
    println!("\nTuple: Test Case 2: name = 'Bob'");
    let tuple2 = create_text_tuple("Bob");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?}", result2);
    println!("  Expected: False (Bob = Bob, so NOT EQUALS is False)");
    assert_eq!(result2, TriValue::False, "Bob != Bob should evaluate to False");
    println!("\nTest completed successfully.");
}

#[test]
fn test_text_field_comparison() {
    print_separator();
    println!("TEST: TEXT Field Comparison (Lexicographic)");
    print_separator();
    let schema = create_text_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  name > 'Adam'");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Text("Adam".to_string()))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Test Case 1: name = 'Alice' (Alice > Adam)");
    let tuple1 = create_text_tuple("Alice");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?} (Expected: True)", result1);
    assert_eq!(result1, TriValue::True);
    println!("\nTuple: Test Case 2: name = 'Aaron' (Aaron < Adam)");
    let tuple2 = create_text_tuple("Aaron");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?} (Expected: False)", result2);
    assert_eq!(result2, TriValue::False);
    println!("\nTest completed successfully.");
}

#[test]
fn test_text_field_less_than() {
    print_separator();
    println!("TEST: TEXT Field Less Than");
    print_separator();
    let schema = create_text_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Text("Charlie".to_string()))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple1 = create_text_tuple("Alice");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    assert_eq!(result1, TriValue::True, "Alice < Charlie should be True");
    let tuple2 = create_text_tuple("David");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    assert_eq!(result2, TriValue::False, "David < Charlie should be False");
    println!("Test completed successfully.");
}

#[test]
fn test_date_field_comparison() {
    print_separator();
    println!("TEST: DATE Field Comparison");
    print_separator();

    println!("\nTest Case 1: date > '2024-01-01'");
    let predicate_gt = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("created_date".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Date("2024-01-01".to_string()))),
    );
    let executor_gt = SelectionExecutor::new(predicate_gt, create_date_schema()).unwrap();
    let tuple1 = create_date_tuple("2024-05-01");
    let result1 = executor_gt.evaluate_tuple(&tuple1).unwrap();
    println!("  Tuple: '2024-05-01'");
    println!("  Result: {:?}", result1);
    println!("  Expected: True");
    assert_eq!(result1, TriValue::True, "2024-05-01 > 2024-01-01 should be True");

    println!("\nTest Case 2: date < '2024-01-01'");
    let predicate_lt = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("created_date".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Date("2024-01-01".to_string()))),
    );
    let executor_lt = SelectionExecutor::new(predicate_lt, create_date_schema()).unwrap();
    let tuple2 = create_date_tuple("2024-05-01");
    let result2 = executor_lt.evaluate_tuple(&tuple2).unwrap();
    println!("  Tuple: '2024-05-01'");
    println!("  Result: {:?}", result2);
    println!("  Expected: False");
    assert_eq!(result2, TriValue::False, "2024-05-01 < 2024-01-01 should be False");

    println!("\nTest completed successfully.");
}
