// How to run these tests:
//   cargo test --test test_selection_null
//   cargo test --test test_selection_null -- --nocapture

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

#[test]
fn test_null_comparison_returns_unknown() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Null)),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "NULL comparisons must return Unknown");
    println!("\nTest completed successfully.");
}

#[test]
fn test_and_with_unknown() {
    assert_eq!(apply_and(TriValue::True, TriValue::Unknown), TriValue::Unknown);
    assert_eq!(apply_and(TriValue::False, TriValue::Unknown), TriValue::False);
    assert_eq!(apply_and(TriValue::Unknown, TriValue::Unknown), TriValue::Unknown);
    println!("\nTest completed successfully.");
}

#[test]
fn test_or_with_unknown() {
    assert_eq!(apply_or(TriValue::True, TriValue::Unknown), TriValue::True);
    assert_eq!(apply_or(TriValue::False, TriValue::Unknown), TriValue::Unknown);
    assert_eq!(apply_or(TriValue::Unknown, TriValue::Unknown), TriValue::Unknown);
    println!("\nTest completed successfully.");
}

#[test]
fn test_null_value_in_column() {
    print_separator();
    println!("TEST: NULL Value in Column");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id = 42");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Tuple with NULL id:\n  id = NULL\n  name = 'Alice'");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("\n🔍 Verification:\n  Column 0 is NULL: {}", accessor.is_null(0).unwrap());
    assert!(accessor.is_null(0).unwrap(), "First column should be NULL");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Evaluation Result: {:?}", result);
    println!("Expected: Expected: Unknown (NULL comparison)");
    assert_eq!(result, TriValue::Unknown, "NULL = 42 should return Unknown");
    println!("\nTest completed successfully.");
}

#[test]
fn test_null_in_both_columns() {
    print_separator();
    println!("TEST: NULL in Both Columns");
    print_separator();
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("Tuple: Tuple with both columns NULL");
    let tuple = create_tuple_with_null(None, None);
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    assert!(accessor.is_null(0).unwrap(), "First column should be NULL");
    assert!(accessor.is_null(1).unwrap(), "Second column should be NULL");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "NULL = 42 should return Unknown");
    println!("Test completed successfully.");
}

#[test]
fn test_null_with_and_logic() {
    print_separator();
    println!("TEST: NULL with AND Logic");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  (id = 42) AND (id > 30)");
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Tuple: id = NULL");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "(NULL = 42) AND (NULL > 30) should be Unknown");
    println!("\nTest completed successfully.");
}

#[test]
fn test_null_and_true() {
    print_separator();
    println!("TEST: NULL AND True");
    print_separator();
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Text("Alice".to_string()))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("Tuple: Tuple: id = NULL, name = 'Alice'");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "Unknown AND True must equal Unknown");
    println!("Test completed successfully.");
}

#[test]
fn test_null_and_false() {
    print_separator();
    println!("TEST: NULL AND False");
    print_separator();
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("Tuple: Tuple: id = NULL");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown);
    println!("Test completed successfully.");
}

#[test]
fn test_null_with_or_logic() {
    print_separator();
    println!("TEST: NULL with OR Logic");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  (id = 42) OR (id > 30)");
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Tuple: id = NULL");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "(NULL = 42) OR (NULL > 30) should be Unknown");
    println!("\nTest completed successfully.");
}

#[test]
fn test_null_or_true() {
    print_separator();
    println!("TEST: NULL OR True");
    print_separator();
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(99))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Text("Alice".to_string()))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("Tuple: Tuple: id = NULL, name = 'Alice'");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::True, "Unknown OR True must equal True");
    println!("Test completed successfully.");
}

#[test]
fn test_null_or_false() {
    print_separator();
    println!("TEST: NULL OR False");
    print_separator();
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(99))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Text("Bob".to_string()))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("Tuple: Tuple: id = NULL, name = 'Alice'");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "Unknown OR False should be Unknown");
    println!("Test completed successfully.");
}

#[test]
fn test_null_not_equals() {
    print_separator();
    println!("TEST: NULL NOT EQUALS");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate: id != 5");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(5))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: id = NULL, name = 'Alice'");
    let tuple = create_tuple_with_null(None, Some("Alice"));
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::Unknown, "NULL != 5 must return Unknown");
    println!("Test completed successfully.");
}

#[test]
fn test_filter_tuples_with_null() {
    print_separator();
    println!("TEST: Filter Tuples With NULL");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate: id = 42");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nInput tuples:");
    println!("  1. id = 42, name = 'Alice'");
    println!("  2. id = NULL, name = 'Bob'");
    let tuples = vec![
        create_test_tuple(42, "Alice"),
        create_tuple_with_null(None, Some("Bob")),
    ];
    let filtered = filter_tuples(&executor, &tuples).unwrap();
    println!("\nExpected: 1 tuple matches (id = 42)");
    println!("Actual: {} tuple(s) match", filtered.len());
    assert_eq!(filtered.len(), 1, "Only the non-NULL tuple with id=42 should match");
    println!("Test completed successfully.");
}
