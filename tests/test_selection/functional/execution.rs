// How to run these tests:
//   cargo test --test test_selection_execution
//   cargo test --test test_selection_execution -- --nocapture

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
fn test_filter_tuples_basic() {
    print_separator();
    println!("TEST: Filter Tuples Basic");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id > 50");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Input Tuples:");
    let tuple_data = [(30, "Alice"), (60, "Bob"), (70, "Charlie"), (40, "David")];
    for (id, name) in &tuple_data {
        println!("  id={}, name={}", id, name);
    }
    let tuples = vec![
        create_test_tuple(30, "Alice"),
        create_test_tuple(60, "Bob"),
        create_test_tuple(70, "Charlie"),
        create_test_tuple(40, "David"),
    ];
    let filtered = filter_tuples(&executor, tuples).unwrap();
    println!("\n✅ Filtered Results:");
    for tuple in &filtered {
        let accessor = TupleAccessor::new(tuple, 2).unwrap();
        let id = accessor.get_value(0, "INT").unwrap();
        println!("  id = {:?}", id);
    }
    println!("\nStatistics:");
    println!("  Total input tuples: 4");
    println!("  Matching tuples: {}", filtered.len());
    println!("  Expected matches: 2");
    assert_eq!(filtered.len(), 2, "Should have 2 tuples with id > 50");
    println!("\nTest completed successfully.");
}

#[test]
fn test_filter_tuples_none_match() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuples = vec![
        create_test_tuple(30, "Alice"),
        create_test_tuple(60, "Bob"),
    ];
    let filtered = filter_tuples(&executor, tuples).unwrap();
    assert_eq!(filtered.len(), 0, "No tuples should match id > 100");
    println!("\nTest completed successfully.");
}

#[test]
fn test_filter_tuples_all_match() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuples = vec![
        create_test_tuple(30, "Alice"),
        create_test_tuple(60, "Bob"),
        create_test_tuple(70, "Charlie"),
    ];
    let filtered = filter_tuples(&executor, tuples.clone()).unwrap();
    assert_eq!(filtered.len(), 3, "All tuples should match id > 0");
    println!("\nTest completed successfully.");
}

#[test]
fn test_count_matching_tuples() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuples = vec![
        create_test_tuple(30, "Alice"),
        create_test_tuple(60, "Bob"),
        create_test_tuple(70, "Charlie"),
        create_test_tuple(40, "David"),
    ];
    let count = count_matching_tuples(&executor, tuples).unwrap();
    assert_eq!(count, 2, "Should count 2 tuples with id > 50");
    println!("\nTest completed successfully.");
}

#[test]
fn test_filter_tuples_detailed() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuples = vec![
        create_test_tuple(30, "Alice"),
        create_test_tuple(60, "Bob"),
        create_test_tuple(70, "Charlie"),
    ];
    let (matched, rejected, unknown) = filter_tuples_detailed(&executor, tuples).unwrap();
    assert_eq!(matched.len(), 2, "2 tuples should match");
    assert_eq!(rejected.len(), 1, "1 tuple should be rejected");
    assert_eq!(unknown.len(), 0, "No unknown results (no NULLs)");
    println!("\nTest completed successfully.");
}

#[test]
fn test_empty_tuple_list() {
    let schema = create_test_schema();
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuples: Vec<Vec<u8>> = vec![];
    let filtered = filter_tuples(&executor, tuples).unwrap();
    assert_eq!(filtered.len(), 0, "Empty input should return empty result");
    println!("\nTest completed successfully.");
}
