// How to run these tests:
//   cargo test --test test_selection_logic
//   cargo test --test test_selection_logic -- --nocapture

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
fn test_and_true_true() {
    print_separator();
    println!("TEST: AND Logic (True AND True)");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  (id > 30) AND (id < 50)");
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Constructed Tuple:\n  id = 42");
    let tuple = create_test_tuple(42, "Alice");
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    let id_val = accessor.get_value(0, "INT").unwrap();
    println!("\nDecoded Tuple:\n  id = {:?}", id_val);
    println!("\n🧠 Evaluation Steps:\n  42 > 30 = True\n  42 < 50 = True\n  True AND True = True");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Final Evaluation Result: {:?}", result);
    println!("Expected: Expected Result: True");
    assert_eq!(result, TriValue::True, "42 > 30 AND 42 < 50 should be True");
    println!("\nTest completed successfully.");
}

#[test]
fn test_and_true_false() {
    print_separator();
    println!("TEST: AND Logic (True AND False)");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  (id > 30) AND (id > 50)");
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Tuple: id = 42");
    let tuple = create_test_tuple(42, "Alice");
    println!("\n🧠 Evaluation Steps:\n  42 > 30 = True\n  42 > 50 = False\n  True AND False = False");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Final Result: {:?}", result);
    println!("Expected: Expected: False");
    assert_eq!(result, TriValue::False, "True AND False should be False");
    println!("\nTest completed successfully.");
}

#[test]
fn test_and_false_false() {
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::and(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::False, "False AND False should be False");
    println!("\nTest completed successfully.");
}

#[test]
fn test_or_true_true() {
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::True, "True OR True should be True");
    println!("\nTest completed successfully.");
}

#[test]
fn test_or_true_false() {
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::True, "True OR False should be True");
    println!("\nTest completed successfully.");
}

#[test]
fn test_or_false_false() {
    let schema = create_test_schema();
    let left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let predicate = Predicate::or(left, right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::False, "False OR False should be False");
    println!("\nTest completed successfully.");
}

#[test]
fn test_complex_nested_predicate() {
    print_separator();
    println!("TEST: Complex Nested Predicate");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  ((id > 30) AND (id < 50)) OR (id = 100)");
    let and_left = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let and_right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let and_pred = Predicate::and(and_left, and_right);
    let or_right = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let predicate = Predicate::or(and_pred, or_right);
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Tuple: id = 42");
    let tuple = create_test_tuple(42, "Alice");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Final Result: {:?}", result);
    println!("Expected: Expected: True");
    assert_eq!(result, TriValue::True, "Complex predicate should evaluate to True");
    println!("\nTest completed successfully.");
}

#[test]
fn test_deeply_nested_predicate_tree() {
    print_separator();
    println!("TEST: Deeply Nested Predicate Tree");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    let gt30 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let lt50 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let and_level3 = Predicate::and(gt30, lt50);
    let eq100 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let or_level2 = Predicate::or(and_level3, eq100);
    let ne45 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(45))),
    );
    let final_predicate = Predicate::and(or_level2, ne45);
    let executor = SelectionExecutor::new(final_predicate, schema).unwrap();
    let tuple1 = create_test_tuple(42, "Alice");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    assert_eq!(result1, TriValue::True);
    let tuple2 = create_test_tuple(45, "Bob");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    assert_eq!(result2, TriValue::False);
    let tuple3 = create_test_tuple(100, "Charlie");
    let result3 = executor.evaluate_tuple(&tuple3).unwrap();
    assert_eq!(result3, TriValue::True);
    let tuple4 = create_test_tuple(25, "David");
    let result4 = executor.evaluate_tuple(&tuple4).unwrap();
    assert_eq!(result4, TriValue::False);
    println!("\nTest completed successfully.");
}

#[test]
fn test_five_level_nested_tree() {
    print_separator();
    println!("TEST: Five-Level Nested Predicate Tree");
    print_separator();
    let schema = create_test_schema();
    let p1 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(10))),
    );
    let p2 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(90))),
    );
    let l4 = Predicate::and(p1, p2);
    let p3 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let l3 = Predicate::or(l4, p3);
    let p4 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let l2 = Predicate::and(l3, p4);
    let p5 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let root = Predicate::or(l2, p5);
    let executor = SelectionExecutor::new(root, schema).unwrap();
    let tuple = create_test_tuple(42, "Test");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result, TriValue::True, "Deep tree evaluation should work");
    println!("\nTest completed successfully.");
}

#[test]
fn test_alternating_and_or_tree() {
    print_separator();
    println!("TEST: Alternating AND/OR Tree");
    print_separator();
    let schema = create_test_schema();
    let a = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(20))),
    );
    let b = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let c = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(40))),
    );
    let d = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let a_or_b = Predicate::or(a, b);
    let c_or_d = Predicate::or(c, d);
    let left = Predicate::and(a_or_b, c_or_d);
    let e = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let f = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(200))),
    );
    let g = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let e_and_f = Predicate::and(e, f);
    let right = Predicate::or(e_and_f, g);
    let root = Predicate::or(left, right);
    let executor = SelectionExecutor::new(root, schema).unwrap();
    let tuple = create_test_tuple(42, "Test");
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("  Result: {:?} (Expected: True - matches G)", result);
    assert_eq!(result, TriValue::True);
    println!("\nTest completed successfully.");
}
