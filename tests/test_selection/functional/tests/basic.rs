// How to run these tests:
//   cargo test --test test_selection_basic
//   cargo test --test test_selection_basic -- --nocapture

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
fn test_trivalue_equality() {
    print_separator();
    println!("TEST: TriValue Equality");
    print_separator();
    println!("Testing three-valued logic equality operations:");
    println!("  True == True: {}", TriValue::True == TriValue::True);
    println!("  False == False: {}", TriValue::False == TriValue::False);
    println!("  Unknown == Unknown: {}", TriValue::Unknown == TriValue::Unknown);
    println!("  True != False: {}", TriValue::True != TriValue::False);
    assert_eq!(TriValue::True, TriValue::True);
    assert_eq!(TriValue::False, TriValue::False);
    assert_eq!(TriValue::Unknown, TriValue::Unknown);
    assert_ne!(TriValue::True, TriValue::False);
    println!("Test completed successfully.\n");
}

#[test]
fn test_predicate_equals() {
    print_separator();
    println!("TEST: Predicate Equals");
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
    println!("\nConstructed Tuple:");
    println!("  id = 42");
    println!("  name = Alice");
    let tuple = create_test_tuple(42, "Alice");
    println!("\nTuple Bytes (first 20): {:?}", &tuple[..20.min(tuple.len())]);
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("\nDecoded Tuple:");
    println!("  id = {:?}", accessor.get_value(0, "INT").unwrap());
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nEvaluation Result: {:?}", result);
    println!("Expected Result: True");
    assert_eq!(result, TriValue::True, "id=42 should match predicate");
    println!("Test completed successfully.\n");
}

#[test]
fn test_predicate_not_equals() {
    print_separator();
    println!("TEST: Predicate Not Equals");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id != 99");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(99))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Constructed Tuple:\n  id = 42\n  name = Alice");
    let tuple = create_test_tuple(42, "Alice");
    println!("\nTuple Bytes: {:?}", &tuple[..20.min(tuple.len())]);
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("\nDecoded Tuple:");
    println!("  id = {:?}", accessor.get_value(0, "INT").unwrap());
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Evaluation Result: {:?}", result);
    println!("Expected: Expected Result: True (42 != 99)");
    assert_eq!(result, TriValue::True, "id=42 should not equal 99");
    println!("\nTest completed successfully.");
}

#[test]
fn test_predicate_less_than() {
    print_separator();
    println!("TEST: Predicate Less Than");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id < 50");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Constructed Tuple:\n  id = 42");
    let tuple = create_test_tuple(42, "Alice");
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("\nDecoded Tuple:");
    println!("  id = {:?}", accessor.get_value(0, "INT").unwrap());
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Evaluation Result: {:?}", result);
    println!("Expected: Expected Result: True (42 < 50)");
    assert_eq!(result, TriValue::True, "42 < 50 should be true");
    println!("\nTest completed successfully.");
}

#[test]
fn test_predicate_greater_than() {
    print_separator();
    println!("TEST: Predicate Greater Than");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id > 30");
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Constructed Tuple:\n  id = 42");
    let tuple = create_test_tuple(42, "Alice");
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("\nDecoded Tuple:");
    println!("  id = {:?}", accessor.get_value(0, "INT").unwrap());
    let result = executor.evaluate_tuple(&tuple).unwrap();
    println!("\nResult: Evaluation Result: {:?}", result);
    println!("Expected: Expected Result: True (42 > 30)");
    assert_eq!(result, TriValue::True, "42 > 30 should be true");
    println!("\nTest completed successfully.");
}

#[test]
fn test_predicate_less_or_equal() {
    print_separator();
    println!("TEST: Predicate Less Or Equal");
    print_separator();

    println!("\n🔍 Test Part 1: Equal Case");
    println!("Predicate: id <= 42");
    let pred_eq = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(pred_eq, create_test_schema()).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("Tuple: id = {:?}", accessor.get_value(0, "INT").unwrap());
    println!("Result: {:?} (Expected: True)", executor.evaluate_tuple(&tuple).unwrap());
    assert_eq!(executor.evaluate_tuple(&tuple).unwrap(), TriValue::True);

    println!("\n🔍 Test Part 2: Less Case");
    println!("Predicate: id <= 50");
    let pred_lt = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor2 = SelectionExecutor::new(pred_lt, create_test_schema()).unwrap();
    println!("Tuple: id = {:?}", accessor.get_value(0, "INT").unwrap());
    println!("Result: {:?} (Expected: True)", executor2.evaluate_tuple(&tuple).unwrap());
    assert_eq!(executor2.evaluate_tuple(&tuple).unwrap(), TriValue::True);

    println!("\nTest completed successfully.");
}

#[test]
fn test_predicate_greater_or_equal() {
    print_separator();
    println!("TEST: Predicate Greater Or Equal");
    print_separator();

    println!("\n🔍 Test Part 1: Equal Case");
    println!("Predicate: id >= 42");
    let pred_eq = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(pred_eq, create_test_schema()).unwrap();
    let tuple = create_test_tuple(42, "Alice");
    let accessor = TupleAccessor::new(&tuple, 2).unwrap();
    println!("Tuple: id = {:?}", accessor.get_value(0, "INT").unwrap());
    println!("Result: {:?} (Expected: True)", executor.evaluate_tuple(&tuple).unwrap());
    assert_eq!(executor.evaluate_tuple(&tuple).unwrap(), TriValue::True);

    println!("\n🔍 Test Part 2: Greater Case");
    println!("Predicate: id >= 30");
    let pred_gt = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Int(30))),
    );
    let executor2 = SelectionExecutor::new(pred_gt, create_test_schema()).unwrap();
    println!("Tuple: id = {:?}", accessor.get_value(0, "INT").unwrap());
    println!("Result: {:?} (Expected: True)", executor2.evaluate_tuple(&tuple).unwrap());
    assert_eq!(executor2.evaluate_tuple(&tuple).unwrap(), TriValue::True);

    println!("\nTest completed successfully.");
}

#[test]
fn test_predicate_with_float() {
    let schema = Table {
        columns: vec![
            Column { name: "price".to_string(), data_type: "FLOAT".to_string() },
        ],
    };
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(19.99))),
    );
    let executor = SelectionExecutor::new(predicate, schema);
    assert!(executor.is_ok(), "Float predicate should be valid");
    println!("Test completed successfully.");
}

#[test]
fn test_int_max_boundary() {
    print_separator();
    println!("TEST: INT Maximum Boundary");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id >= {}", i32::MAX);
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Int(i32::MAX))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Test Case 1: id = {}", i32::MAX);
    let tuple1 = create_test_tuple(i32::MAX, "Max");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?} (Expected: True)", result1);
    assert_eq!(result1, TriValue::True);
    println!("\nTuple: Test Case 2: id = {}", i32::MAX - 1);
    let tuple2 = create_test_tuple(i32::MAX - 1, "AlmostMax");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?} (Expected: False)", result2);
    assert_eq!(result2, TriValue::False);
    println!("\nTest completed successfully.");
}

#[test]
fn test_int_min_boundary() {
    print_separator();
    println!("TEST: INT Minimum Boundary");
    print_separator();
    let schema = create_test_schema();
    print_schema(&schema);
    println!("\nPredicate:\n  id <= {}", i32::MIN);
    let predicate = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Int(i32::MIN))),
    );
    let executor = SelectionExecutor::new(predicate, schema).unwrap();
    println!("\nTuple: Test Case 1: id = {}", i32::MIN);
    let tuple1 = create_test_tuple(i32::MIN, "Min");
    let result1 = executor.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?} (Expected: True)", result1);
    assert_eq!(result1, TriValue::True);
    println!("\nTuple: Test Case 2: id = {}", i32::MIN + 1);
    let tuple2 = create_test_tuple(i32::MIN + 1, "AlmostMin");
    let result2 = executor.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?} (Expected: False)", result2);
    assert_eq!(result2, TriValue::False);
    println!("\nTest completed successfully.");
}

#[test]
fn test_zero_and_negative_values() {
    print_separator();
    println!("TEST: Zero and Negative Values");
    print_separator();

    println!("\n🔍 Test Part 1: id = 0");
    let pred1 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor1 = SelectionExecutor::new(pred1, create_test_schema()).unwrap();
    let tuple1 = create_test_tuple(0, "Zero");
    let result1 = executor1.evaluate_tuple(&tuple1).unwrap();
    println!("  Result: {:?} (Expected: True)", result1);
    assert_eq!(result1, TriValue::True);

    println!("\n🔍 Test Part 2: id < 0 (negative boundary)");
    let pred2 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor2 = SelectionExecutor::new(pred2, create_test_schema()).unwrap();
    let tuple2 = create_test_tuple(-42, "Negative");
    let result2 = executor2.evaluate_tuple(&tuple2).unwrap();
    println!("  Result: {:?} (Expected: True)", result2);
    assert_eq!(result2, TriValue::True);

    println!("\n🔍 Test Part 3: id > -100");
    let pred3 = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(-100))),
    );
    let executor3 = SelectionExecutor::new(pred3, create_test_schema()).unwrap();
    let tuple3 = create_test_tuple(-50, "NegativeFifty");
    let result3 = executor3.evaluate_tuple(&tuple3).unwrap();
    println!("  Result: {:?} (Expected: True)", result3);
    assert_eq!(result3, TriValue::True);

    println!("\nTest completed successfully.");
}

#[test]
fn test_boundary_equal_vs_greater() {
    print_separator();
    println!("TEST: Boundary Equal vs Greater");
    print_separator();

    let value = 100;

    println!("Test 1: id = {}", value);
    let pred_eq = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(value))),
    );
    let executor_eq = SelectionExecutor::new(pred_eq, create_test_schema()).unwrap();
    let tuple = create_test_tuple(value, "Test");
    let result_eq = executor_eq.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result_eq, TriValue::True);

    println!("Test 2: id > {}", value);
    let pred_gt = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(value))),
    );
    let executor_gt = SelectionExecutor::new(pred_gt, create_test_schema()).unwrap();
    let result_gt = executor_gt.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result_gt, TriValue::False);

    println!("Test 3: id >= {}", value);
    let pred_gte = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Int(value))),
    );
    let executor_gte = SelectionExecutor::new(pred_gte, create_test_schema()).unwrap();
    let result_gte = executor_gte.evaluate_tuple(&tuple).unwrap();
    assert_eq!(result_gte, TriValue::True);

    println!("Test completed successfully.");
}
