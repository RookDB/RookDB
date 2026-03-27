// Predicate normalization tests
// normalize_predicate() is called inside SelectionExecutor::new.
// Verifiable effects:
//  1. Constant-left swap reverses the comparison operator.
//  2. BETWEEN is rewritten to (col >= low) AND (col <= high).
//  3. LIKE pattern is compiled to a pre-compiled regex at planning time.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(), data_type: "INT".to_string() },
        ],
    }
}

fn text_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
        ],
    }
}

fn make_int_tuple(id: i32) -> Vec<u8> {
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 4;
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t
}

fn make_text_tuple(s: &str) -> Vec<u8> {
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let bytes = s.as_bytes();
    let total_length = data_start + bytes.len();
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    t[data_start..data_start+bytes.len()].copy_from_slice(bytes);
    t
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_constant_left_swap_less_than() {
    // Written as: 50 < id  (constant on left, column on right)
    // After swap: id > 50
    // id=60 → True; id=40 → False
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Constant(Constant::Int(50))),
        ComparisonOp::LessThan,
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(60)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(40)).unwrap(), TriValue::False);
}

#[test]
fn test_constant_left_swap_greater_than() {
    // Written as: 100 > id  (constant left)
    // After swap: id < 100
    // id=80 → True; id=120 → False
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Constant(Constant::Int(100))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(80)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(120)).unwrap(), TriValue::False);
}

#[test]
fn test_constant_left_swap_equals_unchanged() {
    // 42 = id → swapped to id = 42; Equals stays Equals.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Constant(Constant::Int(42))),
        ComparisonOp::Equals,
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(43)).unwrap(), TriValue::False);
}

#[test]
fn test_between_rewritten_to_and() {
    // BETWEEN 20 AND 80  ≡  id >= 20 AND id <= 80
    let schema = int_schema();
    let pred = Predicate::Between(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        Box::new(Expr::Constant(Constant::Int(20))),
        Box::new(Expr::Constant(Constant::Int(80))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(20)).unwrap(), TriValue::True,  "boundary low");
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(80)).unwrap(), TriValue::True,  "boundary high");
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(50)).unwrap(), TriValue::True,  "inside");
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(19)).unwrap(), TriValue::False, "below low");
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(81)).unwrap(), TriValue::False, "above high");
}

#[test]
fn test_like_percent_wildcard_compiled_at_planning() {
    // LIKE 'A%' — regex compiled once at SelectionExecutor::new
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "A%".to_string(),
        None,
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Alice")).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Bob")).unwrap(),   TriValue::False);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Aaron")).unwrap(), TriValue::True);
}

#[test]
fn test_like_underscore_wildcard() {
    // LIKE 'Bo_' matches exactly 3-char strings starting with Bo
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "Bo_".to_string(),
        None,
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Bob")).unwrap(),   TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Bo")).unwrap(),    TriValue::False);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Bobby")).unwrap(), TriValue::False);
}

#[test]
fn test_like_exact_match_no_wildcards() {
    // LIKE 'Alice' with no wildcards — exact match only
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "Alice".to_string(),
        None,
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Alice")).unwrap(),  TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("Alice ")).unwrap(), TriValue::False);
    assert_eq!(executor.evaluate_tuple(&make_text_tuple("alice")).unwrap(),  TriValue::False);
}
