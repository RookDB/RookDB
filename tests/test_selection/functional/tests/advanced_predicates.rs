// Advanced predicate tests — IN, LIKE, Exists
// Tests membership (IN), pattern matching (LIKE), and the Exists predicate wrapper.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table { columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }] }
}

fn text_schema() -> Table {
    Table { columns: vec![Column { name: "name".to_string(), data_type: "TEXT".to_string() }] }
}

fn make_int_tuple(id: i32) -> Vec<u8> {
    let num_cols = 1usize;
    let header_size = 8usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 4;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t
}

fn make_null_int_tuple() -> Vec<u8> {
    let mut t = make_int_tuple(0);
    t[8] = 0b00000001; // mark column 0 as NULL
    t
}

fn make_text_tuple(s: &str) -> Vec<u8> {
    let num_cols = 1usize;
    let header_size = 8usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let bytes = s.as_bytes();
    let total = data_start + bytes.len();
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
    t[data_start..data_start+bytes.len()].copy_from_slice(bytes);
    t
}

// ── IN tests ─────────────────────────────────────────────────────────────────

#[test]
fn test_in_match() {
    // id IN (10, 20, 30) — id=20 matches
    let schema = int_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        vec![
            Expr::Constant(Constant::Int(10)),
            Expr::Constant(Constant::Int(20)),
            Expr::Constant(Constant::Int(30)),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(20)).unwrap(), TriValue::True);
}

#[test]
fn test_in_no_match() {
    // id IN (10, 20, 30) — id=99 does not match
    let schema = int_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        vec![
            Expr::Constant(Constant::Int(10)),
            Expr::Constant(Constant::Int(20)),
            Expr::Constant(Constant::Int(30)),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(99)).unwrap(), TriValue::False);
}

#[test]
fn test_in_boundary_first_element() {
    let schema = int_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        vec![
            Expr::Constant(Constant::Int(1)),
            Expr::Constant(Constant::Int(2)),
            Expr::Constant(Constant::Int(3)),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(1)).unwrap(), TriValue::True);
}

#[test]
fn test_in_boundary_last_element() {
    let schema = int_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        vec![
            Expr::Constant(Constant::Int(1)),
            Expr::Constant(Constant::Int(2)),
            Expr::Constant(Constant::Int(3)),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(3)).unwrap(), TriValue::True);
}

#[test]
fn test_in_null_column_returns_unknown() {
    // NULL column IN (10, 20) → Unknown
    let schema = int_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        vec![
            Expr::Constant(Constant::Int(10)),
            Expr::Constant(Constant::Int(20)),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_null_int_tuple()).unwrap(), TriValue::Unknown);
}

#[test]
fn test_in_text_match() {
    // name IN ('Alice', 'Bob') — 'Alice' matches
    let schema = text_schema();
    let pred = Predicate::In(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        vec![
            Expr::Constant(Constant::Text("Alice".to_string())),
            Expr::Constant(Constant::Text("Bob".to_string())),
        ],
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Alice")).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Charlie")).unwrap(), TriValue::False);
}

// ── LIKE tests ────────────────────────────────────────────────────────────────

#[test]
fn test_like_percent_start() {
    // name LIKE '%son' — matches 'Johnson', 'Wilson'; not 'Alice'
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "%son".to_string(),
        None,
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Johnson")).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Alice")).unwrap(),   TriValue::False);
}

#[test]
fn test_like_percent_both_ends() {
    // name LIKE '%li%' — matches 'Alice', 'Olivia'; not 'Bob'
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "%li%".to_string(),
        None,
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Alice")).unwrap(),  TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Olivia")).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Bob")).unwrap(),    TriValue::False);
}

#[test]
fn test_like_underscore_single_char() {
    // name LIKE 'B_b' — matches exactly 'Bob', 'Bab'; not 'Boob'
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "B_b".to_string(),
        None,
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Bob")).unwrap(),  TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Bab")).unwrap(),  TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("Boob")).unwrap(), TriValue::False);
}

#[test]
fn test_like_empty_pattern_matches_empty_string() {
    // name LIKE '' — only empty string matches
    let schema = text_schema();
    let pred = Predicate::Like(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        "".to_string(),
        None,
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("")).unwrap(),   TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_text_tuple("A")).unwrap(),  TriValue::False);
}

// ── Exists tests ──────────────────────────────────────────────────────────────

#[test]
fn test_exists_wrapping_true_predicate() {
    // Exists wraps a predicate; if inner is True → Exists is True.
    let schema = int_schema();
    let inner = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let pred = Predicate::Exists(Box::new(inner));
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    // id=5 > 0 → inner True → Exists True
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(5)).unwrap(), TriValue::True);
}

#[test]
fn test_exists_wrapping_false_predicate() {
    // Exists wraps a predicate; if inner is False → Exists is False.
    let schema = int_schema();
    let inner = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let pred = Predicate::Exists(Box::new(inner));
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    // id=5 < 0 → inner False → Exists False
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(5)).unwrap(), TriValue::False);
}

#[test]
fn test_exists_wrapping_unknown_inner_returns_false() {
    // Exists maps: True → True, anything else (including Unknown) → False.
    // (See evaluate_predicate: `_ => Ok(TriValue::False)`)
    let schema = int_schema();
    let inner = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Null)),
    );
    let pred = Predicate::Exists(Box::new(inner));
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    // Inner evaluates to Unknown (col vs NULL); Exists maps Unknown → False.
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::False);
}
