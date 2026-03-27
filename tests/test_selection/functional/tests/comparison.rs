// Comparison and coercion tests
// Tests float precision, INT↔FLOAT promotion, and NULL comparison semantics.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table { columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }] }
}

fn float_schema() -> Table {
    Table { columns: vec![Column { name: "price".to_string(), data_type: "FLOAT".to_string() }] }
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
    t[8] = 0b00000001; // NULL bitmap: column 0 is NULL
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t
}

fn make_float_tuple(price: f64) -> Vec<u8> {
    let num_cols = 1usize;
    let header_size = 8usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 8;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&8u32.to_le_bytes());
    t[data_start..data_start+8].copy_from_slice(&price.to_le_bytes());
    t
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_int_equals_int_constant() {
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(43)).unwrap(), TriValue::False);
}

#[test]
fn test_int_column_vs_float_constant_promoted() {
    // INT column (42) vs Float constant (42.0) — promoted to float for comparison.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Float(42.0))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::True);
}

#[test]
fn test_int_column_vs_float_constant_not_equal() {
    // INT 42 vs Float 42.5 — 42 != 42.5 after coercion.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Float(42.5))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::False);
}

#[test]
fn test_float_column_greater_than_int_constant() {
    // FLOAT col (60.5) > INT constant (60) — promoted comparison. True.
    let schema = float_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(60))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_float_tuple(60.5)).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_float_tuple(59.9)).unwrap(), TriValue::False);
}

#[test]
fn test_float_distinct_values() {
    // 1.5 > 1.0 → True; 0.5 > 1.0 → False. Both survive f64 round-trip.
    let schema = float_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(1.0))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_float_tuple(1.5)).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_float_tuple(0.5)).unwrap(), TriValue::False);
    assert_eq!(ex.evaluate_tuple(&make_float_tuple(1.0)).unwrap(), TriValue::False);
}

#[test]
fn test_null_column_comparison_any_op_returns_unknown() {
    // NULL column compared with any constant → Unknown, regardless of operator.
    let schema = int_schema();
    for op in [
        ComparisonOp::Equals,
        ComparisonOp::NotEquals,
        ComparisonOp::LessThan,
        ComparisonOp::GreaterThan,
        ComparisonOp::LessOrEqual,
        ComparisonOp::GreaterOrEqual,
    ] {
        let pred = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            op,
            Box::new(Expr::Constant(Constant::Int(100))),
        );
        let ex = SelectionExecutor::new(pred, int_schema()).unwrap();
        assert_eq!(
            ex.evaluate_tuple(&make_null_int_tuple()).unwrap(),
            TriValue::Unknown,
            "NULL column vs constant should always be Unknown"
        );
    }
}

#[test]
fn test_compare_with_null_constant_returns_unknown() {
    // Non-null column compared with Null constant → Unknown.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Null)),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    // Even a non-null value compared with NULL constant → Unknown.
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(42)).unwrap(), TriValue::Unknown);
}

#[test]
fn test_negative_int_comparison() {
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let ex = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(-1)).unwrap(), TriValue::True);
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(0)).unwrap(),  TriValue::False);
    assert_eq!(ex.evaluate_tuple(&make_int_tuple(1)).unwrap(),  TriValue::False);
}
