// ============================================================================
// test_basic.rs — Basic predicate correctness for SelectionExecutor
//
// Covers: >, <, >=, <=, =, !=, AND, OR, NOT, BETWEEN, multi-column schemas,
//         filter_tuples, count_matching_tuples, error paths.
// ============================================================================

use storage_manager::catalog::types::{Column, Table};
use storage_manager::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
    SelectionExecutor, TriValue,
    filter_tuples, count_matching_tuples,
};
use storage_manager::types::{DataType, serialize_nullable_row};

// ── helpers ───────────────────────────────────────────────────────────────────

fn int_table(col_name: &str) -> (Vec<DataType>, Table) {
    let schema = vec![DataType::Int];
    let table = Table {
        columns: vec![Column::new(col_name.to_string(), DataType::Int)],
    };
    (schema, table)
}

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column(ColumnReference::new(name.to_string())))
}

fn int_c(v: i32) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Int(v)))
}

fn int_row(val: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(&val.to_string())]).unwrap()
}

// ── greater-than ─────────────────────────────────────────────────────────────

#[test]
fn basic_greater_than_true() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(18));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(25, &schema)).unwrap(), TriValue::True);
}

#[test]
fn basic_greater_than_false() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(18));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(10, &schema)).unwrap(), TriValue::False);
}

#[test]
fn basic_greater_than_boundary_is_false() {
    // age > 18 where age == 18 → strictly greater, so False
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(18));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(18, &schema)).unwrap(), TriValue::False);
}

#[test]
fn basic_greater_or_equal_boundary_is_true() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::GreaterOrEqual, int_c(18));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(18, &schema)).unwrap(), TriValue::True);
}

// ── less-than ─────────────────────────────────────────────────────────────────

#[test]
fn basic_less_than_true_false_boundary() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::LessThan, int_c(100));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(50, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(100, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(101, &schema)).unwrap(), TriValue::False);
}

#[test]
fn basic_less_or_equal_boundary() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::LessOrEqual, int_c(50));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(50, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(51, &schema)).unwrap(), TriValue::False);
}

// ── equality / inequality ─────────────────────────────────────────────────────

#[test]
fn basic_equals_match_and_no_match() {
    let (schema, table) = int_table("salary");
    let pred = Predicate::Compare(col("salary"), ComparisonOp::Equals, int_c(1000));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(1000, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(999, &schema)).unwrap(), TriValue::False);
}

#[test]
fn basic_not_equals() {
    let (schema, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::NotEquals, int_c(42));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(42, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(43, &schema)).unwrap(), TriValue::True);
}

#[test]
fn negative_integer_comparison() {
    let (schema, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::LessThan, int_c(0));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(-1, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(0, &schema)).unwrap(), TriValue::False);
}

// ── filter_tuples / count_matching ───────────────────────────────────────────

#[test]
fn filter_tuples_correct_count_and_exclusion() {
    let (schema, table) = int_table("age");
    let pred = Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(18));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let rows = vec![
        int_row(10, &schema),  // False
        int_row(19, &schema),  // True
        int_row(5, &schema),   // False
        int_row(30, &schema),  // True
        int_row(18, &schema),  // False (boundary)
    ];
    let result = filter_tuples(&exec, &rows).unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn count_matching_tuples_all_pass() {
    let (schema, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::GreaterOrEqual, int_c(0));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let rows: Vec<Vec<u8>> = (0..100).map(|i| int_row(i, &schema)).collect();
    assert_eq!(count_matching_tuples(&exec, &rows).unwrap(), 100);
}

#[test]
fn count_matching_tuples_none_pass() {
    let (schema, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::GreaterThan, int_c(1_000_000));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let rows: Vec<Vec<u8>> = (0..10).map(|i| int_row(i, &schema)).collect();
    assert_eq!(count_matching_tuples(&exec, &rows).unwrap(), 0);
}

#[test]
fn filter_empty_input_returns_empty() {
    let (_, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::GreaterThan, int_c(0));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert!(filter_tuples(&exec, &[]).unwrap().is_empty());
}

// ── AND / OR / NOT ────────────────────────────────────────────────────────────

#[test]
fn and_predicate_both_must_hold() {
    // age > 18 AND age < 65
    let (schema, table) = int_table("age");
    let pred = Predicate::and(
        Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(18)),
        Predicate::Compare(col("age"), ComparisonOp::LessThan, int_c(65)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(17, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(19, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(64, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(65, &schema)).unwrap(), TriValue::False);
}

#[test]
fn or_predicate_either_satisfies() {
    // age < 10 OR age > 90
    let (schema, table) = int_table("age");
    let pred = Predicate::or(
        Predicate::Compare(col("age"), ComparisonOp::LessThan, int_c(10)),
        Predicate::Compare(col("age"), ComparisonOp::GreaterThan, int_c(90)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(5, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(95, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(50, &schema)).unwrap(), TriValue::False);
}

#[test]
fn not_predicate_inverts_result() {
    let (schema, table) = int_table("x");
    let pred = Predicate::not(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(42)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(42, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(99, &schema)).unwrap(), TriValue::True);
}

// ── BETWEEN (expanded to AND at plan time) ────────────────────────────────────

#[test]
fn between_expands_correctly() {
    // BETWEEN 18 AND 65 → AND(>= 18, <= 65)
    let (schema, table) = int_table("age");
    let pred = Predicate::Between(col("age"), int_c(18), int_c(65));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(18, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(65, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(17, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(66, &schema)).unwrap(), TriValue::False);
}

// ── Error paths ───────────────────────────────────────────────────────────────

#[test]
fn unknown_column_name_returns_err_at_construction() {
    let (_, table) = int_table("age");
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("nonexistent".to_string()))),
        ComparisonOp::Equals,
        int_c(1),
    );
    let result = SelectionExecutor::new(pred, table);
    assert!(result.is_err());
    // Extract the error message string without requiring Debug on SelectionExecutor
    let err_msg = result.err().unwrap();
    assert!(err_msg.contains("not found"), "expected 'not found' in: {err_msg}");
}

#[test]
fn malformed_row_too_short_returns_err() {
    let (_, table) = int_table("x");
    let pred = Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(0));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    // Only 2 bytes — header requires 4
    let bad = vec![0u8, 0u8];
    assert!(exec.evaluate_tuple(&bad).is_err());
}

// ── Multi-column: only referenced column is consulted ─────────────────────────

#[test]
fn multi_column_schema_reads_correct_column() {
    let schema = vec![DataType::Int, DataType::Int, DataType::Int];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), DataType::Int),
            Column::new("b".to_string(), DataType::Int),
            Column::new("c".to_string(), DataType::Int),
        ],
    };
    // Filter on 'b' (index 1) only
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("b".to_string()))),
        ComparisonOp::Equals,
        int_c(99),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();

    let row_b_match = serialize_nullable_row(&schema, &[Some("1"), Some("99"), Some("999")]).unwrap();
    let row_b_miss  = serialize_nullable_row(&schema, &[Some("99"), Some("0"), Some("99")]).unwrap();

    assert_eq!(exec.evaluate_tuple(&row_b_match).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&row_b_miss).unwrap(), TriValue::False);
}
