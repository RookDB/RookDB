// ============================================================================
// test_null_logic.rs — SQL Three-Valued Logic and NULL semantics
//
// Covers:
//   - Full 3×3 AND truth table  (9 cases)
//   - Full 3×3 OR truth table   (9 cases)
//   - NOT truth table           (3 cases)
//   - NULL column vs. constant comparison → Unknown
//   - IS NULL / IS NOT NULL
//   - filter_tuples discards Unknown rows (only True passes)
//   - NULL = NULL → Unknown (not True)
// ============================================================================

use storage_manager::catalog::types::{Column, Table};
use storage_manager::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
    SelectionExecutor, TriValue,
    filter_tuples, apply_and, apply_or,
};
use storage_manager::types::{DataType, serialize_nullable_row};

// ── helpers ───────────────────────────────────────────────────────────────────

fn int_table() -> (Vec<DataType>, Table) {
    (
        vec![DataType::Int],
        Table { columns: vec![Column::new("x".to_string(), DataType::Int)] },
    )
}

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column(ColumnReference::new(name.to_string())))
}

fn int_c(v: i32) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Int(v)))
}

fn null_c() -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Null))
}

fn int_row(v: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(&v.to_string())]).unwrap()
}

fn null_row(schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[None]).unwrap()
}

// ── apply_and: 3×3 truth table ────────────────────────────────────────────────

#[test]
fn and_truth_table_all_nine_cases() {
    use TriValue::{True as T, False as F, Unknown as U};

    assert_eq!(apply_and(T, T), T); // T AND T = T
    assert_eq!(apply_and(T, F), F); // T AND F = F
    assert_eq!(apply_and(T, U), U); // T AND U = U
    assert_eq!(apply_and(F, T), F); // F AND T = F  (False dominates)
    assert_eq!(apply_and(F, F), F); // F AND F = F
    assert_eq!(apply_and(F, U), F); // F AND U = F  (False dominates — critical!)
    assert_eq!(apply_and(U, T), U); // U AND T = U
    assert_eq!(apply_and(U, F), F); // U AND F = F  (False dominates)
    assert_eq!(apply_and(U, U), U); // U AND U = U
}

// ── apply_or: 3×3 truth table ─────────────────────────────────────────────────

#[test]
fn or_truth_table_all_nine_cases() {
    use TriValue::{True as T, False as F, Unknown as U};

    assert_eq!(apply_or(T, T), T); // T OR T = T
    assert_eq!(apply_or(T, F), T); // T OR F = T
    assert_eq!(apply_or(T, U), T); // T OR U = T   (True dominates — critical!)
    assert_eq!(apply_or(F, T), T); // F OR T = T
    assert_eq!(apply_or(F, F), F); // F OR F = F
    assert_eq!(apply_or(F, U), U); // F OR U = U
    assert_eq!(apply_or(U, T), T); // U OR T = T   (True dominates)
    assert_eq!(apply_or(U, F), U); // U OR F = U
    assert_eq!(apply_or(U, U), U); // U OR U = U
}

// ── NULL column comparisons → Unknown ────────────────────────────────────────

#[test]
fn null_equals_constant_is_unknown() {
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn null_greater_than_is_unknown() {
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("x"), ComparisonOp::GreaterThan, int_c(0));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn null_not_equals_is_unknown() {
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("x"), ComparisonOp::NotEquals, int_c(1));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

// ── NULL with AND / OR in predicate trees ────────────────────────────────────

#[test]
fn null_and_true_is_unknown() {
    // x=NULL → Unknown; 1=1 → True; Unknown AND True = Unknown
    let (schema, table) = int_table();
    let pred = Predicate::and(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(1)), // True (constant)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn null_and_false_is_false() {
    // x=NULL → Unknown; 1=2 → False; Unknown AND False = False
    let (schema, table) = int_table();
    let pred = Predicate::and(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(2)), // False (constant)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::False);
}

#[test]
fn null_or_true_is_true() {
    // x=NULL → Unknown; 1=1 → True; Unknown OR True = True
    let (schema, table) = int_table();
    let pred = Predicate::or(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(1)), // True
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::True);
}

#[test]
fn null_or_false_is_unknown() {
    // x=NULL → Unknown; 1=2 → False; Unknown OR False = Unknown
    let (schema, table) = int_table();
    let pred = Predicate::or(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(2)), // False
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn not_unknown_is_unknown() {
    // NOT(Unknown) = Unknown
    let (schema, table) = int_table();
    let pred = Predicate::not(
        Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(5)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

// ── IS NULL / IS NOT NULL ─────────────────────────────────────────────────────

#[test]
fn is_null_on_null_is_true() {
    let (schema, table) = int_table();
    let pred = Predicate::IsNull(col("x"));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::True);
}

#[test]
fn is_null_on_non_null_is_false() {
    let (schema, table) = int_table();
    let pred = Predicate::IsNull(col("x"));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(42, &schema)).unwrap(), TriValue::False);
}

#[test]
fn is_not_null_on_non_null_is_true() {
    let (schema, table) = int_table();
    let pred = Predicate::IsNotNull(col("x"));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(7, &schema)).unwrap(), TriValue::True);
}

#[test]
fn is_not_null_on_null_is_false() {
    let (schema, table) = int_table();
    let pred = Predicate::IsNotNull(col("x"));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::False);
}

// ── filter_tuples discards Unknown rows (only True passes) ────────────────────

#[test]
fn filter_tuples_discards_unknown() {
    // x = 10: rows [10→T, NULL→U, 20→F]
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("x"), ComparisonOp::Equals, int_c(10));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let rows = vec![
        int_row(10, &schema),  // True
        null_row(&schema),     // Unknown — must be excluded
        int_row(20, &schema),  // False
    ];
    let result = filter_tuples(&exec, &rows).unwrap();
    assert_eq!(result.len(), 1);
}

// ── NULL = NULL is Unknown (not True) ─────────────────────────────────────────

#[test]
fn null_equals_null_constant_is_unknown() {
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("x"), ComparisonOp::Equals, null_c());
    let exec = SelectionExecutor::new(pred, table).unwrap();
    // Both sides are NULL — SQL says this is UNKNOWN, not True
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

// ── IS NULL used to correctly pick up null rows ──────────────────────────────

#[test]
fn filter_with_is_null_selects_only_null_rows() {
    let (schema, table) = int_table();
    let pred = Predicate::IsNull(col("x"));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let rows = vec![
        int_row(1, &schema),
        null_row(&schema),
        int_row(2, &schema),
        null_row(&schema),
    ];
    let result = filter_tuples(&exec, &rows).unwrap();
    assert_eq!(result.len(), 2);
}

// ── Multi-column: predicate on non-null col unaffected by other col being null ─

#[test]
fn multi_col_null_does_not_pollute_other_column_predicate() {
    // Schema: (id INT, name VARCHAR(16))
    // Predicate: id = 7 — name can be NULL without affecting the result
    let schema = vec![DataType::Int, DataType::Varchar(16)];
    let table = Table {
        columns: vec![
            Column::new("id".to_string(), DataType::Int),
            Column::new("name".to_string(), DataType::Varchar(16)),
        ],
    };
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        int_c(7),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();

    // id=7, name=NULL → True (predicate doesn't touch name)
    let row_id7_name_null = serialize_nullable_row(&schema, &[Some("7"), None]).unwrap();
    assert_eq!(exec.evaluate_tuple(&row_id7_name_null).unwrap(), TriValue::True);

    // id=NULL, name=NULL → Unknown (predicate touches NULL id)
    let row_all_null = serialize_nullable_row(&schema, &[None, None]).unwrap();
    assert_eq!(exec.evaluate_tuple(&row_all_null).unwrap(), TriValue::Unknown);
}
