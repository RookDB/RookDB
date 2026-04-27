// ============================================================================
// test_streaming.rs — Streaming and iterator filter APIs
//
// Covers:
//   filter_tuples_streaming:
//     - Callback called only for True rows (not for False or Unknown)
//     - Returned count matches callback call count
//     - Error from iterator propagates as Err, halts iteration
//     - Empty iterator → Ok(0)
//     - All rows match → count = len(input)
//     - No rows match → count = 0
//
//   filter_iter:
//     - Produces only True rows as Ok items
//     - Does NOT yield False or Unknown rows
//     - Error from inner iterator propagated as Err item
//     - evaluate_tuple error propagated as Err item
//     - Empty input → empty output
//     - Produces same result as filter_tuples (consistency)
//
//   filter_tuples_detailed:
//     - Correctly separates matched / rejected / unknown buckets
//     - All True  → matched=n, rejected=0, unknown=0
//     - All False → matched=0, rejected=n, unknown=0
//     - All Unknown → matched=0, rejected=0, unknown=n
// ============================================================================

use storage_manager::catalog::types::{Column, Table};
use storage_manager::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
    SelectionExecutor,
    filter_tuples, filter_tuples_detailed, filter_tuples_streaming, filter_iter,
};
use storage_manager::types::{DataType, serialize_nullable_row};

// ── helpers ───────────────────────────────────────────────────────────────────

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column(ColumnReference::new(name.to_string())))
}
fn int_c(v: i32) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Int(v)))
}

fn int_table() -> (Vec<DataType>, Table) {
    (
        vec![DataType::Int],
        Table { columns: vec![Column::new("v".to_string(), DataType::Int)] },
    )
}

fn int_row(v: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(&v.to_string())]).unwrap()
}

fn null_row(schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[None]).unwrap()
}

/// Builds an executor for `v > threshold`
fn gt_exec(threshold: i32) -> (Vec<DataType>, SelectionExecutor) {
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("v"), ComparisonOp::GreaterThan, int_c(threshold));
    let exec = SelectionExecutor::new(pred, table).unwrap();
    (schema, exec)
}

// ── filter_tuples_streaming ───────────────────────────────────────────────────

#[test]
fn streaming_callback_invoked_only_for_true_rows() {
    let (schema, exec) = gt_exec(10);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Ok(int_row(5, &schema)),    // v=5  → False
        Ok(int_row(15, &schema)),   // v=15 → True  ✓
        Ok(int_row(3, &schema)),    // v=3  → False
        Ok(int_row(20, &schema)),   // v=20 → True  ✓
    ];

    let mut seen_count = 0usize;
    let count = filter_tuples_streaming(&exec, rows.into_iter(), |_| {
        seen_count += 1;
    }).unwrap();

    assert_eq!(count, 2);
    assert_eq!(seen_count, 2);
}

#[test]
fn streaming_unknown_rows_not_passed_to_callback() {
    let (schema, exec) = gt_exec(10);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Ok(null_row(&schema)),    // Unknown — must NOT call callback
        Ok(int_row(20, &schema)), // True   — must call callback
        Ok(null_row(&schema)),    // Unknown
    ];
    let mut cb_count = 0usize;
    let count = filter_tuples_streaming(&exec, rows.into_iter(), |_| {
        cb_count += 1;
    }).unwrap();
    assert_eq!(count, 1);
    assert_eq!(cb_count, 1);
}

#[test]
fn streaming_empty_iterator_returns_zero() {
    let (_, exec) = gt_exec(0);
    let count = filter_tuples_streaming(
        &exec,
        std::iter::empty::<Result<Vec<u8>, String>>(),
        |_| {},
    ).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn streaming_all_rows_match() {
    let (schema, exec) = gt_exec(-1_000_000); // v > -1M → all positive integers match
    let rows: Vec<Result<Vec<u8>, String>> =
        (0..50).map(|i| Ok(int_row(i, &schema))).collect();
    let count = filter_tuples_streaming(&exec, rows.into_iter(), |_| {}).unwrap();
    assert_eq!(count, 50);
}

#[test]
fn streaming_no_rows_match() {
    let (schema, exec) = gt_exec(1_000_000); // v > 1M → nothing matches
    let rows: Vec<Result<Vec<u8>, String>> =
        (0..20).map(|i| Ok(int_row(i, &schema))).collect();
    let count = filter_tuples_streaming(&exec, rows.into_iter(), |_| {}).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn streaming_error_from_iterator_propagates() {
    let (_, exec) = gt_exec(10);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Err("simulated I/O error".to_string()),
    ];
    let result = filter_tuples_streaming(&exec, rows.into_iter(), |_| {});
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("I/O error"));
}

#[test]
fn streaming_error_after_some_matches_propagates() {
    let (schema, exec) = gt_exec(10);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Ok(int_row(15, &schema)),                          // True
        Err("disk failure".to_string()),                   // Error
        Ok(int_row(20, &schema)),                          // would be True, but error stops us
    ];
    let result = filter_tuples_streaming(&exec, rows.into_iter(), |_| {});
    assert!(result.is_err());
}

// ── filter_iter ───────────────────────────────────────────────────────────────

#[test]
fn filter_iter_yields_only_matching_rows() {
    let (schema, exec) = gt_exec(5);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Ok(int_row(3, &schema)),    // False
        Ok(int_row(10, &schema)),   // True  ✓
        Ok(int_row(1, &schema)),    // False
        Ok(int_row(7, &schema)),    // True  ✓
        Ok(int_row(6, &schema)),    // True  ✓
    ];
    let result: Result<Vec<Vec<u8>>, String> =
        filter_iter(&exec, rows.into_iter()).collect();
    assert_eq!(result.unwrap().len(), 3);
}

#[test]
fn filter_iter_does_not_yield_unknown_rows() {
    let (schema, exec) = gt_exec(5);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Ok(null_row(&schema)),    // Unknown — must NOT appear in output
        Ok(int_row(10, &schema)), // True    ✓
    ];
    let result: Result<Vec<Vec<u8>>, String> =
        filter_iter(&exec, rows.into_iter()).collect();
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn filter_iter_consistent_with_filter_tuples() {
    let (schema, exec) = gt_exec(50);
    let rows: Vec<Vec<u8>> = (0..100).map(|i| int_row(i, &schema)).collect();

    let batch   = filter_tuples(&exec, &rows).unwrap();
    let iter_in: Vec<Result<Vec<u8>, String>> = rows.into_iter().map(Ok).collect();
    let via_iter: Result<Vec<Vec<u8>>, String> =
        filter_iter(&exec, iter_in.into_iter()).collect();

    assert_eq!(batch, via_iter.unwrap());
}

#[test]
fn filter_iter_error_from_inner_iterator_is_err_item() {
    let (_, exec) = gt_exec(0);
    let rows: Vec<Result<Vec<u8>, String>> = vec![
        Err("read failure".to_string()),
    ];
    let results: Vec<Result<Vec<u8>, String>> =
        filter_iter(&exec, rows.into_iter()).collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
    assert!(results[0].as_ref().unwrap_err().contains("read failure"));
}

#[test]
fn filter_iter_evaluate_error_is_err_item() {
    // Row bytes too short → evaluate_tuple returns Err → appears as Err item in iter
    let (_, exec) = gt_exec(0);
    let bad_row: Vec<u8> = vec![0u8]; // only 1 byte — header needs at least 4
    let rows: Vec<Result<Vec<u8>, String>> = vec![Ok(bad_row)];
    let results: Vec<Result<Vec<u8>, String>> =
        filter_iter(&exec, rows.into_iter()).collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].is_err());
}

#[test]
fn filter_iter_empty_input_yields_nothing() {
    let (_, exec) = gt_exec(0);
    let results: Vec<Result<Vec<u8>, String>> =
        filter_iter(&exec, std::iter::empty()).collect();
    assert!(results.is_empty());
}

// ── filter_tuples_detailed ────────────────────────────────────────────────────

#[test]
fn detailed_correctly_separates_all_three_buckets() {
    // v > 10: [5→F, 15→T, NULL→U, 20→T, 3→F]
    let (schema, table) = int_table();
    let pred = Predicate::Compare(col("v"), ComparisonOp::GreaterThan, int_c(10));
    let exec = SelectionExecutor::new(pred, table).unwrap();

    let rows = vec![
        int_row(5, &schema),    // False
        int_row(15, &schema),   // True
        null_row(&schema),      // Unknown
        int_row(20, &schema),   // True
        int_row(3, &schema),    // False
    ];
    let (matched, rejected, unknown) = filter_tuples_detailed(&exec, rows).unwrap();
    assert_eq!(matched.len(),  2, "matched");
    assert_eq!(rejected.len(), 2, "rejected");
    assert_eq!(unknown.len(),  1, "unknown");
}

#[test]
fn detailed_all_match() {
    let (schema, table) = int_table();
    let exec = SelectionExecutor::new(
        Predicate::Compare(col("v"), ComparisonOp::GreaterThan, int_c(-1)),
        table,
    ).unwrap();
    let rows: Vec<Vec<u8>> = (0..5).map(|i| int_row(i, &schema)).collect();
    let (m, r, u) = filter_tuples_detailed(&exec, rows).unwrap();
    assert_eq!((m.len(), r.len(), u.len()), (5, 0, 0));
}

#[test]
fn detailed_all_rejected() {
    let (schema, table) = int_table();
    let exec = SelectionExecutor::new(
        Predicate::Compare(col("v"), ComparisonOp::GreaterThan, int_c(1000)),
        table,
    ).unwrap();
    let rows: Vec<Vec<u8>> = (0..5).map(|i| int_row(i, &schema)).collect();
    let (m, r, u) = filter_tuples_detailed(&exec, rows).unwrap();
    assert_eq!((m.len(), r.len(), u.len()), (0, 5, 0));
}

#[test]
fn detailed_all_unknown() {
    let (schema, table) = int_table();
    let exec = SelectionExecutor::new(
        Predicate::Compare(col("v"), ComparisonOp::Equals, int_c(42)),
        table,
    ).unwrap();
    let rows: Vec<Vec<u8>> = (0..4).map(|_| null_row(&schema)).collect();
    let (m, r, u) = filter_tuples_detailed(&exec, rows).unwrap();
    assert_eq!((m.len(), r.len(), u.len()), (0, 0, 4));
}
