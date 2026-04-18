//! Tests for projections over empty tables and NULL-heavy rows.

use storage_manager::executor::expr::Expr;
use storage_manager::executor::projection::{apply_distinct, eval_projection_list, filter_rows, ProjectionItem};
use storage_manager::executor::value::Value;
use storage_manager::catalog::types::Column;

fn col(name: &str, dt: &str) -> Column {
    Column { name: name.to_string(), data_type: dt.to_string() }
}

#[test]
fn test_filter_empty_table_returns_empty() {
    let pred = Expr::gt(Expr::col(0), Expr::int(0));
    let result = filter_rows(vec![], &Some(pred)).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_projection_empty_table_returns_correct_schema() {
    let schema = vec![col("id", "INT"), col("name", "TEXT")];
    let items = vec![ProjectionItem::Star];
    let (cols, rows) = eval_projection_list(vec![], &items, &schema).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[1].name, "name");
    assert!(rows.is_empty());
}

#[test]
fn test_distinct_empty_table() {
    let result = apply_distinct(vec![]);
    assert!(result.is_empty());
}

#[test]
fn test_filter_all_nulls_row() {
    // Row of all NULLs; any > predicate should return false (not error)
    let rows = vec![vec![Value::Null, Value::Null]];
    let pred = Expr::gt(Expr::col(0), Expr::int(0));
    let result = filter_rows(rows, &Some(pred)).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_filter_null_with_is_null() {
    // WHERE col IS NULL should match
    let rows = vec![
        vec![Value::Null],
        vec![Value::Int(1)],
    ];
    let pred = Expr::IsNull(Box::new(Expr::col(0)));
    let result = filter_rows(rows, &Some(pred)).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Value::Null);
}

#[test]
fn test_projection_with_null_values_in_expr() {
    // SELECT col + 1 where col is NULL → NULL
    let schema = vec![col("x", "INT")];
    let rows = vec![vec![Value::Null]];
    let items = vec![ProjectionItem::Expr(
        Expr::add(Expr::col(0), Expr::int(1)),
        "x_plus_1".to_string(),
    )];
    let (_, out_rows) = eval_projection_list(rows, &items, &schema).unwrap();
    assert_eq!(out_rows[0][0], Value::Null);
}

#[test]
fn test_no_predicate_returns_all_rows() {
    let rows = vec![
        vec![Value::Int(1)],
        vec![Value::Int(2)],
        vec![Value::Int(3)],
    ];
    let result = filter_rows(rows, &None).unwrap();
    assert_eq!(result.len(), 3);
}
