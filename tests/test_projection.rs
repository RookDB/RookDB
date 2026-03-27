//! End-to-end projection tests using an in-memory ResultTable (no disk I/O).
//! Tests SELECT *, SELECT expr, computed columns, DISTINCT, CTEs.

use std::collections::HashMap;
use storage_manager::executor::expr::Expr;
use storage_manager::executor::projection::{
    apply_distinct, eval_projection_list, filter_rows, OutputColumn, ProjectionItem, ResultTable,
};
use storage_manager::executor::value::Value;
use storage_manager::catalog::types::{Column, DataType};

fn col(name: &str, dt: &str) -> Column {
    Column { name: name.to_string(), data_type: dt.to_string() }
}

fn out_col(name: &str) -> OutputColumn {
    OutputColumn { name: name.to_string(), data_type: DataType::Text }
}

fn make_schema() -> Vec<Column> {
    vec![col("id", "INT"), col("name", "TEXT"), col("salary", "INT")]
}

fn make_rows() -> Vec<Vec<Value>> {
    vec![
        vec![Value::Int(1), Value::Text("Alice".to_string()), Value::Int(1000)],
        vec![Value::Int(2), Value::Text("Bob".to_string()), Value::Int(2000)],
        vec![Value::Int(3), Value::Text("Carol".to_string()), Value::Int(1500)],
    ]
}

// ─── filter_rows ────────────────────────────────────────────────────────────

#[test]
fn test_filter_no_predicate() {
    let rows = make_rows();
    let result = filter_rows(rows.clone(), &None).unwrap();
    assert_eq!(result.len(), 3);
}

#[test]
fn test_filter_with_predicate() {
    let rows = make_rows();
    // salary > 1000
    let pred = Expr::gt(Expr::col(2), Expr::int(1000));
    let result = filter_rows(rows, &Some(pred)).unwrap();
    assert_eq!(result.len(), 2); // Bob and Carol
}

#[test]
fn test_filter_empty_result() {
    let rows = make_rows();
    // id > 999 → no rows
    let pred = Expr::gt(Expr::col(0), Expr::int(999));
    let result = filter_rows(rows, &Some(pred)).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_filter_on_empty_table() {
    let result = filter_rows(vec![], &Some(Expr::bool_val(true))).unwrap();
    assert!(result.is_empty());
}

// ─── eval_projection_list ───────────────────────────────────────────────────

#[test]
fn test_select_star() {
    let schema = make_schema();
    let rows = make_rows();
    let items = vec![ProjectionItem::Star];
    let (cols, out_rows) = eval_projection_list(rows, &items, &schema).unwrap();
    assert_eq!(cols.len(), 3);
    assert_eq!(out_rows.len(), 3);
    assert_eq!(out_rows[0][0], Value::Int(1));
}

#[test]
fn test_select_single_column() {
    let schema = make_schema();
    let rows = make_rows();
    // SELECT name
    let items = vec![ProjectionItem::Expr(Expr::col(1), "name".to_string())];
    let (cols, out_rows) = eval_projection_list(rows, &items, &schema).unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].name, "name");
    assert_eq!(out_rows[0][0], Value::Text("Alice".to_string()));
}

#[test]
fn test_select_computed_column() {
    let schema = make_schema();
    let rows = make_rows();
    // SELECT salary * 2 AS double_salary
    let items = vec![
        ProjectionItem::Expr(
            Expr::mul(Expr::col(2), Expr::int(2)),
            "double_salary".to_string(),
        ),
    ];
    let (cols, out_rows) = eval_projection_list(rows, &items, &schema).unwrap();
    assert_eq!(cols[0].name, "double_salary");
    assert_eq!(out_rows[0][0], Value::Int(2000));
    assert_eq!(out_rows[1][0], Value::Int(4000));
}

#[test]
fn test_select_mix_star_and_expr() {
    let schema = make_schema();
    let rows = make_rows();
    // SELECT *, salary + 100 AS bonus
    let items = vec![
        ProjectionItem::Star,
        ProjectionItem::Expr(Expr::add(Expr::col(2), Expr::int(100)), "bonus".to_string()),
    ];
    let (cols, out_rows) = eval_projection_list(rows, &items, &schema).unwrap();
    assert_eq!(cols.len(), 4); // id, name, salary, bonus
    assert_eq!(out_rows[0][3], Value::Int(1100));
}

#[test]
fn test_projection_on_empty_table() {
    let schema = make_schema();
    let items = vec![ProjectionItem::Star];
    let (cols, out_rows) = eval_projection_list(vec![], &items, &schema).unwrap();
    assert_eq!(cols.len(), 3);
    assert!(out_rows.is_empty());
}

// ─── apply_distinct ─────────────────────────────────────────────────────────

#[test]
fn test_distinct_removes_duplicates() {
    let rows = vec![
        vec![Value::Int(1), Value::Text("a".to_string())],
        vec![Value::Int(2), Value::Text("b".to_string())],
        vec![Value::Int(1), Value::Text("a".to_string())], // duplicate
        vec![Value::Int(3), Value::Text("c".to_string())],
    ];
    let result = apply_distinct(rows);
    assert_eq!(result.len(), 3);
}

#[test]
fn test_distinct_no_duplicates() {
    let rows = make_rows();
    let result = apply_distinct(rows.clone());
    assert_eq!(result.len(), rows.len());
}

#[test]
fn test_distinct_all_duplicates() {
    let row = vec![Value::Int(1)];
    let rows = vec![row.clone(), row.clone(), row.clone()];
    let result = apply_distinct(rows);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_distinct_empty() {
    assert!(apply_distinct(vec![]).is_empty());
}

// ─── CTE-style test (using cte_tables map directly) ─────────────────────────

#[test]
fn test_cte_table_filter() {
    // Simulate a CTE named "high_earners" that already has rows in memory.
    let cte_rows = vec![
        vec![Value::Int(2), Value::Text("Bob".to_string()), Value::Int(2000)],
    ];
    let cte = ResultTable {
        columns: vec![
            OutputColumn { name: "id".to_string(), data_type: DataType::Int },
            OutputColumn { name: "name".to_string(), data_type: DataType::Text },
            OutputColumn { name: "salary".to_string(), data_type: DataType::Int },
        ],
        rows: cte_rows,
    };

    // Filter CTE rows: salary > 1500
    let pred = Expr::gt(Expr::col(2), Expr::int(1500));
    let filtered = filter_rows(cte.rows, &Some(pred)).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0][1], Value::Text("Bob".to_string()));
}
