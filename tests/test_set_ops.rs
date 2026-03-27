//! Tests for UNION, INTERSECT, EXCEPT.

use storage_manager::executor::projection::{OutputColumn, ResultTable};
use storage_manager::executor::set_ops::{except, intersect, union};
use storage_manager::executor::value::Value;
use storage_manager::catalog::types::DataType;

fn make_table(rows: Vec<Vec<Value>>) -> ResultTable {
    ResultTable {
        columns: vec![
            OutputColumn { name: "a".to_string(), data_type: DataType::Int },
            OutputColumn { name: "b".to_string(), data_type: DataType::Text },
        ],
        rows,
    }
}

fn int_row(n: i64, s: &str) -> Vec<Value> {
    vec![Value::Int(n), Value::Text(s.to_string())]
}

// ─── UNION ──────────────────────────────────────────────────────────────────

#[test]
fn test_union_dedup() {
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b")]);
    let b = make_table(vec![int_row(2, "b"), int_row(3, "c")]);
    let result = union(a, b, false).unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_union_all_keeps_duplicates() {
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b")]);
    let b = make_table(vec![int_row(2, "b"), int_row(3, "c")]);
    let result = union(a, b, true).unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn test_union_empty_tables() {
    let a = make_table(vec![]);
    let b = make_table(vec![]);
    let result = union(a, b, false).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_union_schema_mismatch() {
    let a = make_table(vec![int_row(1, "a")]);
    let b = ResultTable {
        columns: vec![OutputColumn { name: "x".to_string(), data_type: DataType::Int }],
        rows: vec![vec![Value::Int(1)]],
    };
    assert!(union(a, b, false).is_err());
}

// ─── INTERSECT ──────────────────────────────────────────────────────────────

#[test]
fn test_intersect() {
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b"), int_row(3, "c")]);
    let b = make_table(vec![int_row(2, "b"), int_row(3, "c"), int_row(4, "d")]);
    let result = intersect(a, b, false).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.contains(&int_row(2, "b")));
    assert!(result.rows.contains(&int_row(3, "c")));
}

#[test]
fn test_intersect_no_common_rows() {
    let a = make_table(vec![int_row(1, "a")]);
    let b = make_table(vec![int_row(2, "b")]);
    let result = intersect(a, b, false).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_intersect_all() {
    // INTERSECT ALL: (1,a),(2,b),(2,b) ∩ (2,b),(2,b),(3,c) → (2,b),(2,b)
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b"), int_row(2, "b")]);
    let b = make_table(vec![int_row(2, "b"), int_row(2, "b"), int_row(3, "c")]);
    let result = intersect(a, b, true).unwrap();
    assert_eq!(result.rows.len(), 2);
}

// ─── EXCEPT ─────────────────────────────────────────────────────────────────

#[test]
fn test_except() {
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b"), int_row(3, "c")]);
    let b = make_table(vec![int_row(2, "b")]);
    let result = except(a, b, false).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(!result.rows.contains(&int_row(2, "b")));
}

#[test]
fn test_except_removes_all() {
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b")]);
    let b = make_table(vec![int_row(1, "a"), int_row(2, "b")]);
    let result = except(a, b, false).unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_except_all() {
    // EXCEPT ALL: (1,a),(2,b),(2,b) EXCEPT ALL (2,b) → (1,a),(2,b) [removes one occurrence]
    let a = make_table(vec![int_row(1, "a"), int_row(2, "b"), int_row(2, "b")]);
    let b = make_table(vec![int_row(2, "b")]);
    let result = except(a, b, true).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!(result.rows.contains(&int_row(1, "a")));
    assert!(result.rows.contains(&int_row(2, "b")));
}
