//! Integration tests for Nested Loop Join (NLJ).
//! Creates a fresh test database, inserts known data, and verifies join results.

use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::catalog::{create_database, create_table, save_catalog, load_catalog, init_catalog};
use storage_manager::heap::{init_table, insert_tuple};
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;

fn setup_test_db() -> String {
    let db_name = "test_nlj_db";
    init_catalog();
    let mut catalog = load_catalog();

    // Remove existing test database if present
    catalog.databases.remove(db_name);
    save_catalog(&catalog);

    // Clean up existing database directory
    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);

    // Create database
    create_database(&mut catalog, db_name);

    // Create employees table (id INT, dept_id INT, name TEXT)
    create_table(&mut catalog, db_name, "employees", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "dept_id".to_string(), data_type: "INT".to_string() },
        Column { name: "name".to_string(), data_type: "TEXT".to_string() },
    ]);

    // Create departments table (id INT, dname TEXT)
    create_table(&mut catalog, db_name, "departments", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "dname".to_string(), data_type: "TEXT".to_string() },
    ]);

    // Insert employees
    let emp_path = format!("database/base/{}/employees.dat", db_name);
    let mut emp_file = OpenOptions::new().read(true).write(true).open(&emp_path).unwrap();
    insert_tuple(&mut emp_file, &make_emp_tuple(1, 10, "Alice")).unwrap();
    insert_tuple(&mut emp_file, &make_emp_tuple(2, 20, "Bob")).unwrap();
    insert_tuple(&mut emp_file, &make_emp_tuple(3, 10, "Charlie")).unwrap();
    insert_tuple(&mut emp_file, &make_emp_tuple(4, 30, "Diana")).unwrap();

    // Insert departments
    let dept_path = format!("database/base/{}/departments.dat", db_name);
    let mut dept_file = OpenOptions::new().read(true).write(true).open(&dept_path).unwrap();
    insert_tuple(&mut dept_file, &make_dept_tuple(10, "Engineering")).unwrap();
    insert_tuple(&mut dept_file, &make_dept_tuple(20, "Marketing")).unwrap();
    insert_tuple(&mut dept_file, &make_dept_tuple(40, "Sales")).unwrap();

    db_name.to_string()
}

fn make_emp_tuple(id: i32, dept_id: i32, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    bytes.extend_from_slice(&dept_id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    if name_bytes.len() > 10 { name_bytes.truncate(10); }
    else if name_bytes.len() < 10 { name_bytes.extend(vec![b' '; 10 - name_bytes.len()]); }
    bytes.extend_from_slice(&name_bytes);
    bytes
}

fn make_dept_tuple(id: i32, dname: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = dname.as_bytes().to_vec();
    if name_bytes.len() > 10 { name_bytes.truncate(10); }
    else if name_bytes.len() < 10 { name_bytes.extend(vec![b' '; 10 - name_bytes.len()]); }
    bytes.extend_from_slice(&name_bytes);
    bytes
}

#[test]
fn test_nlj_inner_join() {
    let db = setup_test_db();
    let catalog = load_catalog();

    let executor = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: vec![JoinCondition {
            left_table: "employees".to_string(),
            left_col: "dept_id".to_string(),
            operator: JoinOp::Eq,
            right_table: "departments".to_string(),
            right_col: "id".to_string(),
        }],
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Simple,
    };

    let result = executor.execute(&db, &catalog).unwrap();
    // Employees 1,3 match dept 10 (Engineering), employee 2 matches dept 20 (Marketing)
    // Employee 4 (dept_id=30) has no match, dept 40 (Sales) has no match
    assert_eq!(result.tuples.len(), 3, "Inner join should produce 3 matching tuples");
}

#[test]
fn test_nlj_left_outer_join() {
    let db = setup_test_db();
    let catalog = load_catalog();

    let executor = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: vec![JoinCondition {
            left_table: "employees".to_string(),
            left_col: "dept_id".to_string(),
            operator: JoinOp::Eq,
            right_table: "departments".to_string(),
            right_col: "id".to_string(),
        }],
        join_type: JoinType::LeftOuter,
        block_size: 2,
        mode: NLJMode::Simple,
    };

    let result = executor.execute(&db, &catalog).unwrap();
    // All 4 employees appear: 3 matched + 1 unmatched (Diana with NULLs)
    assert_eq!(result.tuples.len(), 4, "Left outer join should produce 4 tuples");
}

#[test]
fn test_nlj_cross_join() {
    let db = setup_test_db();
    let catalog = load_catalog();

    let executor = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: vec![],
        join_type: JoinType::Cross,
        block_size: 2,
        mode: NLJMode::Simple,
    };

    let result = executor.execute(&db, &catalog).unwrap();
    // 4 employees × 3 departments = 12
    assert_eq!(result.tuples.len(), 12, "Cross join should produce 4*3=12 tuples");
}

#[test]
fn test_nlj_no_matches() {
    let db = setup_test_db();
    let catalog = load_catalog();

    // Join on a condition that will never match
    let executor = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: vec![JoinCondition {
            left_table: "employees".to_string(),
            left_col: "id".to_string(), // employee ids are 1-4
            operator: JoinOp::Eq,
            right_table: "departments".to_string(),
            right_col: "id".to_string(), // department ids are 10, 20, 40
        }],
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Simple,
    };

    let result = executor.execute(&db, &catalog).unwrap();
    assert_eq!(result.tuples.len(), 0, "No matching tuples expected");
}

#[test]
fn test_nlj_block_mode_matches_simple() {
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "employees".to_string(),
        left_col: "dept_id".to_string(),
        operator: JoinOp::Eq,
        right_table: "departments".to_string(),
        right_col: "id".to_string(),
    }];

    let simple = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: condition.clone(),
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Simple,
    };

    let block = NLJExecutor {
        outer_table: "employees".to_string(),
        inner_table: "departments".to_string(),
        conditions: condition,
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Block,
    };

    let simple_result = simple.execute(&db, &catalog).unwrap();
    let block_result = block.execute(&db, &catalog).unwrap();

    assert_eq!(simple_result.tuples.len(), block_result.tuples.len(),
        "Block NLJ should produce same count as Simple NLJ");
}
