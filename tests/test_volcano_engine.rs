//! Volcano engine integration tests.
//!
//! Full pipeline: SQL string → rook-parser → logical planner → physical
//! planner → operator tree → collected tuples. Exercises WHERE filtering,
//! ORDER BY, LIMIT/OFFSET, DISTINCT and projections against a real heap file.
//!
//! Run with: cargo test --test test_volcano_engine -- --test-threads=1
//! (tests share the process working directory, so they serialise on a mutex)

mod common;

use common::load_catalog;
use rook_ast::{QueryPlan, SelectPlan};
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

/// Unique per-process workspace name so parallel test binaries never collide.
fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

/// Create an employees table (5 rows) and return the current catalog.
fn setup_table(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "employees",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("salary", DataType::Int),
            column("dept", DataType::Varchar(20)),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    for row in [
        vec!["1", "Alice", "85000", "eng"],
        vec!["2", "Bob", "62000", "sales"],
        vec!["3", "Cara", "75000", "eng"],
        vec!["4", "Dan", "48000", "hr"],
        vec!["5", "Eve", "91000", "sales"],
    ] {
        insert_single_tuple(&catalog, db, "employees", &row).unwrap();
    }
    load_catalog()
}

/// Parse a SELECT, run it through both planners and render tuples as strings.
fn run_select(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let plan = match parse_sql(sql).unwrap() {
        QueryPlan::Select(select) => select,
        other => panic!("expected Select plan, got {:?}", other),
    };
    run_select_plan(catalog, db, &plan)
}

fn run_select_plan(catalog: &Catalog, db: &str, select: &SelectPlan) -> Vec<Vec<String>> {
    // The SelectPlan is re-wrapped in a QueryPlan for the logical planner.
    let logical =
        storage_manager::planner::plan_query(&QueryPlan::Select(select.clone()), catalog, db)
            .expect("logical planning failed");

    let tuples: Vec<Tuple> =
        execute_plan_collect(&logical, catalog, db).expect("physical execution failed");

    tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(dv) => format!("{}", dv),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn select_all_rows() {
    let _ws = common::TestWorkspace::new("volcano", "all");
    let catalog = setup_table("emp_db");

    let out = run_select(&catalog, "emp_db", "SELECT * FROM employees");
    assert_eq!(out.len(), 5);
    // String values render with SQL single quotes.
    assert_eq!(out[0], vec!["1", "'Alice'", "85000", "'eng'"]);
}

#[test]
fn where_filter_and_projection() {
    let _ws = common::TestWorkspace::new("volcano", "where");
    let catalog = setup_table("emp_db");

    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT name FROM employees WHERE salary > 70000",
    );
    assert_eq!(out.len(), 3, "Alice, Cara and Eve earn more than 70000");
    assert!(out.iter().all(|r| r.len() == 1), "only 'name' projected");
    assert_eq!(out[0][0], "'Alice'");
}

#[test]
fn where_with_and_predicate() {
    let _ws = common::TestWorkspace::new("volcano", "and");
    let catalog = setup_table("emp_db");

    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT name FROM employees WHERE dept = 'eng' AND salary < 80000",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], "'Cara'");
}

#[test]
fn order_by_descending() {
    let _ws = common::TestWorkspace::new("volcano", "order");
    let catalog = setup_table("emp_db");

    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT name FROM employees ORDER BY salary DESC",
    );
    assert_eq!(out[0][0], "'Eve'");
    assert_eq!(out[4][0], "'Dan'");
}

#[test]
fn limit_and_offset() {
    let _ws = common::TestWorkspace::new("volcano", "limit");
    let catalog = setup_table("emp_db");

    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT id FROM employees ORDER BY id LIMIT 2 OFFSET 1",
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], "2");
    assert_eq!(out[1][0], "3");
}

#[test]
fn distinct_values() {
    let _ws = common::TestWorkspace::new("volcano", "distinct");
    let catalog = setup_table("emp_db");

    let out = run_select(&catalog, "emp_db", "SELECT DISTINCT dept FROM employees");
    let mut got: Vec<String> = out.iter().map(|r| r[0].clone()).collect();
    got.sort();
    assert_eq!(
        got,
        vec!["'eng'", "'hr'", "'sales'"],
        "DISTINCT must yield exactly these departments"
    );
}

#[test]
fn arithmetic_projection() {
    let _ws = common::TestWorkspace::new("volcano", "arith");
    let catalog = setup_table("emp_db");

    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT name, salary * 2 FROM employees WHERE id = 1",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], "170000");
}

#[test]
fn distinct_with_limit_and_offset() {
    let _ws = common::TestWorkspace::new("volcano", "dist_limit");
    let catalog = setup_table("emp_db");

    // All distinct depts are 'eng', 'hr', 'sales' (3 distinct).
    // ORDER BY dept LIMIT 2 OFFSET 1 must yield exactly 2 rows: 'hr', 'sales'.
    let out = run_select(
        &catalog,
        "emp_db",
        "SELECT DISTINCT dept FROM employees ORDER BY dept LIMIT 2 OFFSET 1",
    );
    assert_eq!(
        out.len(),
        2,
        "LIMIT 2 OFFSET 1 on 3 distinct departments must return 2 rows"
    );
    assert_eq!(out[0][0], "'hr'");
    assert_eq!(out[1][0], "'sales'");
}
