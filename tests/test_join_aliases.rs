//! Integration tests for table-alias resolution in JOINs
//! (ANALYSIS.md Tier 1 #4).
//!
//! Regression scenario: `SELECT e.name FROM employees e JOIN departments d
//! ON e.dept_id = d.id` used to fail because qualified identifiers kept the
//! alias while scan operators stamp real table names into tuple schemas.

mod common;

use common::TestWorkspace;
use common::load_catalog;
use rook_ast::QueryPlan;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn col(name: &str, ty: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        constraints: Default::default(),
    }
}

/// Create employees/departments and populate both.
fn setup(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "employees",
        vec![
            col("id", DataType::Int),
            col("name", DataType::Varchar(50)),
            col("salary", DataType::Int),
            col("dept_id", DataType::Int),
        ],
    )
    .unwrap();
    create_table(
        &mut catalog,
        db,
        "departments",
        vec![
            col("id", DataType::Int),
            col("dept_name", DataType::Varchar(50)),
            col("floor", DataType::Int),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for row in [
        ["1", "Alice", "85000", "1"],
        ["2", "Bob", "62000", "2"],
        ["3", "Cara", "75000", "1"],
        ["4", "Dan", "48000", "3"],
    ] {
        insert_single_tuple(&catalog, db, "employees", &row).unwrap();
    }
    for row in [["1", "eng", "3"], ["2", "sales", "2"], ["3", "hr", "1"]] {
        insert_single_tuple(&catalog, db, "departments", &row).unwrap();
    }
    load_catalog()
}

/// Parse a SELECT, run it through both planners and render tuples as strings.
fn run_select(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let select = match parse_sql(sql) {
        Ok(QueryPlan::Select(select)) => select,
        other => panic!("expected Select plan, got {:?}", other.err()),
    };
    let logical = storage_manager::planner::plan_query(&QueryPlan::Select(select), catalog, db)
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

fn parse_sql(s: &str) -> Result<QueryPlan, String> {
    rook_parser::parse_sql(s)
}

#[test]
fn aliased_join_on_condition() {
    let _ws = TestWorkspace::new("alias", "join_on");
    let catalog = setup("adb");

    // THE regression case from ANALYSIS.md / SHOWCASE.md.
    let out = run_select(
        &catalog,
        "adb",
        "SELECT e.name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE d.dept_name = 'eng'",
    );
    let mut names: Vec<String> = out.iter().map(|r| r[0].clone()).collect();
    names.sort();
    assert_eq!(names, vec!["'Alice'", "'Cara'"]);
}

#[test]
fn aliased_columns_in_projection_and_where() {
    let _ws = TestWorkspace::new("alias", "proj_where");
    let catalog = setup("adb");

    let out = run_select(
        &catalog,
        "adb",
        "SELECT e.name, e.salary FROM employees AS e WHERE e.salary >= 75000",
    );
    let mut rows = out.clone();
    rows.sort();
    assert_eq!(
        rows,
        vec![vec!["'Alice'", "85000"], vec!["'Cara'", "75000"],]
    );
}

#[test]
fn alias_in_order_by() {
    let _ws = TestWorkspace::new("alias", "order");
    let catalog = setup("adb");

    let out = run_select(
        &catalog,
        "adb",
        "SELECT e.name FROM employees e ORDER BY e.salary DESC LIMIT 2",
    );
    assert_eq!(out, vec![vec!["'Alice'"], vec!["'Cara'"]]);
}

#[test]
fn full_table_name_qualifier_still_works() {
    let _ws = TestWorkspace::new("alias", "realname");
    let catalog = setup("adb");

    let out = run_select(
        &catalog,
        "adb",
        "SELECT employees.name FROM employees JOIN departments ON employees.dept_id = departments.id WHERE departments.floor >= 2",
    );
    let mut names: Vec<String> = out.iter().map(|r| r[0].clone()).collect();
    names.sort();
    assert_eq!(names, vec!["'Alice'", "'Bob'", "'Cara'"]);
}

#[test]
fn self_join_with_two_aliases_disambiguates() {
    let _ws = TestWorkspace::new("alias", "selfjoin");
    let catalog = setup("adb");

    // Same-department pairs using two aliases over one table.
    let out = run_select(
        &catalog,
        "adb",
        "SELECT e1.name, e2.name FROM employees e1 JOIN employees e2 \
         ON e1.dept_id = e2.dept_id AND e1.id < e2.id",
    );
    let mut pairs: Vec<Vec<String>> = out;
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            vec!["'Alice'", "'Cara'"], // eng pair
        ]
    );
}

#[test]
fn mixed_alias_and_bare_references() {
    let _ws = TestWorkspace::new("alias", "mixed");
    let catalog = setup("adb");

    // Bare `name` binds to the left side; `d.dept_name` via alias.
    let out = run_select(
        &catalog,
        "adb",
        "SELECT name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE salary > 70000",
    );
    let mut rows = out;
    rows.sort();
    assert_eq!(
        rows,
        vec![vec!["'Alice'", "'eng'"], vec!["'Cara'", "'eng'"],]
    );
}

#[test]
fn group_by_with_alias_qualifiers() {
    let _ws = TestWorkspace::new("alias", "groupby");
    let catalog = setup("adb");

    let out = run_select(
        &catalog,
        "adb",
        "SELECT d.dept_name, COUNT(*) AS n FROM employees e JOIN departments d \
         ON e.dept_id = d.id GROUP BY d.dept_name",
    );
    let mut rows = out;
    rows.sort();
    assert_eq!(
        rows,
        vec![vec!["'eng'", "2"], vec!["'hr'", "1"], vec!["'sales'", "1"],]
    );
}
