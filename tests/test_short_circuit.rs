//! Regression tests for short-circuit boolean evaluation
//! (ANALYSIS.md Tier 2 #10).
//!
//! Code review found that both execution engines ALREADY short-circuit:
//! `evaluate_predicate` returns early on `Some(false)` (AND) / `Some(true)`
//! (OR), and the legacy stack VM compiles `JumpIfFalse` / `JumpIfTrue`.
//! These tests pin that behaviour observably: if the right-hand side of an
//! AND/OR were ever evaluated eagerly, its division by zero would abort the
//! whole statement.

mod common;

use common::TestWorkspace;
use common::load_catalog;
use rook_ast::QueryPlan;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn setup(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "emp",
        vec![
            Column {
                name: "id".into(),
                data_type: DataType::Int,
                nullable: true,
                constraints: Default::default(),
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(30),
                nullable: true,
                constraints: Default::default(),
            },
            Column {
                name: "salary".into(),
                data_type: DataType::Int,
                nullable: true,
                constraints: Default::default(),
            },
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for row in [["1", "Ann", "50"], ["2", "Ben", "70"]] {
        insert_single_tuple(&catalog, db, "emp", &row).unwrap();
    }
    load_catalog()
}

fn run(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let select = match rook_parser::parse_sql(sql) {
        Ok(QueryPlan::Select(s)) => s,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical = storage_manager::planner::plan_query(&QueryPlan::Select(select), catalog, db)
        .expect("plan failed");
    let tuples: Vec<Tuple> = execute_plan_collect(&logical, catalog, db).expect("execution failed");
    tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|d| format!("{}", d))
                        .unwrap_or_else(|| "NULL".into())
                })
                .collect()
        })
        .collect()
}

#[test]
fn and_never_evaluates_rhs_when_lhs_is_false() {
    let _ws = TestWorkspace::new("shortc", "and");
    let catalog = setup("sdb");

    // LHS false for every row → RHS (division by zero) must NEVER run.
    let out = run(
        &catalog,
        "sdb",
        "SELECT name FROM emp WHERE salary > 1000 AND 1/0 = 1",
    );
    assert!(
        out.is_empty(),
        "no rows match; eager RHS would have errored"
    );
}

#[test]
fn or_never_evaluates_rhs_when_lhs_is_true() {
    let _ws = TestWorkspace::new("shortc", "or");
    let catalog = setup("sdb");

    // LHS true for every row → RHS (division by zero) must NEVER run.
    let out = run(
        &catalog,
        "sdb",
        "SELECT name FROM emp WHERE salary > 0 OR 1/0 = 1",
    );
    assert_eq!(out.len(), 2, "every row passes via the left side alone");
}

#[test]
fn and_still_evaluates_rhs_for_surviving_rows() {
    let _ws = TestWorkspace::new("shortc", "mixed");
    let catalog = setup("sdb");

    // Sanity: when LHS is true, RHS IS evaluated — here safely.
    let out = run(
        &catalog,
        "sdb",
        "SELECT name FROM emp WHERE salary > 0 AND salary < 1000",
    );
    assert_eq!(out.len(), 2);
}
