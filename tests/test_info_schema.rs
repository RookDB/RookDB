//! INFORMATION_SCHEMA integration tests.
//!
//! Metadata views are ordinary SQL: they read their backing system tables
//! (sys_databases, sys_tables, ...) and expose the SQL-99 column names.
//!
//! Run with: cargo test --test test_info_schema -- --test-threads=1

mod common;

use common::load_catalog;
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::types::DataType;

/// One database with one two-column table.
fn setup() -> storage_manager::catalog::Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, "meta_db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "meta_db",
        "widgets",
        vec![
            storage_manager::catalog::Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                constraints: Default::default(),
            },
            storage_manager::catalog::Column {
                name: "label".to_string(),
                data_type: DataType::Varchar(40),
                nullable: true,
                constraints: Default::default(),
            },
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    load_catalog()
}

fn query(catalog: &storage_manager::catalog::Catalog, sql: &str) -> Vec<Vec<String>> {
    let plan = parse_sql(sql).unwrap();
    let logical =
        storage_manager::planner::plan_query(&plan, catalog, "meta_db").expect("planning failed");
    execute_plan_collect(&logical, catalog, "meta_db")
        .expect("execution failed")
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
fn schemata_lists_databases() {
    let _ws = common::TestWorkspace::new("info", "schemata");
    let catalog = setup();

    let out = query(
        &catalog,
        "SELECT schema_name FROM information_schema.schemata",
    );
    assert!(out.iter().any(|r| r[0] == "'meta_db'"), "meta_db listed");
}

#[test]
fn tables_view_reports_user_tables() {
    let _ws = common::TestWorkspace::new("info", "tables");
    let catalog = setup();

    let out = query(&catalog, "SELECT table_name FROM information_schema.tables");
    assert!(out.iter().any(|r| r[0] == "'widgets'"), "widgets listed");
}

#[test]
fn columns_view_exposes_sql99_names() {
    let _ws = common::TestWorkspace::new("info", "columns");
    let catalog = setup();

    // The COLUMNS view exposes SQL-99 names; its positional mapping makes
    // `column_name` report the actual column names of the table.
    let out = query(
        &catalog,
        "SELECT column_name FROM information_schema.columns",
    );
    let names: Vec<&String> = out.iter().map(|r| &r[0]).collect();
    assert!(
        names.contains(&&"'id'".to_string()),
        "id present: {:?}",
        names
    );
    assert!(
        names.contains(&&"'label'".to_string()),
        "label present: {:?}",
        names
    );
}

#[test]
fn where_filtering_works_on_views() {
    let _ws = common::TestWorkspace::new("info", "filter");
    let catalog = setup();

    let out = query(
        &catalog,
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'widgets'",
    );
    assert_eq!(out.len(), 1);
}
