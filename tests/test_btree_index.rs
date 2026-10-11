//! B+ Tree index integration tests.
//!
//! CREATE INDEX builds a persistent `{table}.{index}.idx` file; the physical
//! planner then prefers the index for scans and DML keeps it up to date.
//!
//! Run with: cargo test --test test_btree_index -- --test-threads=1

use std::path::Path;
mod common;

use rook_ast::logical::{LogicalPlan, LogicalTableScan};
use rook_ast::{QueryPlan, SelectPlan};
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::planner::PhysicalPlanner;
use storage_manager::catalog::{
    Catalog, Column, create_database, create_table, load_catalog, save_catalog,
};
use storage_manager::executor::create_index::create_index;
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

/// staff(id INT, name VARCHAR, salary INT) with 6 rows.
fn setup_table(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "staff",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("salary", DataType::Int),
        ],
    );
    save_catalog(&catalog).unwrap();

    for row in [
        vec!["1", "Ann", "50000"],
        vec!["2", "Ben", "62000"],
        vec!["3", "Cy", "75000"],
        vec!["4", "Dee", "48000"],
        vec!["5", "Eli", "91000"],
        vec!["6", "Fay", "62000"], // duplicate salary exercises duplicate keys
    ] {
        insert_single_tuple(&catalog, db, "staff", &row).unwrap();
    }
    load_catalog()
}

fn run_select(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    match parse_sql(sql).unwrap() {
        QueryPlan::Select(SelectPlan { .. }) => {}
        other => panic!("expected Select plan, got {:?}", other),
    }
    let logical =
        storage_manager::planner::plan_query(&parse_sql(sql).unwrap(), catalog, db).unwrap();
    let tuples = execute_plan_collect(&logical, catalog, db).unwrap();
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
fn create_index_builds_persistent_files() {
    let _ws = common::TestWorkspace::new("btree", "files");
    let catalog = setup_table("idx_db");

    let entries = create_index(
        &catalog,
        "idx_db",
        "staff",
        "by_salary",
        &[String::from("salary")],
    )
    .expect("create_index");
    assert_eq!(entries, 6, "one index entry per row");

    assert!(
        Path::new("database/base/idx_db/staff.by_salary.idx").exists(),
        "index pages file must exist"
    );
    assert!(
        Path::new("database/base/idx_db/staff.by_salary.idx.meta").exists(),
        "index metadata file must exist"
    );
}

#[test]
fn planner_prefers_existing_index_for_scans() {
    let _ws = common::TestWorkspace::new("btree", "planpref");
    let catalog = setup_table("idx_db");

    let scan_plan = || {
        let cols = &catalog.databases["idx_db"].tables["staff"].columns;
        LogicalPlan::TableScan(LogicalTableScan {
            table: "staff".to_string(),
            alias: None,
            schema: storage_manager::planner::semantic::catalog_columns_to_schema(cols),
            system_table_name: None,
        })
    };

    let planner = PhysicalPlanner::new(catalog.clone(), "idx_db".to_string());

    // Without an index the scan is sequential.
    assert_eq!(planner.plan(&scan_plan()).unwrap().name(), "SeqScan");

    // With an index present it drives the scan.
    create_index(
        &catalog,
        "idx_db",
        "staff",
        "by_salary",
        &[String::from("salary")],
    )
    .unwrap();
    assert_eq!(
        planner.plan(&scan_plan()).unwrap().name(),
        "IndexScan(Full)"
    );
}

#[test]
fn index_scan_returns_identical_rows_to_seq_scan() {
    let _ws = common::TestWorkspace::new("btree", "equiv");
    let catalog = setup_table("eq_db");

    let before = run_select(&catalog, "eq_db", "SELECT name FROM staff ORDER BY id");

    create_index(
        &catalog,
        "eq_db",
        "staff",
        "by_salary",
        &[String::from("salary")],
    )
    .unwrap();
    let after = run_select(&catalog, "eq_db", "SELECT name FROM staff ORDER BY id");

    assert_eq!(before, after, "index-driven scan must not change results");
    assert_eq!(before.len(), 6);
}

#[test]
fn index_is_maintained_on_insert() {
    let _ws = common::TestWorkspace::new("btree", "maint");
    let catalog = setup_table("mnt_db");

    create_index(
        &catalog,
        "mnt_db",
        "staff",
        "by_salary",
        &[String::from("salary")],
    )
    .unwrap();

    // Insert a new row after index creation, the way the DML handlers do:
    // raw tuple insert gives us the heap location, then the index is told
    // about the new (key -> location) entry.
    let row_bytes = storage_manager::types::serialize_nullable_typed_row(
        &[DataType::Int, DataType::Varchar(50), DataType::Int],
        &[
            Some(storage_manager::types::DataValue::Int(7)),
            Some(storage_manager::types::DataValue::Varchar(
                "Gus".to_string(),
            )),
            Some(storage_manager::types::DataValue::Int(55000)),
        ],
    )
    .unwrap();
    let (page_id, slot_id) =
        storage_manager::executor::insert_raw_tuple("mnt_db", "staff", &row_bytes).unwrap();
    storage_manager::executor::create_index::update_index_on_insert(
        "mnt_db",
        "staff",
        &storage_manager::executor::parse_text_row(
            &catalog.databases["mnt_db"].tables["staff"].columns,
            &["7", "Gus", "55000"],
        )
        .unwrap(),
        page_id,
        slot_id,
    )
    .unwrap();

    let out = run_select(
        &catalog,
        "mnt_db",
        "SELECT name FROM staff WHERE salary = 55000",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], "'Gus'");
}

#[test]
fn index_scan_walks_keys_in_ascending_order() {
    // B+ Tree leaf pages are linked in key order: a full index scan MUST
    // visit rows by ascending indexed value no matter how they were
    // inserted. This pins the leaf-link capability itself.
    let _ws = common::TestWorkspace::new("btree", "keyorder");
    let catalog = setup_table("ko_db");

    create_index(
        &catalog,
        "ko_db",
        "staff",
        "by_salary",
        &[String::from("salary")],
    )
    .unwrap();

    let out = run_select(&catalog, "ko_db", "SELECT salary FROM staff");
    let salaries: Vec<String> = out.iter().map(|r| r[0].clone()).collect();

    let mut sorted = salaries.clone();
    sorted.sort_by_key(|s| s.parse::<i64>().unwrap());
    assert_eq!(
        salaries, sorted,
        "index-driven scan must walk keys ascending\ninsertion-independent order expected"
    );
}
