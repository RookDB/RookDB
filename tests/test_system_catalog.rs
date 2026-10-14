//! System-catalog integration tests.
//!
//! The catalog's single source of truth is the set of heap files under
//! `database/system/` (sys_databases, sys_tables, sys_columns, ...). These
//! tests cover bootstrap, persistence across sessions and the refusal of a
//! data directory from before the system catalog.
//!
//! Run with: cargo test --test test_system_catalog -- --test-threads=1

use common::{init_catalog, load_catalog};
use std::path::Path;
mod common;

use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::types::DataType;

#[test]
fn init_bootstraps_system_tables() {
    let _ws = common::TestWorkspace::new("syscat", "bootstrap");

    init_catalog();

    // All six system tables must exist as heap files after bootstrap.
    for file in [
        "databases.dat",
        "tables.dat",
        "columns.dat",
        "constraints.dat",
        "indexes.dat",
        "views.dat",
    ] {
        let path = common::path(format!("database/system/{}", file));
        assert!(Path::new(&path).exists(), "{} should exist", path.display());
    }
}

#[test]
fn catalog_round_trips_through_system_tables() {
    let _ws = common::TestWorkspace::new("syscat", "roundtrip");

    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "inv");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "inv",
        "items",
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
    );
    save_catalog(&catalog).unwrap();

    // A fresh load (as a new session would do) sees the same metadata.
    let reloaded = load_catalog();
    let db = reloaded.databases.get("inv").expect("database persisted");
    assert!(db.tables.contains_key("items"));
    let cols = &db.tables["items"].columns;
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert!(!cols[0].nullable);
}

#[test]
fn old_json_catalog_is_not_migrated() {
    let _ws = common::TestWorkspace::new("syscat", "migrate");

    // Simulate a pre-system-tables installation: wipe the system tables the
    // guard bootstrapped and leave only a catalog.json.
    let _ = std::fs::remove_dir_all(common::path("database/system"));
    std::fs::create_dir_all(common::path("database/global")).unwrap();
    let json = r#"{"databases": {"old_db": {"tables": {}}}}"#;
    std::fs::write(common::path("database/global/catalog.json"), json).unwrap();

    init_catalog();

    // The old file is left alone, nothing is imported and no system tables
    // are created over it.
    assert!(common::path("database/global/catalog.json").exists());
    assert!(!common::path("database/system").exists());
    assert!(load_catalog().databases.is_empty());
}
