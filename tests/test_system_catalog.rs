//! System-catalog integration tests.
//!
//! The catalog's single source of truth is the set of heap files under
//! `database/system/` (sys_databases, sys_tables, sys_columns, ...). These
//! tests cover bootstrap, persistence across sessions and the one-time
//! migration from the legacy `catalog.json`.
//!
//! Run with: cargo test --test test_system_catalog -- --test-threads=1

use std::path::Path;
mod common;

use storage_manager::catalog::{
    create_database, create_table, init_catalog, load_catalog, save_catalog,
};
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
        let path = format!("{}/{}", "database/system", file);
        assert!(Path::new(&path).exists(), "{} should exist", path);
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
fn legacy_json_catalog_is_migrated_once() {
    let _ws = common::TestWorkspace::new("syscat", "migrate");

    // Simulate a pre-system-tables installation: the guard bootstrapped the
    // system tables on entry, so wipe them (and any stray JSON) to restore
    // the "only a catalog.json exists" state this test needs.
    let _ = std::fs::remove_dir_all("database/system");
    let _ = std::fs::remove_file("database/global/catalog.json");

    std::fs::create_dir_all("database/global").unwrap();
    let json = r#"{
        "databases": {
            "old_db": {
                "tables": {
                    "legacy": {
                        "columns": [
                            {"name": "id", "data_type": "Int", "nullable": false,
                             "constraints": {"not_null": true, "unique": false,
                                             "default": null, "check": null}}
                        ]
                    }
                }
            }
        }
    }"#;
    std::fs::write("database/global/catalog.json", json).unwrap();

    init_catalog();

    // The JSON file is renamed out of the way and its content lives in the
    // system tables from now on.
    assert!(
        Path::new(&format!("{}.migrated", "database/global/catalog.json")).exists(),
        "catalog.json must be renamed to catalog.json.migrated"
    );
    let catalog = load_catalog();
    assert!(
        catalog.databases.contains_key("old_db"),
        "migrated databases survive"
    );
    assert!(catalog.databases["old_db"].tables.contains_key("legacy"));
}
