use std::collections::HashMap;
use std::fs;
use std::path::Path;

use storage_manager::catalog::{init_catalog, load_catalog, save_catalog};
use storage_manager::catalog::types::{Column, DataType, Database, Encoding, Table, TableStatistics, TableType};
use storage_manager::layout::CATALOG_FILE;

#[test]
fn test_save_catalog() {
    // Step 1: Ensure the catalog file exists (create if missing)
    if !Path::new(CATALOG_FILE).exists() {
        init_catalog();
    }

    // Step 2: Load catalog into memory
    let mut catalog = load_catalog();

    // Step 3: Ensure a test database exists
    let db_name = "test_db";
    if !catalog.databases.contains_key(db_name) {
        catalog.databases.insert(
            db_name.to_string(),
            Database {
                db_oid: 9999,
                db_name: db_name.to_string(),
                tables: HashMap::new(),
                owner: "test_user".to_string(),
                encoding: Encoding::UTF8,
                created_at: 0,
            },
        );
    }

    // Step 4: Add a new test table entry inside the test database
    let make_col = |pos: u16, name: &str, dt: DataType| Column {
        column_oid: 0,
        name: name.to_string(),
        column_position: pos,
        data_type: dt,
        type_modifier: None,
        is_nullable: true,
        default_value: None,
        constraints: vec![],
    };

    let test_table = Table {
        table_oid: 9999,
        table_name: "users".to_string(),
        db_oid: 9999,
        columns: vec![
            make_col(1, "id",    DataType::int()),
            make_col(2, "name",  DataType::text()),
            make_col(3, "email", DataType::text()),
        ],
        constraints: vec![],
        indexes: vec![],
        table_type: TableType::UserTable,
        statistics: TableStatistics::default(),
    };

    let db = catalog.databases.get_mut(db_name).unwrap();
    db.tables.insert("users".to_string(), test_table);

    // Step 5: Save catalog back to disk
    save_catalog(&catalog);

    // Step 6: Reload catalog from disk and verify it contains the database and table
    let reloaded_catalog = load_catalog();

    assert!(
        reloaded_catalog.databases.contains_key(db_name),
        "Saved catalog does not contain expected database '{}'",
        db_name
    );

    let reloaded_db = reloaded_catalog.databases.get(db_name).unwrap();

    assert!(
        reloaded_db.tables.contains_key("users"),
        "Saved catalog does not contain 'users' table inside database '{}'",
        db_name
    );

    let users_table = reloaded_db.tables.get("users").unwrap();
    assert_eq!(
        users_table.columns.len(),
        3,
        "Expected 3 columns in 'users' table"
    );

    // Step 7: Clean up
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to clean up test catalog.json");
    }
}
