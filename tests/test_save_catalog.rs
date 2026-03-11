use std::fs;
use std::path::Path;

use storage_manager::catalog::{
    init_catalog, load_catalog, save_catalog, Database, Table, Column,
};

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
        catalog.databases.insert(db_name.to_string(), Database { tables: Default::default() });
    }

    // Step 4: Add a new test table entry inside the test database
    let test_table = Table {
        columns: vec![
            Column { name: "id".to_string(), data_type: "INT".to_string() },
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
            Column { name: "email".to_string(), data_type: "TEXT".to_string() },
        ],
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

    // Step 7: Clean up (optional)
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to clean up test catalog.json");
    }
}
