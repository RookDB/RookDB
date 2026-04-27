use std::fs;
use std::path::Path;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::page_manager::CatalogPageManager;
use storage_manager::catalog::{
    bootstrap_catalog, create_database, create_table, get_database, get_table,
};
use storage_manager::catalog::types::{Catalog, ColumnDefinition, Encoding};
use storage_manager::layout::{CATALOG_PAGES_DIR, OID_COUNTER_FILE};

fn cleanup_save_catalog_test() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    for dir in &["save_test_db"] {
        let path = format!("database/base/{}", dir);
        if Path::new(&path).exists() {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

#[test]
fn test_save_catalog() {
    cleanup_save_catalog_test();

    // Step 1: Bootstrap a fresh page-based catalog
    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");
    let mut pm = CatalogPageManager::new();
    let mut catalog = Catalog::new();
    catalog.page_backend_active = true;

    // Step 2: Create a test database
    let db_name = "save_test_db";
    create_database(&mut catalog, &mut pm, &mut bm, db_name, "admin", Encoding::UTF8)
        .expect("create_database should succeed");

    // Step 3: Create a table with three columns in that database
    let col_defs = vec![
        ColumnDefinition {
            name: "id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "name".to_string(),
            type_name: "VARCHAR(10)".to_string(),
            type_modifier: Some(10),
            is_nullable: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "email".to_string(),
            type_name: "VARCHAR(10)".to_string(),
            type_modifier: Some(10),
            is_nullable: true,
            default_value: None,
        },
    ];
    create_table(&mut catalog, &mut pm, &mut bm, db_name, "users", col_defs, vec![])
        .expect("create_table should succeed");

    // Step 4: Reload — verify the database is retrievable from the page backend
    let db = get_database(&mut catalog, &mut pm, &mut bm, db_name)
        .expect("database should be found after creation");
    assert_eq!(db.db_name, db_name);

    // Step 5: Verify the table is retrievable
    let table = get_table(&mut catalog, &mut pm, &mut bm, db.db_oid, "users")
        .expect("table should be found after creation");
    assert_eq!(table.table_name, "users");

    // Cleanup
    cleanup_save_catalog_test();
}

