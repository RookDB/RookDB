//! Tests for index creation, metadata persistence, and B-Tree operations (Spec §7.4).
//!
//! Covers:
//! - 7.4.1: Create B-Tree index, verify pg_index entry and file creation
//! - 7.4.2: Index-backed unique constraint – insert and lookup
//! - B-Tree insert and lookup correctness

use std::fs;
use std::path::Path;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::indexes::*;
use storage_manager::catalog::page_manager::CatalogPageManager;
use storage_manager::catalog::types::*;
use storage_manager::catalog::{
    bootstrap_catalog, create_database, create_table,
};
use storage_manager::layout::{CATALOG_PAGES_DIR, INDEX_DIR_TEMPLATE, OID_COUNTER_FILE};

fn cleanup() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    if Path::new("database/base/idx_test_db").exists() {
        let _ = fs::remove_dir_all("database/base/idx_test_db");
    }
}

fn setup_test_table() -> (Catalog, CatalogPageManager, BufferManager, u32) {
    cleanup();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    let mut pm = CatalogPageManager::new();
    let mut catalog = Catalog::new();
    catalog.page_backend_active = true;

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "idx_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create database should succeed");

    let col_defs = vec![
        ColumnDefinition {
            name: "id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: false,
            default_value: None,
        },
        ColumnDefinition {
            name: "name".to_string(),
            type_name: "TEXT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
    ];

    let table_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "idx_test_db",
        "items",
        col_defs,
        vec![],
    )
    .expect("create table should succeed");

    (catalog, pm, bm, table_oid)
}

// ─────────────────────────────────────────────────────────────
// Test 7.4.1: Create B-Tree Index
// ─────────────────────────────────────────────────────────────

#[test]
fn test_create_index_btree() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // Get column OIDs
    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };
    let id_oid = col_oids[0];

    let result = create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![id_oid],
        true,
        false,
        Some("idx_items_id".to_string()),
    );
    assert!(
        result.is_ok(),
        "create_index should succeed: {:?}",
        result.err()
    );

    let index_oid = result.unwrap();
    assert!(index_oid > 0, "Index OID should be positive");

    // Verify pg_index has the entry
    let indexes = get_indexes_for_table(&pm, &mut bm, table_oid).expect("should get indexes");
    assert!(indexes.len() >= 1, "Should have at least 1 index");
    let idx = indexes.iter().find(|i| i.index_oid == index_oid);
    assert!(idx.is_some(), "Should find the created index");
    let idx = idx.unwrap();
    assert_eq!(idx.index_name, "idx_items_id");
    assert_eq!(idx.index_type, IndexType::BTree);
    assert!(idx.is_unique);
    assert!(!idx.is_primary);
    assert_eq!(idx.column_oids, vec![id_oid]);

    // Verify index file exists on disk
    let idx_file = format!("database/base/idx_test_db/indexes/idx_items_id.idx");
    assert!(
        Path::new(&idx_file).exists(),
        "Index file should exist at {}",
        idx_file
    );

    // Verify table's indexes list is updated
    let db = catalog.databases.get("idx_test_db").unwrap();
    let table = db.tables.get("items").unwrap();
    assert!(
        table.indexes.contains(&index_oid),
        "Table should reference the new index OID"
    );

    cleanup();
}

#[test]
fn test_create_index_default_name() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };

    // Create without explicit name
    let result = create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![col_oids[0]],
        false,
        false,
        None,
    );
    assert!(
        result.is_ok(),
        "create_index with default name should succeed"
    );

    let indexes = get_indexes_for_table(&pm, &mut bm, table_oid).unwrap();
    assert!(!indexes.is_empty(), "Should have an index");
    // Name should follow pattern idx_{table_oid}_{col_oid}
    let idx = &indexes[0];
    assert!(
        idx.index_name.starts_with("idx_"),
        "Default name should start with 'idx_', got '{}'",
        idx.index_name
    );

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: Drop Index
// ─────────────────────────────────────────────────────────────

#[test]
fn test_drop_index() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };

    let index_oid = create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![col_oids[0]],
        false,
        false,
        Some("idx_to_drop".to_string()),
    )
    .expect("create should succeed");

    let idx_file = "database/base/idx_test_db/indexes/idx_to_drop.idx";
    assert!(
        Path::new(idx_file).exists(),
        "Index file should exist after creation"
    );

    // Drop the index
    let result = drop_index(&mut catalog, &mut pm, &mut bm, index_oid);
    assert!(
        result.is_ok(),
        "drop_index should succeed: {:?}",
        result.err()
    );

    // Verify index is removed from pg_index
    let indexes = get_indexes_for_table(&pm, &mut bm, table_oid).unwrap();
    assert!(
        indexes.iter().find(|i| i.index_oid == index_oid).is_none(),
        "Dropped index should not appear in pg_index"
    );

    // Table should no longer reference this index
    let db = catalog.databases.get("idx_test_db").unwrap();
    let table = db.tables.get("items").unwrap();
    assert!(!table.indexes.contains(&index_oid));

    // File should be removed
    assert!(
        !Path::new(idx_file).exists(),
        "Index file should be deleted after drop"
    );

    cleanup();
}

#[test]
fn test_drop_nonexistent_index() {
    let (mut catalog, mut pm, mut bm, _table_oid) = setup_test_table();

    let result = drop_index(&mut catalog, &mut pm, &mut bm, 99999);
    assert!(result.is_err(), "Should fail to drop non-existent index");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: B-Tree insert + lookup
// ─────────────────────────────────────────────────────────────

#[test]
fn test_btree_insert_and_lookup() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };

    create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![col_oids[0]],
        true,
        false,
        Some("idx_btree_test".to_string()),
    )
    .expect("create index should succeed");

    // Insert a key into the B-Tree
    let key = 42i32.to_le_bytes();
    let result = insert_index_entry(&mut bm, "idx_test_db", "idx_btree_test", &key, 1, 0);
    assert!(
        result.is_ok(),
        "B-Tree insert should succeed: {:?}",
        result.err()
    );

    // Lookup should find it
    let found = index_lookup(&mut bm, "idx_test_db", "idx_btree_test", &key)
        .expect("lookup should succeed");
    assert!(found, "Should find the inserted key");

    // Lookup a non-existent key
    let missing_key = 999i32.to_le_bytes();
    let found = index_lookup(&mut bm, "idx_test_db", "idx_btree_test", &missing_key)
        .expect("lookup should succeed");
    assert!(!found, "Should NOT find a non-existent key");

    cleanup();
}

#[test]
fn test_btree_multiple_inserts() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };

    create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![col_oids[0]],
        true,
        false,
        Some("idx_multi_test".to_string()),
    )
    .expect("create index should succeed");

    // Insert 100 keys
    for i in 0..100i32 {
        let key = i.to_le_bytes();
        insert_index_entry(&mut bm, "idx_test_db", "idx_multi_test", &key, 1, i as u32)
            .expect(&format!("Insert key {} should succeed", i));
    }

    // All keys should be findable
    for i in 0..100i32 {
        let key = i.to_le_bytes();
        let found = index_lookup(&mut bm, "idx_test_db", "idx_multi_test", &key)
            .expect(&format!("Lookup key {} should succeed", i));
        assert!(found, "Key {} should be found", i);
    }

    // Keys not inserted should not be found
    let key = 200i32.to_le_bytes();
    let found = index_lookup(&mut bm, "idx_test_db", "idx_multi_test", &key)
        .expect("lookup should succeed");
    assert!(!found, "Key 200 should NOT be found");

    cleanup();
}

#[test]
fn test_index_lookup_nonexistent_file() {
    let mut bm = BufferManager::new();
    let result = index_lookup(&mut bm, "nonexistent_db", "nonexistent_idx", &[1, 2, 3, 4]);
    assert!(result.is_ok());
    assert!(
        !result.unwrap(),
        "Should return false for non-existent index file"
    );
}

// ─────────────────────────────────────────────────────────────
// Test: Index directory creation
// ─────────────────────────────────────────────────────────────

#[test]
fn test_index_creates_directory() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let col_oids: Vec<u32> = {
        let db = catalog.databases.get("idx_test_db").unwrap();
        let table = db.tables.get("items").unwrap();
        table.columns.iter().map(|c| c.column_oid).collect()
    };

    let idx_dir = INDEX_DIR_TEMPLATE.replace("{database}", "idx_test_db");

    create_index(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec![col_oids[0]],
        false,
        false,
        Some("idx_dir_test".to_string()),
    )
    .expect("create index should succeed");

    assert!(
        Path::new(&idx_dir).exists(),
        "Index directory should be created at {}",
        idx_dir
    );

    cleanup();
}
