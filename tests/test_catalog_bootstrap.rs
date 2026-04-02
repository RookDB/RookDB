//! Tests for catalog bootstrapping (Spec §7.1) and OID persistence.
//!
//! These tests verify:
//! - bootstrap_catalog() creates all 6 system catalog files
//! - Each file has a header page + at least one data page
//! - Built-in types are registered in pg_type
//! - OID counter persists across restarts

use std::fs;
use std::path::Path;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::page_manager::CatalogPageManager;
use storage_manager::catalog::serialize::deserialize_type_tuple;
use storage_manager::catalog::{bootstrap_catalog, init_catalog, load_catalog};
use storage_manager::catalog::types::Catalog;
use storage_manager::layout::{
    CATALOG_PAGES_DIR, PG_COLUMN_FILE, PG_CONSTRAINT_FILE, PG_DATABASE_FILE,
    PG_INDEX_FILE, PG_TABLE_FILE, PG_TYPE_FILE, OID_COUNTER_FILE,
};

/// Helper: clean up catalog pages directory and OID counter for test isolation.
fn cleanup_catalog() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    // Also remove legacy JSON if present
    let catalog_json = "database/global/catalog.json";
    if Path::new(catalog_json).exists() {
        let _ = fs::remove_file(catalog_json);
    }
}

// ─────────────────────────────────────────────────────────────
// Test 7.1.1: Bootstrap System Catalogs
// ─────────────────────────────────────────────────────────────

#[test]
fn test_bootstrap_creates_all_catalog_files() {
    cleanup_catalog();

    let mut bm = BufferManager::new();
    let result = bootstrap_catalog(&mut bm);
    assert!(result.is_ok(), "bootstrap_catalog() should succeed: {:?}", result.err());

    // Verify all 6 system catalog files are created
    let catalog_files = [
        PG_DATABASE_FILE,
        PG_TABLE_FILE,
        PG_COLUMN_FILE,
        PG_CONSTRAINT_FILE,
        PG_INDEX_FILE,
        PG_TYPE_FILE,
    ];

    for file in &catalog_files {
        assert!(
            Path::new(file).exists(),
            "System catalog file '{}' should be created by bootstrap",
            file
        );
    }

    // Verify pg_oid_counter.dat is created
    assert!(
        Path::new(OID_COUNTER_FILE).exists(),
        "pg_oid_counter.dat should exist after bootstrap"
    );

    cleanup_catalog();
}

#[test]
fn test_bootstrap_files_have_valid_page_structure() {
    cleanup_catalog();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    // Each catalog file should have at least 2 pages (header + 1 data page)
    // = at least 2 * 8192 = 16384 bytes
    let catalog_files = [
        PG_DATABASE_FILE,
        PG_TABLE_FILE,
        PG_COLUMN_FILE,
        PG_CONSTRAINT_FILE,
        PG_INDEX_FILE,
        PG_TYPE_FILE,
    ];

    for file in &catalog_files {
        let metadata = fs::metadata(file).expect(&format!("Should be able to stat {}", file));
        assert!(
            metadata.len() >= 16384, // 2 * 8192 (header page + at least 1 data page)
            "File '{}' should have at least 2 pages (16384 bytes), got {} bytes",
            file,
            metadata.len()
        );
    }

    cleanup_catalog();
}

#[test]
fn test_bootstrap_registers_builtin_types() {
    cleanup_catalog();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    // Scan pg_type to verify built-in types are present
    let pm = CatalogPageManager::new();
    let tuples = pm
        .scan_catalog(&mut bm, "pg_type")
        .expect("Should be able to scan pg_type");

    // We expect 10 built-in types: INT, BIGINT, FLOAT, DOUBLE, BOOL, TEXT, VARCHAR, DATE, TIMESTAMP, BYTES
    assert!(
        tuples.len() >= 10,
        "pg_type should contain at least 10 built-in types, found {}",
        tuples.len()
    );

    // Verify we can deserialize each type
    let mut type_names: Vec<String> = Vec::new();
    for t in &tuples {
        let dt = deserialize_type_tuple(t).expect("Should deserialize type tuple");
        type_names.push(dt.type_name.clone());
    }

    // Check for specific expected types
    let expected = ["INT", "BIGINT", "FLOAT", "DOUBLE", "BOOL", "TEXT", "DATE", "TIMESTAMP", "BYTES"];
    for exp in &expected {
        assert!(
            type_names.iter().any(|n| n == exp),
            "Built-in type '{}' should be registered in pg_type. Found: {:?}",
            exp,
            type_names
        );
    }
    // VARCHAR is stored as "VARCHAR(255)"
    assert!(
        type_names.iter().any(|n| n.starts_with("VARCHAR")),
        "A VARCHAR type should be registered in pg_type. Found: {:?}",
        type_names
    );

    cleanup_catalog();
}

// ─────────────────────────────────────────────────────────────
// Test 7.1.2: OID Counter Persistence
// ─────────────────────────────────────────────────────────────

#[test]
fn test_oid_counter_persists_across_restarts() {
    cleanup_catalog();

    // Step 1: Initialize catalog and allocate some OIDs
    let mut bm1 = BufferManager::new();
    init_catalog(&mut bm1);
    let mut catalog1 = load_catalog(&mut bm1);

    // Make sure page backend is active for OID persistence
    catalog1.page_backend_active = true;

    let mut last_oid = 0u32;
    for _ in 0..10 {
        last_oid = catalog1.alloc_oid();
    }
    // last_oid should be oid_counter_start + 9

    // Step 2: "Restart" – create a fresh BufferManager and reload
    let mut bm2 = BufferManager::new();
    let mut catalog2 = load_catalog(&mut bm2);

    // Step 3: Allocate next OID and verify it doesn't collide
    let next_oid = catalog2.alloc_oid();
    assert!(
        next_oid > last_oid,
        "After restart, next OID ({}) should be greater than last allocated OID ({})",
        next_oid,
        last_oid,
    );

    cleanup_catalog();
}

#[test]
fn test_oid_counter_file_format() {
    cleanup_catalog();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    // OID counter file should exist and contain a valid u32
    assert!(Path::new(OID_COUNTER_FILE).exists());

    let data = fs::read(OID_COUNTER_FILE).expect("Should read OID counter file");
    assert!(
        data.len() >= 4,
        "OID counter file should contain at least 4 bytes, found {}",
        data.len()
    );

    let counter = u32::from_le_bytes(data[0..4].try_into().unwrap());
    assert!(
        counter >= 10000,
        "OID counter should be >= USER_OID_START (10000), found {}",
        counter
    );

    cleanup_catalog();
}
