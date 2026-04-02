//! Tests for the extended data type system (Spec §7.2).
//!
//! Covers:
//! - 7.2.1: Register built-in types and verify metadata
//! - 7.2.2: Lookup type by name (valid and invalid)
//! - Type from_name() resolution for all supported aliases

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::page_manager::CatalogPageManager;
use storage_manager::catalog::serialize::deserialize_type_tuple;
use storage_manager::catalog::types::{DataType, TypeCategory, CatalogError};
use storage_manager::catalog::{bootstrap_catalog, lookup_type_by_name, register_builtin_types};
use storage_manager::layout::{
    CATALOG_PAGES_DIR, OID_COUNTER_FILE,
    OID_TYPE_INT, OID_TYPE_BIGINT, OID_TYPE_FLOAT, OID_TYPE_DOUBLE,
    OID_TYPE_BOOL, OID_TYPE_TEXT, OID_TYPE_VARCHAR,
    OID_TYPE_DATE, OID_TYPE_TIMESTAMP, OID_TYPE_BYTES,
};
use std::fs;
use std::path::Path;

fn cleanup() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    let catalog_json = "database/global/catalog.json";
    if Path::new(catalog_json).exists() {
        let _ = fs::remove_file(catalog_json);
    }
}

// ─────────────────────────────────────────────────────────────
// Test 7.2.1: Register Built-in Types
// ─────────────────────────────────────────────────────────────

#[test]
fn test_builtin_types_all_present() {
    // Verify DataType::all_builtins() returns exactly 10 types
    let builtins = DataType::all_builtins();
    assert_eq!(builtins.len(), 10, "Should have 10 built-in types");
}

#[test]
fn test_builtin_type_oids_correct() {
    let int = DataType::int();
    assert_eq!(int.type_oid, OID_TYPE_INT);
    assert_eq!(int.type_name, "INT");
    assert_eq!(int.type_length, 4);
    assert_eq!(int.type_align, 4);
    assert!(matches!(int.type_category, TypeCategory::Numeric));

    let bigint = DataType::bigint();
    assert_eq!(bigint.type_oid, OID_TYPE_BIGINT);
    assert_eq!(bigint.type_length, 8);
    assert_eq!(bigint.type_align, 8);

    let float = DataType::float();
    assert_eq!(float.type_oid, OID_TYPE_FLOAT);
    assert_eq!(float.type_length, 4);

    let double = DataType::double();
    assert_eq!(double.type_oid, OID_TYPE_DOUBLE);
    assert_eq!(double.type_length, 8);

    let bool_t = DataType::bool_type();
    assert_eq!(bool_t.type_oid, OID_TYPE_BOOL);
    assert_eq!(bool_t.type_length, 1);
    assert!(matches!(bool_t.type_category, TypeCategory::Boolean));

    let text = DataType::text();
    assert_eq!(text.type_oid, OID_TYPE_TEXT);
    assert_eq!(text.type_length, -1); // variable
    assert!(matches!(text.type_category, TypeCategory::String));

    let varchar = DataType::varchar(100);
    assert_eq!(varchar.type_oid, OID_TYPE_VARCHAR);
    assert_eq!(varchar.type_length, -1); // variable
    assert_eq!(varchar.type_name, "VARCHAR(100)");

    let date = DataType::date();
    assert_eq!(date.type_oid, OID_TYPE_DATE);
    assert_eq!(date.type_length, 4);
    assert!(matches!(date.type_category, TypeCategory::DateTime));

    let ts = DataType::timestamp();
    assert_eq!(ts.type_oid, OID_TYPE_TIMESTAMP);
    assert_eq!(ts.type_length, 8);
    assert!(matches!(ts.type_category, TypeCategory::DateTime));

    let bytes = DataType::bytes();
    assert_eq!(bytes.type_oid, OID_TYPE_BYTES);
    assert_eq!(bytes.type_length, -1); // variable
    assert!(matches!(bytes.type_category, TypeCategory::Binary));
}

// ─────────────────────────────────────────────────────────────
// Test 7.2.2: Lookup Type by Name
// ─────────────────────────────────────────────────────────────

#[test]
fn test_type_from_name_valid_types() {
    // Standard names
    assert!(DataType::from_name("INT").is_some());
    assert!(DataType::from_name("BIGINT").is_some());
    assert!(DataType::from_name("FLOAT").is_some());
    assert!(DataType::from_name("DOUBLE").is_some());
    assert!(DataType::from_name("BOOL").is_some());
    assert!(DataType::from_name("TEXT").is_some());
    assert!(DataType::from_name("DATE").is_some());
    assert!(DataType::from_name("TIMESTAMP").is_some());
    assert!(DataType::from_name("BYTES").is_some());
    assert!(DataType::from_name("VARCHAR(50)").is_some());
}

#[test]
fn test_type_from_name_aliases() {
    // Aliases should also resolve
    assert!(DataType::from_name("INTEGER").is_some());
    assert!(DataType::from_name("INT32").is_some());
    assert!(DataType::from_name("INT64").is_some());
    assert!(DataType::from_name("REAL").is_some());
    assert!(DataType::from_name("FLOAT32").is_some());
    assert!(DataType::from_name("FLOAT64").is_some());
    assert!(DataType::from_name("BOOLEAN").is_some());
    assert!(DataType::from_name("STRING").is_some());
    assert!(DataType::from_name("BYTEA").is_some());
    assert!(DataType::from_name("BLOB").is_some());
}

#[test]
fn test_type_from_name_case_insensitive() {
    assert!(DataType::from_name("int").is_some());
    assert!(DataType::from_name("Int").is_some());
    assert!(DataType::from_name("varchar(255)").is_some());
    assert!(DataType::from_name("Varchar(100)").is_some());
}

#[test]
fn test_type_from_name_invalid() {
    assert!(DataType::from_name("INVALID_TYPE").is_none());
    assert!(DataType::from_name("DECIMAL").is_none());
    assert!(DataType::from_name("").is_none());
}

#[test]
fn test_varchar_default_length() {
    // VARCHAR without explicit length should default to 255
    let dt = DataType::from_name("VARCHAR").unwrap();
    assert_eq!(dt.type_name, "VARCHAR(255)");
}

#[test]
fn test_lookup_type_by_name_from_catalog() {
    cleanup();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    let pm = CatalogPageManager::new();

    // Valid type lookup
    let result = lookup_type_by_name(&pm, &mut bm, "INT");
    assert!(result.is_ok(), "Should find INT type: {:?}", result.err());
    let dt = result.unwrap();
    assert_eq!(dt.type_name, "INT");

    // Invalid type lookup
    let result = lookup_type_by_name(&pm, &mut bm, "INVALID_TYPE");
    assert!(result.is_err(), "Should fail for INVALID_TYPE");
    match result.err().unwrap() {
        CatalogError::TypeNotFound(name) => assert_eq!(name, "INVALID_TYPE"),
        other => panic!("Expected TypeNotFound, got {:?}", other),
    }

    cleanup();
}

#[test]
fn test_register_builtin_types_idempotent() {
    cleanup();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    let mut pm = CatalogPageManager::new();

    // Count types before
    let before = pm.scan_catalog(&mut bm, "pg_type").unwrap().len();

    // Register again – should not create duplicates
    register_builtin_types(&mut pm, &mut bm).expect("register should succeed");

    let after = pm.scan_catalog(&mut bm, "pg_type").unwrap().len();
    assert_eq!(
        before, after,
        "register_builtin_types should not create duplicates"
    );

    cleanup();
}
