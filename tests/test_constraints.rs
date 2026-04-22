//! Tests for constraint creation and validation (Spec §7.3).
//!
//! Covers:
//! - 7.3.1: Primary Key constraint – creation and duplicate detection
//! - 7.3.2: Foreign Key constraint – creation and referential integrity
//! - 7.3.5: UNIQUE constraint – creation and duplicate detection
//! - 7.3.6: NOT NULL constraint – creation and NULL rejection
//! - 7.3.7: Composite Primary Key
//!
//! Note: These tests require page-based catalog infrastructure so they
//! bootstrap the catalog, create databases/tables, and then test constraints.
//! Tests MUST run serially due to shared filesystem state.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::constraints::*;
use storage_manager::catalog::indexes::get_indexes_for_table;
use storage_manager::catalog::page_manager::CatalogPageManager;

use storage_manager::catalog::types::*;
use storage_manager::catalog::{
    bootstrap_catalog, create_database, create_table,
};
use storage_manager::layout::{CATALOG_PAGES_DIR, OID_COUNTER_FILE};

fn cleanup() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    // Clean up any test database directories
    if Path::new("database/base/constraint_test_db").exists() {
        let _ = fs::remove_dir_all("database/base/constraint_test_db");
    }
}

/// Setup: bootstrap catalog, create a test database with a table that has columns.
/// Returns (Catalog, CatalogPageManager, BufferManager, table_oid, column_oids)
fn setup_test_table() -> (Catalog, CatalogPageManager, BufferManager, u32) {
    cleanup();

    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");

    let mut pm = CatalogPageManager::new();
    let mut catalog = Catalog::new();
    catalog.page_backend_active = true;

    // Create a test database
    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "constraint_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create database should succeed");

    // Create a test table: users(id INT, name TEXT, email TEXT)
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
            type_name: "TEXT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "email".to_string(),
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
        "constraint_test_db",
        "users",
        col_defs,
        vec![],
    )
    .expect("create table should succeed");

    (catalog, pm, bm, table_oid)
}

// ─────────────────────────────────────────────────────────────
// Test 7.3.1: Primary Key Constraint
// ─────────────────────────────────────────────────────────────

#[test]
fn test_add_primary_key_constraint() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // Add PK on "id" column
    let result = add_primary_key_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["id".to_string()],
        Some("pk_users_id".to_string()),
    );
    assert!(
        result.is_ok(),
        "Should add PK constraint: {:?}",
        result.err()
    );

    let pk_oid = result.unwrap();
    assert!(pk_oid > 0, "PK constraint OID should be positive");

    // Verify constraint is persisted in pg_constraint
    let constraints = get_constraints_for_table(&catalog, &pm, &mut bm, table_oid)
        .expect("should get constraints");
    assert!(constraints.len() >= 1, "Should have at least 1 constraint");
    let pk = constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey);
    assert!(pk.is_some(), "Should find PK constraint");
    assert_eq!(pk.unwrap().constraint_name, "pk_users_id");

    // Verify backing index was created in pg_index
    let indexes = get_indexes_for_table(&pm, &mut bm, table_oid).expect("should get indexes");
    assert!(indexes.len() >= 1, "PK should create a backing index");
    let pk_idx = indexes.iter().find(|i| i.is_primary);
    assert!(pk_idx.is_some(), "Should find a primary index");
    assert!(pk_idx.unwrap().is_unique, "PK index should be unique");

    // Verify is_nullable was set to false for PK column
    let meta = storage_manager::catalog::get_table_metadata(&mut catalog, &mut pm, &mut bm, "constraint_test_db", "users").expect("should get metadata");
    let id_col = meta.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(!id_col.is_nullable, "PK column should be NOT NULL");

    cleanup();
}

#[test]
fn test_add_duplicate_primary_key_rejected() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // First PK succeeds
    add_primary_key_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["id".to_string()],
        None,
    )
    .expect("First PK should succeed");

    // Second PK should fail
    let result = add_primary_key_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["name".to_string()],
        None,
    );
    assert!(result.is_err(), "Second PK should be rejected");
    match result.err().unwrap() {
        CatalogError::AlreadyHasPrimaryKey => {}
        other => panic!("Expected AlreadyHasPrimaryKey, got {:?}", other),
    }

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.3.5: UNIQUE Constraint
// ─────────────────────────────────────────────────────────────

#[test]
fn test_add_unique_constraint() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let result = add_unique_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["email".to_string()],
        Some("uq_email".to_string()),
    );
    assert!(
        result.is_ok(),
        "Should add UNIQUE constraint: {:?}",
        result.err()
    );

    // Verify constraint persisted
    let constraints = get_constraints_for_table(&catalog, &pm, &mut bm, table_oid)
        .expect("should get constraints");
    let uq = constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::Unique);
    assert!(uq.is_some(), "Should find UNIQUE constraint");

    // Verify backing index exists and is unique but NOT primary
    let indexes = get_indexes_for_table(&pm, &mut bm, table_oid).expect("should get indexes");
    let uq_idx = indexes.iter().find(|i| i.is_unique && !i.is_primary);
    assert!(
        uq_idx.is_some(),
        "UNIQUE constraint should create a unique non-primary index"
    );

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.3.6: NOT NULL Constraint
// ─────────────────────────────────────────────────────────────

#[test]
fn test_add_not_null_constraint() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // Get the "name" column OID
    let name_col_oid = {
        let cols = storage_manager::catalog::get_columns(&pm, &mut bm, table_oid).expect("should get columns");
        cols.iter()
            .find(|c| c.name == "name")
            .unwrap()
            .column_oid
    };

    // Should be nullable initially
    {
        let cols = storage_manager::catalog::get_columns(&pm, &mut bm, table_oid).expect("should get columns");
        let name_col = cols.iter().find(|c| c.name == "name").unwrap();
        assert!(
            name_col.is_nullable,
            "Column should be nullable before NOT NULL constraint"
        );
    }

    let result = add_not_null_constraint(&mut catalog, &mut pm, &mut bm, table_oid, name_col_oid);
    assert!(
        result.is_ok(),
        "Should add NOT NULL constraint: {:?}",
        result.err()
    );

    // Verify is_nullable is now false
    let cols = storage_manager::catalog::get_columns(&pm, &mut bm, table_oid).expect("should get columns");
    let name_col = cols.iter().find(|c| c.name == "name").unwrap();
    assert!(
        !name_col.is_nullable,
        "Column should be NOT NULL after constraint"
    );

    cleanup();
}

#[test]
fn test_add_not_null_invalid_column() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    let result = add_not_null_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        99999, // non-existent column OID
    );
    assert!(result.is_err(), "Should fail for non-existent column");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.3.7: Composite Primary Key
// ─────────────────────────────────────────────────────────────

#[test]
fn test_composite_primary_key() {
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
        "constraint_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create database should succeed");

    // Create enrollment table: (student_id INT, course_id INT)
    let col_defs = vec![
        ColumnDefinition {
            name: "student_id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "course_id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
    ];

    let table_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "constraint_test_db",
        "enrollment",
        col_defs,
        vec![],
    )
    .expect("create table should succeed");

    // Add composite PK on (student_id, course_id)
    let result = add_primary_key_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["student_id".to_string(), "course_id".to_string()],
        Some("pk_enrollment".to_string()),
    );
    assert!(
        result.is_ok(),
        "Composite PK should succeed: {:?}",
        result.err()
    );

    // Verify constraint has 2 column OIDs
    let constraints = get_constraints_for_table(&catalog, &pm, &mut bm, table_oid)
        .expect("should get constraints");
    let pk = constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey)
        .unwrap();
    assert_eq!(
        pk.column_oids.len(),
        2,
        "Composite PK should reference 2 columns"
    );

    // Both columns should be NOT NULL
    let meta = storage_manager::catalog::get_table_metadata(&mut catalog, &mut pm, &mut bm, "constraint_test_db", "enrollment").expect("should get metadata");
    for col in &meta.columns {
        assert!(
            !col.is_nullable,
            "Column '{}' should be NOT NULL in composite PK",
            col.name
        );
    }

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: validate_constraints - NOT NULL violation
// ─────────────────────────────────────────────────────────────

#[test]
fn test_validate_not_null_violation() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // Get the "name" column OID and add NOT NULL
    let name_col_oid = {
        let cols = storage_manager::catalog::get_columns(&pm, &mut bm, table_oid).expect("should get columns");
        cols.iter()
            .find(|c| c.name == "name")
            .unwrap()
            .column_oid
    };

    add_not_null_constraint(&mut catalog, &mut pm, &mut bm, table_oid, name_col_oid)
        .expect("NOT NULL should succeed");

    // Create tuple values with NULL for the NOT NULL column
    let mut tuple_values: HashMap<u32, Option<Vec<u8>>> = HashMap::new();
    tuple_values.insert(name_col_oid, None); // NULL value for NOT NULL column

    let result = validate_constraints(&catalog, &pm, &mut bm, table_oid, &tuple_values);
    assert!(result.is_err(), "Should reject NULL for NOT NULL column");
    match result.err().unwrap() {
        ConstraintViolation::NotNullViolation { .. } => {}
        other => panic!("Expected NotNullViolation, got {:?}", other),
    }

    cleanup();
}

#[test]
fn test_validate_constraints_pass() {
    let (catalog, pm, mut bm, table_oid) = setup_test_table();

    // No constraints added yet, so validation should pass
    let tuple_values: HashMap<u32, Option<Vec<u8>>> = HashMap::new();
    let result = validate_constraints(&catalog, &pm, &mut bm, table_oid, &tuple_values);
    assert!(result.is_ok(), "Validation should pass with no constraints");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: Foreign Key - column count mismatch
// ─────────────────────────────────────────────────────────────

#[test]
fn test_foreign_key_column_count_mismatch() {
    let (mut catalog, mut pm, mut bm, table_oid) = setup_test_table();

    // Try to add FK with mismatched column counts
    let result = add_foreign_key_constraint(
        &mut catalog,
        &mut pm,
        &mut bm,
        table_oid,
        vec!["id".to_string(), "name".to_string()], // 2 columns
        table_oid,                                  // self-reference
        vec!["id".to_string()],                     // only 1 referenced column
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
        None,
    );
    assert!(
        result.is_err(),
        "Should reject FK with column count mismatch"
    );
    match result.err().unwrap() {
        CatalogError::ColumnCountMismatch => {}
        other => panic!("Expected ColumnCountMismatch, got {:?}", other),
    }

    cleanup();
}
