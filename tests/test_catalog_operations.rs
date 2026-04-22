//! End-to-end integration tests for catalog operations (Spec §7.7, §7.8).
//!
//! Covers:
//! - 7.7.1: End-to-end table creation with constraints
//! - 7.7.2: Catalog persistence across restarts
//! - 7.7.3: ALTER TABLE ADD COLUMN
//! - 7.8.1: Drop table with dependent FK (error)
//! - 7.8.2: Invalid type name (error)
//! - Database CRUD: create, show, drop
//! - Table CRUD: create, show, drop, alter

use std::fs;
use std::path::Path;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::page_manager::CatalogPageManager;
use storage_manager::catalog::types::*;
use storage_manager::catalog::*;
use storage_manager::layout::{CATALOG_PAGES_DIR, OID_COUNTER_FILE};

fn cleanup() {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        let _ = fs::remove_dir_all(CATALOG_PAGES_DIR);
    }
    if Path::new(OID_COUNTER_FILE).exists() {
        let _ = fs::remove_file(OID_COUNTER_FILE);
    }
    // Clean up test database directories
    for dir_name in &[
        "ops_test_db",
        "ops_test_db2",
        "persist_db",
        "fk_dep_db",
        "invalid_type_db",
        "alter_db",
    ] {
        let path = format!("database/base/{}", dir_name);
        if Path::new(&path).exists() {
            let _ = fs::remove_dir_all(&path);
        }
    }
}
fn fresh_setup() -> (Catalog, CatalogPageManager, BufferManager) {
    cleanup();
    let mut bm = BufferManager::new();
    bootstrap_catalog(&mut bm).expect("bootstrap should succeed");
    let pm = CatalogPageManager::new();
    let mut catalog = Catalog::new();
    catalog.page_backend_active = true;
    (catalog, pm, bm)
}

// ─────────────────────────────────────────────────────────────
// Database Operations
// ─────────────────────────────────────────────────────────────

#[test]
fn test_create_database_success() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    let result = create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    );
    assert!(
        result.is_ok(),
        "create_database should succeed: {:?}",
        result.err()
    );

    let db_oid = result.unwrap();
    assert!(db_oid > 0);

    // Verify via page-based catalog
    let db = get_database(&mut catalog, &mut pm, &mut bm, "ops_test_db").expect("db should exist");
    assert_eq!(db.db_oid, db_oid);
    assert_eq!(db.owner, "admin");
    assert!(matches!(db.encoding, Encoding::UTF8));

    // Verify database directory was created
    assert!(Path::new("database/base/ops_test_db").exists());

    cleanup();
}

#[test]
fn test_create_database_duplicate_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("first create should succeed");

    let result = create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    );
    assert!(result.is_err(), "Duplicate database should be rejected");
    match result.err().unwrap() {
        CatalogError::DatabaseAlreadyExists(name) => assert_eq!(name, "ops_test_db"),
        other => panic!("Expected DatabaseAlreadyExists, got {:?}", other),
    }

    cleanup();
}

#[test]
fn test_create_database_empty_name_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    let result =
        create_database(&mut catalog, &mut pm, &mut bm, "", "admin", Encoding::UTF8);
    assert!(result.is_err(), "Empty database name should be rejected");

    cleanup();
}

#[test]
fn test_drop_database() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create should succeed");

    assert!(get_database(&mut catalog, &mut pm, &mut bm, "ops_test_db").is_ok());

    let result = drop_database(&mut catalog, &mut pm, &mut bm, "ops_test_db");
    assert!(
        result.is_ok(),
        "drop_database should succeed: {:?}",
        result.err()
    );

    assert!(get_database(&mut catalog, &mut pm, &mut bm, "ops_test_db").is_err());
    assert!(!Path::new("database/base/ops_test_db").exists());

    cleanup();
}

#[test]
fn test_drop_nonexistent_database() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    let result = drop_database(&mut catalog, &mut pm, &mut bm, "nonexistent_db");
    assert!(result.is_err(), "Should fail for non-existent database");
    match result.err().unwrap() {
        CatalogError::DatabaseNotFound(name) => assert_eq!(name, "nonexistent_db"),
        other => panic!("Expected DatabaseNotFound, got {:?}", other),
    }

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.7.1: End-to-end table creation with constraints
// ─────────────────────────────────────────────────────────────

#[test]
fn test_e2e_table_creation_with_pk_constraint() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db should succeed");

    // Create table with PK constraint
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
    ];
    let constraint_defs = vec![ConstraintDefinition::PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("pk_users_id".to_string()),
    }];

    let result = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "users",
        col_defs,
        constraint_defs,
    );
    assert!(
        result.is_ok(),
        "Table creation with PK should succeed: {:?}",
        result.err()
    );

    let table_oid = result.unwrap();

    // Verify table exists in catalog via page manager
    let meta = get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "users").expect("table should exist");
    assert_eq!(meta.table_oid, table_oid);
    assert_eq!(meta.columns.len(), 2);

    // Verify PK constraint was applied
    assert!(
        !meta.constraints.is_empty(),
        "Table should have constraints"
    );
    let pk = meta
        .constraints
        .iter()
        .find(|c| c.constraint_type == ConstraintType::PrimaryKey);
    assert!(pk.is_some(), "PK constraint should be present");

    // Verify id column is NOT NULL
    let id_col = meta.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(!id_col.is_nullable, "PK column should be NOT NULL");

    // Verify backing index exists
    assert!(
        !meta.indexes.is_empty(),
        "Table should have indexes from PK"
    );

    // Verify table data file exists
    let table_file = format!("database/base/ops_test_db/users.dat");
    assert!(Path::new(&table_file).exists());

    cleanup();
}

#[test]
fn test_e2e_table_creation_with_not_null() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db should succeed");

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
    ];
    let constraint_defs = vec![ConstraintDefinition::NotNull {
        column: "name".to_string(),
    }];

    let result = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "users",
        col_defs,
        constraint_defs,
    );
    assert!(
        result.is_ok(),
        "Table creation with NOT NULL should succeed: {:?}",
        result.err()
    );

    let meta = get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "users").expect("table should exist");
    let name_col = meta.columns.iter().find(|c| c.name == "name").unwrap();
    assert!(!name_col.is_nullable, "name column should be NOT NULL");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Table operations
// ─────────────────────────────────────────────────────────────

#[test]
fn test_create_table_duplicate_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db should succeed");

    let col_defs = vec![ColumnDefinition {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    }];

    create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "t1",
        col_defs.clone(),
        vec![],
    )
    .expect("first create should succeed");

    let result = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "t1",
        col_defs,
        vec![],
    );
    assert!(result.is_err());
    match result.err().unwrap() {
        CatalogError::TableAlreadyExists(name) => assert_eq!(name, "t1"),
        other => panic!("Expected TableAlreadyExists, got {:?}", other),
    }

    cleanup();
}

#[test]
fn test_create_table_in_nonexistent_db() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    let result = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "nonexistent_db",
        "t1",
        vec![ColumnDefinition {
            name: "id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        }],
        vec![],
    );
    assert!(result.is_err());
    match result.err().unwrap() {
        CatalogError::DatabaseNotFound(_) => {}
        other => panic!("Expected DatabaseNotFound, got {:?}", other),
    }

    cleanup();
}

#[test]
fn test_drop_table() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let col_defs = vec![ColumnDefinition {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    }];

    let table_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "to_drop",
        col_defs,
        vec![],
    )
    .expect("create table");

    assert!(
        get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "to_drop").is_ok(),
        "Table should exist before drop"
    );

    let result = drop_table(&mut catalog, &mut pm, &mut bm, table_oid);
    assert!(
        result.is_ok(),
        "drop_table should succeed: {:?}",
        result.err()
    );

    assert!(
        get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "to_drop").is_err(),
        "Table should not exist after drop"
    );

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.7.2: Catalog Persistence
// ─────────────────────────────────────────────────────────────

#[test]
fn test_catalog_persistence_across_restart() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    // Create database and table
    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "persist_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

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

    create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "persist_db",
        "persistent_table",
        col_defs,
        vec![],
    )
    .expect("create table");

    // Flush buffer manager
    bm.flush_pages().expect("flush should succeed");

    // "Restart" – create completely fresh buffer manager and load catalog
    let mut bm2 = BufferManager::new();
    let mut catalog2 = load_catalog(&mut bm2);
    let mut pm2 = CatalogPageManager::new();

    // Verify the database survives
    assert!(
        get_database(&mut catalog2, &mut pm2, &mut bm2, "persist_db").is_ok(),
        "Database 'persist_db' should survive restart"
    );

    // Verify the table and columns survive
    let meta = get_table_metadata(&mut catalog2, &mut pm2, &mut bm2, "persist_db", "persistent_table")
        .expect("Table 'persistent_table' should survive restart");
    assert_eq!(
        meta.columns.len(),
        2,
        "Table should have 2 columns after restart"
    );

    let id_col = meta.columns.iter().find(|c| c.name == "id");
    assert!(id_col.is_some(), "Column 'id' should survive restart");

    let name_col = meta.columns.iter().find(|c| c.name == "name");
    assert!(name_col.is_some(), "Column 'name' should survive restart");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.7.3: ALTER TABLE ADD COLUMN
// ─────────────────────────────────────────────────────────────

#[test]
fn test_alter_table_add_column() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "alter_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

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
        "alter_db",
        "users",
        col_defs,
        vec![],
    )
    .expect("create table");

    // Add a new column
    let new_col = ColumnDefinition {
        name: "age".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: Some(DefaultValue::Integer(0)),
    };

    let result = alter_table_add_column(&mut catalog, &mut pm, &mut bm, table_oid, new_col);
    assert!(
        result.is_ok(),
        "ALTER TABLE ADD COLUMN should succeed: {:?}",
        result.err()
    );

    // Verify column was added via page-based catalog
    let meta = get_table_metadata(&mut catalog, &mut pm, &mut bm, "alter_db", "users").expect("table should exist");
    assert_eq!(meta.columns.len(), 3, "Table should have 3 columns");

    let age_col = meta.columns.iter().find(|c| c.name == "age");
    assert!(age_col.is_some(), "New column 'age' should exist");
    let age_col = age_col.unwrap();
    assert_eq!(age_col.column_position, 3);
    assert!(age_col.is_nullable);
    assert_eq!(age_col.default_value, Some(DefaultValue::Integer(0)));

    cleanup();
}

#[test]
fn test_alter_table_add_not_null_without_default_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "alter_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let col_defs = vec![ColumnDefinition {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    }];

    let table_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "alter_db",
        "users",
        col_defs,
        vec![],
    )
    .expect("create table");

    // Add NOT NULL column without default – should fail
    let new_col = ColumnDefinition {
        name: "required_col".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: false,
        default_value: None,
    };

    let result = alter_table_add_column(&mut catalog, &mut pm, &mut bm, table_oid, new_col);
    assert!(
        result.is_err(),
        "NOT NULL without default should be rejected"
    );

    cleanup();
}

#[test]
fn test_alter_table_add_duplicate_column_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "alter_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let col_defs = vec![ColumnDefinition {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    }];

    let table_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "alter_db",
        "users",
        col_defs,
        vec![],
    )
    .expect("create table");

    // Try to add column with existing name
    let dup_col = ColumnDefinition {
        name: "id".to_string(),
        type_name: "INT".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    };

    let result = alter_table_add_column(&mut catalog, &mut pm, &mut bm, table_oid, dup_col);
    assert!(result.is_err(), "Duplicate column name should be rejected");

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.8.1: Drop Table with Dependent FK
// ─────────────────────────────────────────────────────────────

#[test]
fn test_drop_table_with_dependent_fk_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "fk_dep_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    // Create parent table (users) with PK
    let parent_cols = vec![
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
    ];
    let parent_constraints = vec![ConstraintDefinition::PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("pk_users".to_string()),
    }];

    let parent_oid = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "fk_dep_db",
        "users",
        parent_cols,
        parent_constraints,
    )
    .expect("create parent table");

    // Create child table (orders) with FK referencing users
    let child_cols = vec![
        ColumnDefinition {
            name: "order_id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "user_id".to_string(),
            type_name: "INT".to_string(),
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        },
    ];
    let child_constraints = vec![ConstraintDefinition::ForeignKey {
        columns: vec!["user_id".to_string()],
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
        name: Some("fk_orders_user".to_string()),
    }];

    create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "fk_dep_db",
        "orders",
        child_cols,
        child_constraints,
    )
    .expect("create child table");

    // Try to drop parent table (users) – should fail due to FK dependency
    let result = drop_table(&mut catalog, &mut pm, &mut bm, parent_oid);
    assert!(
        result.is_err(),
        "Should not be able to drop table with FK dependents"
    );
    match result.err().unwrap() {
        CatalogError::ForeignKeyDependency(_) => {}
        other => panic!("Expected ForeignKeyDependency, got {:?}", other),
    }

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test 7.8.2: Invalid Type Name
// ─────────────────────────────────────────────────────────────

#[test]
fn test_create_table_invalid_type_rejected() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "invalid_type_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let col_defs = vec![ColumnDefinition {
        name: "id".to_string(),
        type_name: "INVALID_TYPE".to_string(),
        type_modifier: None,
        is_nullable: true,
        default_value: None,
    }];

    let result = create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "invalid_type_db",
        "test",
        col_defs,
        vec![],
    );
    assert!(result.is_err(), "Invalid type should be rejected");
    match result.err().unwrap() {
        CatalogError::TypeNotFound(name) => assert_eq!(name, "INVALID_TYPE"),
        other => panic!("Expected TypeNotFound, got {:?}", other),
    }

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: get_table_metadata
// ─────────────────────────────────────────────────────────────

#[test]
fn test_get_table_metadata() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let col_defs = vec![
        ColumnDefinition {
            name: "id".to_string(),
            type_name: "INT".to_string(),
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
    let constraint_defs = vec![ConstraintDefinition::PrimaryKey {
        columns: vec!["id".to_string()],
        name: None,
    }];

    create_table(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "meta_test",
        col_defs,
        constraint_defs,
    )
    .expect("create table");

    let meta = get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "meta_test");
    assert!(
        meta.is_ok(),
        "get_table_metadata should succeed: {:?}",
        meta.err()
    );

    let meta = meta.unwrap();
    assert_eq!(meta.table_name, "meta_test");
    assert_eq!(meta.columns.len(), 2);
    assert!(!meta.constraints.is_empty(), "Should have constraints");
    assert!(!meta.indexes.is_empty(), "Should have indexes");

    cleanup();
}

#[test]
fn test_get_table_metadata_nonexistent() {
    let (mut catalog, mut pm, mut bm) = fresh_setup();

    create_database(
        &mut catalog,
        &mut pm,
        &mut bm,
        "ops_test_db",
        "admin",
        Encoding::UTF8,
    )
    .expect("create db");

    let result = get_table_metadata(&mut catalog, &mut pm, &mut bm, "ops_test_db", "nonexistent_table");
    assert!(result.is_err());

    cleanup();
}

// ─────────────────────────────────────────────────────────────
// Test: Catalog struct basic operations
// ─────────────────────────────────────────────────────────────

#[test]
fn test_catalog_new() {
    let catalog = Catalog::new();
    assert_eq!(catalog.oid_counter, 10_000);
    assert!(!catalog.bootstrap_mode);
    assert!(!catalog.page_backend_active);
}

#[test]
fn test_catalog_alloc_oid() {
    let mut catalog = Catalog::new();
    let oid1 = catalog.alloc_oid();
    let oid2 = catalog.alloc_oid();
    let oid3 = catalog.alloc_oid();

    assert_eq!(oid1, 10_000);
    assert_eq!(oid2, 10_001);
    assert_eq!(oid3, 10_002);
    assert_eq!(catalog.oid_counter, 10_003);
}

// ─────────────────────────────────────────────────────────────
// Test: Error display messages
// ─────────────────────────────────────────────────────────────

#[test]
fn test_catalog_error_display() {
    assert_eq!(
        format!("{}", CatalogError::DatabaseNotFound("mydb".into())),
        "Database 'mydb' not found"
    );
    assert_eq!(
        format!("{}", CatalogError::TableAlreadyExists("users".into())),
        "Table 'users' already exists"
    );
    assert_eq!(
        format!("{}", CatalogError::TypeNotFound("DECIMAL".into())),
        "Type 'DECIMAL' not found"
    );
    assert_eq!(
        format!("{}", CatalogError::AlreadyHasPrimaryKey),
        "Table already has a primary key"
    );
    assert_eq!(
        format!("{}", CatalogError::ColumnCountMismatch),
        "Referencing and referenced column counts differ"
    );
}

#[test]
fn test_constraint_violation_display() {
    assert_eq!(
        format!(
            "{}",
            ConstraintViolation::NotNullViolation {
                column: "name".into()
            }
        ),
        "NOT NULL violation on column 'name'"
    );
    assert_eq!(
        format!(
            "{}",
            ConstraintViolation::UniqueViolation {
                constraint: "uq_email".into()
            }
        ),
        "UNIQUE violation on constraint 'uq_email'"
    );
    assert_eq!(
        format!(
            "{}",
            ConstraintViolation::ForeignKeyViolation {
                constraint: "fk_ref".into()
            }
        ),
        "FOREIGN KEY violation on constraint 'fk_ref'"
    );
}
