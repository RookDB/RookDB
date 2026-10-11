//! End-to-end integration tests for recursive FOREIGN KEY actions.
//!
//! Tests the full constraint enforcement pipeline by creating tables with
//! FOREIGN KEY constraints (parent → child → grandchild), inserting data,
//! and verifying that ON DELETE/UPDATE CASCADE/SET NULL actions propagate
//! correctly through the entire chain.
//!
//! Also tests that circular FK chains are safely handled by cycle detection.
//!
//! Run with:
//!   cargo test --test test_fk_actions -- --test-threads=1

use std::sync::Mutex;

use rook_parser::parse_set_clause;
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::catalog::types::{Catalog, Column, Constraints};
use storage_manager::catalog::{create_database, create_table, init_catalog, load_catalog};
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::heap::HeapManager;
use storage_manager::types::datatype::DataType;
use storage_manager::types::row::deserialize_nullable_row;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

use std::path::PathBuf;

/// Per-test isolated workspace: creates `database_ws_<pid>_<tag>` under the
/// crate root, switches the process into it, and removes it on drop — even
/// when the test panics. Restores the previous working directory first.
struct TestWorkspace {
    prev_cwd: PathBuf,
    path: PathBuf,
}

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        let prev_cwd = std::env::current_dir().expect("read cwd");
        let path = prev_cwd.join(format!("database_ws_p{}_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("base")).expect("create workspace");
        std::env::set_current_dir(&path).expect("chdir into workspace");
        storage_manager::backend::executor::row_select::register_where_parser(
            rook_parser::parse_where_text,
        );
        storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
        Self { prev_cwd, path }
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        if std::env::set_current_dir(&self.prev_cwd).is_ok() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse a WHERE string with the real SQL grammar, select matching rows on
/// the Volcano engine, then delete them by pointer.
fn exec_delete_where(
    catalog: &Catalog,
    db_name: &str,
    table: &str,
    where_text: &str,
) -> storage_manager::executor::DeleteResult {
    let selection = parse_where_text(where_text).expect("parse WHERE");
    let pointers =
        select_matching_pointers(catalog, db_name, table, selection).expect("select pointers");
    storage_manager::executor::delete_by_pointers(catalog, db_name, table, &pointers)
        .expect("delete_by_pointers")
}

/// Volcano-selected UPDATE by pointer.
fn exec_update_where(
    catalog: &Catalog,
    db_name: &str,
    table: &str,
    where_text: &str,
    assignments: &[storage_manager::executor::SetAssignment],
) -> storage_manager::executor::UpdateResult {
    let selection = parse_where_text(where_text).expect("parse WHERE");
    let pointers =
        select_matching_pointers(catalog, db_name, table, selection).expect("select pointers");
    storage_manager::executor::update_by_pointers(catalog, db_name, table, &pointers, assignments)
        .expect("update_by_pointers")
}

/// Create the standard 3-table schema used by most FK action tests:
///   users(id:INT PK, name:VARCHAR(50))
///   orders(id:INT PK, user_id:INT FK→users.id, amount:INT)
///   order_items(id:INT PK, order_id:INT FK→orders.id, item:VARCHAR(50))
fn create_three_tier_schema(catalog: &mut Catalog, db_name: &str) {
    let users_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Varchar(50),
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    let orders_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
        Column {
            name: "amount".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    let order_items_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "order_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
        Column {
            name: "item".to_string(),
            data_type: DataType::Varchar(50),
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(catalog, db_name, "users", users_cols);
    create_table(catalog, db_name, "orders", orders_cols);
    create_table(catalog, db_name, "order_items", order_items_cols);
}

/// Count the number of tuples (rows) in a table by scanning the heap.
fn count_tuples(db_name: &str, table_name: &str) -> usize {
    let path: std::path::PathBuf = format!("database/base/{}/{}.dat", db_name, table_name).into();
    if !path.exists() {
        return 0;
    }
    match HeapManager::open(path) {
        Ok(heap) => heap.scan().filter_map(|r| r.ok()).count(),
        Err(_) => 0,
    }
}

/// Scan a table and return all values of a specific column as debug-formatted strings.
/// NULL values appear as "NULL".
fn get_column_values(db_name: &str, table_name: &str, column_name: &str) -> Vec<String> {
    let path: std::path::PathBuf = format!("database/base/{}/{}.dat", db_name, table_name).into();
    if !path.exists() {
        return Vec::new();
    }

    let heap = match HeapManager::open(path) {
        Ok(h) => h,
        Err(_) => return Vec::new(),
    };

    // Load schema to decode properly
    let catalog = load_catalog();
    let db = match catalog.databases.get(db_name) {
        Some(d) => d,
        None => return Vec::new(),
    };
    let table = match db.tables.get(table_name) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let col_pos = match table
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column_name))
    {
        Some(p) => p,
        None => return Vec::new(),
    };

    let schema_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
    let mut values = Vec::new();

    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) = match result {
            Ok(triple) => triple,
            Err(_) => continue,
        };
        let decoded = match deserialize_nullable_row(&schema_types, &raw_bytes) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if let Some(Some(dv)) = decoded.get(col_pos) {
            values.push(format!("{:?}", dv));
        } else {
            values.push("NULL".to_string());
        }
    }

    values
}

// ═══════════════════════════════════════════════════════════════════════════════
// MINIMAL CASCADE — Verify basic ON DELETE CASCADE works with 2 tables
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_minimal_delete_cascade() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("01");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    let users_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Varchar(50),
            nullable: true,
            constraints: Constraints::default(),
        },
    ];
    let orders_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
        Column {
            name: "amount".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, "users", users_cols);
    create_table(&mut catalog, db_name, "orders", orders_cols);

    // FK: orders.user_id → users.id ON DELETE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON DELETE CASCADE",
        "user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1", "Alice"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1", "100"]).unwrap(),
        "insert into orders"
    );

    // Execute: DELETE FROM users WHERE id = 1
    let result = exec_delete_where(&catalog, db_name, "users", "id = 1");

    assert_eq!(result.deleted_count, 1, "Should delete 1 user");
    assert_eq!(count_tuples(db_name, "users"), 0, "users should be empty");
    assert_eq!(
        count_tuples(db_name, "orders"),
        0,
        "orders should be empty after CASCADE"
    );

    let _ws = TestWorkspace::new("02");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON DELETE CASCADE — Recursive (parent → child → grandchild)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_recursive_delete_cascade() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("03");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );
    create_three_tier_schema(&mut catalog, db_name);

    // Insert FK constraints:
    //   orders.user_id → users.id ON DELETE CASCADE
    //   order_items.order_id → orders.id ON DELETE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON DELETE CASCADE",
        "user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON DELETE CASCADE",
        "order_id",
        Some("orders"),
        Some("id"),
    )
    .expect("Failed to insert order_items FK constraint");

    // Reload catalog so metadata is fresh from system tables
    let catalog = load_catalog();

    // Insert data:
    //   users: (1, 'Alice')
    //   orders: (1, 1, 100), (2, 1, 200)
    //   order_items: (1, 1, 'Widget'), (2, 1, 'Gadget'), (3, 2, 'Doohickey')
    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1", "Alice"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1", "100"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1", "200"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1", "Widget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1", "Gadget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "2", "Doohickey"]).unwrap(),
        "insert into order_items"
    );

    // Verify initial state
    assert_eq!(count_tuples(db_name, "users"), 1);
    assert_eq!(count_tuples(db_name, "orders"), 2);
    assert_eq!(count_tuples(db_name, "order_items"), 3);

    // Execute: DELETE FROM users WHERE id = 1
    let result = exec_delete_where(&catalog, db_name, "users", "id = 1");
    assert_eq!(result.deleted_count, 1, "Should delete 1 user");

    // Verify cascade: all 3 tables should be empty
    assert_eq!(count_tuples(db_name, "users"), 0, "users should be empty");
    assert_eq!(count_tuples(db_name, "orders"), 0, "orders should be empty");
    assert_eq!(
        count_tuples(db_name, "order_items"),
        0,
        "order_items should be empty"
    );

    let _ws = TestWorkspace::new("04");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON DELETE SET NULL — Recursive (parent → child → grandchild)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_recursive_delete_set_null() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("05");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );
    create_three_tier_schema(&mut catalog, db_name);

    // FK: orders.user_id → users.id ON DELETE SET NULL
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON DELETE SET NULL",
        "user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    // FK: order_items.order_id → orders.id ON DELETE SET NULL
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON DELETE SET NULL",
        "order_id",
        Some("orders"),
        Some("id"),
    )
    .expect("Failed to insert order_items FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1", "Alice"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1", "100"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1", "200"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1", "Widget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1", "Gadget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "2", "Doohickey"]).unwrap(),
        "insert into order_items"
    );

    assert_eq!(count_tuples(db_name, "users"), 1);
    assert_eq!(count_tuples(db_name, "orders"), 2);
    assert_eq!(count_tuples(db_name, "order_items"), 3);

    // DELETE FROM users WHERE id = 1
    let result = exec_delete_where(&catalog, db_name, "users", "id = 1");
    assert_eq!(result.deleted_count, 1);

    // Verify: users deleted, children/grandchildren still exist with NULL FK columns
    assert_eq!(count_tuples(db_name, "users"), 0);
    assert_eq!(
        count_tuples(db_name, "orders"),
        2,
        "orders should still exist"
    );
    assert_eq!(
        count_tuples(db_name, "order_items"),
        3,
        "order_items should still exist"
    );

    let order_user_ids = get_column_values(db_name, "orders", "user_id");
    for val in &order_user_ids {
        assert_eq!(val, "NULL", "orders.user_id should be NULL, got {:?}", val);
    }

    let item_order_ids = get_column_values(db_name, "order_items", "order_id");
    for val in &item_order_ids {
        assert!(
            val == "Int(1)" || val == "Int(2)",
            "order_items.order_id should be unchanged, got {:?}",
            val
        );
    }

    let _ws = TestWorkspace::new("06");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON UPDATE CASCADE — Recursive (parent → child)
// ═══════════════════════════════════════════════════════════════════════════════
// Verifies 1-level cascade (child updated). Grandchild won't cascade because
// the FK references a different column (orders.id) than the one updated (orders.user_id).

#[test]
fn test_recursive_update_cascade() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("07");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );
    create_three_tier_schema(&mut catalog, db_name);

    // FK: orders.user_id → users.id ON UPDATE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON UPDATE CASCADE",
        "user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    // FK: order_items.order_id → orders.id ON UPDATE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON UPDATE CASCADE",
        "order_id",
        Some("orders"),
        Some("id"),
    )
    .expect("Failed to insert order_items FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1", "Alice"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1", "100"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1", "200"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1", "Widget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1", "Gadget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "2", "Doohickey"]).unwrap(),
        "insert into order_items"
    );

    // UPDATE users SET id = 10 WHERE id = 1
    let assignments = parse_set_clause("id = 10").unwrap();
    let result = exec_update_where(&catalog, db_name, "users", "id = 1", &assignments);
    assert_eq!(result.updated_count, 1);

    // Verify: orders.user_id = 10 (cascaded), but order_items.order_id unchanged
    // because the FK references orders.id (not orders.user_id).
    assert_eq!(count_tuples(db_name, "users"), 1);
    assert_eq!(count_tuples(db_name, "orders"), 2);
    assert_eq!(count_tuples(db_name, "order_items"), 3);

    let order_user_ids = get_column_values(db_name, "orders", "user_id");
    for val in &order_user_ids {
        assert_eq!(
            val, "Int(10)",
            "orders.user_id should be Int(10), got {:?}",
            val
        );
    }

    let item_order_ids = get_column_values(db_name, "order_items", "order_id");
    for val in &item_order_ids {
        assert!(
            val == "Int(1)" || val == "Int(2)",
            "order_items.order_id should be unchanged, got {:?}",
            val
        );
    }

    let _ws = TestWorkspace::new("08");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON UPDATE CASCADE — Recursive through SAME column chain
// ═══════════════════════════════════════════════════════════════════════════════
// Schema where grandchild FK references the SAME column that gets updated:
//   users.id → orders.fk_user_id (FK) → order_items.fk_user_id (FK, references same column)

#[test]
fn test_recursive_update_cascade_same_column_chain() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("09");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    let users_cols = vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int,
        nullable: false,
        constraints: Constraints::default(),
    }];
    let orders_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "fk_user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];
    let order_items_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "fk_user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, "users", users_cols);
    create_table(&mut catalog, db_name, "orders", orders_cols);
    create_table(&mut catalog, db_name, "order_items", order_items_cols);

    // FK: orders.fk_user_id → users.id ON UPDATE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON UPDATE CASCADE",
        "fk_user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    // FK: order_items.fk_user_id → orders.fk_user_id ON UPDATE CASCADE (same column chain!)
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON UPDATE CASCADE",
        "fk_user_id",
        Some("orders"),
        Some("fk_user_id"),
    )
    .expect("Failed to insert order_items FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "1"]).unwrap(),
        "insert into order_items (3, 1)"
    );

    // UPDATE users SET id = 10 WHERE id = 1
    let assignments = parse_set_clause("id = 10").unwrap();
    let result = exec_update_where(&catalog, db_name, "users", "id = 1", &assignments);
    assert_eq!(result.updated_count, 1);

    // Verify recursive CASCADE through entire chain (all 3 order_items have fk_user_id=1):
    let order_fk_values = get_column_values(db_name, "orders", "fk_user_id");
    for val in &order_fk_values {
        assert_eq!(
            val, "Int(10)",
            "orders.fk_user_id should be Int(10), got {:?}",
            val
        );
    }

    let item_fk_values = get_column_values(db_name, "order_items", "fk_user_id");
    for val in &item_fk_values {
        assert_eq!(
            val, "Int(10)",
            "order_items.fk_user_id should be Int(10), got {:?}",
            val
        );
    }

    let _ws = TestWorkspace::new("10");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON UPDATE SET NULL — Recursive (parent → child)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_recursive_update_set_null() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("11");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );
    create_three_tier_schema(&mut catalog, db_name);

    // FK: orders.user_id → users.id ON UPDATE SET NULL
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON UPDATE SET NULL",
        "user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    // FK: order_items.order_id → orders.id ON UPDATE SET NULL
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON UPDATE SET NULL",
        "order_id",
        Some("orders"),
        Some("id"),
    )
    .expect("Failed to insert order_items FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1", "Alice"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1", "100"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1", "200"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1", "Widget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1", "Gadget"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "2", "Doohickey"]).unwrap(),
        "insert into order_items"
    );

    // UPDATE users SET id = 10 WHERE id = 1
    let assignments = parse_set_clause("id = 10").unwrap();
    let result = exec_update_where(&catalog, db_name, "users", "id = 1", &assignments);
    assert_eq!(result.updated_count, 1);

    // Verify: orders.user_id = NULL, order_items.order_id unchanged (different column chain)
    assert_eq!(count_tuples(db_name, "orders"), 2);
    assert_eq!(count_tuples(db_name, "order_items"), 3);

    let order_user_ids = get_column_values(db_name, "orders", "user_id");
    for val in &order_user_ids {
        assert_eq!(val, "NULL", "orders.user_id should be NULL, got {:?}", val);
    }

    let item_order_ids = get_column_values(db_name, "order_items", "order_id");
    for val in &item_order_ids {
        assert!(
            val == "Int(1)" || val == "Int(2)",
            "order_items.order_id should be unchanged, got {:?}",
            val
        );
    }

    let _ws = TestWorkspace::new("12");
}

// ═══════════════════════════════════════════════════════════════════════════════
// ON UPDATE SET NULL — Recursive through SAME column chain
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_recursive_update_set_null_same_column_chain() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("13");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    let users_cols = vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int,
        nullable: false,
        constraints: Constraints::default(),
    }];
    let orders_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "fk_user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];
    let order_items_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "fk_user_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, "users", users_cols);
    create_table(&mut catalog, db_name, "orders", orders_cols);
    create_table(&mut catalog, db_name, "order_items", order_items_cols);

    // FK: orders.fk_user_id → users.id ON UPDATE SET NULL
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "orders",
        "FOREIGN KEY ON UPDATE SET NULL",
        "fk_user_id",
        Some("users"),
        Some("id"),
    )
    .expect("Failed to insert orders FK constraint");
    // FK: order_items.fk_user_id → orders.fk_user_id ON UPDATE SET NULL (same column chain!)
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "order_items",
        "FOREIGN KEY ON UPDATE SET NULL",
        "fk_user_id",
        Some("orders"),
        Some("fk_user_id"),
    )
    .expect("Failed to insert order_items FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "users", &["1"]).unwrap(),
        "insert into users"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["1", "1"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "orders", &["2", "1"]).unwrap(),
        "insert into orders"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["1", "1"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["2", "1"]).unwrap(),
        "insert into order_items"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "order_items", &["3", "1"]).unwrap(),
        "insert into order_items (3, 1)"
    );

    // UPDATE users SET id = 10 WHERE id = 1
    let assignments = parse_set_clause("id = 10").unwrap();
    let result = exec_update_where(&catalog, db_name, "users", "id = 1", &assignments);
    assert_eq!(result.updated_count, 1);

    // Verify recursive SET NULL through entire chain (all 3 order_items have fk_user_id=1):
    let order_fk_values = get_column_values(db_name, "orders", "fk_user_id");
    for val in &order_fk_values {
        assert_eq!(
            val, "NULL",
            "orders.fk_user_id should be NULL, got {:?}",
            val
        );
    }

    let item_fk_values = get_column_values(db_name, "order_items", "fk_user_id");
    for val in &item_fk_values {
        assert_eq!(
            val, "NULL",
            "order_items.fk_user_id should be NULL, got {:?}",
            val
        );
    }

    let _ws = TestWorkspace::new("14");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Cycle detection — Circular FK chain does not infinite-loop
// ═══════════════════════════════════════════════════════════════════════════════
// A.a_ref → B.id ON DELETE CASCADE, B.b_ref → A.id ON DELETE CASCADE

#[test]
fn test_cycle_detection_delete_cascade() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("15");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    let a_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "a_ref".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];
    let b_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "b_ref".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, "table_a", a_cols);
    create_table(&mut catalog, db_name, "table_b", b_cols);

    // Circular FK chain:
    //   table_a.a_ref → table_b.id ON DELETE CASCADE
    //   table_b.b_ref → table_a.id ON DELETE CASCADE
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "table_a",
        "FOREIGN KEY ON DELETE CASCADE",
        "a_ref",
        Some("table_b"),
        Some("id"),
    )
    .expect("Failed to insert table_a FK constraint");
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "table_b",
        "FOREIGN KEY ON DELETE CASCADE",
        "b_ref",
        Some("table_a"),
        Some("id"),
    )
    .expect("Failed to insert table_b FK constraint");

    let catalog = load_catalog();

    // Insert: A(1, NULL), B(2, 1) — circular refs
    assert!(
        insert_single_tuple(&catalog, db_name, "table_a", &["1", "NULL"]).unwrap(),
        "insert into table_a as (1, NULL)"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "table_b", &["2", "1"]).unwrap(),
        "insert into table_b as (2, 1)"
    );

    // Update A(1, NULL) to A(1, 2)
    let assignments = parse_set_clause("a_ref = 2").unwrap();
    let _ = exec_update_where(&catalog, db_name, "table_a", "id = 1", &assignments);

    assert_eq!(count_tuples(db_name, "table_a"), 1);
    assert_eq!(count_tuples(db_name, "table_b"), 1);

    // DELETE FROM table_a WHERE id = 1
    // This should NOT infinite-loop thanks to cycle detection.
    let result = exec_delete_where(&catalog, db_name, "table_a", "id = 1");
    assert_eq!(result.deleted_count, 1, "Should delete 1 row from table_a");

    // Verify no crash and data is consistent
    let a_count = count_tuples(db_name, "table_a");
    let b_count = count_tuples(db_name, "table_b");
    assert!(
        a_count <= 1,
        "table_a should have 0-1 rows, got {}",
        a_count
    );
    assert!(
        b_count <= 1,
        "table_b should have 0-1 rows, got {}",
        b_count
    );

    let _ws = TestWorkspace::new("16");
}

// ═══════════════════════════════════════════════════════════════════════════════
// FOREIGN KEY RESTRICT — blocks DELETE when child rows exist
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn test_fk_restrict_blocks_delete() {
    let _lock = TEST_MUTEX.lock().unwrap();
    let _ws = TestWorkspace::new("17");
    init_catalog();

    let mut catalog = load_catalog();
    let db_name = "test_db";

    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    let parents_cols = vec![Column {
        name: "id".to_string(),
        data_type: DataType::Int,
        nullable: false,
        constraints: Constraints::default(),
    }];
    let children_cols = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "parent_id".to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, "parents", parents_cols);
    create_table(&mut catalog, db_name, "children", children_cols);

    // FK with RESTRICT (default): just "FOREIGN KEY"
    storage_manager::backend::system_table::insert_constraint_metadata(
        db_name,
        "children",
        "FOREIGN KEY",
        "parent_id",
        Some("parents"),
        Some("id"),
    )
    .expect("Failed to insert FK constraint");

    let catalog = load_catalog();

    assert!(
        insert_single_tuple(&catalog, db_name, "parents", &["1"]).unwrap(),
        "insert into parents"
    );
    assert!(
        insert_single_tuple(&catalog, db_name, "children", &["1", "1"]).unwrap(),
        "insert into children"
    );

    assert_eq!(count_tuples(db_name, "parents"), 1);
    assert_eq!(count_tuples(db_name, "children"), 1);

    // DELETE FROM parents WHERE id = 1 → should be blocked by RESTRICT
    let selection = parse_where_text("id = 1").expect("parse WHERE");
    let pointers =
        select_matching_pointers(&catalog, db_name, "parents", selection).expect("select pointers");
    let err =
        storage_manager::executor::delete_by_pointers(&catalog, db_name, "parents", &pointers)
            .err()
            .expect("RESTRICT should block DELETE");
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");
    assert_eq!(
        count_tuples(db_name, "parents"),
        1,
        "parents row should still exist"
    );

    let _ws = TestWorkspace::new("18");
}
