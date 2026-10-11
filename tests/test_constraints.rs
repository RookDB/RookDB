//! Constraint-enforcement integration tests.
//!
//! Covers NOT NULL, UNIQUE (with and without a B+ Tree index), CHECK and
//! FOREIGN KEY (RESTRICT + CASCADE/SET NULL on delete/update) through the
//! same validation entry points the DML handlers use.
//!
//! Run with: cargo test --test test_constraints -- --test-threads=1

mod common;

use storage_manager::backend::constraint::validate_row_insert;
use storage_manager::backend::system_table::insert_constraint_metadata;
use storage_manager::catalog::{
    Catalog, Column, create_database, create_table, init_catalog, load_catalog, save_catalog,
};
use storage_manager::executor::create_index::create_index;
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

// ── NOT NULL ──────────────────────────────────────────────────────────────────

#[test]
fn not_null_rejects_missing_value() {
    let _ws = common::TestWorkspace::new("con", "notnull");

    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "nn");
    let mut catalog = load_catalog();

    let mut id_col = column("id", DataType::Int);
    id_col.constraints.not_null = true;
    id_col.nullable = false;
    create_table(
        &mut catalog,
        "nn",
        "t",
        vec![id_col, column("note", DataType::Varchar(10))],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // NULL in the NOT NULL column is rejected...
    assert!(
        validate_row_insert(
            &catalog,
            "nn",
            "t",
            &typed(&catalog, "nn", "t", &["NULL", "x"])
        )
        .is_err()
    );
    // ...while a real value passes.
    assert!(
        validate_row_insert(
            &catalog,
            "nn",
            "t",
            &typed(&catalog, "nn", "t", &["1", "x"])
        )
        .is_ok()
    );
}

// ── UNIQUE ────────────────────────────────────────────────────────────────────

#[test]
fn unique_with_index_rejects_duplicates() {
    let _ws = common::TestWorkspace::new("con", "uniqidx");

    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "uq");
    let mut catalog = load_catalog();

    let mut email = column("email", DataType::Varchar(50));
    email.constraints.unique = true;
    create_table(
        &mut catalog,
        "uq",
        "users",
        vec![column("id", DataType::Int), email],
    );
    save_catalog(&catalog).unwrap();

    // NOTE: like the CLI shell, we keep one in-memory catalog for the whole
    // session — constraint flags live in memory and in sys_constraints rows.
    insert_single_tuple(&catalog, "uq", "users", &["1", "a@x.io"]).unwrap();

    let index = create_index(
        &catalog,
        "uq",
        "users",
        "by_email",
        &[String::from("email")],
    );
    assert!(index.is_ok(), "index build should succeed: {:?}", index);

    assert!(
        validate_row_insert(
            &catalog,
            "uq",
            "users",
            &typed(&catalog, "uq", "users", &["2", "a@x.io"])
        )
        .is_err(),
        "duplicate must be rejected"
    );
    assert!(
        validate_row_insert(
            &catalog,
            "uq",
            "users",
            &typed(&catalog, "uq", "users", &["2", "b@x.io"])
        )
        .is_ok()
    );
}

#[test]
fn unique_without_index_falls_back_to_heap_scan() {
    let _ws = common::TestWorkspace::new("con", "uniqscan");

    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "uq2");
    let mut catalog = load_catalog();

    let mut name = column("name", DataType::Varchar(50));
    name.constraints.unique = true;
    create_table(
        &mut catalog,
        "uq2",
        "people",
        vec![column("id", DataType::Int), name],
    );
    save_catalog(&catalog).unwrap();
    insert_single_tuple(&catalog, "uq2", "people", &["1", "Zed"]).unwrap();

    // No index exists for `name` — validation must still catch duplicates
    // via the heap-scan fallback.
    assert!(
        validate_row_insert(
            &catalog,
            "uq2",
            "people",
            &typed(&catalog, "uq2", "people", &["2", "Zed"])
        )
        .is_err()
    );
}

// ── CHECK ─────────────────────────────────────────────────────────────────────

#[test]
fn check_expression_is_enforced() {
    let _ws = common::TestWorkspace::new("con", "check");

    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "ck");
    let mut catalog = load_catalog();

    create_table(
        &mut catalog,
        "ck",
        "items",
        vec![column("price", DataType::Int)],
    );
    save_catalog(&catalog).unwrap();

    // Register CHECK (price > 0) the way the DDL handler does.
    let _ = insert_constraint_metadata("ck", "items", "CHECK", "", None, None); // no-op guard
    let _ = insert_constraint_metadata("ck", "items", "CHECK", "price > 0", None, None);

    let catalog = load_catalog();
    assert!(
        validate_row_insert(
            &catalog,
            "ck",
            "items",
            &typed(&catalog, "ck", "items", &["5"])
        )
        .is_ok()
    );
    assert!(
        validate_row_insert(
            &catalog,
            "ck",
            "items",
            &typed(&catalog, "ck", "items", &["-5"])
        )
        .is_err()
    );
}

// ── FOREIGN KEY ───────────────────────────────────────────────────────────────

/// customers(id PK) <- orders(customer_id FK REFERENCES customers(id))
fn setup_fk(db: &str, action: &str) -> Catalog {
    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();

    let mut cust_id = column("id", DataType::Int);
    cust_id.constraints.unique = true;
    create_table(
        &mut catalog,
        db,
        "customers",
        vec![cust_id, column("cname", DataType::Varchar(30))],
    );
    create_table(
        &mut catalog,
        db,
        "orders",
        vec![
            column("oid", DataType::Int),
            column("customer_id", DataType::Int),
        ],
    );
    save_catalog(&catalog).unwrap();

    let _ = insert_constraint_metadata(db, "customers", "PRIMARY KEY", "id", None, None);
    let _ = insert_constraint_metadata(
        db,
        "orders",
        &format!("FOREIGN KEY{}", action),
        "customer_id",
        Some("customers"),
        Some("id"),
    );

    insert_single_tuple(&load_catalog(), db, "customers", &["1", "Ann"]).unwrap();
    load_catalog()
}

#[test]
fn fk_rejects_orphan_insert() {
    let _ws = common::TestWorkspace::new("con", "fkorphan");
    let catalog = setup_fk("fko", "");

    assert!(
        validate_row_insert(
            &catalog,
            "fko",
            "orders",
            &typed(&catalog, "fko", "orders", &["1", "1"])
        )
        .is_ok()
    );
    assert!(
        validate_row_insert(
            &catalog,
            "fko",
            "orders",
            &typed(&catalog, "fko", "orders", &["2", "99"])
        )
        .is_err()
    );
}

#[test]
fn fk_restrict_blocks_parent_delete() {
    let _ws = common::TestWorkspace::new("con", "fkrestrict");
    let catalog = setup_fk("fkr", "");

    insert_single_tuple(&catalog, "fkr", "orders", &["1", "1"]).unwrap();
    insert_single_tuple(&catalog, "fkr", "customers", &["2", "Bob"]).unwrap();

    // RESTRICT: deleting the referenced customer row fails the statement...
    let pointers = {
        use storage_manager::backend::executor::row_select::{
            parse_where_text, select_matching_pointers,
        };
        let selection = parse_where_text("id = 1").unwrap();
        select_matching_pointers(&catalog, "fkr", "customers", selection).unwrap()
    };
    let err =
        storage_manager::executor::delete_by_pointers(&catalog, "fkr", "customers", &pointers)
            .err()
            .expect("referenced parent must not be deletable");
    assert!(err.to_string().contains("FOREIGN KEY"), "{err}");

    // ...while an unreferenced one deletes normally.
    let result = delete_customers_by_id(&catalog, "fkr", 2);
    assert_eq!(result.deleted_count, 1);
}

#[test]
fn fk_cascade_deletes_children() {
    let _ws = common::TestWorkspace::new("con", "fkcascade");
    let catalog = setup_fk("fkc", " ON DELETE CASCADE");

    insert_single_tuple(&catalog, "fkc", "orders", &["1", "1"]).unwrap();
    insert_single_tuple(&catalog, "fkc", "orders", &["2", "1"]).unwrap();

    // Deleting the customer cascades to the referencing orders.
    let result = delete_customers_by_id(&catalog, "fkc", 1);
    assert_eq!(result.deleted_count, 1);

    let out = count_orders(&catalog, "fkc");
    assert_eq!(out, 0, "child rows must be cascade-deleted");
}

#[test]
fn fk_set_null_detaches_children() {
    let _ws = common::TestWorkspace::new("con", "fksetnull");
    let catalog = setup_fk("fkn", " ON DELETE SET NULL");

    insert_single_tuple(&catalog, "fkn", "orders", &["1", "1"]).unwrap();

    let result = delete_customers_by_id(&catalog, "fkn", 1);
    assert_eq!(result.deleted_count, 1);

    // The order survives but its FK column becomes NULL.
    let out = query_orders_customer_ids(&catalog, "fkn");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], "NULL");
}

// ── Helpers to observe effects ────────────────────────────────────────────────

/// Run DELETE FROM customers WHERE id = <id> through the Volcano selection
/// path plus pointer-based deletion.
fn delete_customers_by_id(
    catalog: &Catalog,
    db: &str,
    id: i32,
) -> storage_manager::executor::delete::DeleteResult {
    use storage_manager::backend::executor::row_select::{
        parse_where_text, select_matching_pointers,
    };

    let selection = parse_where_text(&format!("id = {}", id)).unwrap();
    let pointers = select_matching_pointers(catalog, db, "customers", selection).unwrap();
    storage_manager::executor::delete_by_pointers(catalog, db, "customers", &pointers).unwrap()
}

fn count_orders(catalog: &Catalog, db: &str) -> usize {
    let logical = storage_manager::planner::plan_query(
        &rook_parser::parse_sql("SELECT oid FROM orders").unwrap(),
        catalog,
        db,
    )
    .unwrap();
    storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .unwrap()
    .len()
}

fn query_orders_customer_ids(catalog: &Catalog, db: &str) -> Vec<String> {
    let logical = storage_manager::planner::plan_query(
        &rook_parser::parse_sql("SELECT customer_id FROM orders").unwrap(),
        catalog,
        db,
    )
    .unwrap();
    storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .unwrap()
    .iter()
    .map(|t| match &t.values[0] {
        Some(v) => format!("{}", v),
        None => "NULL".to_string(),
    })
    .collect()
}

/// A row given as text, parsed for the table's column types.
fn typed(
    catalog: &storage_manager::catalog::Catalog,
    db: &str,
    table: &str,
    values: &[&str],
) -> Vec<Option<storage_manager::types::DataValue>> {
    let columns = &catalog.databases[db].tables[table].columns;
    storage_manager::executor::parse_text_row(columns, values).unwrap()
}

/// A VARCHAR key holding the word NULL is a value, not SQL NULL. DELETE
/// used to see the parent row as text, read 'NULL' as no key at all, and
/// skip the RESTRICT check.
#[test]
fn fk_restrict_protects_a_key_spelled_null() {
    use storage_manager::backend::executor::row_select::{
        parse_where_text, select_matching_pointers,
    };

    let _ws = common::TestWorkspace::new("con", "fknullword");
    init_catalog();
    let mut catalog = load_catalog();
    create_database(&mut catalog, "fkn");
    let mut catalog = load_catalog();
    let mut code = column("code", DataType::Varchar(10));
    code.constraints.unique = true;
    create_table(&mut catalog, "fkn", "parent", vec![code]);
    create_table(
        &mut catalog,
        "fkn",
        "child",
        vec![
            column("id", DataType::Int),
            column("code", DataType::Varchar(10)),
        ],
    );
    save_catalog(&catalog).unwrap();
    let _ = insert_constraint_metadata("fkn", "parent", "PRIMARY KEY", "code", None, None);
    let _ = insert_constraint_metadata(
        "fkn",
        "child",
        "FOREIGN KEY",
        "code",
        Some("parent"),
        Some("code"),
    );

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "fkn", "parent", &["'NULL'"]).unwrap());
    assert!(insert_single_tuple(&catalog, "fkn", "child", &["1", "'NULL'"]).unwrap());

    let selection = parse_where_text("code = 'NULL'").unwrap();
    let pointers = select_matching_pointers(&catalog, "fkn", "parent", selection).unwrap();
    assert_eq!(pointers.len(), 1);
    let result =
        storage_manager::executor::delete_by_pointers(&catalog, "fkn", "parent", &pointers);
    assert!(
        result.is_err(),
        "the child row still references 'NULL', so the delete must fail"
    );
}
