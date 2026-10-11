//! Integration tests for path sanitisation of database/table names
//! (ANALYSIS.md Tier 1 #5).
//!
//! Names are interpolated into filesystem paths (`database/base/{db}/{table}.dat`),
//! so hostile names like `../../etc` must be rejected before any filesystem
//! work happens — both by the engine (defense-in-depth) and at parse time.

mod common;

use common::load_catalog;
use storage_manager::backend::name_validation::{
    validate_database_name, validate_index_name, validate_table_name,
};
use storage_manager::catalog::{Column, create_database, create_table};
use storage_manager::types::DataType;

fn plain_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Int,
        nullable: true,
        constraints: Default::default(),
    }
}

#[test]
fn engine_rejects_traversal_database_names() {
    let _ws = common::TestWorkspace::new("sanitize", "db");

    let mut catalog = load_catalog();
    for bad in [
        "../evil",
        "base/../../../tmp/x",
        "a\\b",
        "..",
        ".",
        "a/b",
        "nul\0",
    ] {
        assert!(
            !create_database(&mut catalog, bad),
            "create_database must reject '{}'",
            bad
        );
        assert!(
            !catalog.databases.contains_key(bad),
            "'{}' must not enter the catalog",
            bad
        );
    }
    // Nothing escaped the sandbox.
    assert!(!std::path::Path::new("../evil").exists());
}

#[test]
fn engine_rejects_traversal_table_names() {
    let _ws = common::TestWorkspace::new("sanitize", "table");

    let mut catalog = load_catalog();
    create_database(&mut catalog, "safe");
    let mut catalog = load_catalog();

    for bad in ["../escape", "sub/dir", "back\\slash", "..\\..\\win"] {
        create_table(&mut catalog, "safe", bad, vec![plain_col("id")]);
        assert!(
            !catalog.databases["safe"].tables.contains_key(bad),
            "create_table must reject '{}'",
            bad
        );
    }
    assert!(!common::path("database/base/escape.dat").exists());
}

#[test]
fn engine_accepts_ordinary_names() {
    let _ws = common::TestWorkspace::new("sanitize", "ok");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "prod_2024"));
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "prod_2024",
        "order-items",
        vec![plain_col("id")],
    );
    assert!(
        catalog.databases["prod_2024"]
            .tables
            .contains_key("order-items")
    );
}

#[test]
fn validator_rules() {
    // Accepted
    for good in ["users", "my_db", "order-items", "T1", "a..b"] {
        assert!(
            validate_table_name(good).is_ok(),
            "'{}' should be valid",
            good
        );
    }
    // Rejected
    for bad in [
        "",
        "../x",
        "a/b",
        "a\\b",
        "..",
        ".hidden",
        "a\0b",
        &"x".repeat(256),
    ] {
        assert!(
            validate_table_name(bad).is_err(),
            "'{}' should be rejected",
            bad
        );
        assert!(validate_database_name(bad).is_err());
    }
    assert!(validate_index_name("idx_1").is_ok());
}

#[test]
fn parser_rejects_hostile_names_at_parse_time() {
    let _ws = common::TestWorkspace::new("sanitize", "parse");

    use rook_parser::parse_sql;

    // Quoted identifiers pass the SQL grammar but must fail our validator.
    // (Unquoted hostile names are already rejected by the grammar itself.)
    for sql in [
        "CREATE DATABASE `a/b`;",
        "CREATE TABLE `a/b` (id INT);",
        "DROP DATABASE `..`;",
        "DROP TABLE `../t`;",
        "TRUNCATE TABLE `x/y`;",
        "CREATE INDEX idx ON `z/z` (id);",
    ] {
        match parse_sql(sql) {
            Ok(p) => panic!("'{}' should not parse, got {:?}", sql, p.statement_type()),
            Err(e) => {
                let msg = e.to_lowercase();
                assert!(
                    msg.contains("invalid") || msg.contains("forbidden"),
                    "'{}' error should come from name validation, got: {}",
                    sql,
                    e
                );
            }
        }
    }

    // Ordinary statements still parse.
    assert!(parse_sql("CREATE DATABASE ok_db;").is_ok());
    assert!(parse_sql("SELECT 1;").is_ok());
}
