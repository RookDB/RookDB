//! A statement that changes the catalog does so whole or not at all: an
//! undo copy of the system tables is kept until it commits, and put back
//! when it fails or the process dies first.

mod common;

use storage_manager::backend::error::RookResult;
use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::backend::system_table::begin_catalog_change;
use storage_manager::catalog::{Database, save_catalog, try_load_catalog_in};

fn exec(s: &mut Session, sql: &str) -> RookResult<StatementResult> {
    s.execute(rook_parser::parse_sql(sql).unwrap())
}

fn run(s: &mut Session, sql: &str) -> StatementResult {
    exec(s, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("catalog_change", tag);
    let mut s = Session::new(common::load_catalog());
    run(&mut s, "CREATE DATABASE cc_db");
    run(&mut s, "USE cc_db");
    run(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
    (ws, s)
}

fn databases(ws: &common::TestWorkspace) -> Vec<String> {
    let mut names: Vec<String> = try_load_catalog_in(ws.engine())
        .unwrap()
        .databases
        .into_keys()
        .collect();
    names.sort();
    names
}

/// Save a catalog with one more database, as a statement's edits would.
fn edit_catalog(ws: &common::TestWorkspace) {
    let mut catalog = try_load_catalog_in(ws.engine()).unwrap();
    catalog.databases.insert(
        "half_done".to_string(),
        Database {
            tables: Default::default(),
            views: Default::default(),
        },
    );
    save_catalog(&catalog).unwrap();
}

#[test]
fn committed_statements_leave_no_undo_copy() {
    let (ws, _s) = session("commit");
    assert!(!ws.engine().paths().system_undo_dir().exists());
    assert_eq!(databases(&ws), ["cc_db"]);
}

#[test]
fn an_uncommitted_change_is_rolled_back() {
    let (ws, _s) = session("rollback");
    let change = begin_catalog_change(ws.engine()).unwrap();
    edit_catalog(&ws);
    drop(change);
    assert_eq!(databases(&ws), ["cc_db"]);
    assert!(!ws.engine().paths().system_undo_dir().exists());
}

#[test]
fn a_failed_statement_keeps_the_catalog_and_the_session_usable() {
    let (ws, mut s) = session("failed");
    assert!(exec(&mut s, "CREATE TABLE z (s VARCHAR(0))").is_err());
    assert!(!ws.engine().paths().system_undo_dir().exists());
    assert!(!s.catalog.databases["cc_db"].tables.contains_key("z"));
    run(&mut s, "CREATE TABLE z (s VARCHAR(1))");
    run(&mut s, "INSERT INTO t VALUES (1)");
    assert!(
        exec(&mut s, "INSERT INTO t VALUES (1)").is_err(),
        "PK still enforced"
    );
}

#[test]
fn existing_and_missing_objects_are_errors_unless_told_otherwise() {
    let (_ws, mut s) = session("exists");
    assert!(exec(&mut s, "CREATE DATABASE cc_db").is_err());
    run(&mut s, "CREATE DATABASE IF NOT EXISTS cc_db");
    assert!(exec(&mut s, "CREATE TABLE t (id INT)").is_err());
    run(&mut s, "CREATE TABLE IF NOT EXISTS t (id INT)");
    assert!(exec(&mut s, "DROP DATABASE no_such_db").is_err());
    run(&mut s, "DROP DATABASE IF EXISTS no_such_db");
    // The failures changed nothing.
    assert_eq!(s.current_db.as_deref(), Some("cc_db"));
    assert!(s.catalog.databases["cc_db"].tables.contains_key("t"));
}

#[test]
fn bigint_extremes() {
    let (_ws, mut s) = session("bigint");
    let (_, rows) = run(&mut s, "SELECT -9223372036854775808").rows.unwrap();
    assert_eq!(
        rows[0].values[0],
        Some(storage_manager::types::DataValue::BigInt(i64::MIN))
    );
    let err = exec(&mut s, "SELECT -9223372036854775808 / -1").expect_err("overflow");
    assert!(err.to_string().contains("overflow"), "{}", err);
}
