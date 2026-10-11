//! `Session::execute`: statements run against the engine and their results
//! checked as values, without a shell in between.

mod common;

use storage_manager::backend::session::{NO_DATABASE, Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
    storage_manager::catalog::init_catalog();
    Session::new(storage_manager::catalog::load_catalog())
}

fn run(session: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    session.execute(plan).map_err(|e| e.to_string())
}

fn ok(session: &mut Session, sql: &str) -> Vec<String> {
    run(session, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .messages
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    let (_, rows) = run(session, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter().map(|t| t.values).collect()
}

fn int(v: i32) -> Option<DataValue> {
    Some(DataValue::Int(v))
}

#[test]
fn statements_report_what_they_did() {
    let _ws = common::TestWorkspace::new("session", "basics");
    let mut s = session();

    assert_eq!(
        run(&mut s, "CREATE TABLE t (id INT)").unwrap_err(),
        NO_DATABASE
    );
    assert_eq!(
        ok(&mut s, "CREATE DATABASE app"),
        ["Database 'app' created successfully."]
    );
    assert_eq!(ok(&mut s, "USE app"), ["Database 'app' selected."]);
    assert_eq!(s.current_db.as_deref(), Some("app"));
    assert_eq!(
        ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, n INT)"),
        ["Table 't' created successfully."]
    );
    assert_eq!(
        ok(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)"),
        ["3 rows inserted.\n"]
    );
    assert_eq!(
        ok(&mut s, "UPDATE t SET n = n + 1 WHERE id >= 2"),
        ["\nUpdated 2 row(s)."]
    );
    assert_eq!(
        ok(&mut s, "DELETE FROM T WHERE id = 1"),
        ["\nDeleted 1 row(s)."]
    );
    assert_eq!(
        rows(&mut s, "SELECT id, n FROM t ORDER BY id"),
        vec![vec![int(2), int(21)], vec![int(3), int(31)]]
    );
}

#[test]
fn failures_carry_the_statement_context() {
    let _ws = common::TestWorkspace::new("session", "errors");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
    ok(&mut s, "INSERT INTO t VALUES (1)");

    let dup = run(&mut s, "INSERT INTO t VALUES (1)").unwrap_err();
    assert!(
        dup.starts_with("Insert failed: UNIQUE constraint violated"),
        "{}",
        dup
    );

    let unknown = run(&mut s, "UPDATE t SET id = nosuch").unwrap_err();
    assert!(unknown.starts_with("Update failed: "), "{}", unknown);

    let missing = run(&mut s, "DROP TABLE nope").unwrap_err();
    assert_eq!(missing, "Error: Table 'nope' not found in database 'app'");

    let bad_type = run(&mut s, "CREATE TABLE u (x NOSUCHTYPE)").unwrap_err();
    assert!(
        bad_type.ends_with("Table 'u' was not created."),
        "{}",
        bad_type
    );
}

#[test]
fn ddl_round_trip() {
    let _ws = common::TestWorkspace::new("session", "ddl");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(&mut s, "CREATE TABLE t (id INT)");
    ok(&mut s, "INSERT INTO t VALUES (1), (2)");

    ok(
        &mut s,
        "ALTER TABLE t ADD COLUMN tag VARCHAR(5) DEFAULT 'x'",
    );
    assert_eq!(
        rows(&mut s, "SELECT id, tag FROM t ORDER BY id"),
        vec![
            vec![int(1), Some(DataValue::Varchar("x".into()))],
            vec![int(2), Some(DataValue::Varchar("x".into()))],
        ]
    );
    ok(&mut s, "ALTER TABLE t DROP COLUMN tag");
    ok(&mut s, "CREATE TABLE c AS SELECT id FROM t WHERE id > 1");
    assert_eq!(rows(&mut s, "SELECT id FROM c"), vec![vec![int(2)]]);
    assert_eq!(
        ok(&mut s, "TRUNCATE TABLE c"),
        ["Truncated table 'c' (0 rows)."]
    );
    assert_eq!(
        ok(&mut s, "DROP TABLE c"),
        ["Dropped table 'c' from database 'app'."]
    );
}
