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

/// Every file under `dir` with its length, sorted.
fn files_under(dir: &str) -> Vec<(std::path::PathBuf, u64)> {
    let mut found = Vec::new();
    let mut pending = vec![std::path::PathBuf::from(dir)];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let len = entry.metadata().map(|m| m.len()).unwrap_or(0);
                found.push((path, len));
            }
        }
    }
    found.sort();
    found
}

#[test]
fn a_session_keeps_every_file_under_its_engine_root() {
    use storage_manager::backend::engine::Engine;

    let _ws = common::TestWorkspace::new("session", "root");
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);

    // The test workspace keeps its own catalog in the working directory.
    let before = files_under("database");

    // A data directory that is not the working directory.
    let root = std::env::current_dir().unwrap().join("elsewhere");
    std::fs::create_dir_all(&root).unwrap();
    let engine = Engine::open(&root);
    storage_manager::catalog::init_catalog_in(&engine);
    let mut s = Session::new(storage_manager::catalog::load_catalog_in(&engine));

    for sql in [
        "CREATE DATABASE app",
        "USE app",
        "CREATE TABLE p (id INT PRIMARY KEY, n INT CHECK (n >= 0))",
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE)",
        "INSERT INTO p VALUES (1, 10), (2, 20), (3, 30)",
        "INSERT INTO c VALUES (1, 1), (2, 2), (3, 3)",
        "CREATE INDEX p_n ON p (n)",
        "UPDATE p SET n = n + 1 WHERE id = 2",
        "DELETE FROM p WHERE id = 3",
        "VACUUM p",
        "DROP INDEX p_n",
        "ALTER TABLE p ADD COLUMN note VARCHAR(10)",
        "VACUUM p",
        "ALTER TABLE c RENAME TO kids",
    ] {
        ok(&mut s, sql);
    }
    assert_eq!(
        rows(
            &mut s,
            "SELECT p.id, p.n FROM p JOIN kids ON kids.pid = p.id ORDER BY p.id"
        ),
        [vec![int(1), int(10)], vec![int(2), int(21)]]
    );
    storage_manager::backend::cache::checkpoint_durable();

    assert!(root.join("database/base/app/p.dat").exists());
    assert!(root.join("database/base/app/kids.dat").exists());
    assert!(root.join("database/system/tables.dat").exists());
    assert_eq!(
        files_under("database"),
        before,
        "nothing may change under the working directory"
    );

    // A second session on the same root reads what the first one wrote.
    drop(s);
    let mut again = Session::new(storage_manager::catalog::load_catalog_in(&engine));
    ok(&mut again, "USE app");
    assert_eq!(rows(&mut again, "SELECT COUNT(*) FROM kids").len(), 1);
}
