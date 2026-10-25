//! `CREATE UNIQUE INDEX`: the index enforces uniqueness from then on, and
//! cannot be built over rows that already repeat a key.

mod common;

use storage_manager::backend::session::Session;

fn session() -> Session {
    common::init_catalog();
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE app",
        "USE app",
        "CREATE TABLE t (a INT, b VARCHAR(5))",
    ] {
        run(&mut s, sql).unwrap();
    }
    s
}

fn run(s: &mut Session, sql: &str) -> Result<Vec<String>, String> {
    s.execute(rook_parser::parse_sql(sql)?)
        .map(|r| r.messages)
        .map_err(|e| e.to_string())
}

fn count(s: &mut Session) -> String {
    let result = s
        .execute(rook_parser::parse_sql("SELECT COUNT(*) FROM t").unwrap())
        .unwrap();
    let (_, rows) = result.rows.unwrap();
    rows[0].values[0].as_ref().unwrap().to_string()
}

#[test]
fn a_unique_index_rejects_a_repeated_key() {
    let _ws = common::TestWorkspace::new("unique_index", "rejects");
    let mut s = session();
    assert_eq!(
        run(&mut s, "CREATE UNIQUE INDEX ua ON t (a)").unwrap(),
        ["Created unique index 'ua' on app.t(a) with 0 entries."]
    );
    run(&mut s, "INSERT INTO t VALUES (1, 'x')").unwrap();
    let err = run(&mut s, "INSERT INTO t VALUES (1, 'y')").unwrap_err();
    assert!(err.contains("UNIQUE constraint violated"), "{}", err);
    // NULL keys are not indexed, so they never collide.
    run(&mut s, "INSERT INTO t VALUES (NULL, 'y'), (NULL, 'z')").unwrap();
    assert_eq!(count(&mut s), "3");
}

#[test]
fn a_plain_index_allows_repeated_keys() {
    let _ws = common::TestWorkspace::new("unique_index", "plain");
    let mut s = session();
    run(&mut s, "CREATE INDEX ia ON t (a)").unwrap();
    run(&mut s, "INSERT INTO t VALUES (1, 'x'), (1, 'y')").unwrap();
    assert_eq!(count(&mut s), "2");
}

#[test]
fn a_unique_index_is_not_built_over_repeated_keys() {
    let _ws = common::TestWorkspace::new("unique_index", "existing");
    let mut s = session();
    run(
        &mut s,
        "INSERT INTO t VALUES (1, 'x'), (1, 'y'), (2, NULL), (3, NULL)",
    )
    .unwrap();

    let err = run(&mut s, "CREATE UNIQUE INDEX ua ON t (a)").unwrap_err();
    assert!(
        err.contains("value '1' appears more than once in column 'a'; index 'ua' was not created"),
        "{}",
        err
    );
    // Nothing of it remains: the name is free, and inserts are not checked.
    run(&mut s, "INSERT INTO t VALUES (1, 'z')").unwrap();
    run(&mut s, "CREATE INDEX ua ON t (a)").unwrap();

    // NULLs repeat freely; composite keys repeat only as a whole.
    run(&mut s, "CREATE UNIQUE INDEX ub ON t (b)").unwrap();
    run(&mut s, "CREATE UNIQUE INDEX uab ON t (a, b)").unwrap();
    let err = run(&mut s, "INSERT INTO t VALUES (9, 'x')").unwrap_err();
    assert!(err.contains("UNIQUE constraint violated"), "{}", err);
}

#[test]
fn a_composite_unique_index_reports_the_whole_key() {
    let _ws = common::TestWorkspace::new("unique_index", "composite");
    let mut s = session();
    run(&mut s, "INSERT INTO t VALUES (1, 'x'), (1, 'x')").unwrap();
    let err = run(&mut s, "CREATE UNIQUE INDEX uab ON t (a, b)").unwrap_err();
    assert!(
        err.contains("value '1, x' appears more than once in column 'a, b'"),
        "{}",
        err
    );
}
