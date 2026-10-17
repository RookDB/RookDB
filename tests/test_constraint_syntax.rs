//! Column and table constraints written in SQL reach the engine as typed
//! values and behave the same however they are spelled (G13).

mod common;

use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    common::init_catalog();
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    s
}

fn run(session: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    session.execute(plan).map_err(|e| e.to_string())
}

fn ok(session: &mut Session, sql: &str) {
    run(session, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn values(session: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    let (_, rows) = run(session, sql).unwrap().rows.unwrap();
    rows.into_iter().map(|t| t.values).collect()
}

#[test]
fn check_is_enforced_with_or_without_a_space() {
    let _ws = common::TestWorkspace::new("ctsyntax", "check");
    let mut s = session();
    for sql in [
        "CREATE TABLE tight (a INT CHECK(a > 0))",
        "CREATE TABLE spaced (a INT CHECK (a > 0))",
        "CREATE TABLE tbl_tight (a INT, CHECK(a > 0))",
        "CREATE TABLE tbl_spaced (a INT, CHECK (a > 0))",
    ] {
        ok(&mut s, sql);
    }
    for t in ["tight", "spaced", "tbl_tight", "tbl_spaced"] {
        ok(&mut s, &format!("INSERT INTO {t} VALUES (1)"));
        assert!(
            run(&mut s, &format!("INSERT INTO {t} VALUES (-1)")).is_err(),
            "{t}: CHECK not enforced"
        );
    }
}

#[test]
fn check_follows_a_column_rename() {
    let _ws = common::TestWorkspace::new("ctsyntax", "rename");
    let mut s = session();
    ok(&mut s, "CREATE TABLE t (a INT CHECK (a > 0))");
    ok(&mut s, "ALTER TABLE t RENAME COLUMN a TO b");
    ok(&mut s, "INSERT INTO t VALUES (1)");
    assert!(run(&mut s, "INSERT INTO t VALUES (-1)").is_err());
}

#[test]
fn default_is_validated_and_parentheses_are_ignored() {
    let _ws = common::TestWorkspace::new("ctsyntax", "default");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE t (id INT, a VARCHAR(5) DEFAULT ('abc'), n INT DEFAULT -3)",
    );
    ok(&mut s, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        values(&mut s, "SELECT a, n FROM t"),
        [vec![
            Some(DataValue::Varchar("abc".into())),
            Some(DataValue::Int(-3))
        ]]
    );

    let err = run(&mut s, "CREATE TABLE bad (n INT DEFAULT 'zz')").unwrap_err();
    assert!(err.contains("DEFAULT"), "{err}");
    assert!(
        run(&mut s, "SELECT * FROM bad").is_err(),
        "table was created"
    );
}

#[test]
fn add_column_backfills_default_and_refuses_unsupported_constraints() {
    let _ws = common::TestWorkspace::new("ctsyntax", "addcol");
    let mut s = session();
    ok(&mut s, "CREATE TABLE t (id INT)");
    ok(&mut s, "INSERT INTO t VALUES (1)");
    ok(
        &mut s,
        "ALTER TABLE t ADD COLUMN n INT NOT NULL DEFAULT (7)",
    );
    assert_eq!(
        values(&mut s, "SELECT id, n FROM t"),
        [vec![Some(DataValue::Int(1)), Some(DataValue::Int(7))]]
    );
    assert!(run(&mut s, "ALTER TABLE t ADD COLUMN m INT DEFAULT 'zz'").is_err());
    assert!(run(&mut s, "ALTER TABLE t ADD COLUMN u INT UNIQUE").is_err());
    assert!(run(&mut s, "ALTER TABLE t ADD COLUMN c INT CHECK (c > 0)").is_err());
}

#[test]
fn unsupported_column_option_is_a_parse_error() {
    assert!(rook_parser::parse_sql("CREATE TABLE t (a INT COMMENT 'x')").is_err());
}

#[test]
fn keys_and_references_still_enforce() {
    let _ws = common::TestWorkspace::new("ctsyntax", "keys");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (id INT PRIMARY KEY, u INT UNIQUE)");
    ok(
        &mut s,
        "CREATE TABLE c (x INT, y INT, pid INT REFERENCES p(id) ON DELETE CASCADE, PRIMARY KEY (x, y))",
    );
    ok(&mut s, "INSERT INTO p VALUES (1, 1)");
    assert!(run(&mut s, "INSERT INTO p VALUES (1, 2)").is_err());
    assert!(run(&mut s, "INSERT INTO p VALUES (2, 1)").is_err());
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 1)");
    assert!(run(&mut s, "INSERT INTO c VALUES (1, 1, 1)").is_err());
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 2, 99)").is_err());
    ok(&mut s, "DELETE FROM p WHERE id = 1");
    assert!(values(&mut s, "SELECT * FROM c").is_empty());
}

#[test]
fn big_defaults_keep_every_digit() {
    let _ws = common::TestWorkspace::new("ctsyntax", "bigdef");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE big (id INT, v BIGINT DEFAULT 9007199254740993, m BIGINT DEFAULT -9223372036854775808, d NUMERIC(30,10) DEFAULT 123456789012345.1234567891)",
    );
    ok(&mut s, "INSERT INTO big (id) VALUES (1)");
    let row = &values(&mut s, "SELECT v, m, d FROM big")[0];
    assert_eq!(row[0], Some(DataValue::BigInt(9007199254740993)));
    assert_eq!(row[1], Some(DataValue::BigInt(i64::MIN)));
    let expected =
        storage_manager::types::value::parse_numeric_literal("123456789012345.1234567891", 30, 10)
            .unwrap();
    assert_eq!(row[2], Some(DataValue::numeric(expected)));
}

#[test]
fn set_default_and_add_column_keep_every_digit() {
    let _ws = common::TestWorkspace::new("ctsyntax", "bigset");
    let mut s = session();
    ok(&mut s, "CREATE TABLE t (id INT, v BIGINT)");
    ok(
        &mut s,
        "ALTER TABLE t ALTER COLUMN v SET DEFAULT 9007199254740993",
    );
    ok(&mut s, "INSERT INTO t (id) VALUES (1)");
    ok(
        &mut s,
        "ALTER TABLE t ADD COLUMN w BIGINT DEFAULT -9223372036854775808",
    );
    assert_eq!(
        values(&mut s, "SELECT v, w FROM t"),
        [vec![
            Some(DataValue::BigInt(9007199254740993)),
            Some(DataValue::BigInt(i64::MIN))
        ]]
    );
}
