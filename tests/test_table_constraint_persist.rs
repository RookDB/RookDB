//! Table-level PRIMARY KEY, UNIQUE and CHECK rows live only in
//! `sys_constraints`; a catalog save (any later DDL) must keep them.

mod common;

use storage_manager::backend::session::{Session, StatementResult};

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
    common::init_catalog();
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    s
}

/// A new session on the same data directory, as after a restart.
fn restart(session: Session) -> Session {
    drop(session);
    common::engine().invalidate_metadata();
    let mut s = Session::new(common::load_catalog());
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

fn count(session: &mut Session, table: &str) -> usize {
    run(session, &format!("SELECT * FROM {table}"))
        .unwrap()
        .rows
        .unwrap()
        .1
        .len()
}

const K: &str = "CREATE TABLE k (a INT, b INT, CHECK (a > 0), UNIQUE (b))";

fn assert_k_enforced(s: &mut Session) {
    assert!(
        run(s, "INSERT INTO k VALUES (-7, 9)").is_err(),
        "CHECK lost"
    );
    ok(s, "INSERT INTO k VALUES (7, 9)");
    assert!(
        run(s, "INSERT INTO k VALUES (8, 9)").is_err(),
        "UNIQUE lost"
    );
}

#[test]
fn table_level_check_survives_a_later_create_table() {
    let _ws = common::TestWorkspace::new("ctpersist", "create");
    let mut s = session();
    ok(&mut s, K);
    assert!(run(&mut s, "INSERT INTO k VALUES (-1, 1)").is_err());
    ok(&mut s, "CREATE TABLE other (x INT)");
    assert_k_enforced(&mut s);
}

#[test]
fn table_level_constraints_survive_a_restart() {
    let _ws = common::TestWorkspace::new("ctpersist", "restart");
    let mut s = session();
    ok(&mut s, K);
    ok(&mut s, "CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))");
    ok(&mut s, "CREATE TABLE other (x INT)");
    let mut s = restart(s);
    assert_k_enforced(&mut s);
    ok(&mut s, "INSERT INTO p VALUES (1, 1)");
    assert!(run(&mut s, "INSERT INTO p VALUES (1, 1)").is_err());
    // A save after the restart keeps them again.
    ok(&mut s, "CREATE TABLE another (z INT)");
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO k VALUES (-7, 10)").is_err());
    assert!(run(&mut s, "INSERT INTO p VALUES (1, 1)").is_err());
}

#[test]
fn constraints_follow_a_table_rename() {
    let _ws = common::TestWorkspace::new("ctpersist", "rentable");
    let mut s = session();
    ok(&mut s, K);
    ok(&mut s, "ALTER TABLE k RENAME TO k2");
    ok(&mut s, "CREATE TABLE other (x INT)");
    assert!(run(&mut s, "INSERT INTO k2 VALUES (-7, 9)").is_err());
    ok(&mut s, "INSERT INTO k2 VALUES (7, 9)");
    assert!(run(&mut s, "INSERT INTO k2 VALUES (8, 9)").is_err());
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO k2 VALUES (-7, 10)").is_err());
}

#[test]
fn constraints_follow_a_column_rename_and_drop() {
    let _ws = common::TestWorkspace::new("ctpersist", "rencol");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE k (a INT, b INT, c INT, CHECK (a > 0), UNIQUE (b))",
    );
    ok(&mut s, "ALTER TABLE k RENAME COLUMN a TO a2");
    ok(&mut s, "ALTER TABLE k RENAME COLUMN b TO b2");
    ok(&mut s, "CREATE TABLE other (x INT)");
    assert!(run(&mut s, "INSERT INTO k VALUES (-7, 9, 0)").is_err());
    ok(&mut s, "INSERT INTO k VALUES (7, 9, 0)");
    assert!(run(&mut s, "INSERT INTO k VALUES (8, 9, 0)").is_err());

    // Dropping a column removes the constraints that name it, and no others.
    ok(&mut s, "ALTER TABLE k DROP COLUMN b2");
    ok(&mut s, "CREATE TABLE other2 (x INT)");
    ok(&mut s, "INSERT INTO k VALUES (8, 0)");
    assert!(run(&mut s, "INSERT INTO k VALUES (-8, 0)").is_err());
    ok(&mut s, "ALTER TABLE k DROP COLUMN a2");
    ok(&mut s, "CREATE TABLE other3 (x INT)");
    ok(&mut s, "INSERT INTO k VALUES (-1)");
    let mut s = restart(s);
    ok(&mut s, "INSERT INTO k VALUES (-2)");
}

#[test]
fn dropping_a_table_removes_its_constraint_rows() {
    let _ws = common::TestWorkspace::new("ctpersist", "droptable");
    let mut s = session();
    ok(&mut s, K);
    ok(&mut s, "DROP TABLE k");
    ok(&mut s, "CREATE TABLE other (x INT)");
    // A new table of the same name starts clean.
    ok(&mut s, "CREATE TABLE k (a INT, b INT)");
    ok(&mut s, "INSERT INTO k VALUES (-7, 9), (-8, 9)");
    assert_eq!(count(&mut s, "k"), 2);
    let mut s = restart(s);
    ok(&mut s, "INSERT INTO k VALUES (-9, 9)");
}

#[test]
fn same_table_name_in_two_databases_keeps_its_own_constraints() {
    let _ws = common::TestWorkspace::new("ctpersist", "twodb");
    let mut s = session();
    ok(&mut s, K);
    ok(&mut s, "CREATE DATABASE other");
    ok(&mut s, "USE other");
    ok(&mut s, "CREATE TABLE k (a INT, b INT)");
    ok(&mut s, "INSERT INTO k VALUES (-1, 1), (-2, 1)");
    ok(&mut s, "USE app");
    assert!(run(&mut s, "INSERT INTO k VALUES (-7, 9)").is_err());
}
