//! ON UPDATE / ON DELETE actions keep the child table's own constraints:
//! a CASCADE or SET NULL that would break one fails the statement before
//! anything is written, and a failure while cascading is reported.

mod common;

use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    common::init_catalog();
    Session::new(common::load_catalog())
}

fn run(s: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    s.execute(plan).map_err(|e| e.to_string())
}

fn ok(s: &mut Session, sql: &str) {
    run(s, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn fails(s: &mut Session, sql: &str) -> String {
    match run(s, sql) {
        Ok(_) => panic!("{} succeeded", sql),
        Err(e) => e,
    }
}

/// Each row's values as text, NULL as "NULL".
fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(DataValue::Int(i)) => i.to_string(),
                    Some(other) => other.to_string(),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

fn setup(s: &mut Session, child: &str) {
    ok(s, "CREATE DATABASE app");
    ok(s, "USE app");
    ok(s, "CREATE TABLE p (id INT PRIMARY KEY)");
    ok(s, child);
    ok(s, "INSERT INTO p VALUES (1)");
    ok(s, "INSERT INTO c VALUES (10, 1)");
}

fn unchanged(s: &mut Session) {
    assert_eq!(rows(s, "SELECT id FROM p"), [["1"]]);
    assert_eq!(rows(s, "SELECT id, pid FROM c"), [["10", "1"]]);
}

#[test]
fn update_set_null_into_a_not_null_column_fails() {
    let _ws = common::TestWorkspace::new("fk_action", "upd_set_null");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT NOT NULL REFERENCES p(id) ON UPDATE SET NULL)",
    );
    let e = fails(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert!(e.contains("NOT NULL"), "{}", e);
    unchanged(&mut s);
}

#[test]
fn update_cascade_that_breaks_a_child_check_fails() {
    let _ws = common::TestWorkspace::new("fk_action", "upd_check");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON UPDATE CASCADE, CHECK (pid < 2))",
    );
    let e = fails(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert!(e.contains("CHECK"), "{}", e);
    unchanged(&mut s);
}

#[test]
fn delete_set_null_into_a_not_null_column_fails() {
    let _ws = common::TestWorkspace::new("fk_action", "del_set_null");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT NOT NULL REFERENCES p(id) ON DELETE SET NULL)",
    );
    let e = fails(&mut s, "DELETE FROM p WHERE id = 1");
    assert!(e.contains("NOT NULL"), "{}", e);
    unchanged(&mut s);
}

#[test]
fn cascades_that_keep_the_constraints_still_apply() {
    let _ws = common::TestWorkspace::new("fk_action", "valid");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON UPDATE CASCADE ON DELETE SET NULL, CHECK (pid < 5))",
    );
    ok(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert_eq!(rows(&mut s, "SELECT id, pid FROM c"), [["10", "2"]]);
    ok(&mut s, "DELETE FROM p WHERE id = 2");
    assert_eq!(rows(&mut s, "SELECT id, pid FROM c"), [["10", "NULL"]]);
}

/// A grandchild the cascade reaches only after the child is rewritten:
/// the failure is reported instead of the UPDATE claiming success.
#[test]
fn a_grandchild_that_cannot_follow_fails_the_update() {
    let _ws = common::TestWorkspace::new("fk_action", "grandchild");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT UNIQUE REFERENCES p(id) ON UPDATE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE g (id INT PRIMARY KEY, cpid INT NOT NULL REFERENCES c(pid) ON UPDATE SET NULL)",
    );
    ok(&mut s, "INSERT INTO g VALUES (100, 1)");
    let e = fails(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert!(e.contains("NOT NULL"), "{}", e);
    assert_eq!(rows(&mut s, "SELECT id, cpid FROM g"), [["100", "1"]]);
}

/// A grandchild that restricts updates blocks a cascade that would change
/// the key it references.
#[test]
fn a_restricting_grandchild_blocks_the_cascade() {
    let _ws = common::TestWorkspace::new("fk_action", "gc_restrict");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT UNIQUE REFERENCES p(id) ON UPDATE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE g (id INT PRIMARY KEY, cpid INT REFERENCES c(pid))",
    );
    ok(&mut s, "INSERT INTO g VALUES (100, 1)");
    let e = fails(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert!(e.contains("FOREIGN KEY"), "{}", e);
    unchanged(&mut s);
    assert_eq!(rows(&mut s, "SELECT id, cpid FROM g"), [["100", "1"]]);
}

/// A row changed by a cascade is updated like any other: a grandchild's
/// ON UPDATE SET NULL applies to it.
#[test]
fn a_cascade_applies_the_grandchild_set_null() {
    let _ws = common::TestWorkspace::new("fk_action", "gc_set_null");
    let mut s = session();
    setup(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT UNIQUE REFERENCES p(id) ON UPDATE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE g (id INT PRIMARY KEY, cpid INT REFERENCES c(pid) ON UPDATE SET NULL)",
    );
    ok(&mut s, "INSERT INTO g VALUES (100, 1)");
    ok(&mut s, "UPDATE p SET id = 2 WHERE id = 1");
    assert_eq!(rows(&mut s, "SELECT id, pid FROM c"), [["10", "2"]]);
    assert_eq!(rows(&mut s, "SELECT id, cpid FROM g"), [["100", "NULL"]]);
}
