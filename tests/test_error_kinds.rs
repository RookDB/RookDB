//! Statement errors keep their kind through the session, so a library caller
//! can tell a missing table from a bad query from a storage failure without
//! matching on message text.

mod common;

use storage_manager::backend::error::RookError;
use storage_manager::backend::session::Session;

fn error(s: &mut Session, sql: &str) -> RookError {
    match s.execute(rook_parser::parse_sql(sql).unwrap()) {
        Ok(_) => panic!("{} should fail", sql),
        Err(e) => e,
    }
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("error_kinds", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE err_db",
        "USE err_db",
        "CREATE TABLE t (id INT, s VARCHAR(10))",
        "INSERT INTO t VALUES (1, 'a')",
    ] {
        s.execute(rook_parser::parse_sql(sql).unwrap())
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    (ws, s)
}

#[test]
fn missing_objects_are_not_found() {
    let (_ws, mut s) = session("not_found");
    for sql in [
        "SELECT * FROM nope",
        "INSERT INTO nope VALUES (1)",
        "SELECT * FROM t WHERE id IN (SELECT id FROM nope)",
    ] {
        let e = error(&mut s, sql);
        assert!(
            matches!(
                e.root(),
                RookError::NotFound {
                    entity: "Table",
                    ..
                }
            ),
            "{}: {:?}",
            sql,
            e
        );
    }
    let e = error(&mut s, "INSERT INTO t (id, nope) VALUES (1, 2)");
    assert!(
        matches!(
            e.root(),
            RookError::NotFoundIn {
                entity: "Column",
                ..
            }
        ),
        "{:?}",
        e
    );
}

#[test]
fn malformed_queries_are_invalid() {
    let (_ws, mut s) = session("invalid");
    for sql in [
        "SELECT id FROM t ORDER BY 3",
        "SELECT id FROM t GROUP BY 2",
        "SELECT id, s FROM t GROUP BY id",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t (id) VALUES (1, 'a')",
        "SELECT id FROM t UNION SELECT id, s FROM t",
    ] {
        let e = error(&mut s, sql);
        assert!(
            matches!(e.root(), RookError::InvalidQuery(_)),
            "{}: {:?}",
            sql,
            e
        );
    }
}

#[test]
fn runtime_failures_are_evaluation_errors() {
    let (_ws, mut s) = session("evaluation");
    s.execute(rook_parser::parse_sql("INSERT INTO t VALUES (2, 'b')").unwrap())
        .unwrap();
    let e = error(&mut s, "SELECT (SELECT id FROM t) FROM t");
    assert!(matches!(e.root(), RookError::Evaluation(_)), "{:?}", e);
}

#[test]
fn unimplemented_forms_are_unsupported() {
    let (_ws, mut s) = session("unsupported");
    let e = error(&mut s, "EXPLAIN ANALYZE DELETE FROM t");
    assert!(matches!(e.root(), RookError::Unsupported(_)), "{:?}", e);
}

#[test]
fn no_database_selected_is_invalid() {
    let _ws = common::TestWorkspace::new("error_kinds", "no_db");
    let mut s = Session::new(common::load_catalog());
    let e = error(&mut s, "SELECT * FROM t");
    assert!(matches!(e.root(), RookError::InvalidQuery(_)), "{:?}", e);
}
