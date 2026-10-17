//! Prepared statements and the INSERT fast path: a statement's literals are
//! never confused with another statement's, nor with each other's kinds.

mod common;

use storage_manager::backend::planner::plan_cache::{
    Literal, PreparedStatement, execute_cached_insert,
};
use storage_manager::backend::session::Session;

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let plan = rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let result = s.execute(plan).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(_, rows)| {
            rows.into_iter()
                .map(|t| {
                    t.values
                        .iter()
                        .map(|v| v.as_ref().map_or("NULL".to_string(), |d| d.to_string()))
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("prepared", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE ps",
        "USE ps",
        "CREATE TABLE t (id INT, s VARCHAR(10))",
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'b')",
    ] {
        rows(&mut s, sql);
    }
    (ws, s)
}

fn num(n: &str) -> Literal {
    Literal::Number(n.to_string())
}

fn text(t: &str) -> Literal {
    Literal::Text(t.to_string())
}

#[test]
fn a_select_prepared_after_another_keeps_its_own_literals() {
    let (_ws, s) = session("literals");
    let first =
        PreparedStatement::prepare(&s.catalog, "ps", "SELECT id FROM t WHERE id > 2").unwrap();
    assert_eq!(first.execute(&s.catalog, &[num("2")]).unwrap(), 1);
    // Same template, other literal: must not reuse `id > 2`.
    let second =
        PreparedStatement::prepare(&s.catalog, "ps", "SELECT id FROM t WHERE id > 0").unwrap();
    assert_eq!(second.execute(&s.catalog, &[num("0")]).unwrap(), 3);
}

#[test]
fn a_prepared_select_takes_new_parameter_values() {
    let (_ws, s) = session("params");
    let stmt =
        PreparedStatement::prepare(&s.catalog, "ps", "SELECT id FROM t WHERE s = 'a'").unwrap();
    assert_eq!(stmt.param_count, 1);
    assert_eq!(stmt.execute(&s.catalog, &[text("a")]).unwrap(), 1);
    assert_eq!(stmt.execute(&s.catalog, &[text("b")]).unwrap(), 2);
    assert_eq!(
        stmt.execute(&s.catalog, &[text("x' OR 'y' = 'y")]).unwrap(),
        0
    );
    assert!(stmt.execute(&s.catalog, &[]).is_err());
    for bad in ["' OR 1 = 1 --", "4G", "ab'"] {
        assert!(
            stmt.execute(&s.catalog, &[Literal::Hex(bad.to_string())])
                .is_err(),
            "{} accepted as hex digits",
            bad
        );
    }

    let by_id =
        PreparedStatement::prepare(&s.catalog, "ps", "SELECT id FROM t WHERE id = 1").unwrap();
    assert_eq!(by_id.execute(&s.catalog, &[num("3")]).unwrap(), 1);
    for bad in ["1 OR 1 = 1", "inf", "NaN", "1e", "."] {
        assert!(
            by_id.execute(&s.catalog, &[num(bad)]).is_err(),
            "{} accepted as a number",
            bad
        );
    }
}

#[test]
fn a_prepared_insert_stores_its_parameters() {
    let (_ws, mut s) = session("insert");
    let stmt =
        PreparedStatement::prepare(&s.catalog, "ps", "INSERT INTO t VALUES (9, 'z')").unwrap();
    assert_eq!(stmt.execute(&s.catalog, &[num("7"), text("q")]).unwrap(), 1);
    assert_eq!(
        stmt.execute(&s.catalog, &[num("8"), Literal::Null])
            .unwrap(),
        1
    );
    assert_eq!(
        rows(&mut s, "SELECT id, s FROM t WHERE id > 5 ORDER BY id"),
        [["7", "'q'"], ["8", "NULL"]]
    );
}

#[test]
fn empty_and_null_strings_are_not_null() {
    let (_ws, mut s) = session("strings");
    for sql in [
        "INSERT INTO t VALUES (10, '')",
        "INSERT INTO t VALUES (11, 'NULL')",
        "INSERT INTO t VALUES (12, NULL)",
    ] {
        assert!(matches!(
            execute_cached_insert(&s.catalog, "ps", sql),
            Ok(Some(1))
        ));
    }
    assert_eq!(
        rows(
            &mut s,
            "SELECT id, s IS NULL FROM t WHERE id >= 10 ORDER BY id"
        ),
        [["10", "false"], ["11", "false"], ["12", "true"]]
    );
    assert_eq!(rows(&mut s, "SELECT s FROM t WHERE id = 11"), [["'NULL'"]]);
}

#[test]
fn inserts_with_expressions_evaluate_them() {
    let (_ws, mut s) = session("exprinsert");
    for sql in [
        "INSERT INTO t VALUES (ABS(-20), 'x')",
        "INSERT INTO t VALUES (1 + 20, UPPER('y'))",
        "INSERT INTO t VALUES (22, 'z' || 'z')",
    ] {
        match execute_cached_insert(&s.catalog, "ps", sql) {
            Ok(Some(n)) => assert_eq!(n, 1, "{}", sql),
            Ok(None) => {
                rows(&mut s, sql);
            }
            Err(e) => panic!("{}: {}", sql, e),
        }
    }
    let stmt = PreparedStatement::prepare(&s.catalog, "ps", "INSERT INTO t VALUES (ABS(-1), 'a')")
        .unwrap();
    assert_eq!(
        stmt.execute(&s.catalog, &[num("-23"), text("w")]).unwrap(),
        1
    );
    assert_eq!(
        rows(&mut s, "SELECT id, s FROM t WHERE id >= 20 ORDER BY id"),
        [["20", "'x'"], ["21", "'Y'"], ["22", "'zz'"], ["23", "'w'"]]
    );
}
