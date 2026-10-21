//! Predicates used as values (`CASE WHEN x IN (...)`, `SELECT a BETWEEN ..`)
//! and subqueries nested anywhere in an expression: inside CASE, inside an
//! aggregate's argument, as a GROUP BY key. Found by the SQLite differential
//! test (`test_differential_sqlite.rs`).

mod common;

use storage_manager::backend::error::RookError;
use storage_manager::backend::session::Session;

fn run(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let (_, rows) = result.rows.unwrap_or_default();
    rows.iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| v.as_ref().map_or("NULL".to_string(), |v| v.to_string()))
                .collect()
        })
        .collect()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("predicate_values", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE pv_db",
        "USE pv_db",
        "CREATE TABLE t (a INT, s VARCHAR(5))",
        "INSERT INTO t VALUES (1, 'ab'), (2, 'b'), (3, NULL)",
        "CREATE TABLE u (a INT)",
        "INSERT INTO u VALUES (2), (3)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

fn column(rows: &[Vec<String>], i: usize) -> Vec<&str> {
    rows.iter().map(|r| r[i].as_str()).collect()
}

#[test]
fn predicates_are_values() {
    let (_ws, mut s) = session("values");
    let rows = run(
        &mut s,
        "SELECT a, CASE WHEN a IN (1, 3) THEN 'y' ELSE 'n' END FROM t ORDER BY a",
    );
    assert_eq!(column(&rows, 1), ["'y'", "'n'", "'y'"]);
    let rows = run(&mut s, "SELECT a, a BETWEEN 2 AND 3 FROM t ORDER BY a");
    assert_eq!(column(&rows, 1), ["false", "true", "true"]);
    let rows = run(&mut s, "SELECT a, s LIKE 'a%' FROM t ORDER BY a");
    assert_eq!(column(&rows, 1), ["true", "false", "NULL"]);
    let rows = run(
        &mut s,
        "SELECT a, EXISTS (SELECT 1 FROM u WHERE u.a = t.a) FROM t ORDER BY a",
    );
    assert_eq!(column(&rows, 1), ["false", "true", "true"]);
    let rows = run(
        &mut s,
        "SELECT a, a NOT IN (SELECT a FROM u) FROM t ORDER BY a",
    );
    assert_eq!(column(&rows, 1), ["true", "false", "false"]);
}

#[test]
fn correlated_subquery_inside_case_in_where() {
    let (_ws, mut s) = session("case_where");
    let rows = run(
        &mut s,
        "SELECT a FROM t WHERE CASE WHEN a < (SELECT MAX(u.a) FROM u WHERE u.a = t.a + 1) \
         THEN TRUE ELSE FALSE END ORDER BY a",
    );
    assert_eq!(column(&rows, 0), ["1", "2"]);
}

#[test]
fn subqueries_in_aggregates_and_group_keys() {
    let (_ws, mut s) = session("aggregates");
    let rows = run(
        &mut s,
        "SELECT SUM(CASE WHEN a IN (SELECT a FROM u) THEN a ELSE 0 END) FROM t",
    );
    assert_eq!(column(&rows, 0), ["5"]);
    let rows = run(
        &mut s,
        "SELECT MAX(CASE WHEN a < (SELECT MIN(u.a) FROM u WHERE u.a > t.a) THEN a END) FROM t",
    );
    assert_eq!(column(&rows, 0), ["2"]);
    let rows = run(
        &mut s,
        "SELECT (SELECT COUNT(*) FROM u), COUNT(*) FROM t GROUP BY (SELECT COUNT(*) FROM u)",
    );
    assert_eq!(rows, [["2", "3"]]);
}

#[test]
fn view_depends_on_a_table_named_in_a_nested_subquery() {
    let (_ws, mut s) = session("view_dependency");
    run(
        &mut s,
        "CREATE VIEW v AS SELECT CASE WHEN a > (SELECT MIN(a) FROM u) THEN 1 END AS c FROM t",
    );
    let e = s
        .execute(rook_parser::parse_sql("DROP TABLE u").unwrap())
        .expect_err("u is used by v");
    assert!(matches!(e.root(), RookError::InvalidQuery(_)), "{:?}", e);
}
