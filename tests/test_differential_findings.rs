//! Bugs found by the SQLite differential test (`test_differential_sqlite.rs`),
//! pinned here so they stay fixed whatever the generator produces.

mod common;

use storage_manager::backend::error::RookError;
use storage_manager::backend::session::Session;

fn try_run(s: &mut Session, sql: &str) -> Result<Vec<Vec<String>>, RookError> {
    let result = s.execute(rook_parser::parse_sql(sql).unwrap())?;
    let (_, rows) = result.rows.unwrap_or_default();
    Ok(rows
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| v.as_ref().map_or("NULL".to_string(), |v| v.to_string()))
                .collect()
        })
        .collect())
}

fn run(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    try_run(s, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

/// The first column of each row, sorted.
fn firsts(mut rows: Vec<Vec<String>>) -> Vec<String> {
    let mut out: Vec<String> = rows.iter_mut().map(|r| r.remove(0)).collect();
    out.sort();
    out
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("differential_findings", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE df_db",
        "USE df_db",
        "CREATE TABLE t (a INT, s VARCHAR(5))",
        "INSERT INTO t VALUES (1, 'ab'), (2, 'b'), (3, NULL)",
        "CREATE TABLE u (a INT)",
        "INSERT INTO u VALUES (2), (3)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

/// HAVING matched an aggregate by its display name, which is the same
/// ("expr") for any two arithmetic arguments: `MIN(a * 10)` read the
/// SELECT list's `MIN(a - 1)`.
#[test]
fn having_matches_aggregates_by_their_arguments() {
    let (_ws, mut s) = session("having");
    let rows = run(
        &mut s,
        "SELECT a, MIN(a - 1) FROM t GROUP BY a HAVING MIN(a * 10) = 20",
    );
    assert_eq!(rows, [["2", "1"]]);
    // An aggregate below the top of a HAVING operand.
    let rows = run(&mut s, "SELECT a FROM t GROUP BY a HAVING MAX(a) + 1 > 3");
    assert_eq!(firsts(rows), ["3"]);
}

/// sqlparser read `x IS DISTINCT FROM y AND z` as `x IS DISTINCT FROM (y AND z)`.
#[test]
fn is_distinct_from_binds_tighter_than_and() {
    let (_ws, mut s) = session("is_distinct");
    let rows = run(
        &mut s,
        "SELECT a FROM t WHERE s IS DISTINCT FROM 'b' AND a > 1",
    );
    assert_eq!(firsts(rows), ["3"]);
}

/// `a UNION b EXCEPT c` was rejected; INTERSECT binds tighter than the others.
#[test]
fn set_operations_chain() {
    let (_ws, mut s) = session("set_ops");
    let rows = run(
        &mut s,
        "SELECT a FROM t UNION SELECT a FROM u EXCEPT SELECT 3",
    );
    assert_eq!(firsts(rows), ["1", "2"]);
    let rows = run(
        &mut s,
        "SELECT a FROM t EXCEPT SELECT a FROM u INTERSECT SELECT 2",
    );
    assert_eq!(firsts(rows), ["1", "3"]);
    let rows = run(
        &mut s,
        "SELECT a FROM t UNION ALL SELECT a FROM u UNION ALL SELECT a FROM u ORDER BY 1 LIMIT 3",
    );
    assert_eq!(rows, [["1"], ["2"], ["2"]]);
}

/// A COALESCE or CASE over text and numbers gave one column values of both
/// kinds, which then could not be ordered (and made the sort panic).
#[test]
fn branches_of_different_kinds_are_type_errors() {
    let (_ws, mut s) = session("branch_types");
    for sql in [
        "SELECT COALESCE(s, 1) FROM t",
        "SELECT CASE WHEN a > 1 THEN s ELSE a END FROM t",
    ] {
        let e = try_run(&mut s, sql).expect_err(sql);
        assert!(
            matches!(e.root(), RookError::TypeMismatch(_)),
            "{}: {:?}",
            sql,
            e
        );
    }
    // Numbers of different types mix, and a string literal fits any type.
    run(
        &mut s,
        "SELECT COALESCE(a, 1.5), COALESCE(s, 'none') FROM t",
    );
}
