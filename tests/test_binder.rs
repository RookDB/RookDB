//! Name resolution done once, before planning, by the planner's binder:
//! aliases in GROUP BY, qualified references to views, PostgreSQL's rules for
//! which names a FROM item exposes, errors reported up front with their
//! kind, and correlated subqueries found from the bound tree.

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

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("binder", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE b_db",
        "USE b_db",
        "CREATE TABLE t (a INT, b INT)",
        "INSERT INTO t VALUES (1, 10), (2, 20), (2, 30)",
        "CREATE TABLE u (a INT, c INT)",
        "INSERT INTO u VALUES (2, 5), (3, 6)",
        "CREATE VIEW v AS SELECT a, b + 1 AS b1 FROM t",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

#[test]
fn group_by_reads_output_aliases() {
    let (_ws, mut s) = session("group_alias");
    let rows = run(&mut s, "SELECT a AS k FROM t GROUP BY k");
    assert_eq!(sorted(rows), [["1"], ["2"]]);
    let rows = run(&mut s, "SELECT a + 1 AS k, COUNT(*) FROM t GROUP BY k");
    assert_eq!(sorted(rows), [["2", "1"], ["3", "2"]]);
    // An input column of the name wins over the alias, as in SQL.
    let rows = run(&mut s, "SELECT b AS a, COUNT(*) FROM t GROUP BY a, b");
    assert_eq!(rows.len(), 3);
}

#[test]
fn views_are_read_by_their_name_or_alias() {
    let (_ws, mut s) = session("views");
    assert_eq!(run(&mut s, "SELECT v.b1 FROM v WHERE v.a = 1"), [["11"]]);
    assert_eq!(run(&mut s, "SELECT w.b1 FROM v w WHERE w.a = 1"), [["11"]]);
    let rows = run(&mut s, "SELECT w.a, u.c FROM v w JOIN u ON w.a = u.a");
    assert_eq!(sorted(rows), [["2", "5"], ["2", "5"]]);
}

#[test]
fn filters_on_a_view_reach_its_table() {
    let (_ws, mut s) = session("view_pushdown");
    let plan = run(&mut s, "EXPLAIN SELECT * FROM v WHERE b1 > 15");
    let lines: Vec<&str> = plan
        .iter()
        .map(|r| r[0].trim_matches('\'').trim())
        .collect();
    let filter = lines.iter().position(|l| l.starts_with("Filter"));
    let scan = lines.iter().position(|l| l.starts_with("SeqScan"));
    assert!(
        matches!((filter, scan), (Some(f), Some(sc)) if sc == f + 1),
        "{:?}",
        lines
    );
    assert_eq!(
        sorted(run(&mut s, "SELECT * FROM v WHERE b1 > 15")),
        [["2", "21"], ["2", "31"]]
    );
}

#[test]
fn an_aliased_table_is_read_by_its_alias_only() {
    let (_ws, mut s) = session("alias_hides");
    let e = try_run(&mut s, "SELECT t.a FROM t x").expect_err("t is hidden by x");
    assert!(
        matches!(e.root(), RookError::InvalidQuery(m) if m.contains("'t'")),
        "{:?}",
        e
    );
    let e = try_run(&mut s, "SELECT a FROM t JOIN t ON true").expect_err("t twice");
    assert!(matches!(e.root(), RookError::InvalidQuery(_)), "{:?}", e);
}

#[test]
fn unknown_and_ambiguous_names_fail_before_planning() {
    let (_ws, mut s) = session("errors");
    for (sql, ambiguous) in [
        ("SELECT zz FROM t", false),
        ("SELECT b FROM t GROUP BY zz", false),
        ("SELECT a FROM t GROUP BY a HAVING zz > 1", false),
        ("SELECT a FROM t ORDER BY zz", false),
        (
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.zz = 1)",
            false,
        ),
        ("SELECT a FROM t, u", true),
        (
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t t1, t t2 WHERE b = 1)",
            true,
        ),
    ] {
        let e = try_run(&mut s, sql).expect_err(sql);
        match e.root() {
            RookError::AmbiguousColumn(_) if ambiguous => {}
            RookError::NotFound {
                entity: "Column", ..
            } if !ambiguous => {}
            other => panic!("{}: {:?}", sql, other),
        }
        assert!(
            e.to_string().starts_with("Planning error"),
            "{}: {}",
            sql,
            e
        );
    }
}

#[test]
fn correlated_subqueries_at_any_depth() {
    let (_ws, mut s) = session("correlated");
    // A bare name the subquery's tables lack is the outer query's.
    let rows = run(
        &mut s,
        "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE c * 4 = b)",
    );
    assert_eq!(rows, [["2"]]);
    // Two levels out, through a subquery that is not itself correlated
    // with its own enclosing query.
    let rows = run(
        &mut s,
        "SELECT t.a, (SELECT COUNT(*) FROM u WHERE u.c IN \
         (SELECT x.b / 4 FROM t x WHERE x.a = t.a)) FROM t ORDER BY t.b",
    );
    assert_eq!(rows, [["1", "0"], ["2", "1"], ["2", "1"]]);
    // A CTE read by the outer query and named inside the subquery.
    let rows = run(
        &mut s,
        "WITH c AS (SELECT a, b FROM t) \
         SELECT c.b FROM c WHERE EXISTS (SELECT 1 FROM u WHERE u.a = c.a) ORDER BY c.b",
    );
    assert_eq!(rows, [["20"], ["30"]]);
    // A subquery's own column shadows the outer one of the same name.
    let rows = run(
        &mut s,
        "SELECT a FROM t WHERE a = (SELECT MAX(a) FROM u WHERE c < 6)",
    );
    assert_eq!(rows, [["2"], ["2"]]);
}

#[test]
fn star_follows_using_joins() {
    let (_ws, mut s) = session("star");
    let rows = run(
        &mut s,
        "SELECT * FROM t FULL JOIN u USING (a) ORDER BY a, b",
    );
    assert_eq!(
        rows,
        [
            ["1", "10", "NULL"],
            ["2", "20", "5"],
            ["2", "30", "5"],
            ["3", "NULL", "6"]
        ]
    );
    let rows = run(&mut s, "SELECT u.* FROM t JOIN u USING (a) WHERE t.b = 20");
    assert_eq!(rows, [["2", "5"]]);
}
