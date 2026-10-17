//! Queries the planner rewrites before running them, checked against the
//! result the unrewritten query defines. Each of these returned wrong rows
//! or failed to plan; they were found by comparing random queries with
//! SQLite.

mod common;

use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;

fn run(s: &mut Session, sql: &str) -> Vec<Vec<Option<i64>>> {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let (_, rows) = result.rows.unwrap_or_default();
    rows.into_iter()
        .map(|t| {
            t.values
                .into_iter()
                .map(|v| match v {
                    None => None,
                    Some(DataValue::Int(i)) => Some(i as i64),
                    Some(DataValue::BigInt(i)) => Some(i),
                    Some(other) => panic!("{}: unexpected value {:?}", sql, other),
                })
                .collect()
        })
        .collect()
}

fn sorted(mut rows: Vec<Vec<Option<i64>>>) -> Vec<Vec<Option<i64>>> {
    rows.sort();
    rows
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("rewrites", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE rw",
        "USE rw",
        "CREATE TABLE p (id INT, b INT)",
        "CREATE TABLE q (id INT, b INT)",
        "INSERT INTO p VALUES (1, 30), (2, 10), (3, 20), (4, NULL)",
        "INSERT INTO q VALUES (1, 7), (2, 7), (3, 8), (5, 9)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

/// `5 > c` is `c < 5`: an index range built from a comparison with the
/// constant on the left must flip the operator.
#[test]
fn constant_on_the_left_of_an_indexed_comparison() {
    let (_ws, mut s) = session("flip");
    run(&mut s, "CREATE INDEX p_b ON p (b)");
    for (sql, expected) in [
        ("SELECT id FROM p WHERE 20 > b", vec![2]),
        ("SELECT id FROM p WHERE 20 >= b", vec![2, 3]),
        ("SELECT id FROM p WHERE 20 < b", vec![1]),
        ("SELECT id FROM p WHERE 20 <= b", vec![1, 3]),
        ("SELECT id FROM p WHERE 20 = b", vec![3]),
        ("SELECT id FROM p WHERE (5 + 5) < b", vec![1, 3]),
    ] {
        let expected: Vec<_> = expected.into_iter().map(|i| vec![Some(i)]).collect();
        assert_eq!(sorted(run(&mut s, sql)), expected, "{}", sql);
    }
}

/// A WHERE conjunct holding a scalar subquery is not pushed into one join
/// input or into the join condition: the subquery may read the other input,
/// and only a filter above the join evaluates it.
#[test]
fn scalar_subquery_in_where_stays_above_a_join() {
    let (_ws, mut s) = session("scalar_join");
    // Correlated with the right input of the join.
    assert_eq!(
        sorted(run(
            &mut s,
            "SELECT p.id, q.id FROM p JOIN q ON q.id = p.id \
             WHERE p.b > 10 * (SELECT COUNT(*) FROM q q2 WHERE q2.b = q.b)",
        )),
        vec![vec![Some(1), Some(1)], vec![Some(3), Some(3)]]
    );
    // Uncorrelated, compared with an expression over both inputs.
    assert_eq!(
        sorted(run(
            &mut s,
            "SELECT p.id FROM p CROSS JOIN q \
             WHERE p.b + q.b > (SELECT MAX(b) FROM p) AND q.id = 1",
        )),
        vec![vec![Some(1)]]
    );
}

/// A computed GROUP BY key with a constant part (`-3` parses as `0 - 3`) is
/// read by the projection above under the key's own name, which constant
/// folding must not change.
#[test]
fn group_by_expression_with_a_constant_part() {
    let (_ws, mut s) = session("group_const");
    assert_eq!(
        sorted(run(
            &mut s,
            "SELECT -3 - id, COUNT(*) FROM q GROUP BY -3 - id"
        )),
        vec![
            vec![Some(-8), Some(1)],
            vec![Some(-6), Some(1)],
            vec![Some(-5), Some(1)],
            vec![Some(-4), Some(1)]
        ]
    );
    assert_eq!(
        run(&mut s, "SELECT 3 + 12, COUNT(*) FROM q GROUP BY 3 + 12"),
        vec![vec![Some(15), Some(4)]]
    );
    assert_eq!(
        sorted(run(
            &mut s,
            "SELECT b * (1 + 1), COUNT(*) FROM q GROUP BY b * (1 + 1) HAVING COUNT(*) > 1",
        )),
        vec![vec![Some(14), Some(2)]]
    );
}
