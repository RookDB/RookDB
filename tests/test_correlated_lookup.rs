//! Correlated subqueries whose inner rows are hashed once by the columns
//! compared for equality with the outer row (`CorrelatedLookupOperator`):
//! the answers must match the per-row scan they replace.

mod common;

use storage_manager::backend::session::Session;

fn run(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
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

fn ids(s: &mut Session, sql: &str) -> Vec<String> {
    run(s, sql).into_iter().map(|r| r[0].clone()).collect()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("corrlookup", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE cl",
        "USE cl",
        "CREATE TABLE o (id INT, k INT, lo INT)",
        "INSERT INTO o VALUES (1, 10, 0), (2, 20, 5), (3, NULL, 0), (4, 40, 0), (5, 10, 150)",
        "CREATE TABLE i (k BIGINT, v INT)",
        "INSERT INTO i VALUES (10, 100), (10, 200), (20, NULL), (NULL, 1), (30, 300)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

#[test]
fn exists_and_not_exists() {
    let (_ws, mut s) = session("exists");
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY id"
        ),
        ["1", "2", "5"]
    );
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY id"
        ),
        ["3", "4"]
    );
}

#[test]
fn equality_key_with_a_further_condition() {
    let (_ws, mut s) = session("residual");
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE EXISTS \
             (SELECT 1 FROM i WHERE i.k = o.k AND i.v > o.lo AND i.v < 1000) ORDER BY id"
        ),
        ["1", "5"]
    );
}

#[test]
fn correlated_in_keeps_null_logic() {
    let (_ws, mut s) = session("in");
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE 200 IN (SELECT v FROM i WHERE i.k = o.k) ORDER BY id"
        ),
        ["1", "5"]
    );
    // k = 20 has only a NULL v: NOT IN is UNKNOWN there; no rows: TRUE.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE 7 NOT IN (SELECT v FROM i WHERE i.k = o.k) ORDER BY id"
        ),
        ["1", "3", "4", "5"]
    );
}

#[test]
fn correlated_scalar_aggregate() {
    let (_ws, mut s) = session("scalar");
    assert_eq!(
        run(
            &mut s,
            "SELECT id, (SELECT COUNT(*) FROM i WHERE i.k = o.k), \
             (SELECT SUM(v) FROM i WHERE i.k = o.k) FROM o ORDER BY id"
        ),
        [
            ["1", "2", "300"],
            ["2", "1", "NULL"],
            ["3", "0", "NULL"],
            ["4", "0", "NULL"],
            ["5", "2", "300"],
        ]
    );
}

#[test]
fn inner_table_changes_are_seen_by_the_next_statement() {
    let (_ws, mut s) = session("fresh");
    let q = "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY id";
    assert_eq!(ids(&mut s, q), ["1", "2", "5"]);
    run(&mut s, "INSERT INTO i VALUES (40, 4)");
    run(&mut s, "DELETE FROM i WHERE k = 20");
    assert_eq!(ids(&mut s, q), ["1", "4", "5"]);
}

#[test]
fn keys_match_across_numeric_types() {
    let (_ws, mut s) = session("numeric_keys");
    for sql in [
        "CREATE TABLE n (k NUMERIC(6,2), d DOUBLE PRECISION)",
        "INSERT INTO n VALUES (10.00, 20.0), (10.50, 40.5), (99.00, 1.5)",
    ] {
        run(&mut s, sql);
    }
    // NUMERIC 10.00 and INT 10 are one key; 10.50 matches no integer.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM n WHERE n.k = o.k) ORDER BY id"
        ),
        ["1", "5"]
    );
    // DOUBLE 20.0 meets INT 20; the outer column is now the decimal one.
    assert_eq!(
        run(
            &mut s,
            "SELECT k, (SELECT COUNT(*) FROM o WHERE o.k = n.d) FROM n ORDER BY k"
        ),
        [["10.00", "1"], ["10.50", "0"], ["99.00", "0"]]
    );
}

/// `session` plus an index on `i.k`, and `extra` more rows of `i` with keys
/// 1000.. so the index answers up to (5 + extra) / 2 outer rows before the
/// hash table takes over.
fn indexed_session(tag: &str, extra: usize) -> (common::TestWorkspace, Session) {
    let (ws, mut s) = session(tag);
    if extra > 0 {
        let rows: Vec<String> = (0..extra)
            .map(|n| format!("({}, {})", 1000 + n, n))
            .collect();
        run(&mut s, &format!("INSERT INTO i VALUES {}", rows.join(", ")));
    }
    run(&mut s, "CREATE INDEX i_k ON i (k)");
    (ws, s)
}

#[test]
fn index_probe_answers_like_the_hash_table() {
    // 40 extra rows: every outer row is probed. 0 extra: the hash table
    // takes over after two probes, partway through the outer rows.
    for extra in [40, 0] {
        let (_ws, mut s) = indexed_session(&format!("probe{extra}"), extra);
        assert_eq!(
            ids(
                &mut s,
                "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY id"
            ),
            ["1", "2", "5"],
            "extra = {extra}"
        );
        assert_eq!(
            run(
                &mut s,
                "SELECT id, (SELECT COUNT(*) FROM i WHERE i.k = o.k), \
                 (SELECT SUM(v) FROM i WHERE i.k = o.k) FROM o ORDER BY id"
            ),
            [
                ["1", "2", "300"],
                ["2", "1", "NULL"],
                ["3", "0", "NULL"],
                ["4", "0", "NULL"],
                ["5", "2", "300"],
            ],
            "extra = {extra}"
        );
        assert_eq!(
            ids(
                &mut s,
                "SELECT id FROM o WHERE 7 NOT IN (SELECT v FROM i WHERE i.k = o.k) ORDER BY id"
            ),
            ["1", "3", "4", "5"],
            "extra = {extra}"
        );
    }
}

#[test]
fn index_probe_with_inexact_outer_values_uses_the_hash_table() {
    let (_ws, mut s) = indexed_session("probe_inexact", 40);
    for sql in [
        "CREATE TABLE n (id INT, k NUMERIC(6,2))",
        "INSERT INTO n VALUES (1, 10.00), (2, 10.50), (3, 1001.00), (4, NULL)",
    ] {
        run(&mut s, sql);
    }
    // A NUMERIC does not convert to the BIGINT key without loss in general;
    // 10.00 and 1001.00 still find their rows.
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM n WHERE EXISTS (SELECT 1 FROM i WHERE i.k = n.k) ORDER BY id"
        ),
        ["1", "3"]
    );
}

#[test]
fn index_probe_on_text_keys() {
    let (_ws, mut s) = session("probe_text");
    for sql in [
        "CREATE TABLE c (code CHAR(4), v INT)",
        "INSERT INTO c VALUES ('ab', 1), ('ab', 2), ('xyz', 3)",
        "CREATE INDEX c_code ON c (code)",
        "CREATE TABLE w (id INT, code VARCHAR(10))",
        "INSERT INTO w VALUES (1, 'ab'), (2, 'xyz'), (3, 'zz'), (4, NULL)",
    ] {
        run(&mut s, sql);
    }
    assert_eq!(
        run(
            &mut s,
            "SELECT id, (SELECT COUNT(*) FROM c WHERE c.code = w.code) FROM w ORDER BY id"
        ),
        [["1", "2"], ["2", "1"], ["3", "0"], ["4", "0"]]
    ); // VARCHAR outer values are exact keys of a VARCHAR index.
    for sql in [
        "INSERT INTO w VALUES (5, 'ab')",
        "CREATE INDEX w_code ON w (code)",
    ] {
        run(&mut s, sql);
    }
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM w x WHERE EXISTS \
             (SELECT 1 FROM w WHERE w.code = x.code AND w.id <> x.id) ORDER BY id"
        ),
        ["1", "5"]
    );
}

#[test]
fn index_probe_sees_inner_changes() {
    let (_ws, mut s) = indexed_session("probe_fresh", 40);
    let q = "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k) ORDER BY id";
    assert_eq!(ids(&mut s, q), ["1", "2", "5"]);
    run(&mut s, "INSERT INTO i VALUES (40, 4)");
    run(&mut s, "DELETE FROM i WHERE k = 20");
    assert_eq!(ids(&mut s, q), ["1", "4", "5"]);
}
