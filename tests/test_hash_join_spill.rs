//! A hash join that spills to disk must return the rows of the same join run
//! in memory, for every join type, with duplicate, NULL and skewed keys, and
//! for derived columns whose values are not of the column's declared type.

mod common;

use std::sync::atomic::Ordering;

use storage_manager::backend::executor::physical::operators::{
    HASH_JOIN_SPILL_BUDGET_BYTES, HASH_JOIN_SPILL_COUNT,
};
use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;

type Row = Vec<Option<DataValue>>;

fn rows(s: &mut Session, sql: &str) -> Vec<Row> {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e)))
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(_, rows)| rows.into_iter().map(|t| t.values).collect())
        .unwrap_or_default()
}

/// The budget is process-wide, so the tests of this file take turns.
static BUDGET_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Run `sql` with the hash-join build budget set to `budget` bytes (0 = the
/// default). Returns the rows in a canonical order and how many joins spilled.
fn run_with_budget(s: &mut Session, sql: &str, budget: usize) -> (Vec<Row>, usize) {
    HASH_JOIN_SPILL_BUDGET_BYTES.store(budget, Ordering::Relaxed);
    let spills_before = HASH_JOIN_SPILL_COUNT.load(Ordering::Relaxed);
    let mut out = rows(s, sql);
    HASH_JOIN_SPILL_BUDGET_BYTES.store(0, Ordering::Relaxed);
    out.sort_by_cached_key(|row| format!("{:?}", row));
    (
        out,
        HASH_JOIN_SPILL_COUNT.load(Ordering::Relaxed) - spills_before,
    )
}

/// 16 KiB: spilled partitions fit in memory. 2 KiB: they are split again.
const BUDGETS: &[usize] = &[16 * 1024, 2 * 1024];
/// As [`BUDGETS`], plus 1 byte: every block holds a single build row.
const BUDGETS_AND_ONE_BYTE: &[usize] = &[16 * 1024, 2 * 1024, 1];

/// Check that `sql` gives the same rows in memory and under each budget.
fn assert_spill_agrees(
    s: &mut Session,
    sql: &str,
    expected_rows: Option<usize>,
    budgets: &[usize],
) {
    let (in_memory, spills) = run_with_budget(s, sql, 0);
    assert_eq!(spills, 0, "{}: spilled under the default budget", sql);
    if let Some(n) = expected_rows {
        assert_eq!(in_memory.len(), n, "{}", sql);
    }
    for &budget in budgets {
        let (spilled, spills) = run_with_budget(s, sql, budget);
        assert!(spills > 0, "{}: budget {} did not spill", sql, budget);
        assert_eq!(
            spilled.len(),
            in_memory.len(),
            "{} (budget {})",
            sql,
            budget
        );
        assert_eq!(spilled, in_memory, "{} (budget {})", sql, budget);
    }
}

/// `a`: 600 rows, ids 0..599. `b`: 600 rows, even ids 0..1198, so half of
/// each table finds a partner by id. `k` repeats (30 / 20 distinct values)
/// and is NULL in some rows. `hot`: 400 rows that all share k = 3.
fn setup(tag: &str, db: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("hjspill", tag);
    let mut s = Session::new(common::load_catalog());
    rows(&mut s, &format!("CREATE DATABASE {}", db));
    rows(&mut s, &format!("USE {}", db));
    for (table, id_step, distinct) in [("a", 1, 30), ("b", 2, 20)] {
        rows(
            &mut s,
            &format!(
                "CREATE TABLE {} (id INT, k INT, name VARCHAR(20), big BIGINT, score DOUBLE PRECISION)",
                table
            ),
        );
        let values: Vec<String> = (0..600)
            .map(|i| {
                let k = if i % 11 == 0 {
                    "NULL".to_string()
                } else {
                    (i % distinct).to_string()
                };
                format!(
                    "({}, {}, '{}_{}', {}, {}.5)",
                    i * id_step,
                    k,
                    table,
                    i,
                    5_000_000_000i64 + i as i64,
                    i
                )
            })
            .collect();
        rows(
            &mut s,
            &format!("INSERT INTO {} VALUES {}", table, values.join(", ")),
        );
    }
    rows(&mut s, "CREATE TABLE hot (id INT, k INT)");
    let values: Vec<String> = (0..400).map(|i| format!("({}, 3)", i)).collect();
    rows(
        &mut s,
        &format!("INSERT INTO hot VALUES {}", values.join(", ")),
    );
    (ws, s)
}

#[test]
fn spilled_joins_match_in_memory_joins_for_every_join_type() {
    let _lock = BUDGET_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_ws, mut s) = setup("types", "hj");
    for (join, expected) in [
        ("JOIN", 300),
        ("LEFT JOIN", 600),
        ("RIGHT JOIN", 600),
        ("FULL JOIN", 900),
    ] {
        assert_spill_agrees(
            &mut s,
            &format!("SELECT * FROM a {} b ON a.id = b.id", join),
            Some(expected),
            BUDGETS_AND_ONE_BYTE,
        );
    }
}

#[test]
fn spilled_joins_match_with_duplicate_and_null_keys() {
    let _lock = BUDGET_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_ws, mut s) = setup("dups", "hj");
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        // Many partners per key; NULL keys match nothing.
        assert_spill_agrees(
            &mut s,
            &format!(
                "SELECT a.id, a.k, b.id, b.name FROM a {} b ON a.k = b.k WHERE a.id < 90 OR a.id IS NULL",
                join
            ),
            None,
            BUDGETS,
        );
        // A key match that fails the rest of the ON clause is no match.
        assert_spill_agrees(
            &mut s,
            &format!(
                "SELECT a.id, b.id FROM a {} b ON a.id = b.id AND a.k < b.k",
                join
            ),
            None,
            BUDGETS,
        );
    }
}

#[test]
fn spilled_join_on_one_hot_key_matches() {
    let _lock = BUDGET_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_ws, mut s) = setup("hot", "hj");
    // 400 build rows share the key of 18 probe rows: no partitioning can
    // split them.
    assert_spill_agrees(
        &mut s,
        "SELECT hot.id, a.id FROM hot JOIN a ON hot.k = a.k",
        Some(400 * 18),
        BUDGETS,
    );
    assert_spill_agrees(
        &mut s,
        "SELECT hot.id, a.id FROM hot FULL JOIN a ON hot.k = a.k",
        Some(400 * 18 + 600 - 18),
        BUDGETS,
    );
}

#[test]
fn spilled_join_keeps_derived_column_values() {
    let _lock = BUDGET_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (_ws, mut s) = setup("derived", "hj");
    // `id || name` is declared INT and carries text; the CASE is declared
    // BIGINT and carries an INT in its ELSE rows. Read back by declared
    // type, the spilled rows would differ from the in-memory ones.
    let declared = rows(
        &mut s,
        "SELECT id || name AS cat, CASE WHEN id < 100 THEN big ELSE id END AS pick FROM a WHERE id = 250",
    );
    assert_eq!(
        declared,
        vec![vec![
            Some(DataValue::Varchar("250a_250".to_string())),
            Some(DataValue::Int(250)),
        ]]
    );
    assert_spill_agrees(
        &mut s,
        "SELECT * FROM (SELECT id, id || name AS cat, CASE WHEN id < 100 THEN big ELSE id END AS pick \
         FROM a) x JOIN b ON x.id = b.id",
        Some(300),
        BUDGETS,
    );
    // Aggregates and arithmetic over mixed types.
    assert_spill_agrees(
        &mut s,
        "SELECT * FROM (SELECT id, SUM(big) AS total, COUNT(*) AS n, AVG(score) AS mean, \
         MAX(name) AS top FROM a GROUP BY id) x JOIN b ON x.id = b.id",
        Some(300),
        BUDGETS,
    );
    assert_spill_agrees(
        &mut s,
        "SELECT * FROM (SELECT id, big * 2 AS twice, score / 4 AS part, id + big AS mixed \
         FROM a) x JOIN b ON x.id = b.id",
        Some(300),
        BUDGETS,
    );
}
