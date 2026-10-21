//! Round-11 scratch verification tests for correctness issues found in an
//! eleventh manual deep-dive (hash-join key canonicalization, aggregate
//! overflow). Diagnostic probes like the round-1..10 suites — each asserts
//! the SQL-correct behaviour so a failure pinpoints the defect.
//!
//! Findings under test:
//!
//!   K. Cross-type equi-joins silently lose ALL rows. `DataValue` derives
//!      variant-based PartialEq/Eq/Hash (value.rs:127), so `Real(1.5) !=
//!      DoublePrecision(1.5)` and `Numeric{4200,2} != Int(42)` as hash keys.
//!      The physical planner picks HashJoinOperator for every INNER join
//!      with a cross-side equality conjunct (planner/joins.rs step 2) and
//!      `extract_equi_join_keys` performs NO type checking, while
//!      `HashJoinOperator::canonicalize_hash_key` (operators/joins.rs) only
//!      promotes SmallInt/Int → BigInt and Char → Varchar. Every other
//!      cross-type family — REAL ↔ DOUBLE PRECISION, NUMERIC/DECIMAL ↔
//!      INTEGER, REAL ↔ INTEGER, DATE ↔ TIMESTAMP — hashes to different
//!      keys, so the join returns zero rows. The nested-loop join path
//!      (used for LEFT/RIGHT/FULL) compares via `Comparable`
//!      (comparison.rs), which supports all of these — same data, same
//!      predicate, different answer depending on join algorithm.
//!
//!   L. `SUM(BIGINT)` overflow silently returns NULL. `PerGroupState`
//!      accumulates in i128 but `finalize` narrows with
//!      `i64::try_from(self.sum_int).ok()?` (aggregate.rs), so
//!      `SUM(b)` over [i64::MAX, 1] yields NULL instead of an error —
//!      contradicting the engine's checked-overflow contract elsewhere
//!      (arithmetic_op, ROUND, ABS all raise errors on overflow).
//!
//!   M. Integer literals beyond i32 range are silently truncated in
//!      predicates. `constant_from_ast` (expr/convert.rs:229) converts
//!      `ConstantValue::Int(i)` (i64) with `DataValue::Int(*i as i32)` —
//!      a wrapping cast. `WHERE b = 9223372036854775807` compares against
//!      `Int(-1)` (the i32 wrap of i64::MAX) and matches 0 rows even when
//!      the row exists. The index-scan planner's own
//!      `ast_constant_to_data_value` (index_scan.rs:531) does this
//!      correctly (promotes to BigInt when out of i32 range, with a unit
//!      test asserting exactly that), so the two constant paths disagree.
//!
//! Controls (expected to PASS) document the NLJ / same-type / in-range
//! behaviour for each scenario so the divergences cannot be explained away
//! as engine limits.

mod common;

use common::load_catalog;

use storage_manager::backend::executor::load_csv::insert_single_tuple;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr11", tag))
    }
}

fn col(name: &str, ty: DataType, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable,
        constraints: Constraints::default(),
    }
}

/// Create one table and persist the catalog.
fn make_table(db: &str, table: &str, columns: Vec<Column>) {
    let mut catalog = load_catalog();
    create_table(&mut catalog, db, table, columns).unwrap();
    save_catalog(&catalog).unwrap();
}

fn insert(db: &str, table: &str, values: &[&str]) {
    let catalog = load_catalog();
    assert!(
        insert_single_tuple(&catalog, db, table, values).unwrap(),
        "insert into {} failed: {:?}",
        table,
        values
    );
}

/// Run a SELECT through the full parse → plan → execute pipeline,
/// propagating execution errors instead of panicking on them.
fn try_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let plan = rook_parser::parse_sql(sql).map_err(|e| e.to_string())?;
    let logical = plan_query(&plan, catalog, db).map_err(|e| e.to_string())?;
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .map_err(|e| e.to_string())?;
    Ok(tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|d| format!("{}", d))
                        .unwrap_or_else(|| "NULL".into())
                })
                .collect()
        })
        .collect())
}

// ── Finding K: cross-type equi-joins via HashJoin lose all rows ──────────────

#[test]
fn k1_hash_join_real_vs_double_inner_join_loses_all_rows() {
    let _ws = TestWorkspace::new("k1_real_double");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11k1"), "create db");
    make_table("db11k1", "reals", vec![col("r", DataType::Real, true)]);
    make_table(
        "db11k1",
        "doubs",
        vec![col("d", DataType::DoublePrecision, true)],
    );
    insert("db11k1", "reals", &["1.5"]);
    insert("db11k1", "reals", &["2.0"]);
    insert("db11k1", "doubs", &["1.5"]);
    let catalog = load_catalog();

    // Control: LEFT JOIN forces the nested-loop path, whose Comparable
    // cross-type promotion matches Real(1.5) = DoublePrecision(1.5):
    // 2 rows total, exactly 1 with a non-NULL d.
    let control = try_select(
        &catalog,
        "db11k1",
        "SELECT COUNT(doubs.d) FROM reals LEFT JOIN doubs ON reals.r = doubs.d",
    )
    .expect("LEFT JOIN control must execute");
    assert_eq!(
        control,
        vec![vec!["1"]],
        "control: NLJ must match Real(1.5) = DoublePrecision(1.5)"
    );

    // The INNER join must return exactly the matching pair.
    let result = try_select(
        &catalog,
        "db11k1",
        "SELECT COUNT(*) FROM reals INNER JOIN doubs ON reals.r = doubs.d",
    )
    .expect("INNER JOIN must execute");

    assert_eq!(
        result,
        vec![vec!["1"]],
        "BUG K1 CONFIRMED: INNER JOIN on REAL = DOUBLE PRECISION returned \
         {:?} — the HashJoin key canonicalizer does not promote Real to \
         DoublePrecision, and the derived variant-based Eq/Hash treats \
         Real(1.5) and DoublePrecision(1.5) as different hash keys, so \
         every matching pair is silently dropped",
        result
    );
}

#[test]
fn k2_hash_join_numeric_vs_int_inner_join_loses_all_rows() {
    let _ws = TestWorkspace::new("k2_numeric_int");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11k2"), "create db");
    make_table(
        "db11k2",
        "nums",
        vec![col(
            "n",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );
    make_table("db11k2", "ints", vec![col("i", DataType::Int, true)]);
    insert("db11k2", "nums", &["42.00"]);
    insert("db11k2", "ints", &["42"]);
    let catalog = load_catalog();

    // Control: LEFT JOIN (NLJ) matches Numeric(42.00) = Int(42).
    let control = try_select(
        &catalog,
        "db11k2",
        "SELECT COUNT(ints.i) FROM nums LEFT JOIN ints ON nums.n = ints.i",
    )
    .expect("LEFT JOIN control must execute");
    assert_eq!(
        control,
        vec![vec!["1"]],
        "control: NLJ must match Numeric(42.00) = Int(42)"
    );

    let result = try_select(
        &catalog,
        "db11k2",
        "SELECT COUNT(*) FROM nums INNER JOIN ints ON nums.n = ints.i",
    )
    .expect("INNER JOIN must execute");

    assert_eq!(
        result,
        vec![vec!["1"]],
        "BUG K2 CONFIRMED: INNER JOIN on NUMERIC = INT returned {:?} — \
         Numeric keys are not canonicalized to a shared variant with Int, \
         so the hash join finds no matches although the SQL comparison \
         semantics (Comparable) consider the values equal",
        result
    );
}

#[test]
fn k3_hash_join_date_vs_timestamp_inner_join_loses_all_rows() {
    let _ws = TestWorkspace::new("k3_date_ts");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11k3"), "create db");
    make_table("db11k3", "dates", vec![col("d", DataType::Date, true)]);
    make_table(
        "db11k3",
        "stamps",
        vec![col("ts", DataType::Timestamp, true)],
    );
    insert("db11k3", "dates", &["2024-01-01"]);
    insert("db11k3", "stamps", &["2024-01-01 00:00:00"]);
    let catalog = load_catalog();

    // Control: LEFT JOIN (NLJ) matches Date(2024-01-01) = midnight Timestamp.
    let control = try_select(
        &catalog,
        "db11k3",
        "SELECT COUNT(stamps.ts) FROM dates LEFT JOIN stamps ON dates.d = stamps.ts",
    )
    .expect("LEFT JOIN control must execute");
    assert_eq!(
        control,
        vec![vec!["1"]],
        "control: NLJ must match Date = midnight Timestamp"
    );

    let result = try_select(
        &catalog,
        "db11k3",
        "SELECT COUNT(*) FROM dates INNER JOIN stamps ON dates.d = stamps.ts",
    )
    .expect("INNER JOIN must execute");

    assert_eq!(
        result,
        vec![vec!["1"]],
        "BUG K3 CONFIRMED: INNER JOIN on DATE = TIMESTAMP returned {:?} — \
         Date keys are not promoted to Timestamp midnight, so the hash \
         join silently drops the matching pair",
        result
    );
}

// ── Finding L: SUM(BIGINT) overflow silently returns NULL ────────────────────

#[test]
fn l_sum_bigint_overflow_silently_returns_null() {
    let _ws = TestWorkspace::new("l_sum_overflow");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11l"), "create db");
    make_table("db11l", "bigs", vec![col("b", DataType::BigInt, true)]);
    insert("db11l", "bigs", &["9223372036854775807"]); // i64::MAX
    insert("db11l", "bigs", &["1"]);
    let catalog = load_catalog();

    // Control: SUM over an in-range subset (avoids big literals in WHERE —
    // see finding M) returns the exact value.
    let control = try_select(&catalog, "db11l", "SELECT SUM(b) FROM bigs WHERE b = 1")
        .expect("control SUM must execute");
    assert_eq!(
        control,
        vec![vec!["1"]],
        "control: in-range SUM(BIGINT) works"
    );

    // The overflowing SUM must either error (checked-overflow contract,
    // like arithmetic_op / ROUND / ABS) or produce a widened exact value —
    // silently yielding NULL hides the overflow from the caller.
    let result = try_select(&catalog, "db11l", "SELECT SUM(b) FROM bigs");

    match result {
        Err(_) => { /* overflow rejected — acceptable per the engine's contract */ }
        Ok(rows) => assert!(
            rows.len() == 1 && rows[0][0] != "NULL",
            "BUG L CONFIRMED: SUM(b) over [i64::MAX, 1] returned {:?} — \
             finalize() narrows the i128 accumulator with \
             i64::try_from(...).ok()? and silently produces NULL instead of \
             erroring on overflow",
            rows
        ),
    }
}

// ── Finding M: large integer literals truncated to i32 in predicates ────────

#[test]
fn m_where_bigint_literal_beyond_i32_range_matches_zero_rows() {
    let _ws = TestWorkspace::new("m_bigint_literal");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11m"), "create db");
    make_table("db11m", "bigs", vec![col("b", DataType::BigInt, true)]);
    insert("db11m", "bigs", &["9223372036854775807"]); // i64::MAX
    insert("db11m", "bigs", &["3000000000"]); // fits i64, exceeds i32
    insert("db11m", "bigs", &["5"]); // in-range control value
    let catalog = load_catalog();

    // Control: the rows are visible without the literal predicate.
    let all = try_select(&catalog, "db11m", "SELECT COUNT(*) FROM bigs")
        .expect("COUNT control must execute");
    assert_eq!(all, vec![vec!["3"]], "control: all 3 rows visible");

    // A literal within i32 range must keep matching (regression guard).
    let small = try_select(&catalog, "db11m", "SELECT COUNT(*) FROM bigs WHERE b = 5")
        .expect("in-range literal query must execute");
    assert_eq!(small, vec![vec!["1"]], "guard: in-range literal matches");

    // Literals beyond i32 range must compare against the full BIGINT value.
    // 3000000000 as i32 wraps to -1294967296; i64::MAX wraps to -1.
    let big = try_select(
        &catalog,
        "db11m",
        "SELECT COUNT(*) FROM bigs WHERE b = 3000000000",
    )
    .expect("beyond-i32 literal query must execute");
    assert_eq!(
        big,
        vec![vec!["1"]],
        "BUG M CONFIRMED: WHERE b = 3000000000 returned {:?} — the literal \
         is converted with DataValue::Int(*i as i32) (wraps to Int(-1294967296)) \
         instead of being promoted to BigInt, so it matches no rows",
        big
    );

    let max = try_select(
        &catalog,
        "db11m",
        "SELECT COUNT(*) FROM bigs WHERE b = 9223372036854775807",
    )
    .expect("i64::MAX literal query must execute");
    assert_eq!(
        max,
        vec![vec!["1"]],
        "BUG M CONFIRMED: WHERE b = i64::MAX returned {:?} — the literal \
         wraps to Int(-1) and matches no rows although the row exists",
        max
    );
}

// ── Regression guards ────────────────────────────────────────────────────────

#[test]
fn x1_hash_join_promoted_types_still_match() {
    let _ws = TestWorkspace::new("x1_int_bigint");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11x1"), "create db");
    make_table("db11x1", "ints", vec![col("i", DataType::Int, true)]);
    make_table("db11x1", "bigs", vec![col("b", DataType::BigInt, true)]);
    insert("db11x1", "ints", &["7"]);
    insert("db11x1", "bigs", &["7"]);
    let catalog = load_catalog();

    // Int → BigInt IS canonicalized, so the hash join must match here.
    let result = try_select(
        &catalog,
        "db11x1",
        "SELECT COUNT(*) FROM ints INNER JOIN bigs ON ints.i = bigs.b",
    )
    .expect("INNER JOIN must execute");
    assert_eq!(
        result,
        vec![vec!["1"]],
        "guard: hash join must still match the canonicalized Int/BigInt pair"
    );
}

#[test]
fn x2_same_type_inner_join_via_hash_join_matches() {
    let _ws = TestWorkspace::new("x2_same_type");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11x2"), "create db");
    make_table("db11x2", "r1", vec![col("r", DataType::Real, true)]);
    make_table("db11x2", "r2", vec![col("r", DataType::Real, true)]);
    insert("db11x2", "r1", &["1.5"]);
    insert("db11x2", "r2", &["1.5"]);
    let catalog = load_catalog();

    let result = try_select(
        &catalog,
        "db11x2",
        "SELECT COUNT(*) FROM r1 INNER JOIN r2 ON r1.r = r2.r",
    )
    .expect("INNER JOIN must execute");
    assert_eq!(
        result,
        vec![vec!["1"]],
        "guard: same-type REAL = REAL hash join matches"
    );
}

#[test]
fn x3_sum_bigint_within_range_still_exact() {
    let _ws = TestWorkspace::new("x3_sum_range");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db11x3"), "create db");
    make_table("db11x3", "bigs", vec![col("b", DataType::BigInt, true)]);
    insert("db11x3", "bigs", &["100"]);
    insert("db11x3", "bigs", &["23"]);
    let catalog = load_catalog();

    let result = try_select(&catalog, "db11x3", "SELECT SUM(b) FROM bigs")
        .expect("in-range SUM must execute");
    assert_eq!(result, vec![vec!["123"]], "guard: normal SUM(BIGINT) exact");
}
