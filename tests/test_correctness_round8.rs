//! Round-8 scratch verification tests for correctness issues found in an
//! eighth manual deep-dive (physical-operator key semantics, three-valued
//! logic, scalar-function NULL contracts, aggregate exactness, temporal
//! ceiling, FK rewrite, bulk-load validation). Diagnostic probes like the
//! round-1..7 suites — each asserts the SQL-correct behaviour so a failure
//! pinpoints the defect.
//!
//! Findings under test:
//!   A. `HashJoinOperator::make_hash_key` hashes `Vec<DataValue>` with derived
//!      `Hash/Eq`, so cross-type integer equality (`Int(1)` vs `BigInt(1)`)
//!      never matches, while every other comparator widens. Same logical
//!      query returns different answers depending on join operator choice.
//!   B. Non-correlated `IN` (`Predicate::InSubqueryResult`, positive branch)
//!      ignores NULLs in the value list: `2 IN (1, NULL)` returns FALSE
//!      instead of UNKNOWN. The negated branch and the correlated path both
//!      handle it correctly. Distinct from round-6 finding T (correlated).
//!   C. `Predicate::Like` evaluates `like_match(s.trim(), ...)` — strips
//!      leading AND trailing whitespace from `VARCHAR` (significant) and
//!      leading whitespace from `CHAR` (significant).
//!   D. `POSITION` / `SUBSTRING` / `ROUND` / `EXTRACT` turn SQL NULL into a
//!      query-wide error (`flatten().ok_or("... requires a non-NULL ...")`)
//!      instead of returning NULL. Siblings (`UPPER`/`LOWER`/`LENGTH`/`TRIM`/
//!      `ABS`/`MOD`/`FLOOR`/`CEIL`) correctly do `None => Ok(None)`.
//!      Distinct from round-3 H (byte-vs-char) and round-5 R (neg len panic).
//!   E. `SUM` silently saturates `i128 -> i64` to `MAX`/`MIN`, coerces all
//!      `NUMERIC` through `f64`, and drops `sum_int` entirely once any float
//!      appears (`AVG` inherits all three). Contradicts the engine's own
//!      `checked_*` overflow-error contract. Distinct from F (per-row arith)
//!      and Q (`ABS`).
//!   F. `functions::round(int, places)` returns integers unchanged, so
//!      `ROUND(123, -1)` yields `123` instead of SQL `120`. Negative places
//!      round to tens/hundreds/thousands.
//!   G. `date_trunc_ceil(TIMESTAMP, YEAR/MONTH)` advances by fixed
//!      `Duration::days(365)/days(31)` and `is_already` ignores sub-second
//!      nanos. The `DATE` branch in the same function does calendar-correct
//!      `from_ymd_opt(year+1,1,1)` / `month+1`.
//!   H. `ON UPDATE CASCADE` (and `SET NULL`) growing-`VARCHAR` rewrite marks
//!      the slot `DELETED` and drops the replacement instead of relocating
//!      via `insert_raw_tuple` (as `update.rs` does). Row vanishes from heap
//!      after its index entry was already removed. Distinct from round-7 X
//!      (quiesce) — this fires even with a cold cache.
//!   I. Bulk `LOAD CSV` never calls `validate_row_insert`, never maps
//!      `NULL`/`""` to `None`, and splits on naive `row.split(',')`. `NOT
//!      NULL`/`UNIQUE`/`FK`/`CHECK` bypassed, `'NULL'` stored as literal,
//!      `"a,b"` fields split into extra columns. The single-row path in the
//!      same file does all three correctly.

mod common;

use common::load_catalog;

use rook_parser::parse_set_clause;
use storage_manager::backend::error::RookResult;
use storage_manager::backend::executor::physical::expr::{Expr, Predicate, evaluate_predicate};
use storage_manager::backend::executor::physical::operators::{
    AggregateFunction, AggregateInfo, HashJoinOperator, PerGroupState, PhysicalOperator,
};
use storage_manager::backend::executor::physical::tuple::{ColumnInfo, Tuple};
use storage_manager::backend::system_table::insert_constraint_metadata;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::load_csv::{insert_single_tuple, load_csv};
use storage_manager::executor::update::update_by_pointers;
use storage_manager::planner::plan_query;
use storage_manager::types::comparison::compare_nullable;
use storage_manager::types::datatype::DataType;
use storage_manager::types::functions::{DatePart, date_trunc_ceil, round};
use storage_manager::types::row::serialize_nullable_typed_row;
use storage_manager::types::value::{DataValue, NumericValue, OrderedF64};

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr8", tag))
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

fn ci(name: &str, dt: DataType) -> ColumnInfo {
    ColumnInfo {
        merged: false,
        name: name.to_string(),
        data_type: dt,
        table: None,
    }
}

fn run_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Vec<Vec<String>> {
    let plan = rook_parser::parse_sql(sql).expect("parse failed");
    let logical = plan_query(&plan, catalog, db).expect("plan failed");
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .expect("execution failed");
    tuples
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
        .collect()
}

// ── Finding A: HashJoin misses cross-type integer equality ─────────────────
//
// `make_hash_key` (joins.rs:490) keeps the original `DataValue` variant and
// the table is `HashMap<Vec<DataValue>>` (joins.rs:264) with derived
// `Hash/Eq` (value.rs:127), so `Int(1)` and `BigInt(1)` hash differently.
// `compare_nullable` (comparison.rs:96-102) widens and reports Equal — the
// operator used by NLJ/filter/MIN/MAX. The planner picks HashJoin for any
// INNER equi-conjunct without a type check.

struct MockSide {
    tuples: Vec<Tuple>,
    schema: Vec<ColumnInfo>,
    pos: usize,
}

impl PhysicalOperator for MockSide {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.pos < self.tuples.len() {
            let t = self.tuples[self.pos].clone();
            self.pos += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        while batch.len() < 1024 && self.pos < self.tuples.len() {
            batch.push(self.tuples[self.pos].clone());
            self.pos += 1;
        }
        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn name(&self) -> &'static str {
        "MockSide"
    }
}

#[test]
fn a_compare_considers_int_bigint_equal() {
    // Control: documents the widening semantics every non-hash comparator uses.
    let ord = compare_nullable(Some(&DataValue::Int(1)), Some(&DataValue::BigInt(1))).unwrap();
    assert_eq!(ord, Some(std::cmp::Ordering::Equal));
}

#[test]
fn a_hashjoin_cross_type_int_bigint_matches() {
    let build = MockSide {
        tuples: vec![Tuple::new(vec![Some(DataValue::Int(1))])],
        schema: vec![ci("v", DataType::Int)],
        pos: 0,
    };
    let probe = MockSide {
        tuples: vec![Tuple::new(vec![Some(DataValue::BigInt(1))])],
        schema: vec![ci("v", DataType::BigInt)],
        pos: 0,
    };
    let key = Expr::Column {
        table: None,
        column: "v".into(),
    };
    let mut op = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![key.clone()],
        vec![key],
        None,
    );
    let mut out = Vec::new();
    while let Some(t) = op.next().expect("hash join next") {
        out.push(t);
    }
    assert_eq!(
        out.len(),
        1,
        "BUG A CONFIRMED: HashJoin missed Int(1) = BigInt(1) (got {} rows, want 1) — \
         derived Hash/Eq is variant-sensitive while compare_nullable widens",
        out.len()
    );
}

// ── Finding B: non-correlated IN ignores NULL in list ──────────────────────
//
// `InSubqueryResult` positive branch (predicate.rs:310-312) returns
// `Some(found)` without the `has_nulls` check the negated branch (:303-305)
// and the correlated path (:394-400, `saw_null`) both perform. SQL-99:
// `x IN (a, NULL)` with no match is UNKNOWN, not FALSE.

#[test]
fn b_in_positive_with_null_in_list_is_unknown() {
    let schema = vec![ci("x", DataType::Int)];
    let tuple = Tuple::new(vec![Some(DataValue::Int(2))]);
    let pred = Predicate::InSubqueryResult(
        Expr::Column {
            table: None,
            column: "x".into(),
        },
        vec![Some(DataValue::Int(1)), None],
        false,
    );
    let r = evaluate_predicate(&pred, &tuple, &schema).unwrap();
    assert_eq!(
        r, None,
        "BUG B CONFIRMED: 2 IN (1, NULL) returned {:?}; SQL three-valued logic requires UNKNOWN (None)",
        r
    );
}

/// Control: the negated branch already implements the NULL check.
#[test]
fn control_b_not_in_with_null_in_list_is_unknown() {
    let schema = vec![ci("x", DataType::Int)];
    let tuple = Tuple::new(vec![Some(DataValue::Int(2))]);
    let pred = Predicate::InSubqueryResult(
        Expr::Column {
            table: None,
            column: "x".into(),
        },
        vec![Some(DataValue::Int(1)), None],
        true,
    );
    let r = evaluate_predicate(&pred, &tuple, &schema).unwrap();
    assert_eq!(
        r, None,
        "control: 2 NOT IN (1, NULL) must be UNKNOWN; got {:?}",
        r
    );
}

// ── Finding C: LIKE trims significant spaces ───────────────────────────────
//
// predicate.rs:272 `like_match(s.trim(), ...)`. `VARCHAR` spaces are
// significant; `CHAR` ignores only trailing padding.

#[test]
fn c_like_exact_spaces_match() {
    let schema = vec![ci("v", DataType::Varchar(20))];
    let tuple = Tuple::new(vec![Some(DataValue::Varchar("  hello  ".to_string()))]);
    let pred = Predicate::Like(
        Expr::Column {
            table: None,
            column: "v".into(),
        },
        "  hello  ".to_string(),
        None,
    );
    let r = evaluate_predicate(&pred, &tuple, &schema).unwrap();
    assert_eq!(
        r,
        Some(true),
        "BUG C CONFIRMED: '  hello  ' LIKE '  hello  ' returned {:?} — s.trim() destroyed significant spaces",
        r
    );
}

#[test]
fn c_like_trimmed_pattern_no_false_positive() {
    let schema = vec![ci("v", DataType::Varchar(20))];
    let tuple = Tuple::new(vec![Some(DataValue::Varchar("  hello  ".to_string()))]);
    let pred = Predicate::Like(
        Expr::Column {
            table: None,
            column: "v".into(),
        },
        "hello".to_string(),
        None,
    );
    let r = evaluate_predicate(&pred, &tuple, &schema).unwrap();
    assert_eq!(
        r,
        Some(false),
        "BUG C CONFIRMED: '  hello  ' LIKE 'hello' returned {:?} — trimmed value falsely matches",
        r
    );
}

// ── Finding D: scalar functions error on NULL instead of NULL ──────────────
//
// expr/mod.rs:270-273 (POSITION), :448-449 (SUBSTRING), :494-495 (ROUND),
// :521-522 (EXTRACT) use `flatten().ok_or("... non-NULL ...")`, aborting the
// whole query. UPPER/LOWER/LENGTH/TRIM/ABS/MOD/FLOOR/CEIL in the same file
// correctly return `None => Ok(None)`.

#[test]
fn d_position_null_returns_null() {
    let tuple = Tuple::new(vec![]);
    let schema: Vec<ColumnInfo> = vec![];
    for args in [
        vec![
            Expr::Null,
            Expr::Constant(DataValue::Varchar("abc".to_string())),
        ],
        vec![
            Expr::Constant(DataValue::Varchar("b".to_string())),
            Expr::Null,
        ],
    ] {
        let r = Expr::Function {
            name: "POSITION".to_string(),
            args,
        }
        .evaluate(&tuple, &schema);
        assert!(
            matches!(r, Ok(None)),
            "BUG D CONFIRMED: POSITION with a NULL argument returned {:?} instead of NULL",
            r
        );
    }
}

#[test]
fn d_substring_null_returns_null() {
    let tuple = Tuple::new(vec![]);
    let schema: Vec<ColumnInfo> = vec![];
    let r = Expr::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            Expr::Null,
            Expr::Constant(DataValue::Int(1)),
            Expr::Constant(DataValue::Int(1)),
        ],
    }
    .evaluate(&tuple, &schema);
    assert!(
        matches!(r, Ok(None)),
        "BUG D CONFIRMED: SUBSTRING(NULL, 1, 1) returned {:?} instead of NULL",
        r
    );
}

#[test]
fn d_round_null_returns_null() {
    let tuple = Tuple::new(vec![]);
    let schema: Vec<ColumnInfo> = vec![];
    let r = Expr::Function {
        name: "ROUND".to_string(),
        args: vec![Expr::Null, Expr::Constant(DataValue::Int(1))],
    }
    .evaluate(&tuple, &schema);
    assert!(
        matches!(r, Ok(None)),
        "BUG D CONFIRMED: ROUND(NULL, 1) returned {:?} instead of NULL",
        r
    );
}

#[test]
fn d_extract_null_returns_null() {
    let tuple = Tuple::new(vec![]);
    let schema: Vec<ColumnInfo> = vec![];
    let r = Expr::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            Expr::Constant(DataValue::Varchar("YEAR".to_string())),
            Expr::Null,
        ],
    }
    .evaluate(&tuple, &schema);
    assert!(
        matches!(r, Ok(None)),
        "BUG D CONFIRMED: EXTRACT(YEAR FROM NULL) returned {:?} instead of NULL",
        r
    );
}

// ── Finding E: SUM/AVG exactness + silent saturation ────────────────────────
//
// aggregate.rs:98-102 (NUMERIC forced through f64), :150-157 (i128 saturates
// to i64::MAX/MIN; mixed groups drop sum_int), :161 (AVG same). The engine's
// own `arithmetic_op` uses checked_* and returns overflow errors.

/// A SUM declared `output_type`, as `infer_aggregate_output_type` would
/// declare it for the inputs each test feeds in.
fn sum_info(output_type: DataType) -> AggregateInfo {
    AggregateInfo {
        function: AggregateFunction::Sum,
        input: None,
        output_name: "s".into(),
        output_type,
        distinct: false,
    }
}

#[test]
fn e_sum_overflow_must_not_silently_saturate() {
    let mut st = PerGroupState::new();
    st.update(
        AggregateFunction::Sum,
        Some(&DataValue::BigInt(i64::MAX)),
        false,
    )
    .unwrap();
    st.update(AggregateFunction::Sum, Some(&DataValue::BigInt(10)), false)
        .unwrap();
    // SUM over BIGINT is declared NUMERIC, which holds the exact total.
    let numeric = DataType::Numeric {
        precision: 38,
        scale: 0,
    };
    assert_eq!(
        st.finalize(&sum_info(numeric)).unwrap(),
        Some(DataValue::Numeric(NumericValue {
            unscaled: i64::MAX as i128 + 10,
            scale: 0
        })),
        "BUG E CONFIRMED: SUM(i64::MAX, 10) is not the exact total"
    );
    // Declared BIGINT, the same total is an error: never a clamped value,
    // and never a NUMERIC in a BIGINT column.
    assert!(
        st.finalize(&sum_info(DataType::BigInt)).is_err(),
        "BUG E CONFIRMED: an out-of-range SUM declared BIGINT must error"
    );
}

#[test]
fn e_sum_numeric_stays_exact() {
    let mut st = PerGroupState::new();
    st.update(
        AggregateFunction::Sum,
        Some(&DataValue::Numeric(NumericValue {
            unscaled: 10,
            scale: 2,
        })),
        false,
    )
    .unwrap();
    st.update(
        AggregateFunction::Sum,
        Some(&DataValue::Numeric(NumericValue {
            unscaled: 20,
            scale: 2,
        })),
        false,
    )
    .unwrap();
    let out = st
        .finalize(&sum_info(DataType::Numeric {
            precision: 38,
            scale: 2,
        }))
        .unwrap();
    assert_eq!(
        out,
        Some(DataValue::Numeric(NumericValue {
            unscaled: 30,
            scale: 2
        })),
        "BUG E CONFIRMED: SUM(0.10, 0.20 NUMERIC) returned {:?} — exact decimal went through binary f64",
        out
    );
}

#[test]
fn e_sum_mixed_int_and_float_keeps_both() {
    let mut st = PerGroupState::new();
    st.update(AggregateFunction::Sum, Some(&DataValue::Int(100)), false)
        .unwrap();
    st.update(
        AggregateFunction::Sum,
        Some(&DataValue::DoublePrecision(OrderedF64(0.5))),
        false,
    )
    .unwrap();
    let out = st.finalize(&sum_info(DataType::DoublePrecision)).unwrap();
    assert_eq!(
        out,
        Some(DataValue::DoublePrecision(OrderedF64(100.5))),
        "BUG E CONFIRMED: SUM(100 INT, 0.5 DOUBLE) returned {:?} — sum_int dropped once sum_is_float",
        out
    );
}

// ── Finding F: ROUND(int, negative places) is a no-op ──────────────────────
//
// functions.rs:340 returns integers unchanged. SQL rounds to tens/hundreds.

#[test]
fn f_round_int_negative_places() {
    assert_eq!(
        round(&DataValue::Int(123), -1).unwrap(),
        DataValue::Int(120),
        "BUG F CONFIRMED: ROUND(123, -1) did not round to tens"
    );
    assert_eq!(
        round(&DataValue::Int(149), -2).unwrap(),
        DataValue::Int(100),
        "BUG F CONFIRMED: ROUND(149, -2) did not round to hundreds"
    );
    assert_eq!(
        round(&DataValue::BigInt(199), -2).unwrap(),
        DataValue::BigInt(200),
        "BUG F CONFIRMED: ROUND(199::BIGINT, -2) did not round to hundreds"
    );
}

/// Control: non-negative places leave integers alone.
#[test]
fn control_f_round_int_non_negative_places_unchanged() {
    assert_eq!(round(&DataValue::Int(123), 0).unwrap(), DataValue::Int(123));
    assert_eq!(round(&DataValue::Int(123), 2).unwrap(), DataValue::Int(123));
}

// ── Finding G: date_trunc_ceil(TIMESTAMP) calendar + sub-second bugs ───────
//
// functions.rs:518-519 advances YEAR/MONTH by fixed 365/31 days (the DATE
// branch :484-492 does calendar-correct month+1/year+1), and :504-509
// `is_already` ignores nanos so `00:00:00.5` counts as a boundary.

#[test]
fn g_ceil_timestamp_month_uses_calendar() {
    use chrono::NaiveDate;
    let ts = NaiveDate::from_ymd_opt(2024, 2, 15)
        .unwrap()
        .and_hms_opt(10, 0, 0)
        .unwrap();
    let r = date_trunc_ceil(&DataValue::Timestamp(ts), DatePart::Month).unwrap();
    let want = DataValue::Timestamp(
        NaiveDate::from_ymd_opt(2024, 3, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    assert_eq!(
        r, want,
        "BUG G CONFIRMED: CEIL(2024-02-15 10:00 TO MONTH) gave {} (floor +31d lands 2024-03-03)",
        r
    );
}

#[test]
fn g_ceil_timestamp_year_handles_leap_year() {
    use chrono::NaiveDate;
    let ts = NaiveDate::from_ymd_opt(2024, 6, 15)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let r = date_trunc_ceil(&DataValue::Timestamp(ts), DatePart::Year).unwrap();
    let want = DataValue::Timestamp(
        NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    assert_eq!(
        r, want,
        "BUG G CONFIRMED: CEIL(2024-06-15 TO YEAR) gave {} (floor +365d lands 2024-12-31 in a leap year)",
        r
    );
}

#[test]
fn g_ceil_timestamp_subsecond_is_not_a_boundary() {
    use chrono::NaiveDate;
    let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_micro_opt(0, 0, 0, 500_000)
        .unwrap();
    let r = date_trunc_ceil(&DataValue::Timestamp(ts), DatePart::Day).unwrap();
    let want = DataValue::Timestamp(
        NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    assert_eq!(
        r, want,
        "BUG G CONFIRMED: CEIL(2024-01-01 00:00:00.5 TO DAY) gave {} (nanos ignored in is_already)",
        r
    );
}

// ── Finding H: ON UPDATE CASCADE deletes growing VARCHAR rows ─────────────
//
// fk_actions.rs:593-601: when the re-serialised child tuple is longer than
// the old slot, the slot is flagged DELETED and the replacement is dropped
// instead of relocating via `insert_raw_tuple` (as update.rs does). The index
// entry was already removed, so the row vanishes entirely. The pre-quiesce
// below isolates this from the round-7 finding-X stale-read window.

#[test]
fn h_update_cascade_growing_varchar_preserves_row() {
    let _ws = TestWorkspace::new("h_cascade");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db8"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db8",
        "p",
        vec![col("k", DataType::Varchar(50), false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db8",
        "c",
        vec![col("v", DataType::Varchar(50), true)],
    );
    save_catalog(&catalog).unwrap();

    insert_constraint_metadata(
        &common::engine(),
        "db8",
        "c",
        "FOREIGN KEY ON UPDATE CASCADE",
        "v",
        Some("p"),
        Some("k"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db8", "p", &["'a'"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db8", "c", &["'a'"]).unwrap());

    // Sanity: the grown serialisation really does exceed the old slot, i.e.
    // the cascade takes the `new_len > old_len` branch at fk_actions.rs:593.
    let schema = vec![DataType::Varchar(50)];
    let old_len =
        serialize_nullable_typed_row(&schema, &[Some(DataValue::Varchar("a".to_string()))])
            .unwrap()
            .len();
    let new_val = "abcdefghijklmnopqrstuvwxyzABCD";
    let new_len =
        serialize_nullable_typed_row(&schema, &[Some(DataValue::Varchar(new_val.to_string()))])
            .unwrap()
            .len();
    assert!(
        new_len > old_len,
        "test setup: grown value must exceed old slot"
    );

    let assignments = parse_set_clause(&format!("k = '{}'", new_val)).expect("parse set");
    let res = update_by_pointers(&catalog, "db8", "p", &[(1, 0)], &assignments)
        .expect("parent update failed");
    assert_eq!(res.updated_count, 1, "parent row must be updated");

    let rows = run_select(&catalog, "db8", "SELECT v FROM c");
    assert_eq!(
        rows,
        vec![vec![format!("'{}'", new_val)]],
        "BUG H CONFIRMED: ON UPDATE CASCADE to a longer VARCHAR lost the child row (got {:?}) — \
         fk_actions.rs marks the slot DELETED instead of relocating",
        rows
    );
}

// ── Finding I: bulk LOAD CSV bypasses constraints + NULL + quoting ────────
//
// load_csv.rs:128 naive `split(',')`, :190-191 always `Some(*v)` (never NULL),
// and no `validate_row_insert` anywhere in the bulk loop (:141-226). The
// single-row path in the same file validates constraints (:297-302) and maps
// NULL/empty to None (:335-343).

#[test]
fn i_bulk_load_maps_null_literal_to_null() {
    let _ws = TestWorkspace::new("i_null");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db8"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db8",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(50), true),
        ],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    let csv = common::path("i_null.csv");
    std::fs::write(&csv, "1,NULL\n").expect("write csv");
    let n = load_csv(&catalog, "db8", "t", csv.to_str().unwrap()).expect("load csv");
    assert_eq!(n, 1, "one row must load");

    let rows = run_select(&catalog, "db8", "SELECT s FROM t WHERE s IS NULL");
    assert_eq!(
        rows.len(),
        1,
        "BUG I CONFIRMED: bulk LOAD stored literal 'NULL' instead of NULL \
         (IS NULL finds {} rows; SELECT s gives {:?})",
        rows.len(),
        run_select(&catalog, "db8", "SELECT s FROM t")
    );
}

#[test]
fn i_bulk_load_enforces_unique() {
    let _ws = TestWorkspace::new("i_unique");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db8"), "create db");
    let mut catalog = load_catalog();
    let mut id_col = col("id", DataType::Int, false);
    id_col.constraints.unique = true;
    create_table(
        &mut catalog,
        "db8",
        "u",
        vec![id_col, col("v", DataType::Int, true)],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();
    create_index(&catalog, "db8", "u", "idx_u_id", &["id".to_string()]).expect("create index");
    assert!(insert_single_tuple(&catalog, "db8", "u", &["1", "10"]).unwrap());

    let csv = common::path("i_dup.csv");
    std::fs::write(&csv, "1,20\n").expect("write csv");
    load_csv(&catalog, "db8", "u", csv.to_str().unwrap()).expect("load csv");

    let rows = run_select(&catalog, "db8", "SELECT id FROM u");
    assert_eq!(
        rows.len(),
        1,
        "BUG I CONFIRMED: bulk LOAD bypassed UNIQUE (validate_row_insert never called) — \
         duplicate id=1 inserted, heap has {:?}",
        rows
    );
}

#[test]
fn i_bulk_load_handles_quoted_comma() {
    let _ws = TestWorkspace::new("i_quote");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db8"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db8",
        "q",
        vec![
            col("a", DataType::Varchar(50), true),
            col("b", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    let csv = common::path("i_q.csv");
    std::fs::write(&csv, "\"a,b\",1\n").expect("write csv");
    load_csv(&catalog, "db8", "q", csv.to_str().unwrap()).expect("load csv");

    let rows = run_select(&catalog, "db8", "SELECT a, b FROM q");
    assert_eq!(
        rows.len(),
        1,
        "BUG I CONFIRMED: naive split(',') broke the quoted field — heap has {:?} (want 1 row 'a,b',1)",
        rows
    );
}
