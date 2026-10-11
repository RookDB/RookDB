//! Round-2 scratch verification tests for correctness issues found in a
//! second manual deep-dive. Like `test_deep_dive_verification.rs`, these are
//! diagnostic probes — each asserts the SQL-correct behaviour so a failure
//! pinpoints the defect.
//!
//! Findings under test:
//!   A. Global aggregates on empty input return ZERO rows instead of exactly
//!      one row (SQL standard). Only the special-cased single `COUNT(*)`
//!      fast path is correct.
//!   B. `SortOperator::next_batch` never clears the batch, violating the
//!      documented `PhysicalOperator::next_batch` contract ("Clears `batch`
//!      before populating"). Contract-abiding callers see duplicated rows.
//!   C. `SortOperator::new_adaptive` discards ALL buffered rows when the
//!      actual row count crosses the threshold after loading (output is
//!      empty). Latent: the planner currently uses `SortOperator::new`.
//!   D. `parse_set_clause` runs the arithmetic-expression matcher on quoted
//!      string literals: `SET tag = 'n-5'` is parsed as `tag = column n - 5`,
//!      which NULLs the column when no column `n` exists.
//!   E. `parse_set_clause` does not unescape doubled quotes in literals:
//!      `SET name = 'O''Brien'` stores the raw text `O''Brien`.
//!   F. Integer arithmetic overflow panics (`attempt to add with overflow`)
//!      in debug builds instead of returning a query error.

mod common;

use common::load_catalog;
use std::path::PathBuf;

use rook_ast::QueryPlan;

use rook_parser::parse_set_clause;
use storage_manager::backend::error::RookResult;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::operators::{PhysicalOperator, SortOperator};
use storage_manager::backend::executor::physical::tuple::{ColumnInfo, Tuple};
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::delete::delete_by_pointers;
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::executor::update::update_by_pointers;
use storage_manager::heap::HeapManager;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;
use storage_manager::types::row::deserialize_nullable_row;
use storage_manager::types::value::DataValue;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr2", tag))
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

/// Create db + table and leave the table with ZERO live rows (one row is
/// inserted then deleted so the heap file exists but scans produce nothing).
fn setup_empty(db: &str, table: &str, cols: Vec<Column>, tag: &str) {
    let _ws_guard_tag = tag; // workspace is created by the caller
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db");
    let mut catalog = load_catalog();
    create_table(&mut catalog, db, table, cols);
    save_catalog(&catalog).unwrap();

    // Ensure the .dat exists, then empty it.
    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, table, &["1", "10"]).unwrap());
    let sel = parse_where_text("id = 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, db, table, sel).expect("select");
    assert_eq!(ptrs.len(), 1);
    delete_by_pointers(&catalog, db, table, &ptrs).expect("delete");
}

fn setup_with_rows(
    db: &str,
    table: &str,
    cols: Vec<Column>,
    rows: &[&[&str]],
) -> storage_manager::catalog::types::Catalog {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db");
    let mut catalog = load_catalog();
    create_table(&mut catalog, db, table, cols);
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for row in rows {
        assert!(insert_single_tuple(&catalog, db, table, row).unwrap());
    }
    catalog
}

/// Run a SELECT through parser → logical planner → Volcano engine and return
/// the collected tuples.
fn run_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Vec<Tuple> {
    let select = match rook_parser::parse_sql(sql) {
        Ok(QueryPlan::Select(s)) => s,
        other => panic!("parse failed for {:?}: {:?}", sql, other.err()),
    };
    let logical = plan_query(&QueryPlan::Select(select), catalog, db).expect("plan failed");
    execute_plan_collect(&logical, catalog, db).expect("execution failed")
}

// ── Finding A: empty-input global aggregates ────────────────────────────────
//
// `AggregateOperator::consume_if_needed` only creates a group when it sees at
// least one row (except the single plain `COUNT(*)` fast path), so an empty
// input produces an EMPTY result set. SQL requires exactly one row:
//   SELECT COUNT(*), COUNT(x), SUM(x), MIN(x) ... FROM empty → 1 row
// with COUNT = 0 and the others NULL.

#[test]
fn agg_empty_input_multi_aggregate_returns_one_row() {
    let _ws = TestWorkspace::new("aggmulti");
    setup_empty(
        "testdb",
        "agg_t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
        "aggmulti",
    );
    let catalog = load_catalog();

    let tuples = run_select(&catalog, "testdb", "SELECT COUNT(id), SUM(v) FROM agg_t");
    assert_eq!(
        tuples.len(),
        1,
        "BUG A CONFIRMED: global aggregate over empty input returned {} rows (SQL says exactly 1)",
        tuples.len()
    );
    assert_eq!(
        tuples[0].values[0],
        Some(DataValue::BigInt(0)),
        "COUNT(id) over empty must be 0"
    );
    assert!(
        tuples[0].values[1].is_none(),
        "SUM(v) over empty must be NULL"
    );
}

#[test]
fn agg_empty_input_count_col_returns_one_row() {
    let _ws = TestWorkspace::new("aggcount");
    setup_empty(
        "testdb",
        "agg_c",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
        "aggcount",
    );
    let catalog = load_catalog();

    let tuples = run_select(&catalog, "testdb", "SELECT COUNT(v) FROM agg_c");
    assert_eq!(
        tuples.len(),
        1,
        "BUG A CONFIRMED: COUNT(col) over empty input returned {} rows (SQL says exactly 1)",
        tuples.len()
    );
    assert_eq!(tuples[0].values[0], Some(DataValue::BigInt(0)));
}

/// Control: the special-cased single plain `COUNT(*)` fast path already
/// returns one row over empty input. Documents the inconsistency with the
/// two tests above.
#[test]
fn agg_empty_input_count_star_returns_one_row() {
    let _ws = TestWorkspace::new("aggstar");
    setup_empty(
        "testdb",
        "agg_s",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
        "aggstar",
    );
    let catalog = load_catalog();

    let tuples = run_select(&catalog, "testdb", "SELECT COUNT(*) FROM agg_s");
    assert_eq!(
        tuples.len(),
        1,
        "COUNT(*) over empty input must return 1 row"
    );
    assert_eq!(tuples[0].values[0], Some(DataValue::BigInt(0)));
}

// ── Finding B: SortOperator::next_batch does not clear the batch ────────────
//
// The `PhysicalOperator::next_batch` doc contract states "Clears `batch`
// before populating", and every other operator honours it. SortOperator's
// in-memory path appends without clearing, so a caller that reuses the batch
// (trusting the contract) reads duplicated rows.

struct MockSource {
    tuples: Vec<Tuple>,
    schema: Vec<ColumnInfo>,
    pos: usize,
    estimate: usize,
}

impl PhysicalOperator for MockSource {
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
        while batch.len()
            < storage_manager::backend::executor::physical::operators::DEFAULT_BATCH_SIZE
            && self.pos < self.tuples.len()
        {
            batch.push(self.tuples[self.pos].clone());
            self.pos += 1;
        }
        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.pos = 0;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.estimate
    }

    fn name(&self) -> &'static str {
        "MockSource"
    }
}

fn int_tuples(descending: bool, n: usize) -> (Vec<Tuple>, Vec<ColumnInfo>) {
    let schema = vec![ColumnInfo {
        name: "v".to_string(),
        data_type: DataType::Int,
        table: None,
    }];
    let tuples: Vec<Tuple> = (0..n)
        .map(|i| {
            let v = if descending {
                (n - 1 - i) as i64
            } else {
                i as i64
            };
            Tuple::new(vec![Some(DataValue::Int(v as i32))])
        })
        .collect();
    (tuples, schema)
}

#[test]
fn sort_next_batch_clears_batch_per_contract() {
    let (tuples, schema) = int_tuples(true, 5);
    let src = MockSource {
        tuples,
        schema,
        pos: 0,
        estimate: 0,
    };

    let mut sort = SortOperator::new(Box::new(src), vec![(0, false)]);

    let mut batch = Vec::new();
    let n1 = sort.next_batch(&mut batch).expect("first next_batch");
    assert_eq!(n1, 5, "all 5 tuples in the first batch");

    let n2 = sort.next_batch(&mut batch).expect("second next_batch");
    assert_eq!(n2, 0, "exhausted");

    assert!(
        batch.is_empty(),
        "BUG B CONFIRMED: next_batch left {} stale tuples in the batch (contract: operator clears batch)",
        batch.len()
    );
}

// ── Finding C: adaptive sort discards data past the threshold ───────────────
//
// `new_adaptive` picks external sort up-front only from the child's
// `estimate_cardinality`. When the estimate is unknown/low but the ACTUAL row
// count crosses the threshold during `load_if_needed`, the operator flips
// `use_external = true` and returns — leaving the loaded rows stranded in
// `self.buffer` — and then builds the external sort around a fresh
// NullOperator. Every subsequent next()/next_batch() returns NOTHING.

#[test]
fn adaptive_sort_threshold_crossing_keeps_rows() {
    let (tuples, schema) = int_tuples(true, 50);
    // estimate 0 ("unknown") but 50 actual rows > threshold 10 → bug path
    let src = MockSource {
        tuples,
        schema,
        pos: 0,
        estimate: 0,
    };

    let mut sort = SortOperator::new_adaptive(Box::new(src), vec![(0, false)], 10);

    let mut batch = Vec::new();
    let mut out: Vec<Tuple> = Vec::new();
    loop {
        let n = sort.next_batch(&mut batch).expect("next_batch");
        if n == 0 {
            break;
        }
        out.append(&mut batch);
    }

    assert_eq!(
        out.len(),
        50,
        "BUG C CONFIRMED: adaptive sort that crosses the threshold after load returned {} rows instead of 50",
        out.len()
    );
    for (i, t) in out.iter().enumerate() {
        assert_eq!(
            t.values[0],
            Some(DataValue::Int(i as i32)),
            "row {} must be sorted",
            i
        );
    }
}

/// Control: when the estimate exceeds the threshold, external mode is chosen
/// up-front and works correctly.
#[test]
fn adaptive_sort_predeclared_external_works() {
    let (tuples, schema) = int_tuples(true, 50);
    let src = MockSource {
        tuples,
        schema,
        pos: 0,
        estimate: 100,
    };

    let mut sort = SortOperator::new_adaptive(Box::new(src), vec![(0, false)], 10);

    let mut batch = Vec::new();
    let mut out: Vec<Tuple> = Vec::new();
    loop {
        let n = sort.next_batch(&mut batch).expect("next_batch");
        if n == 0 {
            break;
        }
        out.append(&mut batch);
    }

    assert_eq!(
        out.len(),
        50,
        "external mode chosen up-front must return all rows"
    );
    for (i, t) in out.iter().enumerate() {
        assert_eq!(
            t.values[0],
            Some(DataValue::Int(i as i32)),
            "row {} must be sorted",
            i
        );
    }
}

// ── Findings D & E: SET-clause literal parsing ──────────────────────────────

fn read_text_column(
    catalog: &storage_manager::catalog::types::Catalog,
    table: &str,
    column: &str,
) -> Vec<String> {
    let path: PathBuf = common::path(format!("database/base/testdb/{}.dat", table));
    let heap = HeapManager::open(path).expect("open heap");
    let t = catalog
        .databases
        .get("testdb")
        .unwrap()
        .tables
        .get(table)
        .unwrap();
    let schema: Vec<DataType> = t.columns.iter().map(|c| c.data_type.clone()).collect();
    let pos = t
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column))
        .unwrap();

    let mut out = Vec::new();
    for result in heap.scan() {
        let Ok((_page, _slot, raw)) = result else {
            continue;
        };
        let Ok(decoded) = deserialize_nullable_row(&schema, &raw) else {
            continue;
        };
        match decoded.get(pos) {
            Some(Some(DataValue::Varchar(s))) => out.push(s.clone()),
            Some(Some(DataValue::Char(s))) => out.push(s.clone()),
            Some(Some(v)) => out.push(format!("{:?}", v)),
            _ => out.push("NULL".to_string()),
        }
    }
    out
}

/// Finding D: `SET tag = 'n-5'` — the quoted literal contains `-`, so
/// `try_parse_arith_expr` (which never checks for a leading quote) extracts
/// src column `n` and rhs `5`. With no column `n` in the table, the SET
/// evaluates to NULL and the column is silently NULLed.
#[test]
fn update_quoted_string_with_arith_char_is_literal() {
    let _ws = TestWorkspace::new("setlit");
    let catalog = setup_with_rows(
        "testdb",
        "u_q",
        vec![
            col("id", DataType::Int, false),
            col("tag", DataType::Varchar(20), true),
        ],
        &[&["1", "'zzz'"]],
    );

    let assignments = parse_set_clause("tag = 'n-5'").expect("parse set clause");
    let sel = parse_where_text("id = 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "u_q", sel).expect("select");
    update_by_pointers(&catalog, "testdb", "u_q", &ptrs, &assignments).expect("update");

    let values = read_text_column(&catalog, "u_q", "tag");
    assert_eq!(
        values[0], "n-5",
        "BUG D CONFIRMED: quoted literal 'n-5' was misparsed as arithmetic; column now {:?}",
        values[0]
    );
}

/// Finding E: SQL escaped quote `''` inside a literal is stored verbatim
/// instead of collapsing to one quote.
#[test]
fn update_escaped_quote_in_literal() {
    let _ws = TestWorkspace::new("setquote");
    let catalog = setup_with_rows(
        "testdb",
        "u_n",
        vec![
            col("id", DataType::Int, false),
            col("name", DataType::Varchar(30), true),
        ],
        &[&["1", "'x'"]],
    );

    let assignments = parse_set_clause("name = 'O''Brien'").expect("parse set clause");
    let sel = parse_where_text("id = 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "u_n", sel).expect("select");
    update_by_pointers(&catalog, "testdb", "u_n", &ptrs, &assignments).expect("update");

    let values = read_text_column(&catalog, "u_n", "name");
    assert_eq!(
        values[0], "O'Brien",
        "BUG E CONFIRMED: escaped quote not unescaped — got {:?}",
        values[0]
    );
}

// ── Finding F: integer overflow returns query error ─────────────────────────
//
// Expr arithmetic operations use checked arithmetic so integer overflow
// returns a query error instead of panicking with a process crash.

#[test]
fn int_overflow_returns_query_error() {
    let _ws = TestWorkspace::new("ovf");
    let catalog = setup_with_rows(
        "testdb",
        "o_t",
        vec![col("id", DataType::Int, false)],
        &[&["2147483647"]],
    );

    let select = match rook_parser::parse_sql("SELECT id + 1 FROM o_t") {
        Ok(QueryPlan::Select(s)) => s,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical = plan_query(&QueryPlan::Select(select), &catalog, "testdb").expect("plan failed");
    let result = execute_plan_collect(&logical, &catalog, "testdb");
    assert!(
        result.is_err(),
        "integer overflow must return a query error, not panic"
    );
}
