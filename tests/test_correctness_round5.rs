//! Round-5 scratch verification tests for correctness issues found in a
//! fifth manual deep-dive (UPDATE type coverage, scalar-function edge
//! cases, and batch-contract compliance of the materialising operators).
//! Diagnostic probes like the round-1..4 suites — each failure pinpoints
//! a defect.
//!
//! Findings under test:
//!   O. `SET numcol = 5` on a NUMERIC/DECIMAL column silently NULLs the
//!      column: `apply_assignments_typed` falls into
//!      `parse_string_to_value(..).ok()`, and that function hard-rejects
//!      NUMERIC/DECIMAL ("Index maintenance ... not yet supported"),
//!      so the assignment maps to None → NULL.
//!   P. INSERT ... SELECT roundtrips values through DataValue::Display →
//!      literal re-parse. Display wraps VARCHAR in '…' without escaping,
//!      and the re-parse strips quotes with `trim_matches('\'')`, which
//!      removes ALL leading/trailing quotes: a value that ends with an
//!      apostrophe loses it.
//!   Q. `ABS(-2147483648)` panics (`i32::abs` overflow) in debug builds
//!      instead of returning a query error.
//!   R. `SUBSTRING('abc', 2, -1)` panics (`usize` overflow: `from + len`)
//!      in debug builds because a negative length was cast to a huge usize.
//!   S. `SetOpOperator`, `NestedLoopJoinOperator`, and `HashJoinOperator`
//!      `next_batch` never clear the incoming batch (HashJoin explicitly
//!      appends at `initial_len`), violating the documented contract
//!      ("Clears `batch` before populating"). Latent today: the engine
//!      happens to drain via `append(&mut batch)`, but any contract-
//!      abiding caller sees duplicated rows — the same family as round-2
//!      bug B.

mod common;

use common::load_catalog;

use rook_ast::QueryPlan;

use rook_parser::parse_set_clause;
use storage_manager::backend::error::RookResult;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::expr::Expr;
use storage_manager::backend::executor::physical::operators::SetOpType as PhysicalSetOpType;
use storage_manager::backend::executor::physical::operators::{
    HashJoinOperator, JoinType, NestedLoopJoinOperator, PhysicalOperator, SetOpOperator,
    SortOperator,
};
use storage_manager::backend::executor::physical::tuple::{ColumnInfo, Tuple};
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;
use storage_manager::types::value::DataValue;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr5", tag))
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

fn fmt_rows(tuples: &[Tuple]) -> Vec<Vec<String>> {
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

// ── Finding O: SET on a NUMERIC column silently NULLs the column ────────────

#[test]
fn set_numeric_column_preserves_value() {
    let _ws = TestWorkspace::new("set_numeric");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col(
                "n",
                DataType::Numeric {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t5db", "t", &["1", "3.5"]).unwrap());

    use storage_manager::executor::update_by_pointers;
    let assignments = parse_set_clause("n = 9.75").expect("parse set");

    let result =
        update_by_pointers(&catalog, "t5db", "t", &[(1, 0)], &assignments).expect("update failed");
    assert_eq!(result.updated_count, 1, "row should be updated");

    let tuples = run_select(&catalog, "t5db", "SELECT n FROM t WHERE id = 1");
    assert_eq!(
        fmt_rows(&tuples),
        vec![vec!["9.75".to_string()]],
        "BUG O CONFIRMED: SET on NUMERIC column produced {:?} (expected 9.75, not NULL)",
        fmt_rows(&tuples)
    );
}

/// Control for O: SET on an INT column must keep working through the same
/// typed path (it has a dedicated `ColumnValue::Int` arm).
#[test]
fn control_set_int_column_still_works() {
    let _ws = TestWorkspace::new("set_int_ctl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("n", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t5db", "t", &["1", "3"]).unwrap());

    use storage_manager::executor::update_by_pointers;
    let assignments = parse_set_clause("n = 9").expect("parse set");
    let result =
        update_by_pointers(&catalog, "t5db", "t", &[(1, 0)], &assignments).expect("update failed");
    assert_eq!(result.updated_count, 1);

    let tuples = run_select(&catalog, "t5db", "SELECT n FROM t WHERE id = 1");
    assert_eq!(fmt_rows(&tuples), vec![vec!["9".to_string()]]);
}

// ── Finding P: value ending in an apostrophe corrupted through INSERT...SELECT
//
// The SQL parser unescapes 'ends''' → value `ends'`. Display wraps it as
// `'ends''` (no escaping), and InsertOperator's re-parse strips quotes with
// `trim_matches('\'')`, which removes ALL trailing quotes → `ends`.

#[test]
fn insert_select_roundtrip_trailing_apostrophe() {
    let _ws = TestWorkspace::new("ins_sel_quote");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    // src and dst have identical schemas.
    create_table(
        &mut catalog,
        "t5db",
        "src",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(50), true),
        ],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "dst",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(50), true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();

    // Insert via REAL SQL so the parser unescapes the literal: value = ends'
    let ins = match rook_parser::parse_sql("INSERT INTO src VALUES (1, 'ends''')") {
        Ok(QueryPlan::Insert(i)) => i,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical =
        plan_query(&QueryPlan::Insert(ins), &catalog, "t5db").expect("plan insert failed");
    storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, &catalog, "t5db",
    )
    .expect("insert values failed");

    // SQL-correct: src stores value `ends'` (displayed with quotes as 'ends'').
    // The INSERT VALUES path routes through InsertOperator's Display→reparse,
    // where trim_matches strips ALL trailing quotes — so this is itself a
    // manifestation of bug P.
    let src_tuples = run_select(&catalog, "t5db", "SELECT s FROM src");
    assert_eq!(
        fmt_rows(&src_tuples),
        vec![vec!["'ends''".to_string()]],
        "BUG P CONFIRMED: INSERT VALUES with a string ending in an apostrophe lost the apostrophe (got {:?})",
        fmt_rows(&src_tuples)
    );

    // INSERT INTO dst SELECT id, s FROM src — Display → reparse roundtrip.
    let insert = match rook_parser::parse_sql("INSERT INTO dst SELECT id, s FROM src") {
        Ok(QueryPlan::Insert(i)) => i,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical =
        plan_query(&QueryPlan::Insert(insert), &catalog, "t5db").expect("plan insert failed");
    storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, &catalog, "t5db",
    )
    .expect("insert-select failed");

    let tuples = run_select(&catalog, "t5db", "SELECT s FROM dst");
    assert_eq!(
        fmt_rows(&tuples),
        vec![vec!["'ends''".to_string()]],
        "BUG P (INSERT...SELECT leg): trailing apostrophe lost through the roundtrip (got {:?})",
        fmt_rows(&tuples)
    );
}

// ── Finding Q: ABS of the most negative integer panics ──────────────────────

#[test]
fn abs_min_int_returns_error_not_panic() {
    let _ws = TestWorkspace::new("abs_min");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t5db", "t", &["1", "-2147483648"]).unwrap());

    // ABS keeps its argument's type, as the planner declares it, so the
    // magnitude of INT's minimum does not fit: an overflow error, as in
    // PostgreSQL, not a panic or a wrapped negative.
    let plan = match rook_parser::parse_sql("SELECT ABS(v) FROM t") {
        Ok(q) => plan_query(&q, &catalog, "t5db").expect("plan failed"),
        Err(e) => panic!("parse failed: {:?}", e),
    };
    let err = execute_plan_collect(&plan, &catalog, "t5db")
        .expect_err("ABS(-2147483648) must be an overflow error");
    assert!(err.to_string().contains("out of range"), "{}", err);
}

// ── Finding R: SUBSTRING with negative length panics ────────────────────────

#[test]
fn substring_negative_length_returns_error_not_panic() {
    let _ws = TestWorkspace::new("substr_neg");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(50), true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t5db", "t", &["1", "'abcdef'"]).unwrap());

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_select(&catalog, "t5db", "SELECT SUBSTRING(s, 2, -1) FROM t")
    }));
    match result {
        Ok(tuples) => {
            // SQL-correct behaviours: empty string (len < 0 → zero chars) or
            // an error. A wrapped huge length must NOT truncate to the rest
            // of the string.
            let shown = tuples[0].values[0]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_else(|| "NULL".into());
            assert!(
                shown == "NULL" || shown.is_empty(),
                "SUBSTRING(s, 2, -1) returned {:?} — negative length treated as huge usize",
                shown
            );
        }
        Err(_) => panic!(
            "BUG R CONFIRMED: SUBSTRING(s, 2, -1) panicked (usize overflow in from + len) instead of returning an error"
        ),
    }
}

// ── Finding S: batch-contract violations in materialising operators ─────────
//
// The PhysicalOperator::next_batch contract says "Clears `batch` before
// populating". SetOp, NLJ, and HashJoin append instead. Any contract-abiding
// caller (or future engine change) reads duplicated rows.

struct ContractMock {
    tuples: Vec<Tuple>,
    schema: Vec<ColumnInfo>,
    pos: usize,
}

impl PhysicalOperator for ContractMock {
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
        "ContractMock"
    }
}

fn one_col(n: usize) -> (Vec<Tuple>, Vec<ColumnInfo>) {
    let schema = vec![ColumnInfo {
        merged: false,
        name: "v".to_string(),
        data_type: DataType::Int,
        table: None,
    }];
    let tuples = (0..n)
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i as i32))]))
        .collect();
    (tuples, schema)
}

#[test]
fn set_op_next_batch_clears_batch_per_contract() {
    let (lt, ls) = one_col(3);
    let (rt, rs) = one_col(2);
    let left = ContractMock {
        tuples: lt,
        schema: ls,
        pos: 0,
    };
    let right = ContractMock {
        tuples: rt,
        schema: rs,
        pos: 0,
    };

    let mut op = SetOpOperator::new(
        Box::new(left),
        Box::new(right),
        PhysicalSetOpType::Union,
        true,
    );

    let mut batch = vec![Tuple::new(vec![Some(DataValue::Int(999))])]; // sentinel
    let n1 = op.next_batch(&mut batch).expect("first next_batch");
    assert_eq!(n1, 5, "union all of 3+2 rows in the first batch");

    let n2 = op.next_batch(&mut batch).expect("second next_batch");
    assert_eq!(n2, 0, "exhausted");

    assert!(
        batch.is_empty(),
        "BUG S CONFIRMED: SetOpOperator::next_batch left {} stale tuples in the batch (contract: operator clears batch)",
        batch.len()
    );
}

#[test]
fn nested_loop_join_next_batch_clears_batch_per_contract() {
    let (lt, ls) = one_col(3);
    let (rt, rs) = one_col(2);
    let left = ContractMock {
        tuples: lt,
        schema: ls,
        pos: 0,
    };
    let right = ContractMock {
        tuples: rt,
        schema: rs,
        pos: 0,
    };

    let mut op = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(storage_manager::backend::executor::physical::expr::Predicate::AlwaysTrue),
        JoinType::Cross,
    );

    let mut batch = Vec::new();
    let n1 = op.next_batch(&mut batch).expect("first next_batch");
    assert_eq!(n1, 6, "cross join 3x2 in the first batch");

    let n2 = op.next_batch(&mut batch).expect("second next_batch");
    assert_eq!(n2, 0, "exhausted");

    assert!(
        batch.is_empty(),
        "BUG S CONFIRMED: NestedLoopJoinOperator::next_batch left {} stale tuples (contract: operator clears batch)",
        batch.len()
    );
}

#[test]
fn hash_join_next_batch_clears_batch_per_contract() {
    let (bt, bs) = one_col(3);
    let (pt, ps) = one_col(2);
    let build = ContractMock {
        tuples: bt,
        schema: bs,
        pos: 0,
    };
    let probe = ContractMock {
        tuples: pt,
        schema: ps,
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

    let mut batch = Vec::new();
    let n1 = op.next_batch(&mut batch).expect("first next_batch");
    assert_eq!(
        n1, 2,
        "inner join of sets 1..3 and 2..3 should yield 2 rows"
    );

    let n2 = op.next_batch(&mut batch).expect("second next_batch");
    assert_eq!(n2, 0, "exhausted");

    assert!(
        batch.is_empty(),
        "BUG S CONFIRMED: HashJoinOperator::next_batch left {} stale tuples (contract: operator clears batch)",
        batch.len()
    );
}

// ── Control: SortOperator (fixed in round 2) still honours the contract ─────

#[test]
fn control_sort_next_batch_still_clears() {
    let (tuples, schema) = one_col(5);
    let src = ContractMock {
        tuples,
        schema,
        pos: 0,
    };

    let mut sort = SortOperator::new(Box::new(src), vec![(0, false)]);

    let mut batch = Vec::new();
    let n1 = sort.next_batch(&mut batch).expect("first next_batch");
    assert_eq!(n1, 5);

    let n2 = sort.next_batch(&mut batch).expect("second next_batch");
    assert_eq!(n2, 0);
    assert!(
        batch.is_empty(),
        "regression: Sort next_batch no longer clears"
    );
}

// ── Additional Findings: NUMERIC arithmetic exactness & DISTINCT aggregates ──

#[test]
fn numeric_arithmetic_preserves_exact_precision() {
    let _ws = TestWorkspace::new("numeric_arithmetic");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t_num",
        vec![
            col("id", DataType::Int, false),
            col(
                "a",
                DataType::Numeric {
                    precision: 30,
                    scale: 2,
                },
                true,
            ),
            col(
                "b",
                DataType::Numeric {
                    precision: 30,
                    scale: 2,
                },
                true,
            ),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    // 1234567890123456789.12 has 21 significant digits.
    // In f64, 21 digits exceeds the 53-bit mantissa (~15-17 decimal digits),
    // which would truncate lower decimal digits.
    assert!(
        insert_single_tuple(
            &catalog,
            "t5db",
            "t_num",
            &["1", "1234567890123456789.12", "1000000000000000000.01"],
        )
        .unwrap()
    );

    let rows = run_select(&catalog, "t5db", "SELECT a + b, a - b FROM t_num");
    let formatted = fmt_rows(&rows);
    assert_eq!(
        formatted,
        vec![vec![
            "2234567890123456789.13".to_string(),
            "234567890123456789.11".to_string()
        ]],
        "Exact NUMERIC arithmetic must preserve all digits without f64 precision loss"
    );
}

#[test]
fn distinct_aggregate_plans_and_executes_correctly() {
    let _ws = TestWorkspace::new("distinct_agg");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t5db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t5db",
        "t_dist",
        vec![
            col("grp", DataType::Int, false),
            col("val", DataType::Int, false),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t5db", "t_dist", &["1", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t5db", "t_dist", &["1", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t5db", "t_dist", &["1", "20"]).unwrap());

    let rows = run_select(
        &catalog,
        "t5db",
        "SELECT grp, COUNT(DISTINCT val) FROM t_dist GROUP BY grp",
    );
    let formatted = fmt_rows(&rows);
    assert_eq!(
        formatted,
        vec![vec!["1".to_string(), "2".to_string()]],
        "COUNT(DISTINCT val) must count distinct values per group correctly"
    );
}
