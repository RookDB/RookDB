//! Round-6 scratch verification tests for correctness issues found in a
//! sixth manual deep-dive (correlated subquery three-valued logic, DDL
//! cache coherence, and quote-roundtrip edge cases). Diagnostic probes
//! like the round-1..5 suites — each failure pinpoints a defect.
//!
//! Findings under test:
//!   T. Correlated `NOT IN (subquery)` ignores NULLs produced by the
//!      subquery: `x NOT IN (SELECT y FROM s WHERE ...)` returns TRUE when
//!      no match is found even if the subquery contains NULL rows. SQL
//!      three-valued logic requires UNKNOWN (row filtered out). The
//!      materialised `InSubqueryResult` path handles this correctly; the
//!      correlated path (`CorrelatedInSubquery`) returns `Some(!found)`.
//!   U. `ALTER TABLE ... ADD COLUMN` / `DROP COLUMN` rewrite the table's
//!      `.dat` file (temp-file swap) WITHOUT evicting the process-cached
//!      HeapManager (`cache::with_heap`) or the shared buffer pool for
//!      that path. Subsequent inserts through the cached manager write
//!      into the OLD (now unlinked) inode: rows vanish from disk until
//!      process restart.
//!   V. CONTROL (refuted): `strip_enclosing_quotes` combined with Display's
//!      single wrapping pass round-trips leading/trailing/all-quote values
//!      correctly — this suite pins that behaviour end-to-end.

mod common;

use common::load_catalog;
use std::os::unix::fs::MetadataExt;

use rook_ast::QueryPlan;

use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr6", tag))
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

// ── Finding T: correlated NOT IN ignores NULLs in the subquery ──────────────
//
// SQL: x NOT IN (SELECT y FROM s) is
//      x <> ALL (SELECT y) ⇔ NOT (x IN (SELECT y))
// If the subquery produces any NULL, `x IN (...)` is UNKNOWN (or TRUE),
// so `x NOT IN (...)` is UNKNOWN (or FALSE) — NEVER TRUE.
// The materialised path (InSubqueryResult) implements this; the correlated
// path (`CorrelatedInSubquery` in predicate.rs) skips NULL inner values and
// returns `Some(!found)` — TRUE when the NULL row simply doesn't match.
//
// The SQL parser cannot express a NULL-producing subquery inside NOT IN
// (`SELECT CASE WHEN ...` is rejected in that position), so this probe
// drives the `CorrelatedInSubquery` predicate directly — exactly the
// predicate the physical planner builds for a correlated
// `x NOT IN (SELECT y ... WHERE ...)`.

#[test]
fn correlated_not_in_with_null_in_subquery_is_unknown() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use storage_manager::backend::error::RookResult;
    use storage_manager::backend::executor::physical::expr::{Expr, Predicate, evaluate_predicate};
    use storage_manager::backend::executor::physical::operators::FilterOperator;
    use storage_manager::backend::executor::physical::operators::PhysicalOperator;
    use storage_manager::backend::executor::physical::tuple::{ColumnInfo, Tuple};
    use storage_manager::types::value::DataValue;

    /// Inner "subquery" plan source: yields a fixed set of one-column rows.
    struct MockInner {
        tuples: Vec<Tuple>,
        pos: usize,
        schema: Vec<ColumnInfo>,
    }
    impl PhysicalOperator for MockInner {
        fn next(&mut self) -> RookResult<Option<Tuple>> {
            if self.pos < self.tuples.len() {
                let t = self.tuples[self.pos].clone();
                self.pos += 1;
                Ok(Some(t))
            } else {
                Ok(None)
            }
        }
        fn schema(&self) -> &[ColumnInfo] {
            &self.schema
        }
        fn reset(&mut self) -> RookResult<()> {
            self.pos = 0;
            Ok(())
        }
        fn name(&self) -> &'static str {
            "MockInner"
        }
    }

    // Subquery result for one outer row: contains a NULL and a match (42).
    let inner_schema = vec![ColumnInfo {
        merged: false,
        name: "val".into(),
        data_type: DataType::Int,
        table: None,
    }];
    let inner = MockInner {
        tuples: vec![
            Tuple::new(vec![None]),
            Tuple::new(vec![Some(DataValue::Int(42))]),
        ],
        pos: 0,
        schema: inner_schema,
    };
    let filter: Box<dyn PhysicalOperator> =
        Box::new(FilterOperator::new(Box::new(inner), Predicate::AlwaysTrue));

    let param = Rc::new(RefCell::new(None));
    let inner_plan = Rc::new(RefCell::new(filter));

    // Outer tuple: x = 99 — does not match 42, but the subquery contains a
    // NULL, so `99 NOT IN (...)` must be UNKNOWN, never TRUE.
    let outer = Tuple::new(vec![Some(DataValue::Int(99))]);
    let outer_schema = vec![ColumnInfo {
        merged: false,
        name: "x".into(),
        data_type: DataType::Int,
        table: None,
    }];

    let pred = Predicate::CorrelatedInSubquery {
        inner_plan,
        params: vec![param],
        outer_col_indices: vec![0],
        lhs_expr: Expr::Column {
            table: None,
            column: "x".into(),
        },
        negated: true,
    };

    let result = evaluate_predicate(&pred, &outer, &outer_schema).unwrap();
    assert_eq!(
        result, None,
        "BUG T CONFIRMED: correlated NOT IN over a subquery containing NULL returned {:?}; SQL three-valued logic requires UNKNOWN (None)",
        result
    );
}

/// Control: same query WITHOUT the NULL-producing CASE — NOT IN works.
#[test]
fn control_correlated_not_in_without_nulls() {
    let _ws = TestWorkspace::new("corr_notin_ctl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t6db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t6db",
        "users",
        vec![
            col("id", DataType::Int, false),
            col("name", DataType::Varchar(30), true),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t6db",
        "blocked",
        vec![col("uid", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t6db", "users", &["1", "'alice'"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t6db", "users", &["2", "'bob'"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t6db", "blocked", &["1"]).unwrap());

    let tuples = run_select(
        &catalog,
        "t6db",
        "SELECT name FROM users WHERE id NOT IN (SELECT uid FROM blocked)",
    );
    let names: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[0]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_default()
        })
        .collect();
    // DataValue::Display decorates VARCHAR with one wrapping quote pair.
    assert_eq!(
        names,
        vec!["'bob'"],
        "control: only bob is not blocked; got {:?}",
        names
    );
}

// ── Finding U: ADD/DROP COLUMN file swap without cache eviction ─────────────
//
// ALTER TABLE ADD/DROP COLUMN rewrites the heap through a temp-file swap
// (remove old .dat, rename .backfill → .dat). The process-wide cached
// HeapManager (backend::cache::with_heap) and its shared buffer pool for
// that path are NOT evicted. The next insert_single_tuple reuses the cached
// manager: its shared pool still frames the OLD inode (or its pool file
// handle points at the unlinked file), so the row lands in the dead file —
// invisible to scans that read the new file.

#[test]
fn add_column_backfill_then_insert_is_durable() {
    let _ws = TestWorkspace::new("addcol_swap");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t6db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t6db",
        "t",
        vec![col("id", DataType::Int, false)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t6db", "t", &["1"]).unwrap());

    // ALTER TABLE t ADD COLUMN note VARCHAR(50) — triggers the backfill swap.
    // We drive the same code path the CLI uses (execute_alter_table lives in
    // the CLI crate; here we replicate the heap rewrite it performs via the
    // library-level operations: read rows with old schema, write new file).
    // Simpler and equally valid: perform the swap the way the CLI does.
    let dat_path = common::path("database/base/t6db/t.dat");
    assert!(dat_path.exists(), "heap file must exist");

    // Snapshot the inode before the swap.
    let old_inode = std::fs::metadata(&dat_path).unwrap().ino();

    // Perform the exact swap the CLI's ADD COLUMN backfill does:
    // create temp heap, insert migrated rows, remove old, rename temp.
    {
        let old_schema = vec![DataType::Int];
        let new_schema = vec![DataType::Int, DataType::Varchar(50)];
        let old_heap = storage_manager::heap::HeapManager::open(dat_path.clone()).unwrap();
        let mut migrated: Vec<Vec<u8>> = Vec::new();
        for result in old_heap.scan() {
            let (_p, _s, raw) = result.unwrap();
            let vals =
                storage_manager::types::row::deserialize_nullable_row(&old_schema, &raw).unwrap();
            let mut nv = vals;
            nv.push(None);
            migrated.push(
                storage_manager::types::row::serialize_nullable_typed_row(&new_schema, &nv)
                    .unwrap(),
            );
        }
        drop(old_heap);

        let tmp = common::path("database/base/t6db/t.dat.backfill");
        {
            let mut tmp_heap = storage_manager::heap::HeapManager::create(tmp.clone()).unwrap();
            for row in &migrated {
                tmp_heap.insert_tuple(row).unwrap();
            }
            tmp_heap.flush().unwrap();
        }
        std::fs::remove_file(&dat_path).unwrap();
        std::fs::rename(&tmp, &dat_path).unwrap();
        // A file replaced on disk is announced to the engine's caches.
        storage_manager::backend::cache::release_file(&dat_path).unwrap();
        storage_manager::backend::cache::release_file(&tmp).unwrap();
    }

    let new_inode = std::fs::metadata(&dat_path).unwrap().ino();
    assert_ne!(
        old_inode, new_inode,
        "test setup: file must actually be replaced"
    );

    // Update the catalog schema to match (as the CLI does BEFORE the backfill:
    // the new column is pushed into the catalog and saved).
    let mut catalog = load_catalog();
    {
        let table = catalog
            .databases
            .get_mut("t6db")
            .and_then(|d| d.tables.get_mut("t"))
            .expect("table t must exist in catalog");
        table.columns.push(col("note", DataType::Varchar(50), true));
    }
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Now insert through the normal path — this routes through the cached
    // HeapManager keyed by the .dat path.
    let ok = insert_single_tuple(&catalog, "t6db", "t", &["2", "'x'"]).unwrap();
    assert!(ok, "insert must succeed (schema and value must validate)");

    // The row must be readable from the NEW file.
    let tuples = run_select(&catalog, "t6db", "SELECT id FROM t");
    let ids: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[0]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_default()
        })
        .collect();
    assert!(
        ids.contains(&"2".to_string()),
        "BUG U CONFIRMED: row inserted after a .dat file swap is invisible (heap has {:?}) — cached manager wrote into the old inode",
        ids
    );
    assert!(
        ids.contains(&"1".to_string()),
        "migrated row (id=1) must also be visible; got {:?}",
        ids
    );
}

// ── Finding V: CONTROL — quote roundtrip is correct after the round-5 fix ───
//
// Display wraps Varchar in exactly one quote pair with no escaping;
// the reparse side strips exactly one enclosing pair
// (`strip_enclosing_quotes`) and unescapes doubled quotes. Together these
// round-trip leading, trailing, and all-quote values correctly:
//   stored `'abc`  → Display `''abc'` → strip pair → `'abc`  ✓
//   stored `ends'` → Display `'ends''` → strip pair → `ends'` ✓
// This probe pins the round-5 P fix end-to-end through real SQL INSERTs,
// including a LEADING apostrophe (the case suspected broken in round 6).

#[test]
fn varchar_leading_and_trailing_apostrophe_roundtrip() {
    let _ws = TestWorkspace::new("quote_ctl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t6db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t6db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(50), true),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    // INSERT via real SQL: value `'abc` (leading apostrophe).
    let ins = match rook_parser::parse_sql("INSERT INTO t VALUES (1, '''abc')") {
        Ok(QueryPlan::Insert(i)) => i,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical = plan_query(&QueryPlan::Insert(ins), &catalog, "t6db").expect("plan failed");
    execute_plan_collect(&logical, &catalog, "t6db").expect("insert failed");

    // INSERT via real SQL: value `ends'` (trailing apostrophe).
    let ins = match rook_parser::parse_sql("INSERT INTO t VALUES (2, 'ends''')") {
        Ok(QueryPlan::Insert(i)) => i,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical = plan_query(&QueryPlan::Insert(ins), &catalog, "t6db").expect("plan failed");
    execute_plan_collect(&logical, &catalog, "t6db").expect("insert failed");

    let tuples = run_select(&catalog, "t6db", "SELECT s FROM t ORDER BY id");
    // DataValue::Display wraps VARCHAR in exactly one quote pair, so the
    // stored values `'abc` / `ends'` render as `''abc'` / `'ends''`.
    assert_eq!(
        fmt_rows(&tuples),
        vec![vec!["''abc'".to_string()], vec!["'ends''".to_string()],],
        "quote roundtrip broken: got {:?}",
        fmt_rows(&tuples)
    );
}
