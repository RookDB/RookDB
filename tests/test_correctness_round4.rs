//! Round-4 scratch verification tests for correctness issues found in a
//! fourth manual deep-dive (heap/vacuum interplay, buffer pool, cache
//! coherence between VACUUM's index rebuild and the process-level BTree
//! handle cache, and the ordering-claim contract of the fixed FullScan).
//! Diagnostic probes like the round-1/2/3 suites — each failure pinpoints
//! a defect.
//!
//! Findings under test:
//!   K. VACUUM rebuilds `.idx` files via `BTree::create_composite` (which
//!      truncates the file) WITHOUT evicting the process-wide cached BTree
//!      handle (`cache.rs::with_btree`). Any later `update_index_on_insert`
//!      re-uses the stale handle: its buffer pool still frames the OLD
//!      inode, so the fresh on-disk index never sees new keys → point
//!      lookups on rows inserted after VACUUM miss.
//!   L. `IndexScanOperator::FullScan` (after the round-3 fix that unions a
//!      heap scan for NULL-key rows) no longer produces output in index
//!      order, but still reports `ordering() == Some(indexed_cols)`. The
//!      physical planner ELIDES the ORDER BY sort operator based on that
//!      claim. Differential probe: identical table WITH vs WITHOUT an
//!      index. The engine's own sort comparator orders NULL FIRST on ASC
//!      (sort.rs `(None, Some(_)) => Ordering::Less`), so the no-index
//!      table yields [NULL, 10, 30] while the indexed table yields the
//!      elided FullScan order [10, 30, NULL].
//!   M. SET literal 'n-5' through the full update path — REFUTED: the
//!      value is stored correctly end-to-end (DataValue Display merely
//!      decorates VARCHAR with quotes).
//!   N. `get_tuple` / `delete_tuple` pin-leak on error paths (deleted/bad
//!      slot) — resolved by ensuring frames are always unpinned on error paths.

use std::path::PathBuf;
use std::sync::Mutex;

use rook_ast::QueryPlan;

use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, load_catalog, save_catalog};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TestWorkspace {
    prev_cwd: PathBuf,
    path: PathBuf,
}

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        let prev_cwd = std::env::current_dir().expect("read cwd");
        let path = prev_cwd.join(format!("database_ws_p{}_cr4_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("base")).expect("create workspace");
        std::env::set_current_dir(&path).expect("chdir into workspace");
        storage_manager::backend::executor::row_select::register_where_parser(
            rook_parser::parse_where_text,
        );
        storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
        storage_manager::catalog::init_catalog();
        Self { prev_cwd, path }
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        if std::env::set_current_dir(&self.prev_cwd).is_ok() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
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

// ── Finding K: VACUUM's index rebuild vs the cached BTree handle ────────────
//
// vacuum_table() calls compaction (which may renumber slots) and then
// rebuild_indexes(), which recreates each .idx file from scratch via
// BTree::create_composite (truncate + fresh tree). It never evicts the
// process-wide cached BTree handle for that path (cache::with_btree).
// After vacuum, update_index_on_insert writes NEW keys through the STALE
// cached handle (its buffer pool still frames the old inode, root page id
// etc.), so the fresh on-disk index never sees them → point lookups on
// newly inserted rows miss.
//
// Probe: build dead slots (DELETE rows), vacuum (compaction + rebuild),
// then insert a new row and check an equality lookup on the new row's
// value finds it.
#[test]
fn vacuum_stale_btree_handle_loses_new_rows() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("vacuum_stale");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for i in 1..=6 {
        assert!(insert_single_tuple(&catalog, "t4db", "t", &[&i.to_string(), "100"]).unwrap());
    }
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["7", "NULL"]).unwrap());

    create_index(&catalog, "t4db", "t", "idx_v", &["v".to_string()]).expect("create index");

    // Delete two rows → dead slots → vacuum has real work to do.
    // Pointers are (page_id, slot_id); first live rows live on page 1.
    let deleted =
        storage_manager::executor::delete_by_pointers(&catalog, "t4db", "t", &[(1, 0), (1, 1)])
            .expect("delete failed");
    assert_eq!(deleted.deleted_count, 2, "two rows must be soft-deleted");

    // VACUUM: compacts pages (renumbering slots) and rebuilds idx_v from scratch.
    let stats = storage_manager::executor::vacuum::vacuum_table(&catalog, "t4db", "t")
        .expect("vacuum failed");
    assert!(
        stats.pages_compacted > 0,
        "vacuum should compact something (stats={:?})",
        stats
    );

    // Insert a NEW row after vacuum. Its key must enter the FRESH index.
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["8", "100"]).unwrap());

    // Probe with an equality lookup on v=100.
    let tuples = run_select(&catalog, "t4db", "SELECT id FROM t WHERE v = 100");
    let mut ids: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[0]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_default()
        })
        .collect();
    ids.sort();

    // Live rows with v=100: id 3,4,5,6 (1,2 deleted; 7 is NULL) plus the
    // new id 8. If the stale cached BTree handle is used for the new
    // insert, the fresh index misses id=8.
    assert_eq!(
        ids,
        vec!["3", "4", "5", "6", "8"],
        "BUG K CONFIRMED: post-vacuum insert (id=8) missing from equality lookup on indexed column; got {:?}",
        ids
    );
}

// ── Finding L: stale ordering() claim on the fixed FullScan ─────────────────
//
// IndexScanOperator::FullScan (round-3 fix: index rows + heap-scan union)
// no longer yields output in index order — the appended NULL-key rows come
// at the end. But ordering() still claims the indexed columns, so the
// planner eliminates the ORDER BY sort:
//   planner/mod.rs: `already_sorted = child.ordering() == sort_keys …`
// Result: `SELECT … ORDER BY v` returns unsorted rows.
//
// Control: with no NULL-key rows the output IS sorted and passes; with a
// NULL-key row present the appended rows break the ordering.
#[test]
fn order_by_control_sorted_output_when_all_keys_non_null() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("orderby_ctl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["1", "30"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["2", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["3", "20"]).unwrap());

    create_index(&catalog, "t4db", "t", "idx_v", &["v".to_string()]).expect("create index");

    // ORDER BY v: FullScan claims ordering, planner elides the sort. With
    // all keys non-NULL the claim is actually true → passes.
    let tuples = run_select(&catalog, "t4db", "SELECT id, v FROM t ORDER BY v");
    let vs: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[1]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_else(|| "NULL".into())
        })
        .collect();
    assert_eq!(
        vs,
        vec!["10", "20", "30"],
        "control: expected sorted v; got {:?}",
        vs
    );
}

#[test]
fn order_by_with_null_key_row_violates_claimed_ordering() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("orderby_bug");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["1", "30"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["2", "10"]).unwrap());
    // NULL in the indexed column → appended after the index-ordered rows.
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["3", "NULL"]).unwrap());

    create_index(&catalog, "t4db", "t", "idx_v", &["v".to_string()]).expect("create index");

    // ORDER BY v: FullScan claims ordering on v, planner elides the sort.
    // The engine's own sort comparator puts NULL FIRST on ASC, so the
    // expected result (what the no-index control produces) is
    // [NULL, 10, 30]. The elided path yields [10, 30, NULL] instead.
    let tuples = run_select(&catalog, "t4db", "SELECT id, v FROM t ORDER BY v");
    let vs: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[1]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_else(|| "NULL".into())
        })
        .collect();
    assert_eq!(
        vs,
        vec!["NULL", "10", "30"],
        "BUG L CONFIRMED: ORDER BY v elided on stale ordering claim; got {:?} (expected NULL first per engine sort semantics)",
        vs
    );
}

/// Differential control: the SAME table WITHOUT an index. SeqScan makes no
/// ordering claim, the sort runs, and the engine's NULLS-FIRST ASC ordering
/// is observable. This pins the expected output used by the bug probe.
#[test]
fn order_by_no_index_control_nulls_first() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("orderby_noindex");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["1", "30"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["2", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["3", "NULL"]).unwrap());

    // No index → SeqScan → ORDER BY sort actually runs → NULL first.
    let tuples = run_select(&catalog, "t4db", "SELECT id, v FROM t ORDER BY v");
    let vs: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[1]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_else(|| "NULL".into())
        })
        .collect();
    assert_eq!(
        vs,
        vec!["NULL", "10", "30"],
        "control: no-index ORDER BY must sort with engine semantics (NULL first); got {:?}",
        vs
    );
}

// ── Finding M: SET literal roundtrip through update_by_pointers (REFUTED) ───
//
// update_by_pointers re-serialises and re-parses SET assignments
// internally; a quoted literal containing an arithmetic-looking body
// (round-2 fix D) survives the whole path as literal text. DataValue's
// Display decorates VARCHAR with quotes, hence the quoted expectation.
#[test]
fn update_set_literal_with_arith_char_survives_end_to_end() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("update_mask");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Varchar(50), true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "t4db", "t", &["1", "'hello'"]).unwrap());

    use rook_parser::parse_set_clause;

    use storage_manager::executor::update_by_pointers;
    let assignments = parse_set_clause("v = 'n-5'").expect("parse set");

    let result =
        update_by_pointers(&catalog, "t4db", "t", &[(1, 0)], &assignments).expect("update failed");

    // The row must now contain exactly the literal text `n-5`, NOT NULL and
    // NOT a partial arithmetic evaluation.
    assert_eq!(result.updated_count, 1, "row should be updated");
    let tuples = run_select(&catalog, "t4db", "SELECT v FROM t WHERE id = 1");
    assert_eq!(
        fmt_rows(&tuples),
        vec![vec!["'n-5'".to_string()]],
        "SET literal 'n-5' corrupted through update path (Display decorates VARCHAR with quotes)"
    );
}

// ── Control: DELETE pointers actually target the intended rows ──────────────
// Guards finding K's setup against pointer-drift false positives.
#[test]
fn control_delete_pointers_target_expected_rows() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("del_ctl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "t4db"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "t4db",
        "t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for i in 1..=6 {
        assert!(insert_single_tuple(&catalog, "t4db", "t", &[&i.to_string(), "100"]).unwrap());
    }

    let deleted =
        storage_manager::executor::delete_by_pointers(&catalog, "t4db", "t", &[(1, 0), (1, 1)])
            .expect("delete failed");

    assert_eq!(deleted.deleted_count, 2, "two rows must be deleted");
    let tuples = run_select(&catalog, "t4db", "SELECT id FROM t");
    let mut ids: Vec<String> = tuples
        .iter()
        .map(|t| {
            t.values[0]
                .as_ref()
                .map(|d| format!("{}", d))
                .unwrap_or_default()
        })
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["3", "4", "5", "6"],
        "deleted wrong rows: {:?}",
        ids
    );
}

// ── Finding N: get_tuple / delete_tuple pin-leak on error paths ─────────────
//
// Previously, if get_tuple or delete_tuple encountered an error after
// pool.fetch_page (such as a deleted tuple, out-of-bounds slot, or bad slot),
// it returned early without unpinning the frame. Repeated failed accesses
// leaked pins, eventually exhausting the buffer pool.
#[test]
fn get_tuple_error_paths_do_not_leak_buffer_pool_pins() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("pin_leak");

    let heap_path = _ws.path.join("base").join("test_pins.dat");
    let mut manager = storage_manager::backend::heap::heap_manager::HeapManager::create(heap_path)
        .expect("create heap");

    // Insert 1 tuple onto page 1
    let (page_id, slot_id) = manager.insert_tuple(b"hello world").expect("insert tuple");
    assert_eq!(page_id, 1);
    assert_eq!(slot_id, 0);

    // Initial state: no frames pinned
    assert_eq!(manager.pool.lock().unwrap().pinned_count(), 0);

    // Out-of-bounds slot lookup -> returns error, but MUST NOT leak pin
    let err = manager.get_tuple(page_id, 999);
    assert!(err.is_err());
    assert_eq!(
        manager.pool.lock().unwrap().pinned_count(),
        0,
        "out-of-bounds slot leaked pin"
    );

    // Delete the tuple
    manager
        .delete_tuple(page_id, slot_id)
        .expect("delete tuple");
    assert_eq!(manager.pool.lock().unwrap().pinned_count(), 0);

    // Lookup on deleted tuple -> returns NotFound error, but MUST NOT leak pin
    let err = manager.get_tuple(page_id, slot_id);
    assert!(err.is_err());
    assert_eq!(
        manager.pool.lock().unwrap().pinned_count(),
        0,
        "deleted slot lookup leaked pin"
    );

    // Out-of-bounds slot delete -> returns error, but MUST NOT leak pin
    let err = manager.delete_tuple(page_id, 999);
    assert!(err.is_err());
    assert_eq!(
        manager.pool.lock().unwrap().pinned_count(),
        0,
        "bad slot delete leaked pin"
    );
}
