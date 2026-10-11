//! Round-7 scratch verification tests for correctness issues found in a
//! seventh manual deep-dive (set-operation ORDER BY/LIMIT, FK action cache
//! coherence, and LIMIT/OFFSET semantics). Diagnostic probes like the
//! round-1..6 suites — each failure pinpoints a defect.
//!
//! Findings under test:
//!   W. `SELECT ... UNION SELECT ... ORDER BY v LIMIT 2` — the parser
//!      attaches the trailing ORDER BY/LIMIT to the LEFT branch select
//!      (utils.rs builds left_query with the outer order_by/limit_clause)
//!      and hardcodes `order_by: Vec::new(), limit: None` on the
//!      SetOperationPlan. So ORDER BY sorts only the left branch and LIMIT
//!      truncates only the left branch; the union result is unsorted and
//!      unbounded. SQL applies ORDER BY/LIMIT to the whole set operation.
//!   X. `ON DELETE CASCADE` / `SET NULL` / `ON UPDATE CASCADE` (fk_actions.rs)
//!      rewrite the CHILD table via raw `read_page`/`write_page` WITHOUT
//!      quiescing the child's cached HeapManager. The parent path quiesces
//!      (delete.rs:168) but the child never does. Two failure windows:
//!      (a) child dirty frames (insert flushes only every 10k ops) are not
//!      on disk when the cascade scans → rows missed;
//!      (b) after the cascade, the child's cached pool still holds
//!      pre-cascade frames; a later flush writes them back → deleted rows
//!      resurrect on disk.
//!   Y. CONTROL (refuted): LIMIT n OFFSET m with ORDER BY — the logical
//!      planner preserves offset on the Limit node and limit_pushdown only
//!      merges Limit into Sort when offset == 0, widening the sort hint to
//!      limit+offset. LIMIT/OFFSET must return exactly the offset..offset+n
//!      window.
//!   Z. CONTROL: `INSERT INTO t (b, a) VALUES (2, 1)` explicit column-list
//!      reordering — plan_insert maps values positionally via the column
//!      list; rows must land in the named columns.

use std::path::PathBuf;
use std::sync::Mutex;

use storage_manager::backend::system_table::insert_constraint_metadata;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, load_catalog, save_catalog};
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
        let path = prev_cwd.join(format!("database_ws_p{}_cr7_{}", std::process::id(), tag));
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

fn parse_and_plan(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> rook_ast::logical::LogicalPlan {
    let plan = rook_parser::parse_sql(sql).expect("parse failed");
    plan_query(&plan, catalog, db).expect("plan failed")
}

fn run_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Vec<Vec<String>> {
    let logical = parse_and_plan(catalog, db, sql);
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .expect("execution failed");
    tuples_to_strings(&tuples)
}

fn tuples_to_strings(
    tuples: &[storage_manager::backend::executor::physical::tuple::Tuple],
) -> Vec<Vec<String>> {
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

// ── Finding W: set-operation ORDER BY/LIMIT applied to the wrong branch ─────

#[test]
fn set_operation_order_by_and_limit_apply_to_union_result() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("setop_orderby");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db7"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "a",
        vec![col("v", DataType::Int, false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "b",
        vec![col("v", DataType::Int, false)],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    // Left: 10, 30. Right: 20, 5.
    assert!(insert_single_tuple(&catalog, "db7", "a", &["10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db7", "a", &["30"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db7", "b", &["20"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db7", "b", &["5"]).unwrap());

    // ORDER BY + LIMIT apply to the UNION result → 5, 10, 20 (first 3 of 4).
    let rows = run_select(
        &catalog,
        "db7",
        "SELECT v FROM a UNION SELECT v FROM b ORDER BY v LIMIT 3",
    );
    assert_eq!(
        rows,
        vec![
            vec!["5".to_string()],
            vec!["10".to_string()],
            vec!["20".to_string()]
        ],
        "BUG W: ORDER BY/LIMIT must apply to the union result (got {:?}) — \
         parser likely attached them to the left branch only",
        rows
    );
}

// ── Finding X: FK cascade writes child via raw page I/O without quiesce ─────
//
// Deterministic window: insert a CHILD row (cached manager, dirty pool
// frame — flush only every 10k ops), then DELETE the parent. The parent
// path quiesces (delete.rs), but `set_null_child_rows` scans the CHILD via
// raw `read_page` — the unflushed child row is invisible → SET NULL never
// applied. A later read through the live pool flushes the frame and shows
// the row still referencing the deleted parent.

#[test]
fn on_delete_set_null_applies_to_unflushed_child_rows() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("fk_setnull");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db7"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "parent",
        vec![col("id", DataType::Int, false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "child",
        vec![
            col("id", DataType::Int, false),
            col("pid", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    // Declare the FK action in sys_constraints (same as test_constraints.rs).
    insert_constraint_metadata(
        &storage_manager::backend::engine::Engine::current_dir(),
        "db7",
        "child",
        "FOREIGN KEY ON DELETE SET NULL",
        "pid",
        Some("parent"),
        Some("id"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db7", "parent", &["1"]).unwrap());
    // This insert leaves a DIRTY frame in the child's cached pool (not on disk).
    assert!(insert_single_tuple(&catalog, "db7", "child", &["100", "1"]).unwrap());

    // Delete the parent → ON DELETE SET NULL must detach the child row.
    let selection =
        storage_manager::backend::executor::row_select::parse_where_text("id = 1").unwrap();
    let pointers = storage_manager::backend::executor::row_select::select_matching_pointers(
        &catalog, "db7", "parent", selection,
    )
    .unwrap();
    assert_eq!(pointers.len(), 1, "parent row must be found");
    let result =
        storage_manager::executor::delete_by_pointers(&catalog, "db7", "parent", &pointers)
            .unwrap();
    assert_eq!(result.deleted_count, 1, "parent row must be deleted");

    // The child row survives — but its FK column must now be NULL.
    let rows = run_select(&catalog, "db7", "SELECT pid FROM child WHERE id = 100");
    assert_eq!(
        rows,
        vec![vec!["NULL".to_string()]],
        "BUG X (miss): ON DELETE SET NULL missed the child row (got {:?}) — \
         the cascade's raw page scan cannot see rows still dirty in the child's \
         cached buffer pool",
        rows
    );

    // ── Resurrect window ──
    // The cascade wrote child page 1 via raw I/O, but the child's cached
    // buffer pool still holds the PRE-cascade frame for that page. Insert
    // one more child row: insert_tuple fetches the stale frame, appends on
    // top of the pre-cascade image, marks it dirty. The next checkpoint
    // flush then writes the stale image back — silently reverting the
    // SET NULL on row 100.
    assert!(
        insert_single_tuple(&catalog, "db7", "child", &["101", "NULL"]).unwrap(),
        "child insert after delete must succeed"
    );

    let rows_after = run_select(&catalog, "db7", "SELECT id, pid FROM child ORDER BY id");
    assert_eq!(
        rows_after,
        vec![
            vec!["100".to_string(), "NULL".to_string()],
            vec!["101".to_string(), "NULL".to_string()],
        ],
        "BUG X (resurrect): after inserting another child row, the SET NULL \
         was reverted by a stale buffer-pool writeback (got {:?}) — row 100 \
         references the deleted parent again",
        rows_after
    );
}

// ── Finding Y: CONTROL — LIMIT/OFFSET with ORDER BY returns the right window ─

#[test]
fn control_limit_offset_with_order_by_returns_window() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("limit_offset");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db7"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "t",
        vec![col("v", DataType::Int, false)],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    for v in [5, 3, 1, 4, 2] {
        assert!(insert_single_tuple(&catalog, "db7", "t", &[&v.to_string()]).unwrap());
    }

    // ORDER BY v LIMIT 2 OFFSET 2 → rows 3 and 4 of [1,2,3,4,5] = [3, 4].
    let rows = run_select(
        &catalog,
        "db7",
        "SELECT v FROM t ORDER BY v LIMIT 2 OFFSET 2",
    );
    assert_eq!(
        rows,
        vec![vec!["3".to_string()], vec!["4".to_string()]],
        "LIMIT/OFFSET window wrong: got {:?}",
        rows
    );
}

// ── Finding Z: CONTROL — INSERT with explicit column-list reordering ────────

#[test]
fn control_insert_column_list_reorder() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("insert_reorder");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db7"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db7",
        "t",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();

    // INSERT via the SQL path with reordered column list.
    let plan = rook_parser::parse_sql("INSERT INTO t (b, a) VALUES (2, 1)").expect("parse");
    let logical = plan_query(&plan, &catalog, "db7").expect("plan");
    storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, &catalog, "db7",
    )
    .expect("insert failed");

    let rows = run_select(&catalog, "db7", "SELECT a, b FROM t");
    assert_eq!(
        rows,
        vec![vec!["1".to_string(), "2".to_string()]],
        "column-list reorder wrong: got {:?}",
        rows
    );
}
