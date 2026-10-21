//! Round-10 scratch verification tests for correctness issues found in a
//! tenth manual deep-dive (plan-cache INSERT fast path). Diagnostic probes
//! like the round-1..9 suites — each asserts the SQL-correct behaviour so a
//! failure pinpoints the defect.
//!
//! Findings under test — all in `backend/planner/plan_cache.rs`, the fast
//! path used by `execute_cached_insert` (stress_bench.rs:307) and
//! `PreparedStatement::prepare`:
//!
//!   G. `parse_insert_template` (plan_cache.rs:484) locates the VALUES
//!      clause with `rest.to_ascii_lowercase().find("values")` — the FIRST
//!      occurrence of the substring "values" anywhere in the statement. A
//!      table whose name contains "values" (e.g. `my_values`, `old_values`)
//!      splits at the wrong offset: header becomes `"my_"`, so the template
//!      targets a non-existent table. `execute_cached_insert` then fails
//!      with "Table 'my_' not found" — and because the template returned
//!      `Some(...)`, the `?` at plan_cache.rs:539 short-circuits BEFORE the
//!      full-parser fallback, so the statement is never parsed correctly.
//!      Inserting into such tables is impossible via the cached path.
//!
//!   H. Multi-row `INSERT ... VALUES (1),(2)` is flattened into ONE row.
//!      `normalize_sql` extracts literals across the whole statement into a
//!      single flat `params` vec (`["1","2"]`), and `CachedInsert::execute`
//!      treats that as one row's values. Two variants, both confirmed:
//!        - valid SQL (1-column table): the flattened params fail the
//!          single-row arity check and the whole statement errors instead
//!          of inserting 2 rows;
//!        - INVALID SQL (2-column table, 1 value per row — the planner
//!          correctly rejects it): the flattened params happen to match the
//!          table arity, so the cached path silently inserts the corrupted
//!          row (3,4) and reports success — silent data corruption.
//!
//!   I. The explicit-column-list branch of `CachedInsert::execute`
//!      (plan_cache.rs:355-371) silently ignores unknown column names (the
//!      `if let Some(pos)` yields no else) and silently drops extra values
//!      (the `if i < values.len()` guard). `INSERT INTO t (nosuchcol)
//!      VALUES (5)` inserts an all-NULL row and reports success, while the
//!      planner path (planner/mod.rs:131-136) rejects it with
//!      "Column 'nosuchcol' does not exist in table 't'".
//!
//!   J. The same column-list branch materialises every unlisted column as
//!      the literal string "NULL" (plan_cache.rs:354), ignoring declared
//!      column DEFAULTs. `INSERT INTO t (a) VALUES (7)` on a table with
//!      `b INT DEFAULT 42` stores NULL in b, while the planner path
//!      (planner/mod.rs:141-151) fills the declared default.
//!
//! Controls (expected to PASS) document the planner-path behaviour for each
//! scenario so the divergences cannot be explained away as engine limits.

mod common;

use common::load_catalog;

use storage_manager::backend::planner::plan_cache::execute_cached_insert;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;
use storage_manager::types::value::DataValue;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr10", tag))
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

fn col_with_default(name: &str, ty: DataType, nullable: bool, default: DataValue) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable,
        constraints: Constraints {
            not_null: false,
            unique: false,
            default: Some(default),
            check: None,
        },
    }
}

/// Run a SELECT through the full parse → plan → execute pipeline.
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

/// Run any statement (INSERT) through the full parse → plan → execute pipeline.
fn run_planner_insert(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> usize {
    let plan = rook_parser::parse_sql(sql).expect("parse failed");
    let logical = plan_query(&plan, catalog, db).expect("plan failed");
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .expect("execution failed");
    tuples.len()
}

// ── Finding G: table names containing "values" break the template splitter ──

#[test]
fn g_plan_cache_insert_into_table_named_with_values_substring() {
    let _ws = TestWorkspace::new("g_values_name");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10g"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10g",
        "my_values",
        vec![col("a", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Control: the planner path happily inserts into a table with "values"
    // in its name — the name itself is legal.
    let via_planner = run_planner_insert(&catalog, "db10g", "INSERT INTO my_values VALUES (1)");
    assert_eq!(via_planner, 1, "planner control: table name is legal");
    assert_eq!(
        run_select(&catalog, "db10g", "SELECT a FROM my_values"),
        vec![vec!["1"]],
        "planner control: row landed"
    );

    // The cached fast path must insert the same way.
    let result = execute_cached_insert(&catalog, "db10g", "INSERT INTO my_values VALUES (5)");

    assert!(
        result.is_ok(),
        "BUG G CONFIRMED: cached insert into 'my_values' failed — \
         parse_insert_template split at the 'values' substring inside the \
         table name and built a template for table 'my_': {:?}",
        result
    );
    assert_eq!(result.unwrap(), Some(1), "one row inserted");

    assert_eq!(
        run_select(&catalog, "db10g", "SELECT a FROM my_values"),
        vec![vec!["1"], vec!["5"]],
        "the cached insert's row must be visible"
    );
}

// ── Finding H: multi-row VALUES flattened into a single row ─────────────────

#[test]
fn h_plan_cache_multirow_insert_valid_sql_errors_instead() {
    let _ws = TestWorkspace::new("h_multirow_valid");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10h"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10h",
        "mr1",
        vec![col("a", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Control: the planner path inserts two separate rows (valid SQL).
    let via_planner = run_planner_insert(&catalog, "db10h", "INSERT INTO mr1 VALUES (1),(2)");
    assert_eq!(via_planner, 2, "planner control: two rows inserted");
    assert_eq!(
        run_select(&catalog, "db10h", "SELECT a FROM mr1 ORDER BY a"),
        vec![vec!["1"], vec!["2"]],
        "planner control: multi-row VALUES yields two rows"
    );

    let result = execute_cached_insert(&catalog, "db10h", "INSERT INTO mr1 VALUES (3),(4)");

    assert!(
        matches!(result, Ok(Some(2))),
        "BUG H CONFIRMED: cached multi-row insert returned {:?} instead of \
         inserting 2 rows — normalize_sql flattens all rows' literals into \
         one params vec (3,4), which fails CachedInsert::execute's \
         single-row arity check for the 1-column table",
        result
    );
    assert_eq!(
        run_select(&catalog, "db10h", "SELECT a FROM mr1 ORDER BY a"),
        vec![vec!["1"], vec!["2"], vec!["3"], vec!["4"]],
        "rows (3) and (4) must be inserted as separate rows"
    );
}

#[test]
fn h2_plan_cache_silently_inserts_corrupted_row_for_invalid_sql() {
    let _ws = TestWorkspace::new("h2_multirow_corrupt");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10h2"), "create db");
    let mut catalog = load_catalog();
    // Two columns; each VALUES row supplies one value → invalid SQL, the
    // planner must reject it.
    create_table(
        &mut catalog,
        "db10h2",
        "mr",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Control: the planner path correctly rejects the malformed statement.
    let planner_result = rook_parser::parse_sql("INSERT INTO mr VALUES (3),(4)")
        .map_err(|e| e.to_string())
        .and_then(|p| plan_query(&p, &catalog, "db10h2").map_err(|e| e.to_string()));
    assert!(
        planner_result.is_err(),
        "planner control: 1-value rows into a 2-column table must be rejected \
         (got {:?})",
        planner_result
    );

    let result = execute_cached_insert(&catalog, "db10h2", "INSERT INTO mr VALUES (3),(4)");

    assert!(
        result.is_err(),
        "BUG H2 CONFIRMED (silent corruption variant): cached insert returned \
         {:?} for invalid SQL — the flattened params (3,4) happen to match \
         the 2-column arity, so the single corrupted row (3,4) was inserted \
         instead of rejecting the statement like the planner does",
        result
    );
    assert!(
        run_select(&catalog, "db10h2", "SELECT a, b FROM mr").is_empty(),
        "no row may appear for a rejected statement — the corrupted row \
         (a=3, b=4) must not exist"
    );
}

// ── Finding I: unknown columns / extra values silently ignored ──────────────

#[test]
fn i_plan_cache_unknown_column_silently_inserts_all_null_row() {
    let _ws = TestWorkspace::new("i_unknown_col");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10i"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10i",
        "t3",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Control: the planner path rejects the unknown column, SQL-correctly.
    let planner_result = rook_parser::parse_sql("INSERT INTO t3 (nosuchcol) VALUES (5)")
        .map_err(|e| e.to_string())
        .and_then(|p| plan_query(&p, &catalog, "db10i").map_err(|e| e.to_string()));
    assert!(
        planner_result.is_err(),
        "planner control: unknown INSERT column must be rejected (got {:?})",
        planner_result
    );

    let result = execute_cached_insert(&catalog, "db10i", "INSERT INTO t3 (nosuchcol) VALUES (5)");

    assert!(
        result.is_err(),
        "BUG I CONFIRMED: cached insert with unknown column 'nosuchcol' \
         returned {:?} — the column-list branch skips unknown names without \
         validation, so an all-NULL row was inserted instead of erroring",
        result
    );
    assert!(
        run_select(&catalog, "db10i", "SELECT a, b FROM t3").is_empty(),
        "no row may appear for a rejected insert"
    );
}

#[test]
fn i2_plan_cache_extra_value_in_column_list_is_silently_dropped() {
    let _ws = TestWorkspace::new("i2_extra_value");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10i2"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10i2",
        "t4",
        vec![col("a", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    // Two columns listed, two values given, but only one column exists —
    // value 2 targets the non-existent column 'b' and must be rejected.
    let result = execute_cached_insert(&catalog, "db10i2", "INSERT INTO t4 (a, b) VALUES (1, 2)");

    assert!(
        result.is_err(),
        "BUG I2 CONFIRMED: cached insert listing non-existent column 'b' \
         returned {:?} — the extra value is silently dropped and the row is \
         inserted with only column 'a' populated",
        result
    );
    assert!(
        run_select(&catalog, "db10i2", "SELECT a FROM t4").is_empty(),
        "no row may appear for a rejected insert"
    );
}

// ── Finding J: declared column DEFAULTs ignored by the cached path ──────────

#[test]
fn j_plan_cache_missing_column_ignores_declared_default() {
    let _ws = TestWorkspace::new("j_default");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10j"), "create db");
    // Deliberately NOT reloaded from disk: this test isolates the plan-cache
    // divergence from any DEFAULT persistence question — the in-memory
    // catalog carries the declared default and both paths see it.
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10j",
        "t5",
        vec![
            col("a", DataType::Int, true),
            col_with_default("b", DataType::Int, true, DataValue::Int(42)),
        ],
    )
    .unwrap();
    let catalog = catalog; // in-memory catalog with the declared default

    // Control: the planner path fills the declared default for unlisted
    // columns (planner/mod.rs expands missing columns to DEFAULT).
    let via_planner = run_planner_insert(&catalog, "db10j", "INSERT INTO t5 (a) VALUES (1)");
    assert_eq!(via_planner, 1, "planner control: insert succeeded");
    assert_eq!(
        run_select(&catalog, "db10j", "SELECT a, b FROM t5"),
        vec![vec!["1", "42"]],
        "planner control: declared DEFAULT 42 filled in for unlisted column b"
    );

    let result = execute_cached_insert(&catalog, "db10j", "INSERT INTO t5 (a) VALUES (2)");

    assert!(matches!(result, Ok(Some(1))), "cached insert succeeds");
    assert_eq!(
        run_select(&catalog, "db10j", "SELECT a, b FROM t5 WHERE a = 2"),
        vec![vec!["2", "42"]],
        "BUG J CONFIRMED: the cached path materialised unlisted column b as \
         the literal string 'NULL' instead of the declared DEFAULT 42"
    );
}

// ── Regression guard: the plain single-row cached insert still works ────────

#[test]
fn x_regression_plain_single_row_cached_insert_works() {
    let _ws = TestWorkspace::new("x_plain_insert");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db10x"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db10x",
        "plain",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    let first = execute_cached_insert(&catalog, "db10x", "INSERT INTO plain VALUES (7, 8)");
    assert!(matches!(first, Ok(Some(1))), "first (miss-path) insert");

    let second = execute_cached_insert(&catalog, "db10x", "INSERT INTO plain VALUES (7, 8)");
    assert!(matches!(second, Ok(Some(1))), "second (hit-path) insert");

    assert_eq!(
        run_select(&catalog, "db10x", "SELECT a, b FROM plain"),
        vec![vec!["7", "8"], vec!["7", "8"]],
        "plain single-row inserts via cache miss and hit both land"
    );
}
