//! Round-12 scratch verification tests for correctness issues found in a
//! twelfth manual deep-dive. Diagnostic probes like the round-1..11 suites —
//! each asserts the SQL-correct behaviour so a failure pinpoints the defect.
//!
//! Round-10/11 fixes verified green before hunting (all 15 tests pass):
//! plan-cache template split/multi-row/unknown-column/DEFAULT handling,
//! hash-join key canonicalization (Real/Double, Numeric/Int, Date/Timestamp),
//! SUM(BIGINT) overflow → Numeric widening, and big-int literal promotion.
//!
//! New findings under test:
//!
//!   N. `INSERT` with a `NULL` literal breaks the plan-cache path.
//!      `normalize_sql` (plan_cache.rs) parameterizes numbers and strings
//!      but leaves the `NULL` keyword as a Word token, so the extracted
//!      `params` vec excludes NULL positions. The fast template
//!      (`parse_insert_template`, plan_cache.rs:554) only counts
//!      Placeholder/Number/String tokens as values, so a row containing
//!      `NULL` yields `row_arities` sum ≠ params.len() → returns None. The
//!      fallback in `execute_cached_insert` (plan_cache.rs:724-737) then
//!      builds `arity: params.len()` (the NULL position NOT counted) while
//!      `row_arities` comes from the AST (NULL counted), and
//!      `CachedInsert::execute`'s `values.len() != total_values_expected`
//!      guard rejects the statement. `INSERT INTO t VALUES (1, NULL)` —
//!      perfectly valid SQL that the planner path executes — fails through
//!      the public plan-cache entry point (stress_bench.rs:307 route).
//!
//!   O. `parse_set_clause` accepts unknown expression tokens as column
//!      references (update.rs:356-358 "unquoted string ... column
//!      reference!"), but `apply_assignments_typed` (update.rs:152-155)
//!      silently SKIPS assignments whose source column does not exist
//!      (`continue`), and `update_by_pointers` reports the row as updated
//!      anyway. `UPDATE ... SET salarey = 5` (typo, or `SET x = 1, y = 2`
//!      where y is missing) mutates nothing yet reports success — the
//!      caller cannot distinguish "updated" from "assignment dropped".
//!
//!   P. `apply_assignments_typed`'s text-literal branch for NUMERIC
//!      (update.rs:160-164) maps parse failure to NULL
//!      (`.ok().map(DataValue::Numeric)`), silently NULLing the column
//!      instead of rejecting the update — same class as round-5 finding O.
//!      Control: the physical expression evaluator rejects invalid numeric
//!      casts outright.
//!
//!   Q. UPDATE SET arithmetic silently wraps/floats (update.rs:185-212):
//!      Int uses `(n as i64 + rhs_i) as i32` (wrapping cast) and Mul uses
//!      `(n as f64 * rhs_f) as i32` (saturating float cast), and BigInt Mul
//!      likewise rounds through f64 — while the SQL expression evaluator
//!      used by SELECT raises checked-overflow errors ("Integer addition
//!      overflow", expr/mod.rs:114-116). Same assignment, different
//!      overflow contract per statement type.
//!
//!   R. UPDATE SET `x = x / 0` returns the ORIGINAL value instead of
//!      erroring (update.rs:190,199,208,218,228 `if *rhs_f == 0.0 { n }`),
//!      while the SELECT evaluator raises "Division by zero"
//!      (expr/mod.rs:153-157) and numeric division errors too. SQL says
//!      division by zero is an error; here it silently no-ops.
//!
//!   S. `parse_set_clause` truncates BIGINT SET literals to i32 (update.rs:
//!      348-353): a value beyond i32 range becomes `ColumnValue::Text` and
//!      is then parsed by `parse_string_to_value` as the TARGET column's
//!      type — fine for BIGINT columns, but for an INT column the string
//!      path returns None → NULL. Control: planner INSERT into the same INT
//!      column with the same literal errors "out of range" instead of
//!      storing NULL.
//!
//! Controls (expected to PASS) document the planner / evaluator behaviour
//! for each scenario so the divergences cannot be explained away as engine
//! limits.

mod common;

use common::load_catalog;

use rook_parser::parse_set_clause;
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::backend::planner::plan_cache::execute_cached_insert;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::insert_single_tuple;
use storage_manager::executor::update::update_by_pointers;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr12", tag))
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
    create_table(&mut catalog, db, table, columns);
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

/// Read one row's cell via SELECT for the given WHERE clause.
fn cell(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    select_expr: &str,
    from: &str,
    r#where: &str,
) -> String {
    let rows = try_select(
        catalog,
        db,
        &format!("SELECT {} FROM {} WHERE {}", select_expr, from, r#where),
    )
    .expect("control select must execute");
    assert_eq!(rows.len(), 1, "expected exactly one matching row");
    rows[0][0].clone()
}

// ── Finding N: INSERT ... VALUES with NULL breaks the plan-cache path ────────

/// A rejected UPDATE now fails the statement instead of reporting success
/// with zero rows. Either way no row was updated, which is what these tests
/// go on to check.
fn rejected_as_zero(
    result: storage_manager::backend::error::RookResult<storage_manager::executor::UpdateResult>,
) -> storage_manager::executor::UpdateResult {
    result.unwrap_or_else(|_| storage_manager::executor::UpdateResult {
        updated_count: 0,
        returning_rows: Vec::new(),
    })
}

#[test]
fn n_plan_cache_insert_with_null_literal_is_rejected() {
    let _ws = TestWorkspace::new("n_null_insert");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12n"), "create db");
    make_table(
        "db12n",
        "t",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    );
    let catalog = load_catalog();

    // Control: the planner path inserts the row with b = NULL (valid SQL).
    let plan = rook_parser::parse_sql("INSERT INTO t VALUES (1, NULL)").expect("parse");
    let logical = plan_query(&plan, &catalog, "db12n").expect("plan");
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, &catalog, "db12n",
    )
    .expect("planner insert with NULL must execute");
    assert_eq!(tuples.len(), 1, "planner control: row inserted");
    assert_eq!(
        try_select(&catalog, "db12n", "SELECT a, b FROM t WHERE a = 1").expect("select"),
        vec![vec!["1", "NULL"]],
        "planner control: NULL stored in b"
    );

    // The public plan-cache entry point must handle the same statement.
    let result = execute_cached_insert(&catalog, "db12n", "INSERT INTO t VALUES (2, NULL)");

    assert_eq!(
        result,
        Ok(Some(1)),
        "BUG N CONFIRMED: cached INSERT with a NULL literal returned {:?} — \
         normalize_sql does not parameterize the NULL keyword, so \
         parse_insert_template sees a Word token it cannot count and bails; \
         the AST fallback then builds CachedInsert with arity = params.len() \
         (NULL not counted) while row_arities counts it, and the \
         values-len guard rejects valid SQL ('Expected 2 values ... got 1')",
        result
    );
    assert_eq!(
        try_select(&catalog, "db12n", "SELECT a, b FROM t WHERE a = 2").expect("select"),
        vec![vec!["2", "NULL"]],
        "the cached insert's row must store NULL in b"
    );
}

// ── Finding O: unknown SET column silently dropped, row reported updated ─────

#[test]
fn o_update_unknown_set_column_silently_dropped_row_reported_updated() {
    let _ws = TestWorkspace::new("o_unknown_col");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12o"), "create db");
    make_table(
        "db12o",
        "emp",
        vec![col("name", DataType::Varchar(50), true)],
    );
    insert("db12o", "emp", &["'alice'"]);
    let catalog = load_catalog();

    // `salarey` (typo) is not a column: parse_set_clause treats the RHS as
    // an assignment (it parses!), but apply_assignments_typed skips the
    // unknown target, and update_by_pointers still counts the row updated.
    let assignments = parse_set_clause("salarey = 5").expect("SET clause parses");
    let sel = parse_where_text("name = 'alice'").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12o", "emp", sel).expect("select");
    assert_eq!(ptrs.len(), 1, "control: one row matched");

    let result = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12o",
        "emp",
        &ptrs,
        &assignments,
    ));

    assert_eq!(
        result.updated_count, 0,
        "BUG O CONFIRMED: update_by_pointers reported {} row(s) updated although \
         the assignment targeted the non-existent column 'salarey' and mutated \
         nothing — the caller cannot distinguish 'updated' from 'assignment \
         silently dropped' (apply_assignments_typed `continue`s unknown columns)",
        result.updated_count
    );
}

#[test]
fn o2_update_mixed_valid_and_unknown_columns_reports_partial_work() {
    let _ws = TestWorkspace::new("o2_mixed");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12o2"), "create db");
    make_table(
        "db12o2",
        "emp",
        vec![col("name", DataType::Varchar(50), true)],
    );
    insert("db12o2", "emp", &["'bob'"]);
    let catalog = load_catalog();

    // One valid + one bogus assignment: the bogus one is dropped silently,
    // no error is surfaced, and the row still counts as updated.
    let assignments = parse_set_clause("name = 'bobby', nosuchcol = 1").expect("SET parses");
    let sel = parse_where_text("name = 'bob'").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12o2", "emp", sel).expect("select");

    let result = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12o2",
        "emp",
        &ptrs,
        &assignments,
    ));

    assert_eq!(
        result.updated_count, 0,
        "BUG O2 CONFIRMED: row reported updated ({:?}) although the unknown-column \
         assignment must make the statement fail like the SQL planner rejects \
         unknown columns elsewhere",
        result.updated_count
    );
    // The statement must fail: the row must remain unchanged.
    let name = cell(&catalog, "db12o2", "name", "emp", "name = 'bob'");
    assert_eq!(name, "'bob'", "statement must fail: row remains unchanged");
}

// ── Finding P: invalid NUMERIC SET literal silently NULLs the column ────────

#[test]
fn p_update_numeric_text_literal_parse_failure_silently_nulls() {
    let _ws = TestWorkspace::new("p_numeric_null");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12p"), "create db");
    make_table(
        "db12p",
        "acc",
        vec![col(
            "sal",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );
    insert("db12p", "acc", &["100.50"]);
    let catalog = load_catalog();

    // parse_set_clause classifies the unquoted non-numeric-looking token as a
    // column reference… but 'abc' IS numeric-parseable? No: "abc".parse::<f64>()
    // fails, so it lands in the Text-literal branch of apply_assignments_typed,
    // where parse_numeric_literal(...).ok() maps the parse failure to None.
    let assignments = parse_set_clause("sal = 'not a number'").expect("SET parses");
    let sel = parse_where_text("sal = 100.50").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12p", "acc", sel).expect("select");
    assert_eq!(ptrs.len(), 1, "control: row matched");

    let result = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12p",
        "acc",
        &ptrs,
        &assignments,
    ));

    // The update must be REJECTED (error / not applied), never silently NULL.
    let rows = try_select(&catalog, "db12p", "SELECT sal FROM acc").expect("select");
    assert!(
        rows.len() == 1 && rows[0][0] != "NULL",
        "BUG P CONFIRMED: SET sal = 'not a number' on a NUMERIC column silently \
         stored NULL (rows = {:?}, updated_count = {}) — parse_numeric_literal(..)\
         .ok().map(DataValue::Numeric) maps the parse failure to None instead of \
         rejecting the update (same class as round-5 finding O)",
        rows,
        result.updated_count
    );
}

// ── Finding Q: UPDATE SET arithmetic overflow silently wraps ────────────────

#[test]
fn q_update_set_int_overflow_wraps_instead_of_erroring() {
    let _ws = TestWorkspace::new("q_int_overflow");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12q"), "create db");
    make_table("db12q", "t", vec![col("n", DataType::Int, true)]);
    insert("db12q", "t", &["2147483647"]); // i32::MAX
    let catalog = load_catalog();

    // Control: SELECT-level arithmetic raises a checked-overflow error.
    let select_result = try_select(&catalog, "db12q", "SELECT n + 1 FROM t");
    assert!(
        select_result.is_err(),
        "control: SELECT evaluator rejects Int overflow (got {:?})",
        select_result
    );

    // UPDATE SET arithmetic must uphold the same contract.
    let assignments = parse_set_clause("n = n + 1").expect("SET parses");
    let sel = parse_where_text("n = 2147483647").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12q", "t", sel).expect("select");

    let _ = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12q",
        "t",
        &ptrs,
        &assignments,
    ));

    let n = try_select(&catalog, "db12q", "SELECT n FROM t").expect("select");
    assert_ne!(
        n[0][0], "-2147483648",
        "BUG Q CONFIRMED: SET n = n + 1 wrapped i32::MAX to i32::MIN — the \
         SELECT evaluator raises 'Integer addition overflow' for the identical \
         operation (checked-overflow contract), so UPDATE must error too"
    );
}

// ── Finding R: UPDATE SET division by zero returns the original value ───────

#[test]
fn r_update_set_division_by_zero_silently_returns_original() {
    let _ws = TestWorkspace::new("r_div_zero");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12r"), "create db");
    make_table("db12r", "t", vec![col("n", DataType::Int, true)]);
    insert("db12r", "t", &["10"]);
    let catalog = load_catalog();

    // Control: SELECT-level division by zero is an error.
    let select_result = try_select(&catalog, "db12r", "SELECT n / 0 FROM t");
    assert!(
        select_result.is_err(),
        "control: SELECT evaluator rejects division by zero (got {:?})",
        select_result
    );

    let assignments = parse_set_clause("n = n / 0").expect("SET parses");
    let sel = parse_where_text("n = 10").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12r", "t", sel).expect("select");

    let result = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12r",
        "t",
        &ptrs,
        &assignments,
    ));

    let n = try_select(&catalog, "db12r", "SELECT n FROM t").expect("select");
    assert!(
        n[0][0] == "10" && result.updated_count == 0,
        "BUG R CONFIRMED: SET n = n / 0 rewrote the row to {} (updated_count = {}) \
         — apply_assignments_typed's `if *rhs_f == 0.0 {{ n }}` returns the \
         original value instead of erroring like the SELECT evaluator \
         ('Division by zero'); SQL requires the statement to fail",
        n[0][0],
        result.updated_count
    );
    assert_ne!(
        n[0][0], "0",
        "BUG R variant: division by zero must never store 0"
    );
}

// ── Finding S: BIGINT SET literal truncated via the Text fallback ───────────

#[test]
fn s_update_set_bigint_literal_on_int_column_silently_nulls() {
    let _ws = TestWorkspace::new("s_bigint_literal");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12s"), "create db");
    make_table("db12s", "t", vec![col("n", DataType::Int, true)]);
    insert("db12s", "t", &["5"]);
    let catalog = load_catalog();

    // Control: INSERT with the same out-of-range literal is REJECTED.
    let insert_result = insert_single_tuple(&load_catalog(), "db12s", "t", &["3000000000"]);
    assert!(
        !insert_result.unwrap(),
        "control: INSERT of 3000000000 into INT must be rejected (out of range)"
    );

    // 3000000000 does not fit i32, so parse_set_clause's i32 branch misses,
    // its i64-range check fails, and the literal lands in the Text branch.
    let assignments = parse_set_clause("n = 3000000000").expect("SET parses");
    let sel = parse_where_text("n = 5").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12s", "t", sel).expect("select");

    let _ = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12s",
        "t",
        &ptrs,
        &assignments,
    ));

    let rows = try_select(&catalog, "db12s", "SELECT n FROM t WHERE n = 5").expect("select");
    assert!(
        !rows.is_empty(),
        "BUG S CONFIRMED: SET n = 3000000000 on an INT column silently stored NULL \
         (row with n = 5 vanished) — parse_set_clause demotes the literal to \
         ColumnValue::Text and parse_string_to_value(..).ok() maps the out-of-range \
         parse failure to None; INSERT with the same literal is rejected, so \
         UPDATE must be too"
    );
}

// ── Finding N (column-list variant) ─────────────────────────────────────────

#[test]
fn n2_plan_cache_insert_null_via_column_list_rejected_too() {
    // NULL via an explicit column list must behave exactly like the planner
    // path; the same arity mismatch breaks it.
    let _ws = TestWorkspace::new("n2_null_cols");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12n2"), "create db");
    make_table(
        "db12n2",
        "t",
        vec![col("a", DataType::Int, true), col("b", DataType::Int, true)],
    );
    let catalog = load_catalog();

    let result = execute_cached_insert(&catalog, "db12n2", "INSERT INTO t (a, b) VALUES (7, NULL)");
    assert_eq!(
        result,
        Ok(Some(1)),
        "BUG N2 CONFIRMED: cached INSERT with NULL via column list returned {:?} — \
         same normalize_sql NULL gap as finding N",
        result
    );
    assert_eq!(
        try_select(&catalog, "db12n2", "SELECT a, b FROM t").expect("select"),
        vec![vec!["7", "NULL"]],
        "NULL must be stored, not the literal string or 0"
    );
}

#[test]
fn x2_update_set_valid_arithmetic_still_works() {
    // Guard: in-range SET arithmetic keeps working.
    let _ws = TestWorkspace::new("x2_valid_arith");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12x2"), "create db");
    make_table("db12x2", "t", vec![col("n", DataType::Int, true)]);
    insert("db12x2", "t", &["10"]);
    let catalog = load_catalog();

    let assignments = parse_set_clause("n = n + 5").expect("SET parses");
    let sel = parse_where_text("n = 10").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "db12x2", "t", sel).expect("select");

    let result = rejected_as_zero(update_by_pointers(
        &catalog,
        "db12x2",
        "t",
        &ptrs,
        &assignments,
    ));
    assert_eq!(result.updated_count, 1, "row updated");
    assert_eq!(
        try_select(&catalog, "db12x2", "SELECT n FROM t").expect("select"),
        vec![vec!["15"]],
        "guard: in-range SET arithmetic applies"
    );
}

#[test]
fn x3_plan_cache_plain_inserts_still_work() {
    // Guard: the round-10 fast path still works (no NULLs involved).
    let _ws = TestWorkspace::new("x3_plain");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db12x3"), "create db");
    make_table(
        "db12x3",
        "t",
        vec![
            col("a", DataType::Int, true),
            col("b", DataType::Varchar(10), true),
        ],
    );
    let catalog = load_catalog();

    let first = execute_cached_insert(&catalog, "db12x3", "INSERT INTO t VALUES (1, 'x')");
    assert_eq!(first, Ok(Some(1)), "cache-miss plain insert");
    let second = execute_cached_insert(&catalog, "db12x3", "INSERT INTO t VALUES (2, 'y')");
    assert_eq!(second, Ok(Some(1)), "cache-hit plain insert");
    assert_eq!(
        try_select(&catalog, "db12x3", "SELECT a, b FROM t ORDER BY a").expect("select"),
        vec![vec!["1", "'x'"], vec!["2", "'y'"]],
        "guard: plain inserts still land (Display-quoting of varchar is pre-existing)"
    );
}
