//! Round-14 scratch verification tests for correctness issues found in a
//! fourteenth manual deep-dive. Diagnostic probes like the round-1..13
//! suites — each asserts the SQL-correct behaviour so a failure pinpoints
//! the defect.
//!
//! Findings under test:
//!
//!   W. `CASE WHEN <comparison> THEN ...` cannot be parsed at all. The
//!      parser's `convert_expr` (rook-parser/src/utils.rs:974) handles only
//!      arithmetic BinaryOps (Plus/Minus/Multiply/Divide) and errors with
//!      "Unsupported expression" for comparison operators; CASE conditions
//!      are converted through `convert_expr`, so even
//!      `CASE WHEN 1 = 1 THEN 1 ELSE 0 END` — a constant-false/true
//!      condition — fails. Only the separate WHERE-clause path
//!      (`convert_predicate`) understands comparisons. CASE is therefore
//!      unusable with any real condition (in SELECT projections, WHERE
//!      clauses, HAVING, or INSERT .. SELECT), while the physical
//!      evaluator fully implements `Expr::Case` (expr/mod.rs:175) —
//!      the backend is ready, the parser is not.
//!
//!   X. `CAST(<non-integer numeric> AS INT)` errors instead of truncating.
//!      `cast()` (types/functions.rs:717) roundtrips through
//!      `value_to_literal` → `Display` ("1.75") →
//!      `parse_and_encode(INT)` → `i32::from_str`, which rejects the
//!      decimal point: `CAST(2.9 AS INT)` → error "INT value '2.9' is out
//!      of range". SQL:1999 (and PostgreSQL/MySQL) cast numeric→exact
//!      numeric by truncation toward zero: `CAST(2.9 AS INT)` → 2,
//!      `CAST(-2.9 AS INT)` → -2. This blocks `CAST(x AS INT)` for every
//!      fractional DOUBLE/REAL/NUMERIC value, while
//!      `CAST(x AS NUMERIC(8,2))` (the reverse direction) works.
//!
//!   Y. Positional ORDER BY is silently ignored. `ORDER BY 1` becomes
//!      `ExprNode::Constant(Int(1))`, which the physical Sort planner
//!      (planner/mod.rs:163) treats as a "complex expression": it projects
//!      a hidden `__sort_col_0` column with the constant 1 for every row
//!      and sorts on it — a stable no-op that returns rows in scan order.
//!      SQL interprets an unsigned integer in ORDER BY as the ordinal of
//!      the output column. `ORDER BY n` and `ORDER BY n + 0` both sort
//!      correctly (controls), so the constant case is specifically
//!      mis-interpreted.
//!
//!   Z. The SQL string-concatenation operator `||` is unsupported.
//!      `Expr::BinaryOp { op: StringConcat }` also falls into
//!      `convert_expr`'s "Unsupported expression" hole, so
//!      `SELECT s || 'x' FROM t` fails to parse. SQL:1999 defines `||` as
//!      the concatenation operator; the engine's `CONCAT()` function works
//!      (control), but the operator form is rejected.
//!
//! Controls (expected to PASS) document the working behaviour for each
//! scenario so the divergences cannot be explained away as engine limits.

mod common;

use common::load_catalog;

use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr14", tag))
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

fn setup_sc(db: &str, table: &str, rows: &[&[&str]]) {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db");
    make_table(
        db,
        table,
        vec![
            col("n", DataType::Int, true),
            col("s", DataType::Varchar(20), true),
        ],
    );
    for row in rows {
        insert(db, table, row);
    }
}

// ── Finding W: CASE WHEN <comparison> cannot be parsed ───────────────────────

#[test]
fn w_case_when_with_comparison_condition_parses_and_evaluates() {
    let _ws = TestWorkspace::new("w_case");

    setup_sc("db14w", "t", &[&["5", "'a'"], &["15", "'b'"]]);
    let catalog = load_catalog();

    // Control: the same comparison in WHERE works (convert_predicate path).
    let control = try_select(&catalog, "db14w", "SELECT COUNT(*) FROM t WHERE n > 10")
        .expect("control: WHERE comparison works");
    assert_eq!(
        control,
        vec![vec!["1".to_string()]],
        "control: WHERE comparison"
    );

    // CASE with a comparison condition must parse and evaluate.
    let result = try_select(
        &catalog,
        "db14w",
        "SELECT CASE WHEN n > 10 THEN 1 ELSE 0 END FROM t ORDER BY n",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["0".to_string()], vec!["1".to_string()]]),
        "BUG W CONFIRMED: CASE WHEN n > 10 ... returned {:?} — convert_expr \
         handles only arithmetic BinaryOps, so comparison conditions fail with \
         'Unsupported expression: BinaryOp{{op: Gt}}' although the physical \
         evaluator implements Expr::Case and the WHERE path proves comparisons \
         are convertible",
        result
    );
}

#[test]
fn w2_case_when_constant_condition_parses_too() {
    let _ws = TestWorkspace::new("w2_case_const");

    setup_sc("db14w2", "t", &[&["5", "'a'"]]);
    let catalog = load_catalog();

    let result = try_select(
        &catalog,
        "db14w2",
        "SELECT CASE WHEN 1 = 1 THEN 1 ELSE 0 END FROM t",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["1".to_string()]]),
        "BUG W2 CONFIRMED: even the constant condition `1 = 1` inside CASE \
         fails to parse ({:?}) — the gap is the expression converter, not the \
         column resolution",
        result
    );
}

// ── Finding X: CAST(numeric AS INT) rejects fractional values ────────────────

#[test]
fn x_cast_fractional_double_to_int_truncates() {
    let _ws = TestWorkspace::new("x_cast_int");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db14x"), "create db");
    make_table(
        "db14x",
        "t",
        vec![col("x", DataType::DoublePrecision, true)],
    );
    insert("db14x", "t", &["2.9"]);
    let catalog = load_catalog();

    // Control: CAST in the other direction works.
    let widen = try_select(&catalog, "db14x", "SELECT CAST(x AS NUMERIC(8,2)) FROM t")
        .expect("control: widen cast works");
    assert_eq!(
        widen,
        vec![vec!["2.90".to_string()]],
        "control: DOUBLE→NUMERIC cast"
    );

    let result = try_select(&catalog, "db14x", "SELECT CAST(x AS INT) FROM t");
    assert_eq!(
        result,
        Ok(vec![vec!["2".to_string()]]),
        "BUG X CONFIRMED: CAST(2.9 AS INT) returned {:?} — cast() roundtrips \
         through the literal '2.9' and i32::from_str rejects the decimal \
         point; SQL numeric→exact-numeric casts truncate toward zero (→ 2)",
        result
    );
}

#[test]
fn x2_cast_negative_fractional_double_to_int_truncates_toward_zero() {
    let _ws = TestWorkspace::new("x2_cast_neg");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db14x2"), "create db");
    make_table(
        "db14x2",
        "t",
        vec![col("x", DataType::DoublePrecision, true)],
    );
    insert("db14x2", "t", &["-2.9"]);
    let catalog = load_catalog();

    let result = try_select(&catalog, "db14x2", "SELECT CAST(x AS INT) FROM t");
    assert_eq!(
        result,
        Ok(vec![vec!["-2".to_string()]]),
        "BUG X2 CONFIRMED: CAST(-2.9 AS INT) returned {:?} — must truncate \
         toward zero (→ -2), not error",
        result
    );
}

// ── Finding Y: positional ORDER BY silently ignored ──────────────────────────

#[test]
fn y_order_by_positional_constant_sorts_by_output_column() {
    let _ws = TestWorkspace::new("y_order_pos");

    setup_sc("db14y", "t", &[&["3", "'c'"], &["1", "'a'"], &["2", "'b'"]]);
    let catalog = load_catalog();

    // Control: column-name and expression forms both sort.
    let by_name =
        try_select(&catalog, "db14y", "SELECT n FROM t ORDER BY n").expect("control: ORDER BY n");
    assert_eq!(
        by_name,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );

    let by_expr = try_select(&catalog, "db14y", "SELECT n FROM t ORDER BY n + 0")
        .expect("control: ORDER BY n + 0");
    assert_eq!(
        by_expr,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );

    // Positional form: ORDER BY 1 == ORDER BY (the 1st output column).
    let positional = try_select(&catalog, "db14y", "SELECT n FROM t ORDER BY 1");
    assert_eq!(
        positional,
        Ok(vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]),
        "BUG Y CONFIRMED: `SELECT n FROM t ORDER BY 1` returned {:?} — the \
         constant 1 is projected as a hidden __sort_col_0 with value 1 for \
         every row, so the sort is a stable no-op and rows keep scan order \
         instead of SQL's positional interpretation",
        positional
    );
}

// ── Finding Z: || string-concat operator unsupported ─────────────────────────

#[test]
fn z_string_concat_operator_parses_and_concatenates() {
    let _ws = TestWorkspace::new("z_concat");

    setup_sc("db14z", "t", &[&["1", "'ab'"]]);
    let catalog = load_catalog();

    // Control: CONCAT() function works.
    let func = try_select(&catalog, "db14z", "SELECT CONCAT(s, 'x') FROM t")
        .expect("control: CONCAT() works");
    assert_eq!(
        func,
        vec![vec!["'abx'".to_string()]],
        "control: CONCAT() function"
    );

    let op = try_select(&catalog, "db14z", "SELECT s || 'x' FROM t");
    assert_eq!(
        op,
        Ok(vec![vec!["'abx'".to_string()]]),
        "BUG Z CONFIRMED: `SELECT s || 'x'` returned {:?} — StringConcat \
         BinaryOps fall into convert_expr's 'Unsupported expression' hole; \
         SQL:1999 defines || as the concatenation operator",
        op
    );
}

// ── Regression guards ────────────────────────────────────────────────────────

#[test]
fn x3_non_fractional_and_string_int_casts_still_work() {
    // Guard: casts that already work keep working after any fix.
    let _ws = TestWorkspace::new("x3_cast_ok");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db14x3"));
    make_table(
        "db14x3",
        "t",
        vec![col("x", DataType::DoublePrecision, true)],
    );
    insert("db14x3", "t", &["100.0"]);
    let catalog = load_catalog();

    let int = try_select(&catalog, "db14x3", "SELECT CAST(x AS INT) FROM t")
        .expect("integral double → INT cast works");
    assert_eq!(int, vec![vec!["100".to_string()]]);

    let from_str = try_select(&catalog, "db14x3", "SELECT CAST('42' AS INT)")
        .expect("string → INT cast works");
    assert_eq!(from_str, vec![vec!["42".to_string()]]);

    let big = try_select(&catalog, "db14x3", "SELECT CAST(1.0e2 AS INT)")
        .expect("scientific literal → INT cast works");
    assert_eq!(big, vec![vec!["100".to_string()]]);
}

#[test]
fn y2_limit_offset_and_in_still_work() {
    // Guard: LIMIT/OFFSET, IN, BETWEEN, NOT IN with NULLs keep working.
    let _ws = TestWorkspace::new("y2_limit");

    setup_sc(
        "db14y2",
        "t",
        &[
            &["3", "'c'"],
            &["1", "'a'"],
            &["2", "'b'"],
            &["5", "'e'"],
            &["4", "'d'"],
        ],
    );
    let catalog = load_catalog();

    let limit = try_select(
        &catalog,
        "db14y2",
        "SELECT n FROM t ORDER BY n LIMIT 2 OFFSET 1",
    )
    .expect("limit/offset");
    assert_eq!(limit, vec![vec!["2".to_string()], vec!["3".to_string()]]);

    let in_list = try_select(
        &catalog,
        "db14y2",
        "SELECT COUNT(*) FROM t WHERE n IN (1, 2, 3)",
    )
    .expect("IN list");
    assert_eq!(in_list, vec![vec!["3".to_string()]]);

    let between = try_select(
        &catalog,
        "db14y2",
        "SELECT COUNT(*) FROM t WHERE n BETWEEN 2 AND 4",
    )
    .expect("BETWEEN");
    assert_eq!(between, vec![vec!["3".to_string()]]);

    let not_in_null = try_select(
        &catalog,
        "db14y2",
        "SELECT COUNT(*) FROM t WHERE n NOT IN (1, NULL)",
    )
    .expect("NOT IN with NULL");
    assert_eq!(
        not_in_null,
        vec![vec!["0".to_string()]],
        "guard: three-valued NOT IN"
    );
}
