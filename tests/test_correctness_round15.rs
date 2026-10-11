//! Round-15 verification tests for correctness issues found in a fifteenth
//! manual deep-dive. Diagnostic probes like the round-1..14 suites — each
//! asserts the SQL-correct behaviour so a failure pinpoints the defect.
//!
//! Findings under test:
//!
//!   AA. Scalar subqueries cannot be used inside WHERE/HAVING predicates.
//!       `SELECT id FROM emp WHERE sal = (SELECT MAX(sal) FROM emp)` fails
//!       with `Err("Scalar subqueries must be materialized before
//!       expr_from_ast")`. The predicate planner
//!       (physical/planner/subqueries.rs:25, `build_predicate_with_subqueries`)
//!       handles `Exists` and `InSubquery` nodes but delegates every other
//!       predicate (including `Compare` and `Between`) to `predicate_from_ast`
//!       → `expr_from_ast`, which rejects `ScalarSubquery` nodes
//!       (expr/convert.rs:75). The *projection* path materializes scalar
//!       subqueries (planner/mod.rs:719 `plan_projection_expr`), so the
//!       identical subquery works in the SELECT list — the predicate path
//!       simply never calls `materialize_nested_subqueries`. Confirmed
//!       broken in every position: `sal = (sub)`, `(sub) = sal`,
//!       `sal = (sub) + 0`, `sal BETWEEN (sub) AND (sub)`, and scalar
//!       subqueries inside HAVING.
//!
//!   AB. Set operations accept mismatched arity and emit ragged rows.
//!       `SELECT id, sal FROM emp UNION SELECT dept FROM emp` returns rows
//!       of width 2 interleaved with rows of width 1 instead of erroring.
//!       SQL requires all set-operation branches to have the same number of
//!       columns; neither the logical planner (planner/mod.rs:203 — plans
//!       both sides independently), the physical planner (physical/planner/
//!       mod.rs:261 — takes the left child's schema verbatim), nor
//!       `SetOpOperator` (operators/set_op.rs — concatenates/drains tuples
//!       positionally) validates arity.
//!
//!   AC. Positional GROUP BY is rejected with a misleading error.
//!       `SELECT dept, COUNT(*) FROM emp GROUP BY 1 ORDER BY dept` fails
//!       with `Err("Column 'dept' not found")` although `dept` is the first
//!       output column and plainly exists; `GROUP BY dept` (control) works.
//!       The logical plan carries `group_by` expressions verbatim
//!       (planner/mod.rs:362) and nothing resolves an unsigned-integer
//!       constant to the corresponding output-column ordinal. This pairs
//!       with round-14's finding Y (positional ORDER BY silently ignored):
//!       SQL-standard positional references are mis-handled in both clauses,
//!       differently.
//!
//!   AD. `IS NOT DISTINCT FROM` is unsupported although `IS DISTINCT FROM`
//!       works. `WHERE sal IS NOT DISTINCT FROM 100` fails with
//!       `Err("Unsupported predicate expression: IsNotDistinctFrom(..)")`:
//!       the parser converts `Expr::IsDistinctFrom` (rook-parser/src/
//!       utils.rs:1425) but has no arm for `Expr::IsNotDistinctFrom`, so the
//!       negated form falls into the `_ => Err("Unsupported predicate
//!       expression")` hole (utils.rs:1479/1481). The physical evaluator
//!       already implements the predicate (`Predicate::IsDistinctFrom`,
//!       predicate.rs:410) and `Predicate::not` exists — only the parser arm
//!       is missing. The negated form is the more commonly used of the two.
//!
//! Controls (expected to PASS) document the working behaviour for each
//! scenario so the divergences cannot be explained away as engine limits:
//! predicate three-valued-logic (NULL propagation through AND/OR/NOT,
//! NULL = n exclusion, BETWEEN bounds), INTERSECT / EXCEPT / INTERSECT ALL,
//! matching-arity UNION deduplication, and GROUP BY by column name.
//!
//! Minor note (no test): `REPLACE()` is not implemented as a scalar function
//! ("Unknown scalar function: 'REPLACE'") — a missing feature rather than a
//! correctness bug, unlike every finding above.

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
        Self(common::TestWorkspace::new("cr15", tag))
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

/// emp(id, dept, sal): (1,10,100) (2,10,200) (3,20,300) (4,20,400)
fn setup_emp(db: &str) {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db {}", db);
    make_table(
        db,
        "emp",
        vec![
            col("id", DataType::Int, true),
            col("dept", DataType::Int, true),
            col("sal", DataType::Int, true),
        ],
    );
    for row in [
        ["1", "10", "100"],
        ["2", "10", "200"],
        ["3", "20", "300"],
        ["4", "20", "400"],
    ] {
        insert(db, "emp", &row);
    }
}

// ── Finding AA: scalar subqueries in WHERE/HAVING predicates fail ────────────

#[test]
fn aa_scalar_subquery_in_where_comparison_works() {
    let _ws = TestWorkspace::new("aa_scalar");
    setup_emp("db15aa");
    let catalog = load_catalog();

    // Control: the identical subquery in the SELECT list is materialized fine.
    let control = try_select(
        &catalog,
        "db15aa",
        "SELECT id, (SELECT MAX(sal) FROM emp) FROM emp WHERE id = 1",
    )
    .expect("control: scalar subquery in SELECT list works");
    assert_eq!(control, vec![vec!["1".to_string(), "400".to_string()]]);

    let result = try_select(
        &catalog,
        "db15aa",
        "SELECT id FROM emp WHERE sal = (SELECT MAX(sal) FROM emp)",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["4".to_string()]]),
        "BUG AA CONFIRMED: `WHERE sal = (SELECT MAX(sal) FROM emp)` returned \
         {:?} — build_predicate_with_subqueries never materializes \
         ScalarSubquery nodes before delegating Compare to expr_from_ast, \
         while the projection path materializes the identical subquery",
        result
    );
}

#[test]
fn aa2_scalar_subquery_positions_both_sides_and_between() {
    let _ws = TestWorkspace::new("aa2_scalar_pos");
    setup_emp("db15aa2");
    let catalog = load_catalog();

    // Subquery on the left-hand side of the comparison.
    let lhs = try_select(
        &catalog,
        "db15aa2",
        "SELECT id FROM emp WHERE (SELECT MAX(sal) FROM emp) = sal",
    );
    assert_eq!(
        lhs,
        Ok(vec![vec!["4".to_string()]]),
        "BUG AA2 CONFIRMED (lhs): reversed comparison also fails — {:?}",
        lhs
    );

    // Subquery inside arithmetic on the predicate.
    let arith = try_select(
        &catalog,
        "db15aa2",
        "SELECT id FROM emp WHERE sal = (SELECT MAX(sal) FROM emp) + 0",
    );
    assert_eq!(
        arith,
        Ok(vec![vec!["4".to_string()]]),
        "BUG AA2 CONFIRMED (arith): subquery inside arithmetic also fails — {:?}",
        arith
    );

    // Subqueries as BETWEEN bounds.
    let between = try_select(
        &catalog,
        "db15aa2",
        "SELECT COUNT(*) FROM emp WHERE sal BETWEEN (SELECT MIN(sal) FROM emp) AND (SELECT MAX(sal) FROM emp)",
    );
    assert_eq!(
        between,
        Ok(vec![vec!["4".to_string()]]),
        "BUG AA2 CONFIRMED (between): scalar subqueries as BETWEEN bounds also fail — {:?}",
        between
    );

    // Scalar subquery inside HAVING.
    let having = try_select(
        &catalog,
        "db15aa2",
        "SELECT dept FROM emp GROUP BY dept HAVING MAX(sal) > (SELECT AVG(sal) FROM emp) ORDER BY dept",
    );
    assert_eq!(
        having,
        Ok(vec![vec!["20".to_string()]]),
        "BUG AA2 CONFIRMED (having): scalar subquery inside HAVING also fails — {:?}",
        having
    );
}

// ── Finding AB: set operations accept mismatched arity ───────────────────────

#[test]
fn ab_set_operation_mismatched_arity_is_rejected() {
    let _ws = TestWorkspace::new("ab_arity");
    setup_emp("db15ab");
    let catalog = load_catalog();

    // Control: matching arity dedups correctly.
    let control = try_select(
        &catalog,
        "db15ab",
        "SELECT dept FROM emp UNION SELECT dept FROM emp ORDER BY dept",
    )
    .expect("control: matching-arity UNION works");
    assert_eq!(
        control,
        vec![vec!["10".to_string()], vec!["20".to_string()]]
    );

    let result = try_select(
        &catalog,
        "db15ab",
        "SELECT id, sal FROM emp UNION SELECT dept FROM emp",
    );
    assert!(
        result.is_err(),
        "BUG AB CONFIRMED: mismatched-arity UNION returned {:?} — SQL requires \
         equal column counts per branch; today the operator concatenates \
         2-wide and 1-wide rows into one ragged result (no layer validates)",
        result
    );
}

// ── Finding AC: positional GROUP BY is rejected ──────────────────────────────

#[test]
fn ac_positional_group_by_resolves_output_ordinal() {
    let _ws = TestWorkspace::new("ac_groupby_pos");
    setup_emp("db15ac");
    let catalog = load_catalog();

    // Control: GROUP BY by column name works.
    let control = try_select(
        &catalog,
        "db15ac",
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept ORDER BY dept",
    )
    .expect("control: GROUP BY dept works");
    assert_eq!(
        control,
        vec![
            vec!["10".to_string(), "2".to_string()],
            vec!["20".to_string(), "2".to_string()]
        ]
    );

    let result = try_select(
        &catalog,
        "db15ac",
        "SELECT dept, COUNT(*) FROM emp GROUP BY 1 ORDER BY dept",
    );
    assert_eq!(
        result,
        Ok(vec![
            vec!["10".to_string(), "2".to_string()],
            vec!["20".to_string(), "2".to_string()]
        ]),
        "BUG AC CONFIRMED: `GROUP BY 1` returned {:?} — an unsigned integer \
         must be resolved to the corresponding output-column ordinal \
         (dept); today it fails with the misleading error \"Column 'dept' \
         not found\" although GROUP BY dept works. Pairs with round-14 Y \
         (positional ORDER BY silently ignored)",
        result
    );
}

// ── Finding AD: IS NOT DISTINCT FROM unsupported ─────────────────────────────

#[test]
fn ad_is_not_distinct_from_is_supported() {
    let _ws = TestWorkspace::new("ad_indf");
    setup_emp("db15ad");
    let catalog = load_catalog();

    // Control: the non-negated form parses and evaluates.
    let control = try_select(
        &catalog,
        "db15ad",
        "SELECT COUNT(*) FROM emp WHERE sal IS DISTINCT FROM NULL",
    )
    .expect("control: IS DISTINCT FROM works");
    assert_eq!(control, vec![vec!["4".to_string()]]);

    let result = try_select(
        &catalog,
        "db15ad",
        "SELECT COUNT(*) FROM emp WHERE sal IS NOT DISTINCT FROM 100",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["1".to_string()]]),
        "BUG AD CONFIRMED: `IS NOT DISTINCT FROM` returned {:?} — the parser \
         converts Expr::IsDistinctFrom but has no arm for \
         Expr::IsNotDistinctFrom, so the negated form falls into the \
         \"Unsupported predicate expression\" hole even though the physical \
         evaluator implements the predicate and Predicate::not exists",
        result
    );
}

// ── Regression guards ────────────────────────────────────────────────────────

/// Documents correct three-valued-logic behaviour (all verified working in
/// round-15 probing) so predicate-evaluator regressions surface here.
#[test]
fn x1_predicate_three_valued_logic_stays_correct() {
    let _ws = TestWorkspace::new("x1_tvl");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db15x1"), "create db");
    make_table(
        "db15x1",
        "t",
        vec![col("n", DataType::Int, true), col("z", DataType::Int, true)],
    );
    insert("db15x1", "t", &["7", "0"]);
    insert("db15x1", "t", &["NULL", "5"]);
    let catalog = load_catalog();

    // NULL = NULL is UNKNOWN → row excluded.
    let eq = try_select(&catalog, "db15x1", "SELECT COUNT(*) FROM t WHERE n = n").unwrap();
    assert_eq!(eq, vec![vec!["1".to_string()]]);

    // NOT (UNKNOWN) is UNKNOWN → NULL row excluded.
    let not = try_select(
        &catalog,
        "db15x1",
        "SELECT COUNT(*) FROM t WHERE NOT (n > 5)",
    )
    .unwrap();
    assert_eq!(not, vec![vec!["0".to_string()]]);

    // UNKNOWN AND FALSE is FALSE… but here: NULL > 5 AND z < 0 → row 2 excluded.
    let and = try_select(
        &catalog,
        "db15x1",
        "SELECT COUNT(*) FROM t WHERE n > 5 AND z < 0",
    )
    .unwrap();
    assert_eq!(and, vec![vec!["0".to_string()]]);

    // NULL arithmetic propagates.
    let null_add = try_select(&catalog, "db15x1", "SELECT n + 1 FROM t WHERE z = 5").unwrap();
    assert_eq!(null_add, vec![vec!["NULL".to_string()]]);

    // Integer division truncates; division by zero errors.
    let div = try_select(&catalog, "db15x1", "SELECT n / 2 FROM t WHERE n = 7").unwrap();
    assert_eq!(div, vec![vec!["3".to_string()]]);
    let div0 = try_select(&catalog, "db15x1", "SELECT n / z FROM t WHERE n = 7");
    assert!(div0.is_err(), "division by zero must error, got {:?}", div0);
}

/// Documents correct INTERSECT/EXCEPT behaviour (verified working) so set-op
/// fixes for AB cannot regress the matching-arity paths.
#[test]
fn x2_intersect_except_matching_arity_stay_correct() {
    let _ws = TestWorkspace::new("x2_setops");
    setup_emp("db15x2");
    let catalog = load_catalog();

    let inter = try_select(
        &catalog,
        "db15x2",
        "SELECT dept FROM emp WHERE id <= 2 INTERSECT SELECT dept FROM emp WHERE id >= 2",
    )
    .unwrap();
    assert_eq!(inter, vec![vec!["10".to_string()]]);

    let inter_all = try_select(
        &catalog,
        "db15x2",
        "SELECT dept FROM emp WHERE id <= 3 INTERSECT ALL SELECT dept FROM emp WHERE id >= 2",
    )
    .unwrap();
    assert_eq!(
        inter_all,
        vec![vec!["10".to_string()], vec!["20".to_string()]]
    );

    let except = try_select(&catalog, "db15x2", "SELECT dept FROM emp EXCEPT SELECT 20").unwrap();
    assert_eq!(except, vec![vec!["10".to_string()]]);
}
