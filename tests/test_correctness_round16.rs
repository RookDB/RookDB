//! Round-16 verification tests for correctness issues found in a sixteenth
//! manual deep-dive. Each test asserts the SQL-correct behaviour so a failure
//! pinpoints the defect.
//!
//! Findings under test:
//!
//!   AE. The optimizer pushes single-side WHERE predicates into the
//!       null-supplying side of outer joins, producing silently wrong
//!       results. `push_filter_down` (planner/optimizer/mod.rs:377) checks
//!       only *which side* a predicate references (`on_left`/`on_right`)
//!       and never checks `j.join_type` before pushing. For
//!       `emp RIGHT JOIN dept ... WHERE emp.id IS NULL` the predicate is
//!       pushed into the emp scan, filtering it to zero rows; the join then
//!       null-extends every dept row, so the IS NULL filter matches all 3
//!       dept rows instead of 1. The mirror case
//!       `emp LEFT JOIN dept ... WHERE dept.name = 'ops'` pushes into the
//!       dept scan and resurrects filtered-out emp rows as NULL-extended
//!       rows (4 rows instead of 2). FULL OUTER is affected on both sides.
//!       Pushing into the *preserved* side is safe (LEFT: left child,
//!       RIGHT: right child); only INNER/CROSS/NATIVE allow both.
//!
//!   AF. Correlated scalar subqueries are silently evaluated as
//!       UNCORRELATED. `materialize_scalar_subquery`
//!       (physical/planner/subqueries.rs:174) plans the subquery with a
//!       fresh `plan_query` call that has no outer scope, so the outer
//!       reference `e2.dept = e.dept` is dropped/never resolved. In WHERE,
//!       `sal = (SELECT MAX(sal) FROM emp e2 WHERE e2.dept = e.dept)`
//!       degenerates to the global max — it returns only the global-max row
//!       instead of each department's max (silent wrong results). In the
//!       SELECT list the same correlation errors outright
//!       (`Err("Column 'dept' not found")`), and in HAVING it silently
//!       degenerates again (returns nothing instead of every group).
//!       Control: uncorrelated scalar subqueries work everywhere, and
//!       correlated EXISTS (handled by a different code path) works.
//!
//!   AG. CTEs are invisible outside the main FROM clause. `plan_select`
//!       (planner/mod.rs:291) plans each CTE body via
//!       `plan_select(&cte_def.query, ...)` WITHOUT passing
//!       `existing_ctes`, so a later CTE cannot reference an earlier one
//!       (`WITH a AS (...), b AS (SELECT ... FROM a)` →
//!       `Err("Table 'a' not found")`). The same standalone-planning hole
//!       makes CTE references fail inside set-operation branches
//!       (each branch is planned as an independent select) and inside
//!       EXISTS / IN / scalar subqueries of the outer query (subqueries are
//!       planned via `plan_query` with no CTE registry). Control: a CTE
//!       referenced from the main FROM clause works, including joins.
//!
//! Controls (expected to PASS) document the working behaviour for each
//! scenario so the divergences cannot be explained away as engine limits:
//! LEFT JOIN with a preserved-side predicate, INNER JOIN with a two-sided
//! predicate, RIGHT JOIN with a preserved-side predicate, uncorrelated
//! scalar subqueries in SELECT/WHERE, correlated EXISTS, basic CTE usage
//! from the main FROM clause, aggregate NULL semantics, set-op NULL
//! dedup, and LIMIT edge cases.

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
        Self(common::TestWorkspace::new("cr16", tag))
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

/// dept(d, name): (10,'eng') (20,'ops') (30,'hr')
/// emp(id, dept, sal): (1,10,100) (2,20,200) (3,20,300) (4,40,400)
///
/// dept 20 has NO matching row in dept only for d=40; dept 30 has no emp.
fn setup_emp_dept(db: &str) {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db {}", db);
    make_table(
        db,
        "dept",
        vec![
            col("d", DataType::Int, true),
            col("name", DataType::Varchar(10), true),
        ],
    );
    make_table(
        db,
        "emp",
        vec![
            col("id", DataType::Int, true),
            col("dept", DataType::Int, true),
            col("sal", DataType::Int, true),
        ],
    );
    for row in [["10", "'eng'"], ["20", "'ops'"], ["30", "'hr'"]] {
        insert(db, "dept", &row);
    }
    for row in [
        ["1", "10", "100"],
        ["2", "20", "200"],
        ["3", "20", "300"],
        ["4", "40", "400"],
    ] {
        insert(db, "emp", &row);
    }
}

/// t(a INT NULL, b VARCHAR NULL):
/// (1,'x') (2,'y') (NULL,'z') (2,NULL) (1,'x')
fn setup_t(db: &str) {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db), "create db {}", db);
    make_table(
        db,
        "t",
        vec![
            col("a", DataType::Int, true),
            col("b", DataType::Varchar(10), true),
        ],
    );
    for row in [
        ["1", "'x'"],
        ["2", "'y'"],
        ["NULL", "'z'"],
        ["2", "NULL"],
        ["1", "'x'"],
    ] {
        insert(db, "t", &row);
    }
}

// ── Finding AE: predicate pushdown into the null-supplying side of outer joins ──

#[test]
fn ae_right_join_is_null_predicate_not_pushed_below_join() {
    let _ws = TestWorkspace::new("ae_right");
    setup_emp_dept("db16ae");
    let catalog = load_catalog();

    // Control: the unfiltered RIGHT JOIN has exactly one emp.id IS NULL row.
    let control = try_select(
        &catalog,
        "db16ae",
        "SELECT emp.id, dept.d FROM emp RIGHT JOIN dept ON emp.dept = dept.d ORDER BY dept.d",
    )
    .expect("control: right join works");
    assert_eq!(
        control,
        vec![
            vec!["1".to_string(), "10".to_string()],
            vec!["2".to_string(), "20".to_string()],
            vec!["3".to_string(), "20".to_string()],
            vec!["NULL".to_string(), "30".to_string()],
        ],
        "control: unfiltered right join shape"
    );

    let result = try_select(
        &catalog,
        "db16ae",
        "SELECT COUNT(*) FROM emp RIGHT JOIN dept ON emp.dept = dept.d WHERE emp.id IS NULL",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["1".to_string()]]),
        "BUG AE CONFIRMED: `RIGHT JOIN ... WHERE emp.id IS NULL` returned {:?} — \
         expected 1 (only dept 30 lacks an emp). push_filter_down pushed the \
         single-side predicate into the emp scan (null-supplying side) without \
         checking join_type, filtering emp to zero rows so every dept row is \
         null-extended and matches IS NULL",
        result
    );
}

#[test]
fn ae2_left_and_full_join_single_side_predicates_not_pushed() {
    let _ws = TestWorkspace::new("ae2_left_full");
    setup_emp_dept("db16ae2");
    let catalog = load_catalog();

    // LEFT JOIN with a predicate on the null-supplying (right) side:
    // SQL keeps only rows whose joined dept.name = 'ops' (emp 2 and 3);
    // pushing `dept.name = 'ops'` into the dept scan resurrects emp 1 and 4
    // as NULL-extended rows.
    let left = try_select(
        &catalog,
        "db16ae2",
        "SELECT COUNT(*) FROM emp LEFT JOIN dept ON emp.dept = dept.d WHERE dept.name = 'ops'",
    );
    assert_eq!(
        left,
        Ok(vec![vec!["2".to_string()]]),
        "BUG AE2 CONFIRMED (left): `LEFT JOIN ... WHERE dept.name = 'ops'` \
         returned {:?} — expected 2; predicate was pushed into the dept scan",
        left
    );

    // Row-level shape: no NULL-name rows may survive the WHERE.
    let rows = try_select(
        &catalog,
        "db16ae2",
        "SELECT emp.id, dept.name FROM emp LEFT JOIN dept ON emp.dept = dept.d WHERE dept.name = 'ops' ORDER BY emp.id",
    );
    assert_eq!(
        rows,
        Ok(vec![
            vec!["2".to_string(), "'ops'".to_string()],
            vec!["3".to_string(), "'ops'".to_string()],
        ]),
        "BUG AE2 CONFIRMED (left rows): NULL-name rows survived the WHERE — {:?}",
        rows
    );

    // FULL OUTER JOIN with a predicate on either side must not be pushed.
    let full = try_select(
        &catalog,
        "db16ae2",
        "SELECT COUNT(*) FROM emp FULL OUTER JOIN dept ON emp.dept = dept.d WHERE emp.id IS NULL",
    );
    assert_eq!(
        full,
        Ok(vec![vec!["1".to_string()]]),
        "BUG AE2 CONFIRMED (full): `FULL OUTER JOIN ... WHERE emp.id IS NULL` \
         returned {:?} — expected 1; predicate was pushed into the emp scan",
        full
    );
}

// ── Finding AF: correlated scalar subqueries silently uncorrelated ───────────

#[test]
fn af_correlated_scalar_subquery_in_where_is_correlated() {
    let _ws = TestWorkspace::new("af_where");
    setup_emp_dept("db16af");
    let catalog = load_catalog();

    // Control: uncorrelated scalar subquery in WHERE works (returns the
    // global-max row).
    let control = try_select(
        &catalog,
        "db16af",
        "SELECT id FROM emp WHERE sal = (SELECT MAX(sal) FROM emp)",
    )
    .expect("control: uncorrelated scalar subquery in WHERE");
    assert_eq!(control, vec![vec!["4".to_string()]]);

    // The correlated form must return each department's top earner:
    // dept 10 → 100 (id 1), dept 20 → 300 (id 3), dept 40 → 400 (id 4).
    let result = try_select(
        &catalog,
        "db16af",
        "SELECT id FROM emp e WHERE sal = (SELECT MAX(sal) FROM emp e2 WHERE e2.dept = e.dept) ORDER BY id",
    );
    assert_eq!(
        result,
        Ok(vec![
            vec!["1".to_string()],
            vec!["3".to_string()],
            vec!["4".to_string()],
        ]),
        "BUG AF CONFIRMED: correlated scalar subquery in WHERE returned {:?} — \
         expected ids 1,3,4. materialize_scalar_subquery plans the subquery \
         with a fresh plan_query that has no outer scope, so the correlation \
         predicate is dropped and the subquery degenerates to the GLOBAL max \
         (silent wrong results)",
        result
    );
}

#[test]
fn af2_correlated_scalar_in_select_list_and_having() {
    let _ws = TestWorkspace::new("af2_select_having");
    setup_emp_dept("db16af2");
    let catalog = load_catalog();

    // Correlated scalar subquery in the SELECT list: per-dept max salary.
    let select_list = try_select(
        &catalog,
        "db16af2",
        "SELECT id, (SELECT MAX(sal) FROM emp e2 WHERE e2.dept = emp.dept) FROM emp ORDER BY id",
    );
    assert_eq!(
        select_list,
        Ok(vec![
            vec!["1".to_string(), "100".to_string()],
            vec!["2".to_string(), "300".to_string()],
            vec!["3".to_string(), "300".to_string()],
            vec!["4".to_string(), "400".to_string()],
        ]),
        "BUG AF2 CONFIRMED (select list): correlated scalar subquery in the \
         SELECT list returned {:?} — expected per-dept maxima 100/200/300/400 \
         (the historical failure mode was `Column 'dept' not found`)",
        select_list
    );

    // Correlated scalar subquery in HAVING: every group satisfies
    // COUNT(*) = (its own size), so all three groups must be returned.
    let having = try_select(
        &catalog,
        "db16af2",
        "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) = (SELECT COUNT(*) FROM emp e2 WHERE e2.dept = emp.dept) ORDER BY dept",
    );
    assert_eq!(
        having,
        Ok(vec![
            vec!["10".to_string(), "1".to_string()],
            vec!["20".to_string(), "2".to_string()],
            vec!["40".to_string(), "1".to_string()],
        ]),
        "BUG AF2 CONFIRMED (having): correlated scalar subquery in HAVING \
         returned {:?} — expected all three groups; the correlation silently \
         degenerated (uncorrelated count never matches a per-group count)",
        having
    );
}

// ── Finding AG: CTEs invisible outside the main FROM clause ──────────────────

#[test]
fn ag_later_cte_can_reference_earlier_cte() {
    let _ws = TestWorkspace::new("ag_chain");
    setup_t("db16ag");
    let catalog = load_catalog();

    // Control: a CTE referenced from the main FROM clause works.
    let control = try_select(
        &catalog,
        "db16ag",
        "WITH a AS (SELECT a FROM t WHERE a IS NOT NULL) SELECT COUNT(*) FROM a",
    )
    .expect("control: single CTE from main FROM");
    assert_eq!(control, vec![vec!["4".to_string()]]);

    // Chained CTE: b references a. t.a IS NOT NULL → [1,2,2,1]; a > 1 → [2,2].
    let result = try_select(
        &catalog,
        "db16ag",
        "WITH a AS (SELECT a FROM t WHERE a IS NOT NULL), b AS (SELECT a FROM a WHERE a > 1) SELECT COUNT(*) FROM b",
    );
    assert_eq!(
        result,
        Ok(vec![vec!["2".to_string()]]),
        "BUG AG CONFIRMED: chained CTE `WITH a AS (...), b AS (SELECT ... FROM a)` \
         returned {:?} — expected 2. plan_select plans each CTE body without \
         passing existing_ctes, so earlier CTEs are not in scope \
         (`Table 'a' not found`)",
        result
    );
}

#[test]
fn ag2_cte_visible_in_setop_branches_and_subqueries() {
    let _ws = TestWorkspace::new("ag2_scope");
    setup_t("db16ag2");
    let catalog = load_catalog();

    // CTE referenced from both branches of UNION ALL: a = [1,2,2,1] → 8 rows.
    let union_all = try_select(
        &catalog,
        "db16ag2",
        "WITH a AS (SELECT a FROM t WHERE a IS NOT NULL) SELECT a FROM a UNION ALL SELECT a FROM a",
    );
    assert_eq!(
        union_all.as_ref().map(|r| r.len()),
        Ok(8),
        "BUG AG2 CONFIRMED (union branch): CTE reference inside a set-op branch \
         failed — {:?}",
        union_all
    );

    // CTE referenced from both branches of UNION (dedup): single row [1].
    let union_dedup = try_select(
        &catalog,
        "db16ag2",
        "WITH a AS (SELECT a FROM t WHERE a = 1) SELECT a FROM a UNION SELECT a FROM a",
    );
    assert_eq!(
        union_dedup,
        Ok(vec![vec!["1".to_string()]]),
        "BUG AG2 CONFIRMED (union dedup): CTE reference inside set-op branches \
         failed — {:?}",
        union_dedup
    );

    // CTE referenced inside an IN subquery of the outer query:
    // big = [2,2]; t rows with a IN (2,2) → the two a=2 rows.
    let in_sub = try_select(
        &catalog,
        "db16ag2",
        "WITH big AS (SELECT a FROM t WHERE a > 1) SELECT COUNT(*) FROM t WHERE a IN (SELECT a FROM big)",
    );
    assert_eq!(
        in_sub,
        Ok(vec![vec!["2".to_string()]]),
        "BUG AG2 CONFIRMED (in-subquery): CTE reference inside an IN subquery \
         failed — {:?}",
        in_sub
    );

    // CTE referenced inside an EXISTS subquery of the outer query:
    // big = [1,1]; t rows with EXISTS(big.a = t.a) → the two a=1 rows.
    let exists_sub = try_select(
        &catalog,
        "db16ag2",
        "WITH big AS (SELECT a FROM t WHERE a = 1) SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM big WHERE big.a = t.a)",
    );
    assert_eq!(
        exists_sub,
        Ok(vec![vec!["2".to_string()]]),
        "BUG AG2 CONFIRMED (exists): CTE reference inside an EXISTS subquery \
         failed — {:?}",
        exists_sub
    );
}

// ── Regression guards (expected to PASS) ─────────────────────────────────────

#[test]
fn x1_preserved_side_pushdown_and_inner_join_still_work() {
    let _ws = TestWorkspace::new("x1_guard");
    setup_emp_dept("db16x1");
    let catalog = load_catalog();

    // LEFT JOIN with a predicate on the PRESERVED (left) side: safe pushdown.
    // emp.sal > 150 → ids 2,3,4.
    let left = try_select(
        &catalog,
        "db16x1",
        "SELECT COUNT(*) FROM emp LEFT JOIN dept ON emp.dept = dept.d WHERE emp.sal > 150",
    )
    .expect("left-side predicate on LEFT JOIN");
    assert_eq!(left, vec![vec!["3".to_string()]]);

    // RIGHT JOIN with a predicate on the PRESERVED (right) side.
    // dept.name = 'hr' → dept 30, which has no emp → one NULL-extended row.
    let right = try_select(
        &catalog,
        "db16x1",
        "SELECT emp.id, dept.d FROM emp RIGHT JOIN dept ON emp.dept = dept.d WHERE dept.name = 'hr'",
    )
    .expect("right-side predicate on RIGHT JOIN");
    assert_eq!(right, vec![vec!["NULL".to_string(), "30".to_string()]]);

    // INNER JOIN with a two-sided predicate: merge-into-condition path.
    let inner = try_select(
        &catalog,
        "db16x1",
        "SELECT emp.id FROM emp JOIN dept ON emp.dept = dept.d WHERE dept.name = 'ops' ORDER BY emp.id",
    )
    .expect("inner join both-side predicate");
    assert_eq!(inner, vec![vec!["2".to_string()], vec!["3".to_string()]]);
}

#[test]
fn x2_uncorrelated_subqueries_and_correlated_exists_still_work() {
    let _ws = TestWorkspace::new("x2_guard");
    setup_emp_dept("db16x2");
    let catalog = load_catalog();

    // Uncorrelated scalar subquery in SELECT list.
    let scalar = try_select(
        &catalog,
        "db16x2",
        "SELECT id, (SELECT MAX(sal) FROM emp) FROM emp WHERE id = 1",
    )
    .expect("uncorrelated scalar subquery in SELECT list");
    assert_eq!(scalar, vec![vec!["1".to_string(), "400".to_string()]]);

    // Uncorrelated scalar subquery in WHERE.
    let where_scalar = try_select(
        &catalog,
        "db16x2",
        "SELECT id FROM emp WHERE sal > (SELECT AVG(sal) FROM emp) ORDER BY id",
    )
    .expect("uncorrelated scalar subquery in WHERE");
    // AVG = 250 → ids 3 (300) and 4 (400).
    assert_eq!(
        where_scalar,
        vec![vec!["3".to_string()], vec!["4".to_string()]]
    );

    // Correlated EXISTS (different code path — works).
    let exists = try_select(
        &catalog,
        "db16x2",
        "SELECT id FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE d.d = e.dept) ORDER BY id",
    )
    .expect("correlated EXISTS");
    assert_eq!(
        exists,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );
}

#[test]
fn x3_basic_cte_and_aggregate_null_semantics_still_work() {
    let _ws = TestWorkspace::new("x3_guard");
    setup_emp_dept("db16x3");
    setup_t("db16x3t");
    let catalog = load_catalog();

    // CTE joined against a real table.
    let cte_join = try_select(
        &catalog,
        "db16x3",
        "WITH big AS (SELECT dept FROM emp WHERE sal > 150) SELECT COUNT(*) FROM big JOIN dept ON big.dept = dept.d",
    )
    .expect("CTE joined to table");
    // big = [20, 20, 40] → dept matches for 20, 20 (40 has no dept) → 2.
    assert_eq!(cte_join, vec![vec!["2".to_string()]]);

    // Aggregates over an empty set.
    let empty = try_select(
        &catalog,
        "db16x3",
        "SELECT COUNT(*), SUM(sal), AVG(sal), MIN(sal), MAX(sal) FROM emp WHERE id = 99",
    )
    .expect("aggregates over empty set");
    assert_eq!(
        empty,
        vec![vec![
            "0".to_string(),
            "NULL".to_string(),
            "NULL".to_string(),
            "NULL".to_string(),
            "NULL".to_string(),
        ]]
    );

    // Set-op NULL dedup and UNION dedup.
    let union_nulls = try_select(&catalog, "db16x3t", "SELECT a FROM t UNION SELECT a FROM t")
        .expect("union null dedup");
    assert_eq!(
        union_nulls,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["NULL".to_string()],
        ]
    );

    // LIMIT edge cases.
    let limit0 =
        try_select(&catalog, "db16x3", "SELECT id FROM emp ORDER BY id LIMIT 0").expect("limit 0");
    assert_eq!(limit0, Vec::<Vec<String>>::new());
    let limit_off = try_select(
        &catalog,
        "db16x3",
        "SELECT id FROM emp ORDER BY id LIMIT 2 OFFSET 10",
    )
    .expect("limit offset past end");
    assert_eq!(limit_off, Vec::<Vec<String>>::new());
}
