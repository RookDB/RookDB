//! Correlated subqueries run the whole subquery, not a one-table special case:
//! the SELECT list, GROUP BY / aggregates / LIMIT, JOINs and extra FROM tables,
//! non-correlation conjuncts, and outer references inside expressions.

mod common;

use common::load_catalog;

use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("corrscope", tag))
    }
}

fn int_cols(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|n| Column {
            name: n.to_string(),
            data_type: DataType::Int,
            nullable: true,
            constraints: Constraints::default(),
        })
        .collect()
}

fn make_table(db: &str, table: &str, cols: &[&str], rows: &[&[&str]]) {
    let mut catalog = load_catalog();
    create_table(&mut catalog, db, table, int_cols(cols)).unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();
    for row in rows {
        assert!(insert_single_tuple(&catalog, db, table, row).unwrap());
    }
}

fn try_select(db: &str, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let catalog = load_catalog();
    let plan = rook_parser::parse_sql(sql).map_err(|e| e.to_string())?;
    let logical = plan_query(&plan, &catalog, db).map_err(|e| e.to_string())?;
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, &catalog, db,
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

fn rows(r: &[&[&str]]) -> Result<Vec<Vec<String>>, String> {
    Ok(r.iter()
        .map(|row| row.iter().map(|s| s.to_string()).collect())
        .collect())
}

/// a(x, y): (1,10) (2,20) (3,30);  b(x, z): (1,10) (2,99) (3,30) (5,20)
fn setup_ab(db: &str) {
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db));
    make_table(
        db,
        "a",
        &["x", "y"],
        &[&["1", "10"], &["2", "20"], &["3", "30"]],
    );
    make_table(
        db,
        "b",
        &["x", "z"],
        &[&["1", "10"], &["2", "99"], &["3", "30"], &["5", "20"]],
    );
    make_table(db, "c", &["k"], &[&["10"], &["30"]]);
}

fn check(db: &str, sql: &str, expected: &[&[&str]]) {
    assert_eq!(try_select(db, sql), rows(expected), "{}", sql);
}

#[test]
fn correlated_in_compares_the_select_list() {
    let _ws = TestWorkspace::new("g_in_sel");
    setup_ab("dbg1");
    // z is not b's first column.
    check(
        "dbg1",
        "SELECT a.x FROM a WHERE a.y IN (SELECT z FROM b WHERE b.x = a.x) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    check(
        "dbg1",
        "SELECT a.x FROM a WHERE a.y NOT IN (SELECT z FROM b WHERE b.x = a.x) ORDER BY a.x",
        &[&["2"]],
    );
}

#[test]
fn correlated_in_honours_aggregate_group_by_and_limit() {
    let _ws = TestWorkspace::new("g_in_agg");
    setup_ab("dbg2");
    check(
        "dbg2",
        "SELECT a.x FROM a WHERE a.y IN (SELECT max(z) FROM b WHERE b.x <= a.x) ORDER BY a.x",
        &[&["1"]],
    );
    check(
        "dbg2",
        "SELECT a.x FROM a WHERE a.y IN \
         (SELECT z FROM b WHERE b.x <= a.x ORDER BY z LIMIT 1) ORDER BY a.x",
        &[&["1"]],
    );
    check(
        "dbg2",
        "SELECT a.x FROM a WHERE a.y IN \
         (SELECT max(z) FROM b WHERE b.x <= a.x GROUP BY b.x HAVING max(z) < 50) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
}

#[test]
fn correlated_subqueries_see_joins_and_extra_from_tables() {
    let _ws = TestWorkspace::new("g_join");
    setup_ab("dbg3");
    for sql in [
        "SELECT a.x FROM a WHERE EXISTS \
         (SELECT 1 FROM b JOIN c ON b.z = c.k WHERE b.x = a.x) ORDER BY a.x",
        "SELECT a.x FROM a WHERE EXISTS \
         (SELECT 1 FROM b, c WHERE b.z = c.k AND b.x = a.x) ORDER BY a.x",
        "SELECT a.x FROM a WHERE a.y IN \
         (SELECT c.k FROM b JOIN c ON b.z = c.k WHERE b.x = a.x) ORDER BY a.x",
        "SELECT a.x FROM a WHERE (SELECT count(*) FROM b JOIN c ON b.z = c.k \
         WHERE b.x = a.x) = 1 ORDER BY a.x",
    ] {
        check("dbg3", sql, &[&["1"], &["3"]]);
    }
    check(
        "dbg3",
        "SELECT a.x, (SELECT count(*) FROM b, c WHERE b.z = c.k AND b.x = a.x) \
         FROM a ORDER BY a.x",
        &[&["1", "1"], &["2", "0"], &["3", "1"]],
    );
}

#[test]
fn outer_reference_inside_an_expression_is_correlated() {
    let _ws = TestWorkspace::new("g_expr");
    setup_ab("dbg4");
    for pred in [
        "b.z = y + 0",
        "y = b.z",
        "b.z BETWEEN y AND y",
        "b.z IN (y)",
        "b.z + 0 = a.y * 1",
        "b.z = CAST(y AS INT)",
    ] {
        check(
            "dbg4",
            &format!("SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE {pred}) ORDER BY a.x"),
            &[&["1"], &["2"], &["3"]],
        );
        check(
            "dbg4",
            &format!(
                "SELECT a.x FROM a WHERE a.x IN (SELECT b.x FROM b WHERE {pred}) ORDER BY a.x"
            ),
            &[&["1"], &["3"]],
        );
    }
    check(
        "dbg4",
        "SELECT a.x, (SELECT count(*) FROM b WHERE b.z = y + 0) FROM a ORDER BY a.x",
        &[&["1", "1"], &["2", "1"], &["3", "1"]],
    );
}

#[test]
fn local_conjuncts_and_or_inside_a_correlated_subquery_are_applied() {
    let _ws = TestWorkspace::new("g_local");
    setup_ab("dbg5");
    // The b.z > 15 conjunct must not be dropped: b(1,10) fails it.
    check(
        "dbg5",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND b.z > 15) ORDER BY a.x",
        &[&["2"], &["3"]],
    );
    // OR is not a conjunction of correlations.
    check(
        "dbg5",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x OR b.z = 20) ORDER BY a.x",
        &[&["1"], &["2"], &["3"]],
    );
    check(
        "dbg5",
        "SELECT a.x FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND b.z = 99) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
}

#[test]
fn in_subquery_or_predicate_over_a_join() {
    let _ws = TestWorkspace::new("g_or_join");
    setup_ab("dbg6");
    // Uncorrelated and correlated forms, OR'd with a join-side predicate.
    check(
        "dbg6",
        "SELECT a.x FROM a JOIN c ON a.y = c.k \
         WHERE a.x IN (SELECT x FROM b WHERE z = 10) OR c.k = 30 ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    check(
        "dbg6",
        "SELECT a.x FROM a JOIN c ON a.y = c.k \
         WHERE a.x IN (SELECT b.x FROM b WHERE b.z = a.y) OR c.k = 100 ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    check(
        "dbg6",
        "SELECT a.x FROM a JOIN c ON a.y = c.k \
         WHERE EXISTS (SELECT 1 FROM b WHERE b.z = a.y) OR c.k = 100 ORDER BY a.x",
        &[&["1"], &["3"]],
    );
}

#[test]
fn not_in_with_nulls_stays_three_valued() {
    let _ws = TestWorkspace::new("g_nulls");
    let db = "dbg7";
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db));
    make_table(
        db,
        "a",
        &["x", "y"],
        &[&["1", "10"], &["2", "20"], &["3", "30"]],
    );
    make_table(
        db,
        "b",
        &["x", "z"],
        &[&["1", "NULL"], &["2", "5"], &["3", "30"]],
    );
    // a1: inner {NULL} -> UNKNOWN; a2: {5} -> 20 NOT IN -> true; a3: {30} -> false.
    check(
        db,
        "SELECT a.x FROM a WHERE a.y NOT IN (SELECT z FROM b WHERE b.x = a.x) ORDER BY a.x",
        &[&["2"]],
    );
    check(
        db,
        "SELECT a.x FROM a WHERE NOT (a.y IN (SELECT z FROM b WHERE b.x = a.x)) ORDER BY a.x",
        &[&["2"]],
    );
    check(
        db,
        "SELECT a.x FROM a WHERE a.y IN (SELECT z FROM b WHERE b.x = a.x) ORDER BY a.x",
        &[&["3"]],
    );
}

#[test]
fn outer_on_the_left_of_an_inequality_is_not_reversed() {
    let _ws = TestWorkspace::new("g_dir");
    setup_ab("dbg8");
    // a.x > b.x: a(1) has no smaller b.x; a(2), a(3) do.
    check(
        "dbg8",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE a.x > b.x) ORDER BY a.x",
        &[&["2"], &["3"]],
    );
    check(
        "dbg8",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x < a.x) ORDER BY a.x",
        &[&["2"], &["3"]],
    );
}
