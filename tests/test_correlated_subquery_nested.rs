//! Correlated subqueries: an outer reference that skips a level (a subquery
//! nested in a correlated subquery naming the outermost query) and outer
//! references in a correlated subquery's ORDER BY.

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
        Self(common::TestWorkspace::new("corrnest", tag))
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
    create_table(&mut catalog, db, table, int_cols(cols));
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
fn nested_subquery_binds_the_outermost_query() {
    let _ws = TestWorkspace::new("n_skip");
    setup_ab("dbn1");
    // c.k = a.y skips the b level.
    check(
        "dbn1",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND \
         EXISTS (SELECT 1 FROM c WHERE c.k = a.y)) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    // The middle subquery reaches the outermost query only through the nested one.
    check(
        "dbn1",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.z > 0 AND \
         EXISTS (SELECT 1 FROM c WHERE c.k = a.y AND b.x = a.x)) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    check(
        "dbn1",
        "SELECT a.x FROM a WHERE a.y IN (SELECT z FROM b WHERE b.x = a.x AND \
         b.z IN (SELECT k FROM c WHERE k >= a.y)) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
}

#[test]
fn nested_subquery_resolves_names_innermost_first() {
    let _ws = TestWorkspace::new("n_inner");
    setup_ab("dbn2");
    // c.k = b.z resolves in the middle scope, not the outermost one.
    check(
        "dbn2",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND \
         EXISTS (SELECT 1 FROM c WHERE c.k = b.z)) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
    // k is a column of c; a.y still reaches the outermost query.
    check(
        "dbn2",
        "SELECT a.x FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND \
         EXISTS (SELECT 1 FROM c WHERE k = a.y + 0)) ORDER BY a.x",
        &[&["1"], &["3"]],
    );
}

#[test]
fn outer_reference_in_order_by_is_bound() {
    let _ws = TestWorkspace::new("n_order");
    setup_ab("dbn3");
    // The b row whose z is closest to a.y.
    check(
        "dbn3",
        "SELECT a.x, (SELECT b.x FROM b ORDER BY abs(b.z - a.y) LIMIT 1) FROM a ORDER BY a.x",
        &[&["1", "1"], &["2", "5"], &["3", "3"]],
    );
    // Descending, correlated by WHERE as well.
    check(
        "dbn3",
        "SELECT a.x, (SELECT b.z FROM b WHERE b.x <= a.x ORDER BY b.z - a.y DESC LIMIT 1) \
         FROM a ORDER BY a.x",
        &[&["1", "10"], &["2", "99"], &["3", "99"]],
    );
}
