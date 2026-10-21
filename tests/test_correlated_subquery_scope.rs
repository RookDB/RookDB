//! Scoping of outer references inside correlated subqueries.
//!
//!   1. An unqualified outer column inside an IN subquery is correlated
//!      (names resolve innermost-first: the subquery's FROM, then the outer
//!      query), the same as in EXISTS and scalar subqueries.
//!   2. A qualified outer reference in a SELECT-list subquery honours its
//!      qualifier when two outer tables share the column name.

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
}

#[test]
fn unqualified_outer_column_in_in_subquery_is_correlated() {
    let _ws = TestWorkspace::new("in_unqual");
    setup_ab("dbcs1");
    // `y` exists only in the outer table a. Row-wise: a.x IN (b.x where b.z = a.y)
    //  a(1,10): b.z=10 -> {1}   -> match
    //  a(2,20): b.z=20 -> {5}   -> no
    //  a(3,30): b.z=30 -> {3}   -> match
    assert_eq!(
        try_select(
            "dbcs1",
            "SELECT a.x FROM a WHERE a.x IN (SELECT b.x FROM b WHERE b.z = y) ORDER BY a.x"
        ),
        rows(&[&["1"], &["3"]])
    );
    // NOT IN is the complement.
    assert_eq!(
        try_select(
            "dbcs1",
            "SELECT a.x FROM a WHERE a.x NOT IN (SELECT b.x FROM b WHERE b.z = y) ORDER BY a.x"
        ),
        rows(&[&["2"]])
    );
}

#[test]
fn unqualified_name_in_both_inner_and_outer_resolves_to_inner() {
    let _ws = TestWorkspace::new("in_inner_first");
    setup_ab("dbcs2");
    // `x` exists in both a and b: it is b.x. `z = x` is then an uncorrelated
    // inner filter (b rows where z = x: (1,10)? no; none have z = x).
    // Add a b-row with z = x to make the result non-empty and distinctive.
    make_table("dbcs2", "c", &["x", "z"], &[&["4", "4"], &["6", "7"]]);
    assert_eq!(
        try_select(
            "dbcs2",
            "SELECT a.x FROM a WHERE a.y IN (SELECT z FROM c WHERE x = z)"
        ),
        rows(&[])
    );
    // x = z on c holds for (4,4); a.y IN (4) matches nothing, but if `x` had
    // been taken from the outer a the filter would differ. Pin with a match:
    make_table("dbcs2", "d", &["x", "z"], &[&["10", "10"], &["20", "5"]]);
    assert_eq!(
        try_select(
            "dbcs2",
            "SELECT a.x FROM a WHERE a.y IN (SELECT z FROM d WHERE x = z) ORDER BY a.x"
        ),
        rows(&[&["1"]])
    );
}

#[test]
fn ambiguous_unqualified_outer_column_in_in_subquery_errors() {
    let _ws = TestWorkspace::new("in_ambig");
    setup_ab("dbcs3");
    make_table("dbcs3", "a2", &["x", "y"], &[&["1", "10"]]);
    let r = try_select(
        "dbcs3",
        "SELECT a.x FROM a JOIN a2 ON a.x = a2.x WHERE a.x IN (SELECT b.x FROM b WHERE b.z = y)",
    );
    let err = r.expect_err("ambiguous outer column must error");
    assert!(
        err.contains("Column reference 'y' is ambiguous"),
        "unexpected error: {err}"
    );
}

#[test]
fn qualified_outer_ref_in_select_list_subquery_honours_qualifier() {
    let _ws = TestWorkspace::new("sel_qual");
    let db = "dbcs4";
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db));
    make_table(db, "l", &["id"], &[&["1"], &["2"]]);
    make_table(db, "r", &["id", "l_id"], &[&["10", "1"], &["20", "2"]]);
    make_table(db, "t", &["k"], &[&["10"], &["10"], &["20"]]);
    // r.id is 10 / 20 -> counts 2 / 1. (Binding l.id would give 0 / 0.)
    assert_eq!(
        try_select(
            db,
            "SELECT l.id, (SELECT count(*) FROM t WHERE t.k = r.id) FROM l JOIN r ON l.id = r.l_id ORDER BY l.id"
        ),
        rows(&[&["1", "2"], &["2", "1"]])
    );
    // And the other qualifier picks the other column.
    assert_eq!(
        try_select(
            db,
            "SELECT l.id, (SELECT count(*) FROM t WHERE t.k = l.id) FROM l JOIN r ON l.id = r.l_id ORDER BY l.id"
        ),
        rows(&[&["1", "0"], &["2", "0"]])
    );
}

#[test]
fn unqualified_outer_column_from_the_other_join_side_is_found() {
    let _ws = TestWorkspace::new("in_join_side");
    setup_ab("dbcs5");
    make_table("dbcs5", "a3", &["x", "w"], &[&["1", "100"], &["2", "200"]]);
    // The IN's left side reads a3; `y` belongs to a. The predicate must not be
    // pushed below the join onto a3, where `y` does not exist.
    //  join rows: (a=1,y=10,w=100) (a=2,y=20,w=200)
    //  a.y=10 -> b.z=10 -> {1}; a3.x=1 matches.  a.y=20 -> {5}; a3.x=2 no.
    assert_eq!(
        try_select(
            "dbcs5",
            "SELECT a3.w FROM a3 JOIN a ON a.x = a3.x \
             WHERE a3.x IN (SELECT b.x FROM b WHERE b.z = y) ORDER BY a3.w"
        ),
        rows(&[&["100"]])
    );
}

#[test]
fn aliased_inner_table_still_resolves_unqualified_names_inner_first() {
    let _ws = TestWorkspace::new("in_alias_inner");
    let db = "dbcs6";
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, db));
    make_table(db, "p", &["x", "y"], &[&["1", "4"], &["2", "9"]]);
    make_table(db, "q", &["x", "z"], &[&["4", "1"], &["9", "9"]]);
    // `x` exists in the subquery's table q (as qq) and in the outer p: it is
    // qq.x. So the filter is `qq.z = qq.x` -> q row (9,9) -> {9}, and only
    // p(2,9) has y in that set. Binding `x` to p.x would give p(1,4) instead.
    for from in ["q qq", "q"] {
        let inner = if from == "q" { "q" } else { "qq" };
        assert_eq!(
            try_select(
                db,
                &format!(
                    "SELECT p.x FROM p WHERE p.y IN \
                     (SELECT {inner}.x FROM {from} WHERE {inner}.z = x)"
                )
            ),
            rows(&[&["2"]]),
            "FROM {from}"
        );
    }
}
