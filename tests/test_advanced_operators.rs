//! Advanced-operator integration tests: JOINs, aggregation, set operations,
//! subqueries, CTEs and INSERT INTO ... SELECT driven end-to-end through the
//! Volcano engine (SQL string -> planners -> operator tree).
//!
//! Run with: cargo test --test test_advanced_operators -- --test-threads=1

mod common;

use common::load_catalog;
use rook_ast::{QueryPlan, SelectPlan};
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

/// Create `employees` (5 rows) and `departments` (3 rows) and return catalog.
///
/// employees(id, name, salary, dept_id)  departments(id, dept_name)
/// ─────────────────────────────────────  ────────────────────────────
/// 1 Alice 85000 10                        10 Engineering
/// 2 Bob   62000 20                        20 Sales
/// 3 Cara  75000 10                        30 HR
/// 4 Dan   48000 99   <- dangling FK       (no department 99 on purpose,
/// 5 Eve   91000 20                         so LEFT JOIN has a NULL side)
fn setup_tables(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "employees",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("salary", DataType::Int),
            column("dept_id", DataType::Int),
        ],
    );
    create_table(
        &mut catalog,
        db,
        "departments",
        vec![
            column("id", DataType::Int),
            column("dept_name", DataType::Varchar(50)),
        ],
    );
    save_catalog(&catalog).unwrap();

    for row in [
        vec!["1", "Alice", "85000", "10"],
        vec!["2", "Bob", "62000", "20"],
        vec!["3", "Cara", "75000", "10"],
        vec!["4", "Dan", "48000", "99"],
        vec!["5", "Eve", "91000", "20"],
    ] {
        insert_single_tuple(&catalog, db, "employees", &row).unwrap();
    }
    for row in [
        vec!["10", "Engineering"],
        vec!["20", "Sales"],
        vec!["30", "HR"],
    ] {
        insert_single_tuple(&catalog, db, "departments", &row).unwrap();
    }
    load_catalog()
}

/// Run any SELECT-shaped query through both planners and render as strings.
fn run_query(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let plan = parse_sql(sql).expect("parse failed");
    let logical =
        storage_manager::planner::plan_query(&plan, catalog, db).expect("logical planning failed");
    let tuples: Vec<Tuple> = execute_plan_collect(&logical, catalog, db).expect("execution failed");
    tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(dv) => format!("{}", dv),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

/// Same as [`run_query`] but asserts the parsed plan shape first.
fn run_select(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    match parse_sql(sql).unwrap() {
        QueryPlan::Select(SelectPlan { .. }) => run_query(catalog, db, sql),
        other => panic!("expected Select plan, got {:?}", other),
    }
}

// ── JOINs ─────────────────────────────────────────────────────────────────────

#[test]
fn inner_join_matches_rows() {
    let _ws = common::TestWorkspace::new("advops", "ijoin");
    let catalog = setup_tables("join_db");

    let out = run_select(
        &catalog,
        "join_db",
        "SELECT name, dept_name FROM employees \
         JOIN departments ON employees.dept_id = departments.id",
    );
    assert_eq!(
        out.len(),
        4,
        "Alice+Cara and Bob+Eve match; Dan's dangling 99 must not"
    );
    assert_eq!(out[0][0], "'Alice'");
    assert_eq!(out[0][1], "'Engineering'");
}

#[test]
fn left_join_pads_missing_right_side() {
    let _ws = common::TestWorkspace::new("advops", "ljoin");
    let catalog = setup_tables("join_db");

    let out = run_select(
        &catalog,
        "join_db",
        "SELECT name, dept_name FROM employees \
         LEFT JOIN departments ON employees.dept_id = departments.id",
    );
    // Full-table check: every employee exactly once with the right values,
    // unmatched right side padded with NULL. (No ORDER BY => compare sets.)
    let mut got = out.clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            vec!["'Alice'", "'Engineering'"],
            vec!["'Bob'", "'Sales'"],
            vec!["'Cara'", "'Engineering'"],
            vec!["'Dan'", "NULL"],
            vec!["'Eve'", "'Sales'"],
        ]
    );
}

#[test]
fn cross_join_produces_cartesian_product() {
    let _ws = common::TestWorkspace::new("advops", "cjoin");
    let catalog = setup_tables("join_db");

    let out = run_select(
        &catalog,
        "join_db",
        "SELECT name, dept_name FROM employees CROSS JOIN departments",
    );
    assert_eq!(out.len(), 15, "5 employees x 3 departments");
}

// ── Aggregation ───────────────────────────────────────────────────────────────

#[test]
fn global_aggregates() {
    let _ws = common::TestWorkspace::new("advops", "agg");
    let catalog = setup_tables("agg_db");

    let out = run_select(
        &catalog,
        "agg_db",
        "SELECT COUNT(*), SUM(salary), MIN(salary), MAX(salary) FROM employees",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], "5");
    assert_eq!(out[0][1], "361000"); // 85000+62000+75000+48000+91000
    assert_eq!(out[0][2], "48000");
    assert_eq!(out[0][3], "91000");
}

#[test]
fn group_by_with_having() {
    let _ws = common::TestWorkspace::new("advops", "group");
    let catalog = setup_tables("agg_db");

    let out = run_select(
        &catalog,
        "agg_db",
        "SELECT dept_id, COUNT(*) FROM employees \
         GROUP BY dept_id HAVING COUNT(*) >= 2 ORDER BY dept_id",
    );
    // dept 10 (Alice, Cara) and dept 20 (Bob, Eve); dept 99 has one row.
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], vec!["10", "2"]);
    assert_eq!(out[1], vec!["20", "2"]);
}

#[test]
fn count_skips_nulls_in_column_mode() {
    let _ws = common::TestWorkspace::new("advops", "countnull");
    let catalog = setup_tables("cnt_db");

    let out = run_select(
        &catalog,
        "cnt_db",
        "SELECT COUNT(dept_name) FROM departments",
    );
    assert_eq!(out[0][0], "3");
}

// ── Set operations ────────────────────────────────────────────────────────────

#[test]
fn union_all_keeps_duplicates() {
    let _ws = common::TestWorkspace::new("advops", "unionall");
    let catalog = setup_tables("set_db");

    let out = run_query(
        &catalog,
        "set_db",
        "SELECT dept_id FROM employees WHERE dept_id = 10 \
         UNION ALL SELECT id FROM departments WHERE id = 10",
    );
    assert_eq!(out.len(), 3, "two matching employees + one department row");
}

#[test]
fn union_deduplicates() {
    let _ws = common::TestWorkspace::new("advops", "union");
    let catalog = setup_tables("set_db");

    let out = run_query(
        &catalog,
        "set_db",
        "SELECT dept_id FROM employees WHERE dept_id = 10 \
         UNION SELECT id FROM departments WHERE id = 10",
    );
    assert_eq!(
        out,
        vec![vec!["10"]],
        "UNION collapses duplicates to one '10'"
    );
}

#[test]
fn except_removes_matching_rows() {
    let _ws = common::TestWorkspace::new("advops", "except");
    let catalog = setup_tables("set_db");

    let out = run_query(
        &catalog,
        "set_db",
        "SELECT id FROM employees EXCEPT SELECT id FROM departments",
    );
    assert_eq!(
        out.len(),
        5,
        "employee ids never collide with department ids"
    );
}

// ── Subqueries ────────────────────────────────────────────────────────────────

#[test]
fn in_subquery_filters() {
    let _ws = common::TestWorkspace::new("advops", "insub");
    let catalog = setup_tables("sub_db");

    let out = run_select(
        &catalog,
        "sub_db",
        "SELECT name FROM employees \
         WHERE dept_id IN (SELECT id FROM departments WHERE dept_name = 'Engineering')",
    );
    assert_eq!(out.len(), 2, "Alice and Cara work in Engineering");
}

#[test]
fn exists_subquery_is_boolean_filter() {
    let _ws = common::TestWorkspace::new("advops", "exists");
    let catalog = setup_tables("sub_db");

    let out = run_select(
        &catalog,
        "sub_db",
        "SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM departments)",
    );
    assert_eq!(out.len(), 5, "EXISTS over a non-empty table keeps everyone");
}

#[test]
fn scalar_subquery_in_projection() {
    let _ws = common::TestWorkspace::new("advops", "scalar");
    let catalog = setup_tables("sub_db");

    let out = run_select(
        &catalog,
        "sub_db",
        "SELECT name, (SELECT AVG(salary) FROM employees) FROM employees WHERE id = 1",
    );
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][1], "72200"); // 361000 / 5
}

// ── CTEs ──────────────────────────────────────────────────────────────────────

#[test]
fn non_recursive_cte_materialises_once() {
    let _ws = common::TestWorkspace::new("advops", "cte");
    let catalog = setup_tables("cte_db");

    let out = run_query(
        &catalog,
        "cte_db",
        "WITH high_earners AS (\
             SELECT name, salary FROM employees WHERE salary > 70000\
         ) SELECT name FROM high_earners WHERE salary < 90000",
    );
    assert_eq!(out.len(), 2, "Alice (85k) and Cara (75k)");
    let names: Vec<&String> = out.iter().map(|r| &r[0]).collect();
    assert!(names.contains(&&"'Alice'".to_string()));
    assert!(names.contains(&&"'Cara'".to_string()));
}

// ── INSERT INTO ... SELECT ────────────────────────────────────────────────────

#[test]
fn insert_into_select_copies_rows() {
    let _ws = common::TestWorkspace::new("advops", "iis");
    let mut catalog = setup_tables("iis_db");

    create_table(
        &mut catalog,
        "iis_db",
        "engineers",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("salary", DataType::Int),
            column("dept_id", DataType::Int),
        ],
    );

    let plan =
        parse_sql("INSERT INTO engineers SELECT * FROM employees WHERE dept_id = 10").unwrap();
    let logical =
        storage_manager::planner::plan_query(&plan, &catalog, "iis_db").expect("planning failed");
    execute_plan_collect(&logical, &catalog, "iis_db").expect("execution failed");

    let out = run_select(&catalog, "iis_db", "SELECT name FROM engineers ORDER BY id");
    assert_eq!(out.len(), 2);
    assert_eq!(out[0][0], "'Alice'");
    assert_eq!(out[1][0], "'Cara'");
}

#[test]
fn group_by_with_aliased_aggregates_and_having() {
    let _ws = common::TestWorkspace::new("advops", "agialias");
    let catalog = setup_tables("agg_alias_db");

    // Aliased aggregates must be projected by their alias and referenceable
    // from HAVING — both by re-calling the aggregate and by using the alias.
    let out = run_select(
        &catalog,
        "agg_alias_db",
        "SELECT dept_id, COUNT(*) AS n FROM employees \
         GROUP BY dept_id HAVING COUNT(*) >= 2 ORDER BY dept_id",
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], vec!["10", "2"]);

    let out = run_select(
        &catalog,
        "agg_alias_db",
        "SELECT dept_id, COUNT(*) AS n FROM employees \
         GROUP BY dept_id HAVING n >= 2 ORDER BY dept_id",
    );
    assert_eq!(out.len(), 2);
}
