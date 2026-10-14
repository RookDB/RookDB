//! Integration tests for Index Nested-Loop Join (INLJ) and ORDER BY sort elimination.

mod common;

use common::load_catalog;
use rook_ast::logical::LogicalPlan;
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::planner::PhysicalPlanner;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::executor::create_index::create_index;
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

fn setup_join_db(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();

    create_table(
        &mut catalog,
        db,
        "departments",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
        ],
    );

    create_table(
        &mut catalog,
        db,
        "employees",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("dept_id", DataType::Int),
        ],
    );
    save_catalog(&catalog).unwrap();

    // Insert departments (larger lookup table)
    for row in [
        vec!["10", "Engineering"],
        vec!["20", "Sales"],
        vec!["30", "Marketing"],
        vec!["40", "Finance"],
        vec!["50", "Legal"],
        vec!["60", "HR"],
        vec!["70", "Operations"],
        vec!["80", "Design"],
    ] {
        insert_single_tuple(&catalog, db, "departments", &row).unwrap();
    }

    // Departments no employee is in: probing an index beats scanning the
    // table only when the table is big enough to make a scan costly.
    for id in 1000..1300 {
        insert_single_tuple(&catalog, db, "departments", &[&id.to_string(), "Unstaffed"]).unwrap();
    }

    // Insert employees (including one with dept_id 99 which has no matching department)
    for row in [
        vec!["1", "Alice", "10"],
        vec!["2", "Bob", "20"],
        vec!["3", "Charlie", "10"],
        vec!["4", "Diana", "20"],
        vec!["5", "Eve", "99"],
    ] {
        insert_single_tuple(&catalog, db, "employees", &row).unwrap();
    }

    // Build index on departments.id
    create_index(
        &catalog,
        db,
        "departments",
        "idx_dept_id",
        &[String::from("id")],
    )
    .expect("create_index departments.id");

    load_catalog()
}

fn run_query(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, catalog, db).unwrap();
    let tuples = execute_plan_collect(&logical, catalog, db).unwrap();
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

#[test]
fn test_inlj_inner_join_matches() {
    let _ws = common::TestWorkspace::new("inlj", "inner");
    let db = "inlj_inner_db";
    let catalog = setup_join_db(db);

    let sql = "SELECT employees.name, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id";
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, &catalog, db).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let logical_child = match &logical {
        LogicalPlan::Project(p) => &p.child,
        other => other,
    };
    let join_phys = planner.plan(logical_child).unwrap();
    assert_eq!(
        join_phys.name(),
        "IndexNestedLoopJoin",
        "Planner must select IndexNestedLoopJoin"
    );

    let mut rows = run_query(&catalog, db, sql);
    rows.sort();

    let mut expected = vec![
        vec!["'Alice'".to_string(), "'Engineering'".to_string()],
        vec!["'Bob'".to_string(), "'Sales'".to_string()],
        vec!["'Charlie'".to_string(), "'Engineering'".to_string()],
        vec!["'Diana'".to_string(), "'Sales'".to_string()],
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn test_inlj_left_join_null_padding() {
    let _ws = common::TestWorkspace::new("inlj", "left");
    let db = "inlj_left_db";
    let catalog = setup_join_db(db);

    let sql = "SELECT employees.name, departments.name FROM employees LEFT JOIN departments ON employees.dept_id = departments.id";
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, &catalog, db).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let logical_child = match &logical {
        LogicalPlan::Project(p) => &p.child,
        other => other,
    };
    let join_phys = planner.plan(logical_child).unwrap();
    assert_eq!(
        join_phys.name(),
        "IndexNestedLoopJoin",
        "Planner must select IndexNestedLoopJoin for LEFT join"
    );

    let mut rows = run_query(&catalog, db, sql);
    rows.sort();

    let mut expected = vec![
        vec!["'Alice'".to_string(), "'Engineering'".to_string()],
        vec!["'Bob'".to_string(), "'Sales'".to_string()],
        vec!["'Charlie'".to_string(), "'Engineering'".to_string()],
        vec!["'Diana'".to_string(), "'Sales'".to_string()],
        vec!["'Eve'".to_string(), "NULL".to_string()],
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn test_inlj_with_residual_predicate() {
    let _ws = common::TestWorkspace::new("inlj", "residual");
    let db = "inlj_residual_db";
    let catalog = setup_join_db(db);

    let sql = "SELECT employees.name, departments.name FROM employees JOIN departments ON employees.dept_id = departments.id WHERE departments.name = 'Engineering'";
    let mut rows = run_query(&catalog, db, sql);
    rows.sort();

    let mut expected = vec![
        vec!["'Alice'".to_string(), "'Engineering'".to_string()],
        vec!["'Charlie'".to_string(), "'Engineering'".to_string()],
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

fn setup_items_db(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();

    create_table(
        &mut catalog,
        db,
        "items",
        vec![column("id", DataType::Int), column("price", DataType::Int)],
    );
    save_catalog(&catalog).unwrap();

    for (id, price) in [(1, 40), (2, 10), (3, 50), (4, 20), (5, 30)] {
        insert_single_tuple(
            &catalog,
            db,
            "items",
            &[&id.to_string(), &price.to_string()],
        )
        .unwrap();
    }

    create_index(&catalog, db, "items", "idx_price", &[String::from("price")])
        .expect("create_index items.price");

    load_catalog()
}

#[test]
fn test_sort_elimination_range_lookup_asc() {
    let _ws = common::TestWorkspace::new("sort_elim", "asc");
    let db = "sort_elim_asc_db";
    let catalog = setup_items_db(db);

    let sql = "SELECT id, price FROM items WHERE price >= 20 ORDER BY price";
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, &catalog, db).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let logical_child = match &logical {
        LogicalPlan::Project(p) => &p.child,
        other => other,
    };
    let phys = planner.plan(logical_child).unwrap();
    // Verify SortOperator is eliminated: the node is IndexScan, NOT Sort
    assert_ne!(phys.name(), "Sort", "SortOperator should be eliminated");

    let rows = run_query(&catalog, db, sql);
    let expected = vec![
        vec!["4".to_string(), "20".to_string()],
        vec!["5".to_string(), "30".to_string()],
        vec!["1".to_string(), "40".to_string()],
        vec!["3".to_string(), "50".to_string()],
    ];
    assert_eq!(rows, expected);
}

#[test]
fn test_sort_elimination_with_limit() {
    let _ws = common::TestWorkspace::new("sort_elim", "limit");
    let db = "sort_elim_limit_db";
    let catalog = setup_items_db(db);

    let sql = "SELECT id, price FROM items WHERE price >= 10 ORDER BY price LIMIT 3";
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, &catalog, db).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let logical_child = match &logical {
        LogicalPlan::Project(p) => &p.child,
        other => other,
    };
    let phys = planner.plan(logical_child).unwrap();
    // When LIMIT is present and Sort is eliminated, node should be LimitOperator
    assert_eq!(
        phys.name(),
        "Limit",
        "Should pipeline directly into LimitOperator"
    );

    let rows = run_query(&catalog, db, sql);
    let expected = vec![
        vec!["2".to_string(), "10".to_string()],
        vec!["4".to_string(), "20".to_string()],
        vec!["5".to_string(), "30".to_string()],
    ];
    assert_eq!(rows, expected);
}

#[test]
fn test_sort_desc_retains_sort_operator() {
    let _ws = common::TestWorkspace::new("sort_elim", "desc");
    let db = "sort_elim_desc_db";
    let catalog = setup_items_db(db);

    let sql = "SELECT id, price FROM items WHERE price >= 20 ORDER BY price DESC";
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, &catalog, db).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let logical_child = match &logical {
        LogicalPlan::Project(p) => &p.child,
        other => other,
    };
    let phys = planner.plan(logical_child).unwrap();
    // Descending order cannot be satisfied by forward index scan, so SortOperator must be retained
    assert_eq!(
        phys.name(),
        "Sort",
        "SortOperator must be retained for DESC order"
    );

    let rows = run_query(&catalog, db, sql);
    let expected = vec![
        vec!["3".to_string(), "50".to_string()],
        vec!["1".to_string(), "40".to_string()],
        vec!["5".to_string(), "30".to_string()],
        vec!["4".to_string(), "20".to_string()],
    ];
    assert_eq!(rows, expected);
}
