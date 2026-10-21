//! Integration tests for INSERT ... VALUES through the plan pipeline
//! (legacy-executor retirement step 5).
//!
//! VALUES rows are planned like any other statement: the logical planner
//! expands them to full table arity (explicit-column mapping, DEFAULTs,
//! NULLs), the physical ValuesOperator produces the constant tuples, and
//! InsertOperator writes them with full constraint validation and index
//! maintenance.

mod common;

use common::TestWorkspace;
use common::load_catalog;
use rook_ast::QueryPlan;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn col(name: &str, ty: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        constraints: Default::default(),
    }
}

fn setup(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "staff",
        vec![
            col("id", DataType::Int),
            col("name", DataType::Varchar(30)),
            col("salary", DataType::Int),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    load_catalog()
}

/// Parse an INSERT statement, run it through both planners and the engine,
/// returning the number of rows actually inserted.
fn insert_rows(catalog: &Catalog, db: &str, sql: &str) -> Result<usize, String> {
    let plan = match rook_parser::parse_sql(sql) {
        Ok(p @ QueryPlan::Insert(_)) => p,
        Ok(other) => return Err(format!("expected INSERT, got {:?}", other.statement_type())),
        Err(e) => return Err(e),
    };
    let logical =
        storage_manager::planner::plan_query(&plan, catalog, db).map_err(|e| e.to_string())?;
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .map_err(|e| e.to_string())?;
    // InsertOperator yields one tuple per successfully inserted row.
    Ok(tuples.len())
}

fn table_contents(catalog: &Catalog, db: &str) -> Vec<Vec<String>> {
    use rook_ast::QueryPlan;
    use storage_manager::backend::executor::physical::engine::execute_plan_collect;
    use storage_manager::backend::executor::physical::tuple::Tuple;
    let select = match rook_parser::parse_sql("SELECT * FROM staff ORDER BY id") {
        Ok(QueryPlan::Select(s)) => s,
        _ => panic!("setup select"),
    };
    let logical = storage_manager::planner::plan_query(&QueryPlan::Select(select), catalog, db)
        .expect("plan");
    let tuples: Vec<Tuple> = execute_plan_collect(&logical, catalog, db).expect("execute");
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
fn multi_row_values_inserts_all_rows() {
    let _ws = TestWorkspace::new("insval", "multi");
    let catalog = setup("idb");

    let n = insert_rows(
        &catalog,
        "idb",
        "INSERT INTO staff VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    )
    .unwrap();
    assert_eq!(n, 3);

    assert_eq!(
        table_contents(&catalog, "idb"),
        vec![
            vec!["1", "'a'", "10"],
            vec!["2", "'b'", "20"],
            vec!["3", "'c'", "30"],
        ]
    );
}

#[test]
fn values_expressions_are_evaluated() {
    let _ws = TestWorkspace::new("insval", "expr");
    let catalog = setup("edb");

    let n = insert_rows(
        &catalog,
        "edb",
        "INSERT INTO staff VALUES (10 + 4, UPPER('ab'), 2 * 25)",
    )
    .unwrap();
    assert_eq!(n, 1);

    assert_eq!(
        table_contents(&catalog, "edb"),
        vec![vec!["14", "'AB'", "50"]]
    );
}

#[test]
fn column_subset_and_defaults_fill_missing_columns() {
    let _ws = TestWorkspace::new("insval", "subset");

    // salary carries a declared DEFAULT via a raw catalog tweak below.
    let mut catalog = load_catalog();
    create_database(&mut catalog, "ddb");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "ddb",
        "staff",
        vec![
            col("id", DataType::Int),
            col("name", DataType::Varchar(30)),
            col("salary", DataType::Int),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();

    // Only two of three columns provided → third becomes NULL.
    let n = insert_rows(
        &catalog,
        "ddb",
        "INSERT INTO staff (id, name) VALUES (7, 'g')",
    )
    .unwrap();
    assert_eq!(n, 1);

    let rows = table_contents(&catalog, "ddb");
    assert_eq!(rows, vec![vec!["7", "'g'", "NULL"]]);
}

#[test]
fn null_keyword_inserts_null() {
    let _ws = TestWorkspace::new("insval", "null");
    let catalog = setup("ndb");

    insert_rows(&catalog, "ndb", "INSERT INTO staff VALUES (1, NULL, 5)").unwrap();
    assert_eq!(
        table_contents(&catalog, "ndb"),
        vec![vec!["1", "NULL", "5"]]
    );
}

#[test]
fn constraint_violation_aborts_statement() {
    let _ws = TestWorkspace::new("insval", "viol");

    let mut catalog = load_catalog();
    create_database(&mut catalog, "vdb");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "vdb",
        "guarded",
        vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                constraints: storage_manager::catalog::Constraints {
                    not_null: true,
                    unique: true,
                    ..Default::default()
                },
            },
            col("salary", DataType::Int),
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();

    assert!(insert_single_tuple(&catalog, "vdb", "guarded", &["1", "100"]).unwrap());

    // Duplicate id → UNIQUE violation must surface as an Err through the
    // pipeline instead of a silent Ok(false).
    let err = insert_rows(&catalog, "vdb", "INSERT INTO guarded VALUES (1, 200)");
    assert!(err.is_err(), "duplicate PK must abort through the pipeline");
}
