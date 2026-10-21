//! CHECK constraints evaluated by the Volcano expression evaluator.
//!
//! Migration step: `check_constraints` now compiles each stored expression
//! with the real SQL grammar (`rook-parser`) and evaluates it with the
//! physical `Predicate` evaluator — replacing the retired legacy DNF
//! matcher. That unlocks full expression support; semantics stay SQL:
//! TRUE passes, FALSE rejects, NULL (UNKNOWN) passes.

mod common;

use common::TestWorkspace;
use common::load_catalog;
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

fn setup(db: &str, check: &str, extra_cols: Vec<Column>) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    let mut cols = vec![
        col("id", DataType::Int),
        col("dept", DataType::Varchar(20)),
        col("salary", DataType::Int),
    ];
    cols.extend(extra_cols);
    cols[2].constraints.check = Some(check.to_string());
    create_table(&mut catalog, db, "staff", cols).unwrap();
    save_catalog(&catalog).unwrap();
    load_catalog()
}

#[test]
fn check_or_of_conditions() {
    let _ws = TestWorkspace::new("chkvol", "or");
    let catalog = setup("odb", "dept = 'HR' OR salary > 50000", vec![]);

    assert!(insert_single_tuple(&catalog, "odb", "staff", &["1", "HR", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "odb", "staff", &["2", "Sales", "90000"]).unwrap());
    // Both sides false → rejected.
    let bad = insert_single_tuple(&catalog, "odb", "staff", &["3", "Sales", "10"]).unwrap();
    assert!(!bad, "row violating both OR branches must be rejected");
}

#[test]
fn check_arithmetic_expression() {
    let _ws = TestWorkspace::new("chkvol", "arith");
    let catalog = setup("adb", "salary / 2 >= 100 AND salary < 100000", vec![]);

    assert!(insert_single_tuple(&catalog, "adb", "staff", &["1", "x", "400"]).unwrap());
    let bad = insert_single_tuple(&catalog, "adb", "staff", &["2", "x", "100"]).unwrap();
    assert!(!bad, "arithmetic CHECK must reject salary/2 < 100");
}

#[test]
fn check_in_list_and_between() {
    let _ws = TestWorkspace::new("chkvol", "inbtw");
    let catalog = setup(
        "idb",
        "salary IN (1000, 2000, 3000) OR salary BETWEEN 5000 AND 6000",
        vec![],
    );

    for ok in ["1000", "3000", "5500"] {
        assert!(
            insert_single_tuple(&catalog, "idb", "staff", &[next_id(), "x", ok]).unwrap(),
            "value {} should pass IN/BETWEEN check",
            ok
        );
    }
    let bad = insert_single_tuple(&catalog, "idb", "staff", &["9", "x", "4000"]).unwrap();
    assert!(!bad, "4000 matches neither branch");
}

fn next_id() -> &'static str {
    // IDs are irrelevant to these checks; reuse a constant — id has no
    // uniqueness constraint here.
    "1"
}

#[test]
fn check_null_passes_as_unknown() {
    let _ws = TestWorkspace::new("chkvol", "null");
    let catalog = setup("ndb", "salary > 0", vec![]);

    // SQL: NULL compared is UNKNOWN → CHECK passes.
    assert!(
        insert_single_tuple(&catalog, "ndb", "staff", &["1", "x", "NULL"]).unwrap(),
        "NULL salary must satisfy CHECK via UNKNOWN"
    );
    let bad = insert_single_tuple(&catalog, "ndb", "staff", &["2", "x", "-5"]).unwrap();
    assert!(!bad, "negative salary must still be rejected");
}

#[test]
fn check_function_call_expression() {
    let _ws = TestWorkspace::new("chkvol", "func");
    let catalog = setup("fdb", "UPPER(dept) = 'ENGINEERING'", vec![]);

    assert!(insert_single_tuple(&catalog, "fdb", "staff", &["1", "engineering", "1"]).unwrap());
    let bad = insert_single_tuple(&catalog, "fdb", "staff", &["2", "sales", "1"]).unwrap();
    assert!(
        !bad,
        "non-engineering dept must be rejected by UPPER comparison"
    );
}
