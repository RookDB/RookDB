//! Integration tests for Symmetric Hash Join (SHJ).
//! Cross-checks results against NLJ for correctness parity.

use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, save_catalog, load_catalog, init_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::shj::SymmetricHashJoinExecutor;

use super::common::test_lock;

fn setup_test_db() -> String {
    let db_name = "test_shj_db";
    init_catalog();
    let mut catalog = load_catalog();
    catalog.databases.remove(db_name);
    save_catalog(&catalog);

    // Clean up existing database directory
    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);

    create_database(&mut catalog, db_name);

    create_table(&mut catalog, db_name, "orders", vec![
        Column { name: "oid".to_string(), data_type: "INT".to_string() },
        Column { name: "customer_id".to_string(), data_type: "INT".to_string() },
        Column { name: "amount".to_string(), data_type: "INT".to_string() },
    ]);

    create_table(&mut catalog, db_name, "customers", vec![
        Column { name: "cid".to_string(), data_type: "INT".to_string() },
        Column { name: "cname".to_string(), data_type: "TEXT".to_string() },
    ]);

    let ord_path = format!("database/base/{}/orders.dat", db_name);
    let mut ord_file = OpenOptions::new().read(true).write(true).open(&ord_path).unwrap();

    // Insert orders
    for i in 1..=6 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(i as i32).to_le_bytes());        // order id
        bytes.extend_from_slice(&((i % 3 + 1) as i32).to_le_bytes()); // customer_id: 2, 3, 1, 2, 3, 1
        bytes.extend_from_slice(&((i * 100) as i32).to_le_bytes()); // amount
        insert_tuple(&mut ord_file, &bytes).unwrap();
    }

    let cust_path = format!("database/base/{}/customers.dat", db_name);
    let mut cust_file = OpenOptions::new().read(true).write(true).open(&cust_path).unwrap();

    // Insert customers
    let names = ["Alice", "Bob", "Charlie"];
    for (i, name) in names.iter().enumerate() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((i as i32) + 1).to_le_bytes());
        let mut name_bytes = name.as_bytes().to_vec();
        if name_bytes.len() > 10 { name_bytes.truncate(10); }
        else if name_bytes.len() < 10 { name_bytes.extend(vec![b' '; 10 - name_bytes.len()]); }
        bytes.extend_from_slice(&name_bytes);
        insert_tuple(&mut cust_file, &bytes).unwrap();
    }

    let _ = fs::create_dir_all("database/tmp");

    db_name.to_string()
}

#[test]
fn test_shj_inner_join_correctness() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "orders".to_string(),
        left_col: "customer_id".to_string(),
        operator: JoinOp::Eq,
        right_table: "customers".to_string(),
        right_col: "cid".to_string(),
    }];

    // NLJ (ground truth)
    let nlj = NLJExecutor {
        outer_table: "orders".to_string(),
        inner_table: "customers".to_string(),
        conditions: condition.clone(),
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Simple,
    };
    let nlj_result = nlj.execute(&db, &catalog).unwrap();

    // SHJ
    let shj = SymmetricHashJoinExecutor {
        left_table: "orders".to_string(),
        right_table: "customers".to_string(),
        conditions: condition,
        join_type: JoinType::Inner,
    };
    let shj_result = shj.execute(&db, &catalog).unwrap();

    assert_eq!(nlj_result.tuples.len(), shj_result.tuples.len(),
        "SHJ and NLJ should produce the same number of tuples");
}

#[test]
fn test_shj_outer_joins() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "orders".to_string(),
        left_col: "customer_id".to_string(),
        operator: JoinOp::Eq,
        right_table: "customers".to_string(),
        right_col: "cid".to_string(),
    }];

    let shj = SymmetricHashJoinExecutor {
        left_table: "orders".to_string(),
        right_table: "customers".to_string(),
        conditions: condition.clone(),
        join_type: JoinType::LeftOuter,
    };
    let res = shj.execute(&db, &catalog).unwrap();
    assert_eq!(res.tuples.len(), 6, "Left Outer Join matches inner join since all orders have customers");
}
