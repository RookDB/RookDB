//! Integration tests for Direct Join.
//! Cross-checks results against NLJ for correctness parity.

use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, save_catalog, load_catalog, init_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::direct::DirectJoinExecutor;

use super::common::test_lock;

fn setup_test_db() -> String {
    let db_name = "test_direct_db";
    init_catalog();
    let mut catalog = load_catalog();
    catalog.databases.remove(db_name);
    save_catalog(&catalog);

    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);

    create_database(&mut catalog, db_name);

    create_table(&mut catalog, db_name, "t1", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "val".to_string(), data_type: "TEXT".to_string() },
    ]);

    create_table(&mut catalog, db_name, "t2", vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "val".to_string(), data_type: "TEXT".to_string() },
    ]);

    let t1_path = format!("database/base/{}/t1.dat", db_name);
    let mut t1_file = OpenOptions::new().read(true).write(true).open(&t1_path).unwrap();
    insert_tuple(&mut t1_file, &make_tuple(1, "A")).unwrap();
    insert_tuple(&mut t1_file, &make_tuple(2, "B")).unwrap();

    let t2_path = format!("database/base/{}/t2.dat", db_name);
    let mut t2_file = OpenOptions::new().read(true).write(true).open(&t2_path).unwrap();
    insert_tuple(&mut t2_file, &make_tuple(2, "X")).unwrap();
    insert_tuple(&mut t2_file, &make_tuple(3, "Y")).unwrap();

    let _ = fs::create_dir_all("database/tmp");

    db_name.to_string()
}

fn make_tuple(id: i32, val: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut txt = val.as_bytes().to_vec();
    if txt.len() > 10 { txt.truncate(10); }
    else if txt.len() < 10 { txt.extend(vec![b' '; 10 - txt.len()]); }
    bytes.extend_from_slice(&txt);
    bytes
}

#[test]
fn test_direct_join() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "t1".to_string(),
        left_col: "id".to_string(),
        operator: JoinOp::Eq,
        right_table: "t2".to_string(),
        right_col: "id".to_string(),
    }];

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::RightOuter, JoinType::FullOuter, JoinType::Cross] {
        let nlj = NLJExecutor {
            outer_table: "t1".to_string(),
            inner_table: "t2".to_string(),
            conditions: if join_type == JoinType::Cross { vec![] } else { condition.clone() },
            join_type: join_type.clone(),
            block_size: 2,
            mode: NLJMode::Simple,
        };
        let nlj_result = nlj.execute(&db, &catalog).unwrap();

        let dj = DirectJoinExecutor {
            outer_table: "t1".to_string(),
            inner_table: "t2".to_string(),
            conditions: if join_type == JoinType::Cross { vec![] } else { condition.clone() },
            join_type: join_type.clone(),
        };
        let dj_result = dj.execute(&db, &catalog).unwrap();

        assert_eq!(nlj_result.tuples.len(), dj_result.tuples.len(), "Direct Join count mismatch for {:?}", join_type);
    }
}
