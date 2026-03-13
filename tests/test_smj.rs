// Integration tests for Sort-Merge Join (SMJ).
// Cross-checks results against NLJ for correctness parity.

use std::fs::{self, OpenOptions};
use std::sync::{Mutex, OnceLock};

use storage_manager::catalog::types::Column;
use storage_manager::catalog::{create_database, create_table, save_catalog, load_catalog, init_catalog};
use storage_manager::heap::insert_tuple;
use storage_manager::join::{JoinType, NLJMode};
use storage_manager::join::condition::{JoinCondition, JoinOp};
use storage_manager::join::nlj::NLJExecutor;
use storage_manager::join::smj::SMJExecutor;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn setup_test_db() -> String {
    let db_name = "test_smj_db";
    init_catalog();
    let mut catalog = load_catalog();
    catalog.databases.remove(db_name);
    save_catalog(&catalog);

    // Clean up existing database directory
    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);

    create_database(&mut catalog, db_name);

    create_table(&mut catalog, db_name, "students", vec![
        Column { name: "sid".to_string(), data_type: "INT".to_string() },
        Column { name: "course_id".to_string(), data_type: "INT".to_string() },
    ]);

    create_table(&mut catalog, db_name, "courses", vec![
        Column { name: "cid".to_string(), data_type: "INT".to_string() },
        Column { name: "cname".to_string(), data_type: "TEXT".to_string() },
    ]);

    let stu_path = format!("database/base/{}/students.dat", db_name);
    let mut stu_file = OpenOptions::new().read(true).write(true).open(&stu_path).unwrap();
    for i in 1..=5 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(i as i32).to_le_bytes());
        bytes.extend_from_slice(&((i % 3 + 1) as i32).to_le_bytes()); // course_id: 2, 3, 1, 2, 3
        insert_tuple(&mut stu_file, &bytes).unwrap();
    }

    let crs_path = format!("database/base/{}/courses.dat", db_name);
    let mut crs_file = OpenOptions::new().read(true).write(true).open(&crs_path).unwrap();
    for i in 1..=3 {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(i as i32).to_le_bytes());
        let mut name = format!("Course{}", i).into_bytes();
        if name.len() > 10 { name.truncate(10); }
        else if name.len() < 10 { name.extend(vec![b' '; 10 - name.len()]); }
        bytes.extend_from_slice(&name);
        insert_tuple(&mut crs_file, &bytes).unwrap();
    }

    // Ensure tmp dir exists
    let _ = fs::create_dir_all("database/tmp");

    db_name.to_string()
}

#[test]
fn test_smj_inner_join_correctness() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let condition = vec![JoinCondition {
        left_table: "students".to_string(),
        left_col: "course_id".to_string(),
        operator: JoinOp::Eq,
        right_table: "courses".to_string(),
        right_col: "cid".to_string(),
    }];

    // NLJ (ground truth)
    let nlj = NLJExecutor {
        outer_table: "students".to_string(),
        inner_table: "courses".to_string(),
        conditions: condition.clone(),
        join_type: JoinType::Inner,
        block_size: 2,
        mode: NLJMode::Simple,
    };
    let nlj_result = nlj.execute(&db, &catalog).unwrap();

    // SMJ
    let smj = SMJExecutor {
        left_table: "students".to_string(),
        right_table: "courses".to_string(),
        conditions: condition,
        join_type: JoinType::Inner,
        memory_pages: 10,
    };
    let smj_result = smj.execute(&db, &catalog).unwrap();

    assert_eq!(nlj_result.tuples.len(), smj_result.tuples.len(),
        "SMJ and NLJ should produce the same number of tuples");
    assert!(smj_result.tuples.len() > 0, "Should have at least one matching tuple");
}

#[test]
fn test_smj_produces_results() {
    let _guard = test_lock().lock().unwrap();
    let db = setup_test_db();
    let catalog = load_catalog();

    let smj = SMJExecutor {
        left_table: "students".to_string(),
        right_table: "courses".to_string(),
        conditions: vec![JoinCondition {
            left_table: "students".to_string(),
            left_col: "course_id".to_string(),
            operator: JoinOp::Eq,
            right_table: "courses".to_string(),
            right_col: "cid".to_string(),
        }],
        join_type: JoinType::Inner,
        memory_pages: 10,
    };

    let result = smj.execute(&db, &catalog).unwrap();
    // 5 students each match one course: should be 5 result tuples
    assert_eq!(result.tuples.len(), 5, "Each student matches exactly one course");
}
