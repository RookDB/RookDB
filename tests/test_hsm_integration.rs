use std::fs;
use std::path::{Path, PathBuf};

use storage_manager::catalog::{
    create_database, create_table, init_catalog, load_catalog, save_catalog,
};
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::heap::HeapManager;
use storage_manager::layout::CATALOG_FILE;
use storage_manager::types::DataType;

use std::sync::Mutex;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

fn setup_clean_env() {
    let _ = env_logger::builder().is_test(true).try_init();

    if Path::new(CATALOG_FILE).exists() {
        let _ = fs::remove_file(CATALOG_FILE);
    }

    let _ = fs::remove_dir_all("database/base/test_db");
}

#[test]
fn test_multiple_columns_insertion() {
    let _lock = TEST_MUTEX.lock().unwrap();

    setup_clean_env();
    init_catalog();

    let mut catalog = load_catalog();

    let db_name = "test_db";
    let table_name = "test_multi_columns";

    // Create database
    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    save_catalog(&catalog).unwrap();

    // Define columns
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "rank".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Varchar(255),
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "phone".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "food".to_string(),
            data_type: DataType::Varchar(255),
            nullable: false,
            constraints: Constraints::default(),
        },
    ];

    // Create table
    create_table(&mut catalog, db_name, table_name, columns);

    save_catalog(&catalog).unwrap();

    // Insert rows
    let values1 = vec!["1", "10", "Alice", "123456789", "Pizza"];

    let success1 =
        insert_single_tuple(&catalog, db_name, table_name, &values1).unwrap();

    assert!(success1, "First tuple insertion failed");

    let values2 = vec!["2", "20", "Bob", "987654321", "Burger"];

    let success2 =
        insert_single_tuple(&catalog, db_name, table_name, &values2).unwrap();

    assert!(success2, "Second tuple insertion failed");

    // Verify inserted tuple count
    let path = PathBuf::from(format!(
        "database/base/{}/{}.dat",
        db_name, table_name
    ));

    let manager =
        HeapManager::open(path).expect("Failed to open heap manager");

    let scanned_count = manager.scan().filter_map(|r| r.ok()).count();

    assert_eq!(scanned_count, 2, "Should have 2 tuples inserted");

    setup_clean_env();
}

#[test]
fn test_multiple_tables_isolation() {
    let _lock = TEST_MUTEX.lock().unwrap();

    setup_clean_env();
    init_catalog();

    let mut catalog = load_catalog();

    let db_name = "test_db";

    // Create database
    assert!(
        create_database(&mut catalog, db_name),
        "Failed to create database"
    );

    save_catalog(&catalog).unwrap();

    // Table 1
    let table1 = "users";

    let cols1 = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "username".to_string(),
            data_type: DataType::Varchar(255),
            nullable: false,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, table1, cols1);

    // Table 2
    let table2 = "orders";

    let cols2 = vec![
        Column {
            name: "order_id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "amount".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "item".to_string(),
            data_type: DataType::Varchar(255),
            nullable: false,
            constraints: Constraints::default(),
        },
    ];

    create_table(&mut catalog, db_name, table2, cols2);

    save_catalog(&catalog).unwrap();

    // Insert into users
    let t1_v1 = vec!["1", "Alice"];

    assert!(
        insert_single_tuple(&catalog, db_name, table1, &t1_v1).unwrap()
    );

    // Insert into orders
    let t2_v1 = vec!["100", "50", "Book"];

    assert!(
        insert_single_tuple(&catalog, db_name, table2, &t2_v1).unwrap()
    );

    // Insert second users row
    let t1_v2 = vec!["2", "Bob"];

    assert!(
        insert_single_tuple(&catalog, db_name, table1, &t1_v2).unwrap()
    );

    // Insert second orders row
    let t2_v2 = vec!["101", "20", "Pen"];

    assert!(
        insert_single_tuple(&catalog, db_name, table2, &t2_v2).unwrap()
    );

    // Verify users table
    let path1 = PathBuf::from(format!(
        "database/base/{}/{}.dat",
        db_name, table1
    ));

    let t1_manager =
        HeapManager::open(path1).expect("Failed to open table1 manager");

    assert_eq!(
        t1_manager.scan().filter_map(|r| r.ok()).count(),
        2,
        "Table1 should have 2 tuples"
    );

    // Verify orders table
    let path2 = PathBuf::from(format!(
        "database/base/{}/{}.dat",
        db_name, table2
    ));

    let t2_manager =
        HeapManager::open(path2).expect("Failed to open table2 manager");

    assert_eq!(
        t2_manager.scan().filter_map(|r| r.ok()).count(),
        2,
        "Table2 should have 2 tuples"
    );

    setup_clean_env();
}