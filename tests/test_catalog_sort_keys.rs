//! Tests for catalog sort key persistence.
//!
//! Verifies that sort key metadata persists correctly via create_table.
//! Uses in-memory catalog verification to avoid race conditions with
//! parallel test execution sharing the catalog file.

use std::collections::HashMap;
use std::fs;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey};
use storage_manager::catalog::{create_table, save_catalog};
use storage_manager::ordered::ordered_file::{FileType, read_ordered_file_header};

#[test]
fn test_create_ordered_table_persists_sort_keys() {
    let db_name = "test_cat_sk_persist_db1";
    let table_name = "test_cat_sk_persist_tbl1";

    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let mut catalog = Catalog {
        databases: HashMap::new(),
    };
    let database = Database {
        tables: HashMap::new(),
    };
    catalog.databases.insert(db_name.to_string(), database);
    save_catalog(&catalog);

    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    create_table(
        &mut catalog,
        db_name,
        table_name,
        columns,
        Some(sort_keys.clone()),
    );

    // Verify in-memory catalog
    let table = catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));
    assert!(table.sort_keys.is_some());
    assert_eq!(table.delta_enabled, Some(true));
    assert_eq!(table.delta_merge_threshold_tuples, Some(500));
    assert_eq!(table.delta_current_tuples, Some(0));
    let keys = table.sort_keys.as_ref().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].column_index, 0);
    assert_eq!(keys[0].direction, SortDirection::Ascending);

    // Verify the .dat file has ordered file header
    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .unwrap();
    let header = read_ordered_file_header(&mut file).unwrap();
    assert_eq!(header.file_type, FileType::Ordered);
    assert_eq!(header.sort_key_count, 1);
    assert_eq!(header.sort_keys[0].column_index, 0);
    assert_eq!(header.sort_keys[0].direction, 0); // ASC

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

#[test]
fn test_create_heap_table_no_sort_keys() {
    let db_name = "test_cat_sk_heap_db2";
    let table_name = "test_cat_sk_heap_tbl2";

    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let mut catalog = Catalog {
        databases: HashMap::new(),
    };
    let database = Database {
        tables: HashMap::new(),
    };
    catalog.databases.insert(db_name.to_string(), database);
    save_catalog(&catalog);

    let columns = vec![Column {
        name: "id".to_string(),
        data_type: "INT".to_string(),
    }];

    create_table(&mut catalog, db_name, table_name, columns, None);

    // Verify in-memory catalog
    let table = catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap();
    assert!(table.sort_keys.is_none());
    assert!(table.file_type.is_none());
    assert!(table.delta_enabled.is_none());
    assert!(table.delta_merge_threshold_tuples.is_none());
    assert!(table.delta_current_tuples.is_none());

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

#[test]
fn test_create_ordered_table_multi_key() {
    let db_name = "test_cat_sk_multi_db3";
    let table_name = "test_cat_sk_multi_tbl3";

    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let mut catalog = Catalog {
        databases: HashMap::new(),
    };
    let database = Database {
        tables: HashMap::new(),
    };
    catalog.databases.insert(db_name.to_string(), database);
    save_catalog(&catalog);

    let columns = vec![
        Column {
            name: "dept".to_string(),
            data_type: "TEXT".to_string(),
        },
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
    ];

    let sort_keys = vec![
        SortKey {
            column_index: 0,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 1,
            direction: SortDirection::Descending,
        },
    ];

    create_table(&mut catalog, db_name, table_name, columns, Some(sort_keys));

    // Verify in-memory catalog
    let table = catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap();
    let keys = table.sort_keys.as_ref().unwrap();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].column_index, 0);
    assert_eq!(keys[0].direction, SortDirection::Ascending);
    assert_eq!(keys[1].column_index, 1);
    assert_eq!(keys[1].direction, SortDirection::Descending);

    // Verify file header
    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .unwrap();
    let header = read_ordered_file_header(&mut file).unwrap();
    assert_eq!(header.file_type, FileType::Ordered);
    assert_eq!(header.sort_key_count, 2);
    assert_eq!(header.sort_keys[0].direction, 0); // ASC
    assert_eq!(header.sort_keys[1].direction, 1); // DESC

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}
