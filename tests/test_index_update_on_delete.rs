use std::collections::HashMap;
use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::{delete_tuple_with_index_maintenance, init_table, insert_tuple_with_index_maintenance};
use storage_manager::index::{AnyIndex, IndexKey, index_file_path};

#[test]
fn test_index_update_on_insert_delete() {
    let db_name = "test_db_index_delete";
    let table_name = "users";
    let index_name = "users_id_idx";

    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);
    let index_path = index_file_path(db_name, table_name, index_name);

    let _ = fs::remove_file(&index_path);
    let _ = fs::remove_file(&table_path);
    let _ = fs::remove_dir_all(&table_dir);

    fs::create_dir_all(&table_dir).expect("Failed to create table directory");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&table_path)
        .expect("Failed to create table file");

    init_table(&mut file).expect("init_table failed");

    let mut catalog = Catalog { databases: HashMap::new() };
    let table = Table {
        columns: vec![Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        }],
        indexes: vec![IndexEntry {
            index_name: index_name.to_string(),
            column_name: "id".to_string(),
            algorithm: IndexAlgorithm::BPlusTree,
            is_clustered: false,
            include_columns: Vec::new(),
        }],
    };

    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    catalog.databases.insert(
        db_name.to_string(),
        Database { tables },
    );

    let empty_index = AnyIndex::new_empty(&IndexAlgorithm::BPlusTree);
    empty_index
        .save(&index_path)
        .expect("Failed to save empty index");

    let tuple = 123i32.to_le_bytes();
    let rid1 = insert_tuple_with_index_maintenance(
        &catalog,
        db_name,
        table_name,
        &mut file,
        &tuple,
    )
    .expect("Insert with index maintenance failed");

    let rid2 = insert_tuple_with_index_maintenance(
        &catalog,
        db_name,
        table_name,
        &mut file,
        &tuple,
    )
    .expect("Second insert with index maintenance failed");

    let index = AnyIndex::load(&index_path, &IndexAlgorithm::BPlusTree)
        .expect("Failed to load index");
    let records = index
        .search(&IndexKey::Int(123))
        .expect("Search failed");

    assert!(
        !records.is_empty(),
        "Expected index to contain key after insert"
    );

    assert!(
        records.len() >= 2,
        "Expected index to contain two records for duplicate key"
    );

    delete_tuple_with_index_maintenance(
        &catalog,
        db_name,
        table_name,
        &mut file,
        rid1.clone(),
    )
    .expect("Delete with index maintenance failed");

    let index = AnyIndex::load(&index_path, &IndexAlgorithm::BPlusTree)
        .expect("Failed to load index");
    let records = index
        .search(&IndexKey::Int(123))
        .expect("Search failed");

    assert!(
        !records.is_empty(),
        "Expected index to retain key after deleting one of two records"
    );

    delete_tuple_with_index_maintenance(
        &catalog,
        db_name,
        table_name,
        &mut file,
        rid2.clone(),
    )
    .expect("Second delete with index maintenance failed");

    let index = AnyIndex::load(&index_path, &IndexAlgorithm::BPlusTree)
        .expect("Failed to load index");
    let records = index
        .search(&IndexKey::Int(123))
        .expect("Search failed");

    assert!(
        records.is_empty(),
        "Expected index to remove key after deleting all records"
    );

    let err = delete_tuple_with_index_maintenance(
        &catalog,
        db_name,
        table_name,
        &mut file,
        rid2,
    )
    .expect_err("Expected delete on already-deleted tuple to error");

    assert!(
        err.to_string().contains("already deleted"),
        "Expected delete error to mention already deleted tuple"
    );

    let _ = fs::remove_file(index_path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
