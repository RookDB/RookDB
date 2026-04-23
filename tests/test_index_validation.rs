use std::collections::HashMap;
use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::{init_table, insert_tuple_with_index_maintenance};
use storage_manager::index::{
    AnyIndex, IndexKey, RecordId, index_file_path, rebuild_table_indexes, validate_index_consistency,
};

#[test]
fn test_index_validation_detects_stale_and_recovers_after_rebuild() {
    let db_name = "test_db_index_validation";
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
            column_name: vec!["id".to_string()],
            algorithm: IndexAlgorithm::BPlusTree,
            is_clustered: false,
            include_columns: Vec::new(),
        }],
    };

    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    catalog
        .databases
        .insert(db_name.to_string(), Database { tables });

    let empty_index = AnyIndex::new_empty(&IndexAlgorithm::BPlusTree);
    empty_index
        .save(&index_path)
        .expect("Failed to create initial index file");

    let tuple = 42i32.to_le_bytes();
    let _rid = insert_tuple_with_index_maintenance(&catalog, db_name, table_name, &mut file, &tuple)
        .expect("Insert with index maintenance failed");

    let mut index = AnyIndex::load(&index_path, &IndexAlgorithm::BPlusTree)
        .expect("Failed to load index");
    index
        .insert(IndexKey::Int(999), RecordId::new(99, 99))
        .expect("Failed to inject stale index entry");
    index.save(&index_path).expect("Failed to save index");

    let err = validate_index_consistency(&catalog, db_name, table_name, index_name)
        .expect_err("Expected validation to fail due to stale entry");
    assert!(
        err.to_string().contains("stale entries"),
        "Expected stale-entry validation error, got: {}",
        err
    );

    rebuild_table_indexes(&catalog, db_name, table_name).expect("Rebuild failed");
    validate_index_consistency(&catalog, db_name, table_name, index_name)
        .expect("Validation should succeed after rebuild");

    let _ = fs::remove_file(index_path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
