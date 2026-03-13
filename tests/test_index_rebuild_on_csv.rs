use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::init_table;
use storage_manager::index::{AnyIndex, IndexKey, index_file_path};

#[test]
fn test_index_rebuild_on_csv() {
    let db_name = "test_db_index_rebuild";
    let table_name = "users";
    let index_name = "users_id_idx";
    let csv_path = "test_index_rebuild.csv";

    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);
    let index_path = index_file_path(db_name, table_name, index_name);

    let _ = fs::remove_file(csv_path);
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
        }],
    };

    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    catalog.databases.insert(
        db_name.to_string(),
        Database { tables },
    );

    let mut stale_index = AnyIndex::new_empty(&IndexAlgorithm::BPlusTree);
    stale_index
        .insert(IndexKey::Int(999), storage_manager::index::RecordId::new(1, 1))
        .expect("Failed to insert stale key");
    stale_index
        .save(&index_path)
        .expect("Failed to save stale index");

    let mut csv_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(csv_path)
        .expect("Failed to create CSV file");

    writeln!(csv_file, "id").unwrap();
    writeln!(csv_file, "1").unwrap();
    writeln!(csv_file, "2").unwrap();
    writeln!(csv_file, "3,extra").unwrap();
    writeln!(csv_file, "").unwrap();

    let mut buffer_manager = BufferManager::new();
    buffer_manager
        .load_table_from_disk(db_name, table_name)
        .expect("Failed to load table into buffer");
    buffer_manager
        .load_csv_to_buffer(&catalog, db_name, table_name, csv_path)
        .expect("CSV load failed");

    let index = AnyIndex::load(&index_path, &IndexAlgorithm::BPlusTree)
        .expect("Failed to load index");
    let records = index
        .search(&IndexKey::Int(1))
        .expect("Search failed");

    assert!(
        !records.is_empty(),
        "Expected index rebuild after CSV ingestion to include key 1"
    );

    let records = index
        .search(&IndexKey::Int(2))
        .expect("Search failed");
    assert!(
        !records.is_empty(),
        "Expected index rebuild after CSV ingestion to include key 2"
    );

    let stale_records = index
        .search(&IndexKey::Int(999))
        .expect("Search failed");
    assert!(
        stale_records.is_empty(),
        "Expected full rebuild to remove stale key 999"
    );

    let _ = fs::remove_file(csv_path);
    let _ = fs::remove_file(index_path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
