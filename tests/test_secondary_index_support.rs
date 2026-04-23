use std::collections::HashMap;
use std::fs::{self, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::{init_table, insert_tuple_with_rid};
use storage_manager::index::{
    AnyIndex, IndexKey, RecordId, index_key_from_values, rebuild_secondary_index,
    secondary_index_file_path,
};

fn setup_table(
    db_name: &str,
    table_name: &str,
    columns: Vec<Column>,
    indexes: Vec<IndexEntry>,
) -> (Catalog, std::fs::File, String, String) {
    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);

    let _ = fs::remove_file(&table_path);
    let _ = fs::remove_dir_all(&table_dir);
    fs::create_dir_all(&table_dir).expect("failed to create table directory");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&table_path)
        .expect("failed to open table file");
    init_table(&mut file).expect("init_table failed");

    let table = Table { columns, indexes };
    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);

    let mut dbs = HashMap::new();
    dbs.insert(db_name.to_string(), Database { tables });

    (Catalog { databases: dbs }, file, table_path, table_dir)
}

fn encode_int_text(id: i32, text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(14);
    out.extend_from_slice(&id.to_le_bytes());

    let mut fixed = [b' '; 10];
    for (i, b) in text.as_bytes().iter().take(10).enumerate() {
        fixed[i] = *b;
    }
    out.extend_from_slice(&fixed);
    out
}

fn encode_int_text_text(id: i32, left: &str, right: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&id.to_le_bytes());

    let mut first = [b' '; 10];
    for (i, b) in left.as_bytes().iter().take(10).enumerate() {
        first[i] = *b;
    }
    out.extend_from_slice(&first);

    let mut second = [b' '; 10];
    for (i, b) in right.as_bytes().iter().take(10).enumerate() {
        second[i] = *b;
    }
    out.extend_from_slice(&second);

    out
}

#[test]
fn test_secondary_btree_single_column_build_and_range() {
    let db_name = "test_db_secondary_btree";
    let table_name = "employees";
    let index_name = "dept_idx";

    let index_entry = IndexEntry {
        index_name: index_name.to_string(),
        column_name: vec!["dept".to_string()],
        algorithm: IndexAlgorithm::BPlusTree,
        is_clustered: false,
        include_columns: Vec::new(),
    };

    let (catalog, mut file, table_path, table_dir) = setup_table(
        db_name,
        table_name,
        vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "dept".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
        vec![index_entry.clone()],
    );

    for (id, dept) in [(1, "sales"), (2, "eng"), (3, "sales"), (4, "legal")] {
        let tuple = encode_int_text(id, dept);
        insert_tuple_with_rid(&mut file, &tuple).expect("failed to insert tuple");
    }

    let built = AnyIndex::build_secondary_index(&catalog, db_name, table_name, &index_entry)
        .expect("build_secondary_index failed");

    let path = secondary_index_file_path(db_name, table_name, index_name);
    let _ = fs::remove_file(&path);
    built.save(&path).expect("save secondary index failed");

    let loaded = AnyIndex::load(&path, &IndexAlgorithm::BPlusTree).expect("load secondary failed");
    let sales_hits = loaded
        .search(&IndexKey::Text("sales".to_string()))
        .expect("search secondary failed");
    assert_eq!(sales_hits.len(), 2, "expected two records for dept='sales'");

    let exact_range = loaded
        .range_scan(
            &IndexKey::Text("sales".to_string()),
            &IndexKey::Text("sales".to_string()),
        )
        .expect("range_scan should work on tree secondary index");
    assert_eq!(exact_range.len(), 2, "exact range should return both sales rows");

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn test_secondary_hash_equality_lookup() {
    let db_name = "test_db_secondary_hash";
    let table_name = "orders";
    let index_name = "order_id_hash_idx";

    let index_entry = IndexEntry {
        index_name: index_name.to_string(),
        column_name: vec!["id".to_string()],
        algorithm: IndexAlgorithm::ChainedHash,
        is_clustered: false,
        include_columns: Vec::new(),
    };

    let (catalog, mut file, table_path, table_dir) = setup_table(
        db_name,
        table_name,
        vec![Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        }],
        vec![index_entry.clone()],
    );

    for id in [5i32, 5, 7] {
        insert_tuple_with_rid(&mut file, &id.to_le_bytes()).expect("failed to insert tuple");
    }

    let built = AnyIndex::build_secondary_index(&catalog, db_name, table_name, &index_entry)
        .expect("build_secondary_index failed");

    let path = secondary_index_file_path(db_name, table_name, index_name);
    let _ = fs::remove_file(&path);
    built.save(&path).expect("save secondary index failed");

    let loaded = AnyIndex::load(&path, &IndexAlgorithm::ChainedHash).expect("load secondary failed");
    let hits = loaded
        .search(&IndexKey::Int(5))
        .expect("hash search should succeed");

    assert_eq!(hits.len(), 2, "hash secondary should support equality lookups");

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn test_secondary_composite_rebuild_removes_stale_entries() {
    let db_name = "test_db_secondary_composite";
    let table_name = "people";
    let index_name = "name_city_idx";

    let index_entry = IndexEntry {
        index_name: index_name.to_string(),
        column_name: vec!["name".to_string(), "city".to_string()],
        algorithm: IndexAlgorithm::BPlusTree,
        is_clustered: false,
        include_columns: Vec::new(),
    };

    let (catalog, mut file, table_path, table_dir) = setup_table(
        db_name,
        table_name,
        vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
            },
            Column {
                name: "city".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
        vec![index_entry.clone()],
    );

    for (id, name, city) in [(1, "alice", "ny"), (2, "alice", "la"), (3, "bob", "ny")] {
        let tuple = encode_int_text_text(id, name, city);
        insert_tuple_with_rid(&mut file, &tuple).expect("failed to insert tuple");
    }

    let built = AnyIndex::build_secondary_index(&catalog, db_name, table_name, &index_entry)
        .expect("build_secondary_index failed");

    let path = secondary_index_file_path(db_name, table_name, index_name);
    let _ = fs::remove_file(&path);
    built.save(&path).expect("save secondary index failed");

    let columns = &catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap()
        .columns;

    let valid_key = index_key_from_values(
        columns,
        &index_entry.column_name,
        &vec!["alice".to_string(), "la".to_string()],
    )
    .expect("failed to parse composite lookup key");

    let stale_key = index_key_from_values(
        columns,
        &index_entry.column_name,
        &vec!["zzzz".to_string(), "zzzz".to_string()],
    )
    .expect("failed to parse stale key");

    let mut tampered = AnyIndex::load(&path, &IndexAlgorithm::BPlusTree).expect("load failed");
    tampered
        .insert(stale_key.clone(), RecordId::new(99, 99))
        .expect("failed to inject stale entry");
    tampered.save(&path).expect("failed to save tampered index");

    rebuild_secondary_index(&catalog, db_name, table_name, index_name)
        .expect("rebuild_secondary_index failed");

    let rebuilt = AnyIndex::load(&path, &IndexAlgorithm::BPlusTree).expect("load rebuilt failed");

    let valid_hits = rebuilt.search(&valid_key).expect("search valid composite failed");
    assert_eq!(valid_hits.len(), 1, "expected one matching composite key");

    let stale_hits = rebuilt.search(&stale_key).expect("search stale key failed");
    assert!(
        stale_hits.is_empty(),
        "secondary rebuild should remove stale injected entries"
    );

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn test_secondary_composite_bplus_large_save_does_not_overflow_page() {
    let db_name = "test_db_secondary_composite_large";
    let table_name = "events";
    let index_name = "id_customer_idx";

    let index_entry = IndexEntry {
        index_name: index_name.to_string(),
        column_name: vec!["id".to_string(), "customer_id".to_string()],
        algorithm: IndexAlgorithm::BPlusTree,
        is_clustered: false,
        include_columns: Vec::new(),
    };

    let (catalog, mut file, table_path, table_dir) = setup_table(
        db_name,
        table_name,
        vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "customer_id".to_string(),
                data_type: "INT".to_string(),
            },
        ],
        vec![index_entry.clone()],
    );

    for i in 0..3000i32 {
        let mut tuple = Vec::with_capacity(8);
        tuple.extend_from_slice(&i.to_le_bytes());
        tuple.extend_from_slice(&((i * 17) % 997).to_le_bytes());
        insert_tuple_with_rid(&mut file, &tuple).expect("failed to insert tuple");
    }

    let built = AnyIndex::build_secondary_index(&catalog, db_name, table_name, &index_entry)
        .expect("build_secondary_index failed");

    let path = secondary_index_file_path(db_name, table_name, index_name);
    let _ = fs::remove_file(&path);
    built
        .save(&path)
        .expect("B+Tree save should not overflow a node page for this workload");

    let loaded = AnyIndex::load(&path, &IndexAlgorithm::BPlusTree).expect("load failed");
    assert!(loaded.entry_count() >= 3000, "expected all entries to persist");

    let _ = fs::remove_file(path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
