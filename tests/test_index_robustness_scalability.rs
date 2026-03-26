use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::{init_table, insert_tuple_with_rid};
use storage_manager::index::{AnyIndex, IndexKey, RecordId, index_file_path, validate_index_consistency};

fn all_algorithms() -> Vec<IndexAlgorithm> {
    vec![
        IndexAlgorithm::StaticHash,
        IndexAlgorithm::ChainedHash,
        IndexAlgorithm::ExtendibleHash,
        IndexAlgorithm::LinearHash,
        IndexAlgorithm::BTree,
        IndexAlgorithm::BPlusTree,
        IndexAlgorithm::RadixTree,
        IndexAlgorithm::SkipList,
        IndexAlgorithm::LsmTree,
    ]
}

fn normalize_entries(mut entries: Vec<(IndexKey, RecordId)>) -> Vec<(IndexKey, RecordId)> {
    entries.sort_by(|a, b| {
        let key_cmp = a.0.cmp(&b.0);
        if key_cmp != std::cmp::Ordering::Equal {
            return key_cmp;
        }
        (a.1.page_no, a.1.item_id).cmp(&(b.1.page_no, b.1.item_id))
    });
    entries
}

fn setup_dummy_db_table_int(
    db_name: &str,
    table_name: &str,
) -> (Catalog, File, String, String) {
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

    let table = Table {
        columns: vec![Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        }],
        indexes: Vec::new(),
    };

    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    let mut dbs = HashMap::new();
    dbs.insert(db_name.to_string(), Database { tables });

    (Catalog { databases: dbs }, file, table_path, table_dir)
}

#[test]
fn test_dummy_db_normal_and_edge_cases_for_all_indexes() {
    let db_name = "dummy_db_normal_edges";
    let table_name = "dummy_table";
    let (catalog, mut file, table_path, table_dir) = setup_dummy_db_table_int(db_name, table_name);

    let mut expected_entries = Vec::new();
    for i in 0..250i32 {
        let v = ((i * 17) % 53) - 26;
        let tuple = v.to_le_bytes();
        let rid = insert_tuple_with_rid(&mut file, &tuple).expect("insert_tuple_with_rid failed");
        expected_entries.push((IndexKey::Int(v as i64), rid));
    }
    let expected_sorted = normalize_entries(expected_entries.clone());

    for algo in all_algorithms() {
        let mut index = AnyIndex::build_from_table(&catalog, db_name, table_name, "id", &algo)
            .expect("build_from_table failed");

        let entries = normalize_entries(index.all_entries().expect("all_entries failed"));
        assert_eq!(
            entries, expected_sorted,
            "algorithm {:?}: index entries do not match tuple data",
            algo
        );

        assert_eq!(
            index.entry_count(),
            expected_sorted.len(),
            "algorithm {:?}: entry_count mismatch",
            algo
        );

        let existing = index
            .search(&IndexKey::Int(0))
            .expect("search existing key failed");
        assert!(!existing.is_empty(), "algorithm {:?}: expected key 0 to exist", algo);

        let missing = index
            .search(&IndexKey::Int(999_999))
            .expect("search missing key failed");
        assert!(missing.is_empty(), "algorithm {:?}: missing key should return empty", algo);

        let removed = index
            .delete(&IndexKey::Int(999_999), &RecordId::new(1, 1))
            .expect("delete missing key should not error");
        assert!(!removed, "algorithm {:?}: delete missing should return false", algo);

        if index.supports_range_scan() {
            let ranged = index
                .range_scan(&IndexKey::Int(-5), &IndexKey::Int(5))
                .expect("tree range_scan failed");
            assert!(
                !ranged.is_empty(),
                "algorithm {:?}: expected non-empty range around zero",
                algo
            );
        } else {
            let err = index
                .range_scan(&IndexKey::Int(-5), &IndexKey::Int(5))
                .expect_err("hash range_scan should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        }

        index
            .validate_structure()
            .expect("validate_structure should pass");

        let index_name = format!("normal_edge_{:?}", algo);
        let path = index_file_path(db_name, table_name, &index_name);
        let _ = fs::remove_file(&path);
        index.save(&path).expect("save failed");

        let meta = fs::metadata(&path).expect("saved index file metadata failed");
        assert!(meta.len() > 0, "saved index file should be non-empty");

        let loaded = AnyIndex::load(&path, &algo).expect("load failed");
        let loaded_entries = normalize_entries(loaded.all_entries().expect("loaded all_entries failed"));
        assert_eq!(
            loaded_entries, expected_sorted,
            "algorithm {:?}: loaded entries differ from expected",
            algo
        );

        fs::remove_file(path).expect("cleanup index file failed");
    }

    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn test_dummy_db_failure_handling_paths() {
    let db_name = "dummy_db_failure_paths";
    let table_name = "dummy_table";
    let (_catalog, _file, table_path, table_dir) = setup_dummy_db_table_int(db_name, table_name);

    let missing_load_err = AnyIndex::load(
        "database/base/does_not_exist/missing.idx",
        &IndexAlgorithm::BPlusTree,
    )
    .err()
    .expect("loading non-existing index should fail");
    assert_eq!(missing_load_err.kind(), std::io::ErrorKind::NotFound);

    let (catalog, mut file, _, _) = setup_dummy_db_table_int("dummy_db_failure_build", table_name);
    let tuple = 7i32.to_le_bytes();
    let _ = insert_tuple_with_rid(&mut file, &tuple).expect("seed insert failed");

    let bad_db = AnyIndex::build_from_table(
        &catalog,
        "missing_db",
        table_name,
        "id",
        &IndexAlgorithm::BPlusTree,
    )
    .err()
    .expect("build_from_table should fail for missing db");
    assert_eq!(bad_db.kind(), std::io::ErrorKind::NotFound);

    let bad_table = AnyIndex::build_from_table(
        &catalog,
        "dummy_db_failure_build",
        "missing_table",
        "id",
        &IndexAlgorithm::BPlusTree,
    )
    .err()
    .expect("build_from_table should fail for missing table");
    assert_eq!(bad_table.kind(), std::io::ErrorKind::NotFound);

    let bad_column = AnyIndex::build_from_table(
        &catalog,
        "dummy_db_failure_build",
        table_name,
        "missing_column",
        &IndexAlgorithm::BPlusTree,
    )
    .err()
    .expect("build_from_table should fail for missing column");
    assert_eq!(bad_column.kind(), std::io::ErrorKind::NotFound);

    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
    let _ = fs::remove_dir_all("database/base/dummy_db_failure_build");
}

#[test]
fn test_dummy_db_scalability_and_consistency_all_algorithms() {
    let db_name = "dummy_db_scale";
    let table_name = "dummy_scale_table";

    let (mut catalog, mut file, table_path, table_dir) = setup_dummy_db_table_int(db_name, table_name);

    let mut expected_entries = Vec::new();
    for i in 0..2000i32 {
        let v = ((i * 31) % 211) - 105;
        let tuple = v.to_le_bytes();
        let rid = insert_tuple_with_rid(&mut file, &tuple).expect("scalability insert failed");
        expected_entries.push((IndexKey::Int(v as i64), rid));
    }
    let expected_sorted = normalize_entries(expected_entries.clone());

    for algo in all_algorithms() {
        let index_name = format!("scale_{:?}", algo);
        {
            let table = catalog
                .databases
                .get_mut(db_name)
                .expect("missing db")
                .tables
                .get_mut(table_name)
                .expect("missing table");

            table.indexes.push(IndexEntry {
                index_name: index_name.clone(),
                column_name: "id".to_string(),
                algorithm: algo.clone(),
                is_clustered: false,
                include_columns: Vec::new(),
            });
        }

        let index = AnyIndex::build_from_table(&catalog, db_name, table_name, "id", &algo)
            .expect("build_from_table scalability failed");

        let path = index_file_path(db_name, table_name, &index_name);
        let _ = fs::remove_file(&path);
        index.save(&path).expect("save scalability index failed");

        let loaded = AnyIndex::load(&path, &algo).expect("load scalability index failed");
        let loaded_entries = normalize_entries(loaded.all_entries().expect("all_entries failed"));
        assert_eq!(
            loaded_entries, expected_sorted,
            "algorithm {:?}: scalability entries mismatch",
            algo
        );

        validate_index_consistency(&catalog, db_name, table_name, &index_name)
            .expect("consistency should pass on scalability dataset");

        fs::remove_file(path).expect("cleanup index file failed");
    }

    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
