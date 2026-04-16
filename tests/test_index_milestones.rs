use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};

use storage_manager::catalog::types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
use storage_manager::heap::{init_table, insert_tuple_with_index_maintenance, insert_tuple_with_rid};
use storage_manager::index::{
    AnyIndex, HashBasedIndex, IndexKey, IndexTrait, RecordId, TreeBasedIndex, cluster_table_by_index,
    index_file_path, validate_all_table_indexes, validate_index_consistency,
};
use storage_manager::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use storage_manager::table::page_count;

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

fn encode_int_text_tuple(id: i32, name: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(14);
    out.extend_from_slice(&id.to_le_bytes());
    let mut fixed = [b' '; 10];
    for (i, b) in name.as_bytes().iter().take(10).enumerate() {
        fixed[i] = *b;
    }
    out.extend_from_slice(&fixed);
    out
}

fn scan_live_int_ids(db_name: &str, table_name: &str) -> std::io::Result<Vec<i32>> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new().read(true).open(table_path)?;
    let total_pages = page_count(&mut file)?;

    let mut ids = Vec::new();
    for page_num in 1..total_pages {
        let page_offset = page_num as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(page_offset))?;

        let mut page_bytes = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut page_bytes)?;

        let lower = u32::from_le_bytes(page_bytes[0..4].try_into().unwrap()) as usize;
        let num_items = (lower - PAGE_HEADER_SIZE as usize) / ITEM_ID_SIZE as usize;

        for item_id in 0..num_items as u32 {
            let slot_base = PAGE_HEADER_SIZE as usize + item_id as usize * ITEM_ID_SIZE as usize;
            let tuple_offset =
                u32::from_le_bytes(page_bytes[slot_base..slot_base + 4].try_into().unwrap())
                    as usize;
            let tuple_len =
                u32::from_le_bytes(page_bytes[slot_base + 4..slot_base + 8].try_into().unwrap())
                    as usize;

            if tuple_len == 0 || tuple_offset + tuple_len > page_bytes.len() || tuple_len < 4 {
                continue;
            }

            let id = i32::from_le_bytes(
                page_bytes[tuple_offset..tuple_offset + 4]
                    .try_into()
                    .unwrap(),
            );
            ids.push(id);
        }
    }

    Ok(ids)
}

#[test]
fn milestone1_trait_hierarchy_and_secondary_representation() {
    fn assert_hash_index<T: HashBasedIndex + IndexTrait>() {}
    fn assert_tree_index<T: TreeBasedIndex + IndexTrait>() {}

    assert_hash_index::<storage_manager::index::hash::StaticHashIndex>();
    assert_hash_index::<storage_manager::index::hash::ChainedHashIndex>();
    assert_hash_index::<storage_manager::index::hash::ExtendibleHashIndex>();
    assert_hash_index::<storage_manager::index::hash::LinearHashIndex>();

    assert_tree_index::<storage_manager::index::tree::BTree>();
    assert_tree_index::<storage_manager::index::tree::BPlusTree>();
    assert_tree_index::<storage_manager::index::tree::RadixTree>();
    assert_tree_index::<storage_manager::index::tree::SkipListIndex>();
    assert_tree_index::<storage_manager::index::tree::LsmTreeIndex>();

    let all_algorithms = vec![
        IndexAlgorithm::StaticHash,
        IndexAlgorithm::ChainedHash,
        IndexAlgorithm::ExtendibleHash,
        IndexAlgorithm::LinearHash,
        IndexAlgorithm::BTree,
        IndexAlgorithm::BPlusTree,
        IndexAlgorithm::RadixTree,
        IndexAlgorithm::SkipList,
        IndexAlgorithm::LsmTree,
    ];

    for algo in all_algorithms {
        let index = AnyIndex::new_empty(&algo);
        assert!(!index.index_type_name().is_empty());
        assert_eq!(index.entry_count(), 0);
        assert!(index.all_entries().expect("all_entries failed").is_empty());
        index
            .validate_structure()
            .expect("new empty index should be structurally valid");
    }

    let table = Table {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
        indexes: vec![IndexEntry {
            index_name: "name_idx".to_string(),
            column_name: "name".to_string(),
            algorithm: IndexAlgorithm::BPlusTree,
            is_clustered: false,
            include_columns: vec!["id".to_string()],
        }],
    };

    let encoded = serde_json::to_string(&table).expect("table serialization failed");
    let decoded: Table = serde_json::from_str(&encoded).expect("table deserialization failed");
    assert_eq!(decoded.indexes.len(), 1);
    assert_eq!(decoded.indexes[0].column_name, "name");
    assert_eq!(decoded.indexes[0].include_columns, vec!["id".to_string()]);
    assert!(!decoded.indexes[0].is_clustered);
}

#[test]
fn milestone2_core_algorithms_build_index_and_verify_contents() {
    let db_name = "test_db_m2_algorithms";
    let table_name = "m2_table";

    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);

    let _ = fs::remove_file(&table_path);
    let _ = fs::remove_dir_all(&table_dir);
    fs::create_dir_all(&table_dir).expect("failed to create table dir");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&table_path)
        .expect("failed to open table file");
    init_table(&mut file).expect("init_table failed");

    let values = vec![5, 1, 9, 1, 7, 2];
    let mut expected_entries = Vec::new();

    for v in values {
        let tuple = (v as i32).to_le_bytes();
        let rid = insert_tuple_with_rid(&mut file, &tuple).expect("insert_tuple_with_rid failed");
        expected_entries.push((IndexKey::Int(v), rid));
    }

    let mut tables = HashMap::new();
    tables.insert(
        table_name.to_string(),
        Table {
            columns: vec![Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            }],
            indexes: Vec::new(),
        },
    );

    let mut dbs = HashMap::new();
    dbs.insert(db_name.to_string(), Database { tables });
    let catalog = Catalog { databases: dbs };

    let algorithms = vec![
        IndexAlgorithm::StaticHash,
        IndexAlgorithm::ExtendibleHash,
        IndexAlgorithm::LinearHash,
        IndexAlgorithm::BTree,
        IndexAlgorithm::BPlusTree,
        IndexAlgorithm::RadixTree,
    ];

    let expected_sorted = normalize_entries(expected_entries.clone());

    for algo in algorithms {
        let index = AnyIndex::build_from_table(&catalog, db_name, table_name, "id", &algo)
            .expect("build_from_table failed");

        let actual_sorted = normalize_entries(index.all_entries().expect("all_entries failed"));
        assert_eq!(
            actual_sorted, expected_sorted,
            "algorithm {:?}: built entries do not match table content",
            algo
        );

        index
            .validate_structure()
            .expect("validate_structure should pass after build");

        let path = index_file_path(db_name, table_name, &format!("m2_{:?}", algo));
        let _ = fs::remove_file(&path);
        index.save(&path).expect("save built index failed");

        let loaded = AnyIndex::load(&path, &algo).expect("load built index failed");
        let loaded_sorted = normalize_entries(loaded.all_entries().expect("all_entries failed"));
        assert_eq!(
            loaded_sorted, expected_sorted,
            "algorithm {:?}: loaded entries differ from saved entries",
            algo
        );

        let point = loaded
            .search(&IndexKey::Int(1))
            .expect("search key 1 failed");
        assert_eq!(point.len(), 2, "algorithm {:?}: expected two rids for key=1", algo);

        if loaded.supports_range_scan() {
            let ranged = loaded
                .range_scan(&IndexKey::Int(2), &IndexKey::Int(7))
                .expect("range scan failed");
            assert!(
                ranged.len() >= 3,
                "algorithm {:?}: expected at least keys 2,5,7 in range",
                algo
            );
        }

        fs::remove_file(path).expect("cleanup index file failed");
    }

    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn milestone3_persistence_and_validation_with_content_checks() {
    let db_name = "test_db_m3_integrity";
    let table_name = "m3_table";

    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);

    let _ = fs::remove_file(&table_path);
    let _ = fs::remove_dir_all(&table_dir);
    fs::create_dir_all(&table_dir).expect("failed to create table dir");

    let algorithms = vec![
        IndexAlgorithm::ChainedHash,
        IndexAlgorithm::LsmTree,
        IndexAlgorithm::SkipList,
    ];

    for algo in algorithms {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&table_path)
            .expect("failed to open table file");
        init_table(&mut file).expect("init_table failed");

        let index_name = format!("m3_{:?}", algo);
        let index_path = index_file_path(db_name, table_name, &index_name);
        let _ = fs::remove_file(&index_path);

        let table = Table {
            columns: vec![Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            }],
            indexes: vec![IndexEntry {
                index_name: index_name.clone(),
                column_name: "id".to_string(),
                algorithm: algo.clone(),
                is_clustered: false,
                include_columns: Vec::new(),
            }],
        };

        let mut tables = HashMap::new();
        tables.insert(table_name.to_string(), table);
        let mut dbs = HashMap::new();
        dbs.insert(db_name.to_string(), Database { tables });
        let catalog = Catalog { databases: dbs };

        AnyIndex::new_empty(&algo)
            .save(&index_path)
            .expect("failed to create empty index file");

        let _ = insert_tuple_with_index_maintenance(
            &catalog,
            db_name,
            table_name,
            &mut file,
            &10i32.to_le_bytes(),
        )
        .expect("insert 10 failed");
        let _ = insert_tuple_with_index_maintenance(
            &catalog,
            db_name,
            table_name,
            &mut file,
            &20i32.to_le_bytes(),
        )
        .expect("insert 20 failed");

        let index = AnyIndex::load(&index_path, &algo).expect("load index failed");
        let entries = normalize_entries(index.all_entries().expect("all_entries failed"));
        assert_eq!(entries.len(), 2, "algorithm {:?}: expected two indexed tuples", algo);

        validate_index_consistency(&catalog, db_name, table_name, &index_name)
            .expect("validation should pass before tamper");

        let mut tampered = AnyIndex::load(&index_path, &algo).expect("load for tamper failed");
        tampered
            .insert(IndexKey::Int(999), RecordId::new(99, 99))
            .expect("tamper insert failed");
        tampered.save(&index_path).expect("tamper save failed");

        let err = validate_index_consistency(&catalog, db_name, table_name, &index_name)
            .expect_err("validation should fail after stale tamper");
        assert!(err.to_string().contains("stale entries"));

        let persisted = fs::read(&index_path).expect("read persisted index failed");
        assert!(persisted.len() >= 8, "persisted index should include header");
        assert_eq!(
            &persisted[..8],
            b"RDBIDXV1",
            "persisted index should use paged binary format",
        );

        let _ = fs::remove_file(index_path);
    }

    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}

#[test]
fn milestone4_secondary_and_clustered_index_integration() {
    let db_name = "test_db_m4_secondary_clustered";
    let table_name = "m4_table";

    let table_dir = format!("database/base/{}", db_name);
    let table_path = format!("{}/{}.dat", table_dir, table_name);

    let _ = fs::remove_file(&table_path);
    let _ = fs::remove_dir_all(&table_dir);
    fs::create_dir_all(&table_dir).expect("failed to create table dir");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&table_path)
        .expect("failed to open table file");
    init_table(&mut file).expect("init_table failed");

    let primary_idx_name = "m4_id_clustered".to_string();
    let secondary_idx_name = "m4_name_secondary".to_string();

    let table = Table {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
        indexes: vec![
            IndexEntry {
                index_name: primary_idx_name.clone(),
                column_name: "id".to_string(),
                algorithm: IndexAlgorithm::BPlusTree,
                is_clustered: true,
                include_columns: Vec::new(),
            },
            IndexEntry {
                index_name: secondary_idx_name.clone(),
                column_name: "name".to_string(),
                algorithm: IndexAlgorithm::LsmTree,
                is_clustered: false,
                include_columns: vec!["id".to_string()],
            },
        ],
    };

    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    let mut dbs = HashMap::new();
    dbs.insert(db_name.to_string(), Database { tables });
    let catalog = Catalog { databases: dbs };

    let primary_path = index_file_path(db_name, table_name, &primary_idx_name);
    let secondary_path = index_file_path(db_name, table_name, &secondary_idx_name);
    let _ = fs::remove_file(&primary_path);
    let _ = fs::remove_file(&secondary_path);

    AnyIndex::new_empty(&IndexAlgorithm::BPlusTree)
        .save(&primary_path)
        .expect("create primary index file failed");
    AnyIndex::new_empty(&IndexAlgorithm::LsmTree)
        .save(&secondary_path)
        .expect("create secondary index file failed");

    let rows = vec![(30, "ccc"), (10, "aaa"), (20, "bbb")];
    for (id, name) in rows {
        let tuple = encode_int_text_tuple(id, name);
        let _ = insert_tuple_with_index_maintenance(&catalog, db_name, table_name, &mut file, &tuple)
            .expect("insert with index maintenance failed");
    }

    let primary = AnyIndex::load(&primary_path, &IndexAlgorithm::BPlusTree)
        .expect("load primary index failed");
    let secondary = AnyIndex::load(&secondary_path, &IndexAlgorithm::LsmTree)
        .expect("load secondary index failed");

    let primary_entries = primary.all_entries().expect("primary all_entries failed");
    let secondary_entries = secondary.all_entries().expect("secondary all_entries failed");

    assert_eq!(primary_entries.len(), 3, "primary index should contain all tuples");
    assert_eq!(secondary_entries.len(), 3, "secondary index should contain all tuples");

    let secondary_hits = secondary
        .search(&IndexKey::Text("aaa".to_string()))
        .expect("search secondary index failed");
    assert_eq!(secondary_hits.len(), 1, "secondary name index should find one row for 'aaa'");

    let validated = validate_all_table_indexes(&catalog, db_name, table_name)
        .expect("validate_all_table_indexes should pass");
    assert_eq!(validated, 2);

    let before_cluster_ids = scan_live_int_ids(db_name, table_name).expect("scan table ids failed");
    assert_eq!(before_cluster_ids, vec![30, 10, 20]);

    cluster_table_by_index(&catalog, db_name, table_name, &primary_idx_name)
        .expect("cluster_table_by_index failed");

    let after_cluster_ids = scan_live_int_ids(db_name, table_name).expect("scan clustered ids failed");
    assert_eq!(after_cluster_ids, vec![10, 20, 30]);

    let validated_after = validate_all_table_indexes(&catalog, db_name, table_name)
        .expect("validate_all_table_indexes after cluster should pass");
    assert_eq!(validated_after, 2);

    let _ = fs::remove_file(primary_path);
    let _ = fs::remove_file(secondary_path);
    let _ = fs::remove_file(table_path);
    let _ = fs::remove_dir_all(table_dir);
}
