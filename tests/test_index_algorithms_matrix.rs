use std::fs;

use storage_manager::catalog::types::IndexAlgorithm;
use storage_manager::index::{AnyIndex, IndexKey, RecordId};

fn test_index_path(test_name: &str, algo: &IndexAlgorithm) -> String {
    let algo_name = match algo {
        IndexAlgorithm::StaticHash => "static_hash",
        IndexAlgorithm::ChainedHash => "chained_hash",
        IndexAlgorithm::ExtendibleHash => "extendible_hash",
        IndexAlgorithm::LinearHash => "linear_hash",
        IndexAlgorithm::BTree => "btree",
        IndexAlgorithm::BPlusTree => "bplus_tree",
        IndexAlgorithm::RadixTree => "radix_tree",
        IndexAlgorithm::SkipList => "skip_list",
        IndexAlgorithm::LsmTree => "lsm_tree",
    };

    format!(
        "database/base/testing/{}_{}.idx",
        test_name, algo_name
    )
}

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

#[test]
fn test_all_index_algorithms_insert_search_delete_and_persistence() {
    let key = IndexKey::Int(42);
    let missing_key = IndexKey::Int(9999);
    let rid1 = RecordId::new(1, 10);
    let rid2 = RecordId::new(2, 20);

    for algo in all_algorithms() {
        let mut index = AnyIndex::new_empty(&algo);

        index
            .insert(key.clone(), rid1.clone())
            .expect("insert rid1 failed");
        index
            .insert(key.clone(), rid2.clone())
            .expect("insert rid2 failed");
        index
            .insert(key.clone(), rid2.clone())
            .expect("duplicate insert should be tolerated");

        let mut found = index.search(&key).expect("search existing key failed");
        found.sort_by_key(|r| (r.page_no, r.item_id));

        assert!(
            found.contains(&rid1) && found.contains(&rid2),
            "algorithm {:?}: expected both record ids for duplicate key",
            algo
        );

        let not_found = index
            .search(&missing_key)
            .expect("search missing key failed");
        assert!(
            not_found.is_empty(),
            "algorithm {:?}: missing key should return empty result",
            algo
        );

        let removed = index
            .delete(&key, &rid1)
            .expect("delete existing record failed");
        assert!(
            removed,
            "algorithm {:?}: delete should report removed=true",
            algo
        );

        let removed_again = index
            .delete(&key, &rid1)
            .expect("delete non-existing record should not error");
        assert!(
            !removed_again,
            "algorithm {:?}: second delete should report removed=false",
            algo
        );

        let after_delete = index
            .search(&key)
            .expect("search after delete failed");
        assert!(
            after_delete.contains(&rid2) && !after_delete.contains(&rid1),
            "algorithm {:?}: expected only rid2 after deleting rid1",
            algo
        );

        let path = test_index_path("matrix_persist", &algo);
        let _ = fs::remove_file(&path);

        index.save(&path).expect("save index failed");
        let loaded = AnyIndex::load(&path, &algo).expect("load index failed");
        let loaded_records = loaded
            .search(&key)
            .expect("search after reload failed");

        assert!(
            loaded_records.contains(&rid2) && !loaded_records.contains(&rid1),
            "algorithm {:?}: persistence should keep post-delete state",
            algo
        );

        fs::remove_file(&path).expect("cleanup index file failed");
    }
}

#[test]
fn test_tree_range_scan_and_hash_rejection() {
    let tree_algorithms = vec![
        IndexAlgorithm::BTree,
        IndexAlgorithm::BPlusTree,
        IndexAlgorithm::RadixTree,
        IndexAlgorithm::SkipList,
        IndexAlgorithm::LsmTree,
    ];

    for algo in tree_algorithms {
        let mut index = AnyIndex::new_empty(&algo);
        index
            .insert(IndexKey::Int(1), RecordId::new(1, 1))
            .expect("insert key 1 failed");
        index
            .insert(IndexKey::Int(5), RecordId::new(1, 5))
            .expect("insert key 5 failed");
        index
            .insert(IndexKey::Int(9), RecordId::new(1, 9))
            .expect("insert key 9 failed");

        let scanned = index
            .range_scan(&IndexKey::Int(2), &IndexKey::Int(8))
            .expect("range scan failed for tree index");

        assert_eq!(
            scanned.len(),
            1,
            "algorithm {:?}: expected exactly one key in [2,8]",
            algo
        );
        assert_eq!(scanned[0], RecordId::new(1, 5));
    }

    let hash_algorithms = vec![
        IndexAlgorithm::StaticHash,
        IndexAlgorithm::ChainedHash,
        IndexAlgorithm::ExtendibleHash,
        IndexAlgorithm::LinearHash,
    ];

    for algo in hash_algorithms {
        let index = AnyIndex::new_empty(&algo);
        let err = index
            .range_scan(&IndexKey::Int(1), &IndexKey::Int(2))
            .expect_err("hash index range_scan should be unsupported");
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    }
}

#[test]
fn test_lsm_and_skip_list_string_key_persistence_regression() {
    let algorithms = vec![IndexAlgorithm::LsmTree, IndexAlgorithm::SkipList];

    for algo in algorithms {
        let mut index = AnyIndex::new_empty(&algo);
        let key = IndexKey::Text("alpha".to_string());
        let rid = RecordId::new(7, 11);

        index
            .insert(key.clone(), rid.clone())
            .expect("insert into tree index failed");

        let path = test_index_path("string_key_regression", &algo);
        let _ = fs::remove_file(&path);

        index
            .save(&path)
            .expect("save should not fail with JSON key-serialization errors");

        let loaded = AnyIndex::load(&path, &algo).expect("load saved index failed");
        let found = loaded.search(&key).expect("search loaded index failed");

        assert_eq!(found, vec![rid]);

        fs::remove_file(&path).expect("cleanup index file failed");
    }
}
