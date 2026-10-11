mod common;

use std::env;

use storage_manager::heap::HeapManager;
use storage_manager::statistics::collect_heap_statistics;
use storage_manager::types::{DataType, serialize_nullable_row};

#[test]
fn test_collect_table_statistics_counts_pages_and_tuples() {
    let mut temp_path = env::temp_dir();
    temp_path.push("rookdb_table_statistics_test.tbl");

    let mut heap = HeapManager::create(temp_path.clone()).expect("Failed to create table");

    let schema = vec![DataType::Int, DataType::Varchar(16)];
    let rows = [
        [Some("1"), Some("alpha")],
        [Some("2"), Some("beta")],
        [Some("3"), Some("gamma")],
    ];

    let mut expected_tuple_bytes = 0u64;
    for row in rows {
        let tuple = serialize_nullable_row(&schema, &row).expect("Failed to serialize row");
        expected_tuple_bytes += tuple.len() as u64;
        heap.insert_tuple(&tuple).expect("Failed to insert tuple");
    }
    heap.flush().expect("Failed to flush table");
    drop(heap);

    let stats = collect_heap_statistics(
        &storage_manager::backend::engine::Engine::open(std::env::temp_dir()),
        &temp_path,
    )
    .expect("Failed to collect stats");

    assert_eq!(stats.total_pages, 2);
    assert_eq!(stats.data_pages, 1);
    assert_eq!(stats.total_tuple_count, 3);
    assert_eq!(stats.pages_with_tuples, 1);
    assert_eq!(stats.total_tuple_bytes, expected_tuple_bytes);
    assert_eq!(stats.total_slot_bytes, 3 * 8);
    assert_eq!(stats.total_header_bytes, 8);
    assert_eq!(stats.page_breakdown.len(), 1);

    let page = &stats.page_breakdown[0];
    assert_eq!(page.page_id, 1);
    assert_eq!(page.tuple_count, 3);
    assert_eq!(page.tuple_bytes, expected_tuple_bytes);
    assert_eq!(page.slot_bytes, 3 * 8);
    assert_eq!(page.header_bytes, 8);

    storage_manager::backend::cache::evict_heap(
        &storage_manager::backend::engine::Engine::open(std::env::temp_dir()),
        &temp_path,
    )
    .expect("Failed to evict table");
    std::fs::remove_file(&temp_path).expect("Failed to remove temp table file");
    let _ = std::fs::remove_file(format!("{}.fsm", temp_path.display()));
}

#[test]
fn test_collect_table_statistics_skips_soft_deleted_rows() {
    use storage_manager::backend::heap::HeapFile;
    use storage_manager::page::Page;

    let mut temp_path = env::temp_dir();
    temp_path.push("rookdb_table_statistics_deleted_test.tbl");

    let mut heap = HeapManager::create(temp_path.clone()).expect("Failed to create table");
    let schema = vec![DataType::Int, DataType::Varchar(16)];
    let mut sizes = Vec::new();
    for row in [[Some("1"), Some("alpha")], [Some("2"), Some("beta")]] {
        let tuple = serialize_nullable_row(&schema, &row).expect("Failed to serialize row");
        sizes.push(tuple.len() as u64);
        heap.insert_tuple(&tuple).expect("Failed to insert tuple");
    }
    heap.flush().expect("Failed to flush table");
    drop(heap);

    // Soft-delete the first row the way SQL DELETE does.
    let file = HeapFile::open(
        &storage_manager::backend::engine::Engine::open(std::env::temp_dir()),
        &temp_path,
    )
    .expect("Failed to open table");
    let mut page = Page::new();
    file.read_page(&mut page, 1).expect("Failed to read page");
    assert!(page.mark_deleted(0));
    file.write_page(&page, 1).expect("Failed to write page");
    drop(file);

    let stats = collect_heap_statistics(
        &storage_manager::backend::engine::Engine::open(std::env::temp_dir()),
        &temp_path,
    )
    .expect("Failed to collect stats");

    // The deleted row's slot still takes space but the row is not counted.
    assert_eq!(stats.total_tuple_count, 1);
    assert_eq!(stats.total_tuple_bytes, sizes[1]);
    assert_eq!(stats.min_tuple_bytes, sizes[1] as u32);
    assert_eq!(stats.max_tuple_bytes, sizes[1] as u32);
    assert_eq!(stats.total_slot_bytes, 2 * 8);

    storage_manager::backend::cache::evict_heap(
        &storage_manager::backend::engine::Engine::open(std::env::temp_dir()),
        &temp_path,
    )
    .expect("Failed to evict table");
    std::fs::remove_file(&temp_path).expect("Failed to remove temp table file");
    let _ = std::fs::remove_file(format!("{}.fsm", temp_path.display()));
}
