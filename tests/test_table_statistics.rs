use std::env;
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::PathBuf;

use storage_manager::heap::{init_table, insert_tuple};
use storage_manager::statistics::collect_table_statistics_from_file;
use storage_manager::types::{serialize_nullable_row, DataType};

#[test]
fn test_collect_table_statistics_counts_pages_and_tuples() {
    let mut temp_path = PathBuf::from(env::temp_dir());
    temp_path.push("rookdb_table_statistics_test.tbl");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)
        .expect("Failed to create temp table file");

    init_table(&mut file).expect("Failed to initialize table");

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
        insert_tuple(&mut file, &tuple).expect("Failed to insert tuple");
    }

    file.rewind().expect("Failed to rewind file");
    let stats = collect_table_statistics_from_file(&mut file).expect("Failed to collect stats");

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

    std::fs::remove_file(&temp_path).expect("Failed to remove temp table file");
}