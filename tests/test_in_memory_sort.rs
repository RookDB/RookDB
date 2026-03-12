//! Tests for in_memory_sort: sorting a small heap table into an ordered file.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey, Table};
use storage_manager::disk::{read_page, write_page};
use storage_manager::ordered::ordered_file::read_ordered_file_header;
use storage_manager::page::{init_page, Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use storage_manager::sorting::in_memory_sort::in_memory_sort;
use storage_manager::table::page_count;

/// Helper: build a tuple from (i32, &str) for schema (INT, TEXT).
fn make_tuple(id: i32, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

/// Insert a tuple into a page. Returns false if page is full.
fn insert_into_page(page: &mut Page, tuple: &[u8]) -> bool {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    let tuple_len = tuple.len() as u32;
    let required = tuple_len + ITEM_ID_SIZE;
    if required > upper - lower {
        return false;
    }
    let start = upper - tuple_len;
    page.data[start as usize..upper as usize].copy_from_slice(tuple);
    page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
    page.data[lower as usize + 4..lower as usize + 8].copy_from_slice(&tuple_len.to_le_bytes());
    let new_lower = lower + ITEM_ID_SIZE;
    let new_upper = start;
    page.data[0..4].copy_from_slice(&new_lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&new_upper.to_le_bytes());
    true
}

/// Extract INT value from a tuple at offset 0.
fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

/// Set up catalog, database directory, and table file for testing.
/// Returns (Catalog, file path).
fn setup_test_env(
    db_name: &str,
    table_name: &str,
    columns: Vec<Column>,
    tuples: &[Vec<u8>],
) -> (Catalog, String) {
    // Ensure directories exist
    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    // Create catalog
    let table = Table {
        columns: columns.clone(),
        sort_keys: None,
        file_type: None,
    };
    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    let database = Database { tables };
    let mut databases = HashMap::new();
    databases.insert(db_name.to_string(), database);
    let catalog = Catalog { databases };

    // Save catalog to disk (needed by in_memory_sort -> save_catalog)
    let catalog_json = serde_json::to_string_pretty(&catalog).unwrap();
    fs::write("database/global/catalog.json", &catalog_json).unwrap();

    // Create table file with tuples
    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();

    // Header page (page 0): page_count = 1 initially
    let mut header = vec![0u8; PAGE_SIZE];
    header[0..4].copy_from_slice(&1u32.to_le_bytes());
    file.write_all(&header).unwrap();

    // Insert tuples into data pages
    let mut current_page = Page::new();
    init_page(&mut current_page);
    let mut page_num = 1u32;

    for tuple in tuples {
        if !insert_into_page(&mut current_page, tuple) {
            // Write full page
            write_page(&mut file, &mut current_page, page_num).unwrap();
            page_num += 1;
            current_page = Page::new();
            init_page(&mut current_page);
            // Extend file for new page
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::End(0)).unwrap();
            file.write_all(&vec![0u8; PAGE_SIZE]).unwrap();
            insert_into_page(&mut current_page, tuple);
        }
    }
    // Write last page (extend file if needed)
    {
        use std::io::{Seek, SeekFrom};
        let needed_size = (page_num + 1) as u64 * PAGE_SIZE as u64;
        let file_size = file.metadata().unwrap().len();
        if needed_size > file_size {
            file.seek(SeekFrom::End(0)).unwrap();
            let gap = needed_size - file_size;
            file.write_all(&vec![0u8; gap as usize]).unwrap();
        }
    }
    write_page(&mut file, &mut current_page, page_num).unwrap();

    // Update page count in header
    let total = page_num + 1;
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&total.to_le_bytes()).unwrap();
        file.flush().unwrap();
    }

    (catalog, file_path)
}

/// Read all tuples from a table file (starting from page 1).
fn read_all_tuples(file_path: &str) -> Vec<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .unwrap();
    let total = page_count(&mut file).unwrap();
    let mut tuples = Vec::new();
    for p in 1..total {
        let mut page = Page::new();
        read_page(&mut file, &mut page, p).unwrap();
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            tuples.push(page.data[offset..offset + length].to_vec());
        }
    }
    tuples
}

/// Cleanup test environment.
fn cleanup(db_name: &str, table_name: &str) {
    let _ = fs::remove_file(format!("database/base/{}/{}.dat", db_name, table_name));
    let _ = fs::remove_dir(format!("database/base/{}", db_name));
}

#[test]
fn test_in_memory_sort_int_asc() {
    let db = "test_ims_db1";
    let tbl = "test_ims_tbl1";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(30, "Charlie"),
        make_tuple(10, "Alice"),
        make_tuple(50, "Eve"),
        make_tuple(20, "Bob"),
        make_tuple(40, "Diana"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    // Verify sorted order
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 5);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![10, 20, 30, 40, 50]);

    // Verify header is ordered
    let mut file2 = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();
    let header = read_ordered_file_header(&mut file2).unwrap();
    assert_eq!(
        header.file_type,
        storage_manager::ordered::ordered_file::FileType::Ordered
    );

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_int_desc() {
    let db = "test_ims_db2";
    let tbl = "test_ims_tbl2";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(10, "Alice"),
        make_tuple(50, "Eve"),
        make_tuple(20, "Bob"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Descending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![50, 20, 10]);

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_text_asc() {
    let db = "test_ims_db3";
    let tbl = "test_ims_tbl3";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(1, "Charlie"),
        make_tuple(2, "Alice"),
        make_tuple(3, "Bob"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Ascending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    // Alice=2, Bob=3, Charlie=1
    assert_eq!(ids, vec![2, 3, 1]);

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_already_sorted() {
    let db = "test_ims_db4";
    let tbl = "test_ims_tbl4";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(1, "Alice"),
        make_tuple(2, "Bob"),
        make_tuple(3, "Charlie"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![1, 2, 3]);

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_single_tuple() {
    let db = "test_ims_db5";
    let tbl = "test_ims_tbl5";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples = vec![make_tuple(42, "Solo")];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 1);
    assert_eq!(extract_int(&result[0]), 42);

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_duplicate_keys() {
    let db = "test_ims_db6";
    let tbl = "test_ims_tbl6";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(5, "Alpha"),
        make_tuple(5, "Beta"),
        make_tuple(5, "Gamma"),
        make_tuple(3, "Delta"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![3, 5, 5, 5]);

    cleanup(db, tbl);
}

#[test]
fn test_in_memory_sort_multicolumn() {
    let db = "test_ims_db7";
    let tbl = "test_ims_tbl7";
    let columns = vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(2, "Bob"),
        make_tuple(1, "Bob"),
        make_tuple(1, "Alice"),
        make_tuple(2, "Alice"),
    ];

    let (mut catalog, file_path) = setup_test_env(db, tbl, columns, &tuples);

    // Sort by name ASC, then id ASC
    let sort_keys = vec![
        SortKey {
            column_index: 1,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 0,
            direction: SortDirection::Ascending,
        },
    ];

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    in_memory_sort(&mut catalog, db, tbl, sort_keys, &mut file).expect("in_memory_sort failed");

    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    // Alice(1), Alice(2), Bob(1), Bob(2)
    assert_eq!(ids, vec![1, 2, 1, 2]);

    cleanup(db, tbl);
}
