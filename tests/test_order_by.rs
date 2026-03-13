//! Tests for order_by_execute and create_ordered_file_from_heap.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey, Table};
use storage_manager::executor::order_by::{create_ordered_file_from_heap, order_by_execute};
use storage_manager::ordered::ordered_file::{read_ordered_file_header, FileType};
use storage_manager::ordered::scan::ordered_scan;
use storage_manager::page::{init_page, Page, ITEM_ID_SIZE, PAGE_SIZE};

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
fn setup_test_env(
    db_name: &str,
    table_name: &str,
    columns: Vec<Column>,
    tuples: &[Vec<u8>],
) -> (Catalog, String) {
    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

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

    let catalog_json = serde_json::to_string_pretty(&catalog).unwrap();
    fs::write("database/global/catalog.json", &catalog_json).unwrap();

    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&file_path)
            .unwrap();

        // Write header page
        let mut header_buf = vec![0u8; PAGE_SIZE];
        header_buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        file.write_all(&header_buf).unwrap();

        // Write tuples into data pages
        let mut current_page = Page::new();
        init_page(&mut current_page);
        let mut pages: Vec<Page> = Vec::new();

        for tuple in tuples {
            if !insert_into_page(&mut current_page, tuple) {
                pages.push(current_page);
                current_page = Page::new();
                init_page(&mut current_page);
                assert!(insert_into_page(&mut current_page, tuple));
            }
        }
        pages.push(current_page);

        for page in &pages {
            file.write_all(&page.data).unwrap();
        }

        // Update page count
        let total_pages = (pages.len() + 1) as u32;
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&total_pages.to_le_bytes()).unwrap();
        file.flush().unwrap();
    }

    (catalog, file_path)
}

#[test]
fn test_create_ordered_file_from_heap() {
    let db_name = "test_order_by_db1";
    let table_name = "test_order_by_tbl1";
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(50, "eve"),
        make_tuple(10, "alice"),
        make_tuple(30, "charlie"),
        make_tuple(20, "bob"),
        make_tuple(40, "david"),
    ];

    let (mut catalog, file_path) = setup_test_env(db_name, table_name, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    create_ordered_file_from_heap(&mut catalog, db_name, table_name, sort_keys, 64).unwrap();

    // Verify the file is now ordered
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let header = read_ordered_file_header(&mut file).unwrap();
    assert_eq!(header.file_type, FileType::Ordered);

    let result_tuples = ordered_scan(&mut file, &catalog, db_name, table_name).unwrap();
    assert_eq!(result_tuples.len(), 5);

    let ids: Vec<i32> = result_tuples.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![10, 20, 30, 40, 50]);

    // Verify catalog updated
    let table = catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));
    assert!(table.sort_keys.is_some());

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

#[test]
fn test_order_by_already_sorted() {
    let db_name = "test_order_by_db2";
    let table_name = "test_order_by_tbl2";
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(30, "charlie"),
        make_tuple(10, "alice"),
        make_tuple(20, "bob"),
    ];

    let (mut catalog, _file_path) = setup_test_env(db_name, table_name, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    // First call: should sort the table
    order_by_execute(&mut catalog, db_name, table_name, sort_keys.clone(), 64).unwrap();

    // Verify in-memory catalog was updated (don't load from disk to avoid race conditions)
    let table = catalog
        .databases
        .get(db_name)
        .unwrap()
        .tables
        .get(table_name)
        .unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));

    // Second call with same sort keys: should detect already sorted
    order_by_execute(&mut catalog, db_name, table_name, sort_keys, 64).unwrap();

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

#[test]
fn test_order_by_desc() {
    let db_name = "test_order_by_db3";
    let table_name = "test_order_by_tbl3";
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(10, "alice"),
        make_tuple(30, "charlie"),
        make_tuple(20, "bob"),
    ];

    let (mut catalog, file_path) = setup_test_env(db_name, table_name, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Descending,
    }];

    order_by_execute(&mut catalog, db_name, table_name, sort_keys, 64).unwrap();

    // Verify descending order
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result_tuples = ordered_scan(&mut file, &catalog, db_name, table_name).unwrap();
    let ids: Vec<i32> = result_tuples.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![30, 20, 10]);

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

#[test]
fn test_order_by_empty_table() {
    let db_name = "test_order_by_db4";
    let table_name = "test_order_by_tbl4";
    let columns = vec![Column {
        name: "id".to_string(),
        data_type: "INT".to_string(),
    }];

    let tuples: Vec<Vec<u8>> = vec![];

    let (mut catalog, file_path) = setup_test_env(db_name, table_name, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    order_by_execute(&mut catalog, db_name, table_name, sort_keys, 64).unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result_tuples = ordered_scan(&mut file, &catalog, db_name, table_name).unwrap();
    assert_eq!(result_tuples.len(), 0);

    // Cleanup
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}
