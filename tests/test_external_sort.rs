//! Tests for external sort: generate_sorted_runs, merge_runs, external_sort end-to-end.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey, Table};
use storage_manager::disk::read_page;
use storage_manager::page::{init_page, Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use storage_manager::sorting::external_sort::external_sort;
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

/// Insert a tuple into a page. Returns false if full.
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
    page.data[0..4].copy_from_slice(&(lower + ITEM_ID_SIZE).to_le_bytes());
    page.data[4..8].copy_from_slice(&start.to_le_bytes());
    true
}

/// Extract INT from tuple.
fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

/// Set up test environment with a heap table containing the given tuples.
fn setup_test_env(
    db_name: &str,
    table_name: &str,
    columns: Vec<Column>,
    tuples: &[Vec<u8>],
) -> Catalog {
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

    // Create table file
    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();

    // Header page
    let mut header = vec![0u8; PAGE_SIZE];
    header[0..4].copy_from_slice(&1u32.to_le_bytes());
    file.write_all(&header).unwrap();

    // Write tuples into pages
    let mut current_page = Page::new();
    init_page(&mut current_page);
    let mut data_page_count = 0u32;

    for tuple in tuples {
        if !insert_into_page(&mut current_page, tuple) {
            // Flush current page
            use std::io::{Seek, SeekFrom};
            data_page_count += 1;
            let offset = data_page_count as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(&current_page.data).unwrap();

            current_page = Page::new();
            init_page(&mut current_page);
            assert!(insert_into_page(&mut current_page, tuple));
        }
    }

    // Flush last page
    data_page_count += 1;
    {
        use std::io::{Seek, SeekFrom};
        let offset = data_page_count as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&current_page.data).unwrap();
    }

    // Update header page count
    let total = data_page_count + 1;
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&total.to_le_bytes()).unwrap();
        file.flush().unwrap();
    }

    catalog
}

/// Read all tuples from a table file.
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

fn cleanup(db_name: &str, table_name: &str) {
    let _ = fs::remove_file(format!("database/base/{}/{}.dat", db_name, table_name));
    // Clean up any leftover temp files
    let db_dir = format!("database/base/{}", db_name);
    if let Ok(entries) = fs::read_dir(&db_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(".sort_tmp_") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    let _ = fs::remove_dir(format!("database/base/{}", db_name));
}

#[test]
fn test_external_sort_basic() {
    let db = "test_es_db1";
    let tbl = "test_es_tbl1";
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

    // 20 unsorted tuples
    let mut tuples = Vec::new();
    for i in (1..=20).rev() {
        tuples.push(make_tuple(i, &format!("name_{:04}", i)));
    }

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 3).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 20);

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] <= ids[i + 1],
            "Not sorted at index {}: {} > {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }
    assert_eq!(ids[0], 1);
    assert_eq!(*ids.last().unwrap(), 20);

    // Verify no temp files remain
    let db_dir = format!("database/base/{}", db);
    if let Ok(entries) = fs::read_dir(&db_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.starts_with(".sort_tmp_"),
                "Temp file {} was not cleaned up",
                name
            );
        }
    }

    cleanup(db, tbl);
}

#[test]
fn test_external_sort_desc() {
    let db = "test_es_db2";
    let tbl = "test_es_tbl2";
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

    let mut tuples = Vec::new();
    for i in 1..=15 {
        tuples.push(make_tuple(i, "x"));
    }

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Descending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 2).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] >= ids[i + 1],
            "Not sorted DESC at index {}: {} < {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }

    cleanup(db, tbl);
}

#[test]
fn test_external_sort_large_dataset() {
    let db = "test_es_db3";
    let tbl = "test_es_tbl3";
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

    // 500 tuples in reverse order, buffer_pool_size = 4
    let mut tuples = Vec::new();
    for i in (1..=500).rev() {
        tuples.push(make_tuple(i, "data"));
    }

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 4).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 500);

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] <= ids[i + 1],
            "Not sorted at index {}: {} > {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }
    assert_eq!(ids[0], 1);
    assert_eq!(*ids.last().unwrap(), 500);

    cleanup(db, tbl);
}

#[test]
fn test_external_sort_single_tuple() {
    let db = "test_es_db4";
    let tbl = "test_es_tbl4";
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

    let tuples = vec![make_tuple(99, "only")];

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 2).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 1);
    assert_eq!(extract_int(&result[0]), 99);

    cleanup(db, tbl);
}

#[test]
fn test_external_sort_duplicate_keys() {
    let db = "test_es_db5";
    let tbl = "test_es_tbl5";
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

    let mut tuples = Vec::new();
    for _ in 0..30 {
        tuples.push(make_tuple(42, "same"));
    }

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 3).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 30);
    for t in &result {
        assert_eq!(extract_int(t), 42);
    }

    cleanup(db, tbl);
}

#[test]
fn test_external_sort_catalog_updated() {
    let db = "test_es_db6";
    let tbl = "test_es_tbl6";
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

    let tuples = vec![make_tuple(3, "c"), make_tuple(1, "a"), make_tuple(2, "b")];

    let mut catalog = setup_test_env(db, tbl, columns, &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 2).expect("external_sort failed");

    // Verify catalog was updated
    let table = catalog.databases[db].tables.get(tbl).unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));
    assert!(table.sort_keys.is_some());
    let sk = table.sort_keys.as_ref().unwrap();
    assert_eq!(sk.len(), 1);
    assert_eq!(sk[0].column_index, 0);

    cleanup(db, tbl);
}
