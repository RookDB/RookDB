//! Integration tests for RookDB Phase 2.
//!
//! B1: Heap -> external sort -> range scan end-to-end
//! B2: Create ordered table -> sorted insert -> ordered scan
//! B3: Large dataset (10,000 rows) external sort
//! B4: Multi-page range scan (400+ tuples across 3+ pages)
//! B5: TEXT as primary sort key
//! B6: DESC sorted insert + multi-column sort

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey, Table};
use storage_manager::disk::read_page;
use storage_manager::executor::order_by::create_ordered_file_from_heap;
use storage_manager::ordered::ordered_file::{
    write_ordered_file_header, FileType, OrderedFileHeader, SortKeyEntry,
};
use storage_manager::ordered::scan::{ordered_scan, range_scan};
use storage_manager::ordered::sorted_insert::sorted_insert;
use storage_manager::page::{init_page, Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use storage_manager::sorting::comparator::TupleComparator;
use storage_manager::sorting::external_sort::external_sort;
use storage_manager::table::page_count;

// ---- Shared helpers ----

fn make_tuple(id: i32, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

fn extract_text(tuple: &[u8]) -> String {
    String::from_utf8_lossy(&tuple[4..14]).trim().to_string()
}

fn schema() -> Vec<Column> {
    vec![
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
    ]
}

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

/// Set up a heap table file with given tuples and return (catalog, file_path).
fn setup_heap_table(
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

    let mut current_page = Page::new();
    init_page(&mut current_page);
    let mut data_page_count = 0u32;

    for tuple in tuples {
        if !insert_into_page(&mut current_page, tuple) {
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

    data_page_count += 1;
    {
        use std::io::{Seek, SeekFrom};
        let offset = data_page_count as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&current_page.data).unwrap();
    }

    let total = data_page_count + 1;
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&total.to_le_bytes()).unwrap();
        file.flush().unwrap();
    }

    catalog
}

/// Create an ordered file with pre-sorted tuples. Returns (catalog, file_path).
fn create_ordered_file_with_tuples(
    db_name: &str,
    table_name: &str,
    columns: Vec<Column>,
    sort_keys_catalog: Vec<SortKey>,
    sort_key_entries: Vec<SortKeyEntry>,
    tuples: &[Vec<u8>],
) -> (Catalog, String) {
    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let file_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let _ = fs::remove_file(&file_path);

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();

    // Placeholder header page
    file.write_all(&vec![0u8; PAGE_SIZE]).unwrap();

    let mut current_page = Page::new();
    init_page(&mut current_page);
    let mut data_page_count = 0u32;

    for tuple in tuples {
        if !insert_into_page(&mut current_page, tuple) {
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

    data_page_count += 1;
    {
        use std::io::{Seek, SeekFrom};
        let offset = data_page_count as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&current_page.data).unwrap();
    }

    let total_pages = data_page_count + 1;

    let header = OrderedFileHeader {
        page_count: total_pages,
        file_type: FileType::Ordered,
        sort_key_count: sort_key_entries.len() as u32,
        sort_keys: sort_key_entries,
    };
    write_ordered_file_header(&mut file, &header).unwrap();

    let table = Table {
        columns,
        sort_keys: Some(sort_keys_catalog),
        file_type: Some("ordered".to_string()),
    };
    let mut tables = HashMap::new();
    tables.insert(table_name.to_string(), table);
    let database = Database { tables };
    let mut databases = HashMap::new();
    databases.insert(db_name.to_string(), database);
    let catalog = Catalog { databases };

    let catalog_json = serde_json::to_string_pretty(&catalog).unwrap();
    fs::write("database/global/catalog.json", &catalog_json).unwrap();

    (catalog, file_path)
}

/// Read all tuples from a table file (pages 1..total).
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
    let db_dir = format!("database/base/{}", db_name);
    if let Ok(entries) = fs::read_dir(&db_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(".sort_tmp_") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    let _ = fs::remove_dir(&db_dir);
}

// ====================================================================
// B1: End-to-end: heap -> external sort -> range scan
// ====================================================================

#[test]
fn test_b1_heap_sort_then_range_scan() {
    let db = "test_integ_b1";
    let tbl = "test_integ_b1_tbl";

    // Create 200 unsorted tuples (reverse order)
    let mut tuples: Vec<Vec<u8>> = Vec::new();
    for i in (1..=200).rev() {
        tuples.push(make_tuple(i, &format!("row{:04}", i)));
    }

    let mut catalog = setup_heap_table(db, tbl, schema(), &tuples);

    // Sort the heap into an ordered file
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    create_ordered_file_from_heap(&mut catalog, db, tbl, sort_keys, 4)
        .expect("create_ordered_file_from_heap failed");

    // Verify catalog was updated
    let table = catalog.databases[db].tables.get(tbl).unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));
    assert!(table.sort_keys.is_some());

    // Now perform a range scan [50, 100]
    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("50"), Some("100"))
        .expect("range_scan failed");

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.len(), 51); // 50..=100
    assert_eq!(ids[0], 50);
    assert_eq!(*ids.last().unwrap(), 100);

    // Verify all results are actually in sorted order
    for i in 0..ids.len() - 1 {
        assert!(ids[i] <= ids[i + 1], "Not sorted at index {}", i);
    }

    // Also do a full ordered scan and verify all 200 are sorted
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
    }
    let all = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    assert_eq!(all.len(), 200);
    let all_ids: Vec<i32> = all.iter().map(|t| extract_int(t)).collect();
    assert_eq!(all_ids, (1..=200).collect::<Vec<i32>>());

    cleanup(db, tbl);
}

// ====================================================================
// B2: Create ordered table -> sorted insert -> ordered scan
// ====================================================================

#[test]
fn test_b2_ordered_table_sorted_insert_scan() {
    let db = "test_integ_b2";
    let tbl = "test_integ_b2_tbl";

    let columns = schema();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let sort_key_entries = vec![SortKeyEntry {
        column_index: 0,
        direction: 0, // ASC
    }];

    // Start with an empty ordered file
    let (catalog, file_path) = create_ordered_file_with_tuples(
        db,
        tbl,
        columns.clone(),
        sort_keys.clone(),
        sort_key_entries,
        &[], // no initial tuples — but we still write one empty page
    );

    // Use sorted_insert to add tuples in random order
    let insert_order = vec![
        50, 10, 90, 30, 70, 20, 60, 40, 80, 5, 95, 15, 85, 25, 75, 35, 65, 45, 55, 100, 1,
    ];

    let comparator = TupleComparator::new(columns.clone(), sort_keys.clone());

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        for id in &insert_order {
            let tuple = make_tuple(*id, &format!("val{:04}", id));
            sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
        }
    }

    // Read back via ordered_scan
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    assert_eq!(result.len(), insert_order.len());

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    // Must be sorted ascending
    let mut expected = insert_order.clone();
    expected.sort();
    assert_eq!(ids, expected);

    cleanup(db, tbl);
}

// ====================================================================
// B3: Large dataset (10,000 rows) external sort with small buffer
// ====================================================================

#[test]
fn test_b3_large_dataset_external_sort() {
    let db = "test_integ_b3";
    let tbl = "test_integ_b3_tbl";

    // 10,000 tuples in reverse order
    let mut tuples: Vec<Vec<u8>> = Vec::new();
    for i in (1..=10000).rev() {
        tuples.push(make_tuple(i, "data"));
    }

    let mut catalog = setup_heap_table(db, tbl, schema(), &tuples);

    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 4).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 10000);

    // Verify sorted
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids[0], 1);
    assert_eq!(*ids.last().unwrap(), 10000);
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] <= ids[i + 1],
            "Not sorted at index {}: {} > {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }

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

    // Verify catalog updated
    let table = catalog.databases[db].tables.get(tbl).unwrap();
    assert_eq!(table.file_type.as_deref(), Some("ordered"));

    cleanup(db, tbl);
}

// ====================================================================
// B4: Multi-page range scan (400+ tuples across 3+ pages)
// ====================================================================

#[test]
fn test_b4_multi_page_range_scan() {
    let db = "test_integ_b4";
    let tbl = "test_integ_b4_tbl";

    // 400 sorted tuples — each is 14 bytes + 8 bytes ItemId = 22 bytes per tuple
    // Page has 8192 - 8 = 8184 usable bytes, so ~372 tuples per page.
    // 400 tuples should span at least 2 data pages.
    let tuples: Vec<Vec<u8>> = (1..=400).map(|i| make_tuple(i, "data")).collect();

    let sort_keys_catalog = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let sort_key_entries = vec![SortKeyEntry {
        column_index: 0,
        direction: 0,
    }];

    let (catalog, file_path) = create_ordered_file_with_tuples(
        db,
        tbl,
        schema(),
        sort_keys_catalog,
        sort_key_entries,
        &tuples,
    );

    // Verify we actually have multiple data pages
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        let total = page_count(&mut file).unwrap();
        assert!(
            total >= 3,
            "Expected at least 3 pages (header + 2+ data), got {}",
            total
        );
    }

    // Range scan crossing a page boundary: [350, 400]
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("350"), Some("400"))
        .expect("range_scan failed");

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.len(), 51); // 350..=400
    assert_eq!(ids[0], 350);
    assert_eq!(*ids.last().unwrap(), 400);

    // Range scan that spans the entire file
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
    }
    let full = range_scan(&mut file, &catalog, db, tbl, "id", Some("1"), Some("400"))
        .expect("full range_scan failed");
    assert_eq!(full.len(), 400);

    // Range scan starting mid-first-page through mid-second-page
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
    }
    let mid = range_scan(&mut file, &catalog, db, tbl, "id", Some("200"), Some("380"))
        .expect("mid range_scan failed");
    let mid_ids: Vec<i32> = mid.iter().map(|t| extract_int(t)).collect();
    assert_eq!(mid_ids.len(), 181); // 200..=380
    assert_eq!(mid_ids[0], 200);
    assert_eq!(*mid_ids.last().unwrap(), 380);

    cleanup(db, tbl);
}

// ====================================================================
// B5: TEXT as primary sort key — lexicographic ordering
// ====================================================================

#[test]
fn test_b5_text_primary_sort_key() {
    let db = "test_integ_b5";
    let tbl = "test_integ_b5_tbl";

    // Schema: (name TEXT, id INT) — TEXT is column 0, INT is column 1
    let columns = vec![
        Column {
            name: "name".into(),
            data_type: "TEXT".into(),
        },
        Column {
            name: "id".into(),
            data_type: "INT".into(),
        },
    ];

    // Build tuples: TEXT (10 bytes) + INT (4 bytes) = 14 bytes
    fn make_text_first_tuple(name: &str, id: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(14);
        let mut name_bytes = name.as_bytes().to_vec();
        name_bytes.resize(10, b' ');
        buf.extend_from_slice(&name_bytes[..10]);
        buf.extend_from_slice(&id.to_le_bytes());
        buf
    }

    fn extract_text_col0(tuple: &[u8]) -> String {
        String::from_utf8_lossy(&tuple[0..10]).trim().to_string()
    }

    // Unsorted names
    let names = vec![
        "zebra",
        "apple",
        "mango",
        "banana",
        "cherry",
        "date",
        "fig",
        "grape",
        "kiwi",
        "lemon",
        "nectarine",
        "orange",
        "papaya",
        "quince",
    ];

    let mut tuples: Vec<Vec<u8>> = names
        .iter()
        .enumerate()
        .map(|(i, name)| make_text_first_tuple(name, i as i32))
        .collect();

    // Reverse to make them definitely unsorted
    tuples.reverse();

    let mut catalog = setup_heap_table(db, tbl, columns.clone(), &tuples);

    // Sort by name (column 0) ASC
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    external_sort(&mut catalog, db, tbl, sort_keys, 3).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), names.len());

    let result_names: Vec<String> = result.iter().map(|t| extract_text_col0(t)).collect();

    // Verify lexicographic order
    let mut expected = names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(result_names, expected);

    // Also verify range scan on text column
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    // Range scan: names starting from "d" to "m" (inclusive lexicographic)
    // "date" >= "d" and "mango" <= "m" ... actually TEXT comparison is byte-by-byte
    // "d" padded to "d         " and "m" padded to "m         "
    // "date      " >= "d         " -> true
    // "fig       " <= "m         " -> true (f < m)
    // "grape     " <= "m         " -> true (g < m)
    // "kiwi      " <= "m         " -> true (k < m)
    // "lemon     " <= "m         " -> true (l < m)
    // "mango     " <= "m         " -> false (m-a vs m- , 'a' > ' ')
    // So range [d, m] should give: date, fig, grape, kiwi, lemon
    let result = range_scan(&mut file, &catalog, db, tbl, "name", Some("d"), Some("m"))
        .expect("range_scan on text failed");

    let range_names: Vec<String> = result.iter().map(|t| extract_text_col0(t)).collect();
    // All results should be >= "d" and <= "m" (space-padded comparison)
    for name in &range_names {
        assert!(name.as_str() >= "d", "name '{}' should be >= 'd'", name);
    }
    // Should include date, fig, grape, kiwi, lemon
    assert!(range_names.contains(&"date".to_string()));
    assert!(range_names.contains(&"fig".to_string()));
    assert!(range_names.contains(&"grape".to_string()));
    assert!(range_names.contains(&"kiwi".to_string()));
    assert!(range_names.contains(&"lemon".to_string()));

    cleanup(db, tbl);
}

// ====================================================================
// B6: DESC sorted insert + multi-column sort
// ====================================================================

#[test]
fn test_b6_desc_and_multicolumn_sort() {
    let db = "test_integ_b6";
    let tbl = "test_integ_b6_tbl";

    let columns = schema(); // (id INT, name TEXT)

    // Multi-column sort: name ASC, id DESC
    // This means: first sort by name alphabetically, then by id descending within same name
    let sort_keys = vec![
        SortKey {
            column_index: 1, // name
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 0, // id
            direction: SortDirection::Descending,
        },
    ];

    // Create tuples with duplicate names
    let mut tuples: Vec<Vec<u8>> = Vec::new();

    // Group "alpha": ids 30, 10, 20
    tuples.push(make_tuple(30, "alpha"));
    tuples.push(make_tuple(10, "alpha"));
    tuples.push(make_tuple(20, "alpha"));

    // Group "beta": ids 5, 15, 25
    tuples.push(make_tuple(5, "beta"));
    tuples.push(make_tuple(15, "beta"));
    tuples.push(make_tuple(25, "beta"));

    // Group "gamma": ids 100, 50, 75
    tuples.push(make_tuple(100, "gamma"));
    tuples.push(make_tuple(50, "gamma"));
    tuples.push(make_tuple(75, "gamma"));

    let mut catalog = setup_heap_table(db, tbl, columns.clone(), &tuples);

    external_sort(&mut catalog, db, tbl, sort_keys, 3).expect("external_sort failed");

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let result = read_all_tuples(&file_path);
    assert_eq!(result.len(), 9);

    // Expected order: name ASC, then id DESC within same name
    // alpha: 30, 20, 10
    // beta: 25, 15, 5
    // gamma: 100, 75, 50
    let expected_ids = vec![30, 20, 10, 25, 15, 5, 100, 75, 50];
    let expected_names = vec![
        "alpha", "alpha", "alpha", "beta", "beta", "beta", "gamma", "gamma", "gamma",
    ];

    let actual_ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    let actual_names: Vec<String> = result.iter().map(|t| extract_text(t)).collect();

    assert_eq!(
        actual_ids, expected_ids,
        "IDs don't match expected multi-column sort order"
    );
    assert_eq!(
        actual_names, expected_names,
        "Names don't match expected multi-column sort order"
    );

    cleanup(db, tbl);
}
