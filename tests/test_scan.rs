//! Tests for ordered_scan and range_scan.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey, Table};
use storage_manager::ordered::append_delta_tuple;
use storage_manager::ordered::ordered_file::{
    FileType, OrderedFileHeader, SortKeyEntry, write_ordered_file_header,
};
use storage_manager::ordered::scan::{ordered_scan, range_scan};
use storage_manager::page::{ITEM_ID_SIZE, PAGE_SIZE, Page, init_page};

/// Helper: build a 14-byte tuple (INT, TEXT).
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

/// Schema and sort keys for (INT, TEXT), sorted by id ASC.
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

fn sort_keys_id_asc() -> Vec<SortKey> {
    vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }]
}

/// Insert tuple into a page. Returns false if full.
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

/// Creates an ordered file with sorted tuples (already sorted by id ASC).
/// Returns (catalog, file_path).
fn create_ordered_file_with_tuples(
    db_name: &str,
    table_name: &str,
    tuples: &[Vec<u8>],
) -> (Catalog, String) {
    create_ordered_file_with_tuples_direction(db_name, table_name, tuples, SortDirection::Ascending)
}

fn create_ordered_file_with_tuples_direction(
    db_name: &str,
    table_name: &str,
    tuples: &[Vec<u8>],
    direction: SortDirection,
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

    // Write placeholder header page
    file.write_all(&vec![0u8; PAGE_SIZE]).unwrap();

    // Write tuples into data pages
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

    let total_pages = data_page_count + 1;

    // Write ordered header
    let dir_u8 = match direction {
        SortDirection::Ascending => 0,
        SortDirection::Descending => 1,
    };
    let header = OrderedFileHeader {
        page_count: total_pages,
        file_type: FileType::Ordered,
        sort_key_count: 1,
        sort_keys: vec![SortKeyEntry {
            column_index: 0,
            direction: dir_u8,
        }],
    };
    write_ordered_file_header(&mut file, &header).unwrap();

    // Set up catalog
    let catalog_sort_keys = vec![SortKey {
        column_index: 0,
        direction,
    }];
    let table = Table {
        columns: schema(),
        sort_keys: Some(catalog_sort_keys),
        file_type: Some("ordered".to_string()),
        delta_enabled: Some(true),
        delta_merge_threshold_tuples: Some(500),
        delta_current_tuples: Some(0),
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

fn cleanup(db_name: &str, table_name: &str) {
    let _ = fs::remove_file(format!("database/base/{}/{}.dat", db_name, table_name));
    let _ = fs::remove_file(format!("database/base/{}/{}.delta", db_name, table_name));
    let _ = fs::remove_dir(format!("database/base/{}", db_name));
}

// ---- ordered_scan ----

#[test]
fn test_ordered_scan_basic() {
    let db = "test_os_db1";
    let tbl = "test_os_tbl1";

    // Create sorted tuples
    let tuples: Vec<Vec<u8>> = (1..=10)
        .map(|i| make_tuple(i, &format!("n{:02}", i)))
        .collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    assert_eq!(result.len(), 10);

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, (1..=10).collect::<Vec<i32>>());

    cleanup(db, tbl);
}

#[test]
fn test_ordered_scan_empty_table() {
    let db = "test_os_db2";
    let tbl = "test_os_tbl2";

    // Create file with just header + empty data page
    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let file_path = format!("database/base/{}/{}.dat", db, tbl);
    let _ = fs::remove_file(&file_path);

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();

    // Header
    file.write_all(&vec![0u8; PAGE_SIZE]).unwrap();
    // Empty data page
    let mut data_page = Page::new();
    init_page(&mut data_page);
    file.write_all(&data_page.data).unwrap();

    // Write page_count = 2
    {
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&2u32.to_le_bytes()).unwrap();
        file.flush().unwrap();
    }

    let table = Table {
        columns: schema(),
        sort_keys: Some(sort_keys_id_asc()),
        file_type: Some("ordered".to_string()),
        delta_enabled: Some(true),
        delta_merge_threshold_tuples: Some(500),
        delta_current_tuples: Some(0),
    };
    let mut tables = HashMap::new();
    tables.insert(tbl.to_string(), table);
    let database = Database { tables };
    let mut databases = HashMap::new();
    databases.insert(db.to_string(), database);
    let catalog = Catalog { databases };

    let result = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    assert_eq!(result.len(), 0);

    cleanup(db, tbl);
}

#[test]
fn test_ordered_scan_large() {
    let db = "test_os_db3";
    let tbl = "test_os_tbl3";

    let tuples: Vec<Vec<u8>> = (1..=100).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    assert_eq!(result.len(), 100);

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, (1..=100).collect::<Vec<i32>>());

    cleanup(db, tbl);
}

#[test]
fn test_ordered_scan_includes_delta_sorted() {
    let db = "test_os_delta_db1";
    let tbl = "test_os_delta_tbl1";

    let base = vec![
        make_tuple(10, "a"),
        make_tuple(30, "b"),
        make_tuple(50, "c"),
    ];
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &base);

    append_delta_tuple(db, tbl, &make_tuple(40, "d")).unwrap();
    append_delta_tuple(db, tbl, &make_tuple(20, "e")).unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = ordered_scan(&mut file, &catalog, db, tbl).expect("ordered_scan failed");
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![10, 20, 30, 40, 50]);

    cleanup(db, tbl);
}

// ---- range_scan ----

#[test]
fn test_range_scan_bounded() {
    let db = "test_rs_db1";
    let tbl = "test_rs_tbl1";

    let tuples: Vec<Vec<u8>> = (1..=100).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("25"), Some("75"))
        .expect("range_scan failed");

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.len(), 51); // 25..=75
    assert_eq!(ids[0], 25);
    assert_eq!(*ids.last().unwrap(), 75);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_unbounded_start() {
    let db = "test_rs_db2";
    let tbl = "test_rs_tbl2";

    let tuples: Vec<Vec<u8>> = (1..=50).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", None, Some("10"))
        .expect("range_scan failed");

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.len(), 10);
    assert_eq!(ids[0], 1);
    assert_eq!(*ids.last().unwrap(), 10);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_unbounded_end() {
    let db = "test_rs_db3";
    let tbl = "test_rs_tbl3";

    let tuples: Vec<Vec<u8>> = (1..=50).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("40"), None)
        .expect("range_scan failed");

    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.len(), 11); // 40..=50
    assert_eq!(ids[0], 40);
    assert_eq!(*ids.last().unwrap(), 50);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_fully_unbounded() {
    let db = "test_rs_db4";
    let tbl = "test_rs_tbl4";

    let tuples: Vec<Vec<u8>> = (1..=20).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result =
        range_scan(&mut file, &catalog, db, tbl, "id", None, None).expect("range_scan failed");

    assert_eq!(result.len(), 20);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_empty_range() {
    let db = "test_rs_db5";
    let tbl = "test_rs_tbl5";

    let tuples: Vec<Vec<u8>> = (1..=100).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    // Range [200, 300] — no tuples have id >= 200
    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("200"), Some("300"))
        .expect("range_scan failed");

    assert_eq!(result.len(), 0);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_single_match() {
    let db = "test_rs_db6";
    let tbl = "test_rs_tbl6";

    let tuples: Vec<Vec<u8>> = (1..=100).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    // Range [50, 50] — exactly one match
    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("50"), Some("50"))
        .expect("range_scan failed");

    assert_eq!(result.len(), 1);
    assert_eq!(extract_int(&result[0]), 50);

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_wrong_column_errors() {
    let db = "test_rs_db7";
    let tbl = "test_rs_tbl7";

    let tuples: Vec<Vec<u8>> = (1..=10).map(|i| make_tuple(i, "data")).collect();
    let (catalog, file_path) = create_ordered_file_with_tuples(db, tbl, &tuples);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    // "name" is not the leading sort key (id is)
    let result = range_scan(&mut file, &catalog, db, tbl, "name", Some("a"), Some("z"));
    assert!(result.is_err());

    cleanup(db, tbl);
}

#[test]
fn test_range_scan_desc_with_delta_normalized_bounds() {
    let db = "test_rs_desc_db1";
    let tbl = "test_rs_desc_tbl1";

    let tuples: Vec<Vec<u8>> = vec![
        make_tuple(50, "a"),
        make_tuple(40, "b"),
        make_tuple(30, "c"),
        make_tuple(20, "d"),
        make_tuple(10, "e"),
    ];
    let (catalog, file_path) =
        create_ordered_file_with_tuples_direction(db, tbl, &tuples, SortDirection::Descending);

    append_delta_tuple(db, tbl, &make_tuple(35, "x")).unwrap();
    append_delta_tuple(db, tbl, &make_tuple(15, "y")).unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .unwrap();

    let result = range_scan(&mut file, &catalog, db, tbl, "id", Some("15"), Some("35"))
        .expect("range_scan failed");
    let ids: Vec<i32> = result.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![35, 30, 20, 15]);

    let result_reversed = range_scan(&mut file, &catalog, db, tbl, "id", Some("35"), Some("15"))
        .expect("range_scan failed");
    let ids_reversed: Vec<i32> = result_reversed.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids_reversed, vec![35, 30, 20, 15]);

    cleanup(db, tbl);
}
