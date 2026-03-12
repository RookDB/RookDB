//! Tests for sorted_insert, find_insert_page, find_insert_slot, split_page.

use std::fs::{self, OpenOptions};
use std::io::Write;

use storage_manager::catalog::types::{Column, SortDirection, SortKey};
use storage_manager::disk::read_page;
use storage_manager::ordered::ordered_file::{
    read_ordered_file_header, FileType, OrderedFileHeader, SortKeyEntry,
};
use storage_manager::ordered::sorted_insert::{find_insert_slot, sorted_insert};
use storage_manager::page::{init_page, Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use storage_manager::sorting::comparator::TupleComparator;
use storage_manager::table::page_count;

/// Helper: build a 14-byte tuple (INT, TEXT).
fn make_tuple(id: i32, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

/// Standard (INT, TEXT) schema.
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

/// Sort keys: id ASC.
fn sort_keys_id_asc() -> Vec<SortKey> {
    vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }]
}

/// Extract INT from tuple.
fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

/// Create an ordered file with an empty data page. Returns file path.
fn create_empty_ordered_file(path: &str) -> std::fs::File {
    let _ = fs::remove_file(path);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();

    // Header page
    let header = OrderedFileHeader {
        page_count: 2,
        file_type: FileType::Ordered,
        sort_key_count: 1,
        sort_keys: vec![SortKeyEntry {
            column_index: 0,
            direction: 0,
        }],
    };
    let mut buf = vec![0u8; PAGE_SIZE];
    buf[0..4].copy_from_slice(&header.page_count.to_le_bytes());
    buf[4] = header.file_type.to_u8();
    buf[5..9].copy_from_slice(&header.sort_key_count.to_le_bytes());
    buf[9..13].copy_from_slice(&header.sort_keys[0].column_index.to_le_bytes());
    buf[13] = header.sort_keys[0].direction;
    file.write_all(&buf).unwrap();

    // Empty data page 1
    let mut data_page = Page::new();
    init_page(&mut data_page);
    file.write_all(&data_page.data).unwrap();
    file.flush().unwrap();

    file
}

/// Read all tuples from all data pages of a file.
fn read_all_tuples(file: &mut std::fs::File) -> Vec<Vec<u8>> {
    let total = page_count(file).unwrap();
    let mut tuples = Vec::new();
    for p in 1..total {
        let mut page = Page::new();
        read_page(file, &mut page, p).unwrap();
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

// ---- find_insert_slot ----

#[test]
fn test_find_insert_slot_empty_page() {
    let mut page = Page::new();
    init_page(&mut page);

    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());
    let tuple = make_tuple(5, "x");

    let slot = find_insert_slot(&page, &tuple, &comparator);
    assert_eq!(slot, 0);
}

#[test]
fn test_find_insert_slot_beginning() {
    let mut page = Page::new();
    init_page(&mut page);

    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    // Insert tuples 10, 20, 30 manually
    for id in [10, 20, 30] {
        let tuple = make_tuple(id, "x");
        let tuple_len = tuple.len() as u32;
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let start = upper - tuple_len;
        page.data[start as usize..upper as usize].copy_from_slice(&tuple);
        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 8].copy_from_slice(&tuple_len.to_le_bytes());
        page.data[0..4].copy_from_slice(&(lower + ITEM_ID_SIZE).to_le_bytes());
        page.data[4..8].copy_from_slice(&start.to_le_bytes());
    }

    // Insert 5 => should go at slot 0
    let new_tuple = make_tuple(5, "x");
    let slot = find_insert_slot(&page, &new_tuple, &comparator);
    assert_eq!(slot, 0);
}

#[test]
fn test_find_insert_slot_middle() {
    let mut page = Page::new();
    init_page(&mut page);

    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    for id in [10, 20, 30] {
        let tuple = make_tuple(id, "x");
        let tuple_len = tuple.len() as u32;
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let start = upper - tuple_len;
        page.data[start as usize..upper as usize].copy_from_slice(&tuple);
        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 8].copy_from_slice(&tuple_len.to_le_bytes());
        page.data[0..4].copy_from_slice(&(lower + ITEM_ID_SIZE).to_le_bytes());
        page.data[4..8].copy_from_slice(&start.to_le_bytes());
    }

    // Insert 15 => should go at slot 1 (between 10 and 20)
    let new_tuple = make_tuple(15, "x");
    let slot = find_insert_slot(&page, &new_tuple, &comparator);
    assert_eq!(slot, 1);
}

#[test]
fn test_find_insert_slot_end() {
    let mut page = Page::new();
    init_page(&mut page);

    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    for id in [10, 20, 30] {
        let tuple = make_tuple(id, "x");
        let tuple_len = tuple.len() as u32;
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let start = upper - tuple_len;
        page.data[start as usize..upper as usize].copy_from_slice(&tuple);
        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 8].copy_from_slice(&tuple_len.to_le_bytes());
        page.data[0..4].copy_from_slice(&(lower + ITEM_ID_SIZE).to_le_bytes());
        page.data[4..8].copy_from_slice(&start.to_le_bytes());
    }

    // Insert 35 => should go at slot 3
    let new_tuple = make_tuple(35, "x");
    let slot = find_insert_slot(&page, &new_tuple, &comparator);
    assert_eq!(slot, 3);
}

// ---- sorted_insert basic ----

#[test]
fn test_sorted_insert_single() {
    let path = "test_si_single.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    let tuple = make_tuple(42, "Hello");
    sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");

    let tuples = read_all_tuples(&mut file);
    assert_eq!(tuples.len(), 1);
    assert_eq!(extract_int(&tuples[0]), 42);

    let _ = fs::remove_file(path);
}

#[test]
fn test_sorted_insert_maintains_order() {
    let path = "test_si_order.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    // Insert in random order
    for id in [30, 10, 50, 20, 40] {
        let tuple = make_tuple(id, "x");
        sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
    }

    let tuples = read_all_tuples(&mut file);
    assert_eq!(tuples.len(), 5);

    let ids: Vec<i32> = tuples.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![10, 20, 30, 40, 50]);

    let _ = fs::remove_file(path);
}

#[test]
fn test_sorted_insert_20_random_tuples() {
    let path = "test_si_20random.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    // Insert 20 tuples in a shuffled order
    let order = [
        15, 3, 18, 7, 12, 1, 20, 9, 14, 6, 11, 4, 17, 2, 19, 8, 13, 5, 16, 10,
    ];
    for id in order {
        let tuple = make_tuple(id, &format!("n{:02}", id));
        sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
    }

    let tuples = read_all_tuples(&mut file);
    assert_eq!(tuples.len(), 20);

    let ids: Vec<i32> = tuples.iter().map(|t| extract_int(t)).collect();
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] <= ids[i + 1],
            "Not sorted at index {}: {} > {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }

    let _ = fs::remove_file(path);
}

#[test]
fn test_sorted_insert_duplicate_keys() {
    let path = "test_si_dupes.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    for id in [5, 5, 5, 3, 3, 7] {
        let tuple = make_tuple(id, "x");
        sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
    }

    let tuples = read_all_tuples(&mut file);
    let ids: Vec<i32> = tuples.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids, vec![3, 3, 5, 5, 5, 7]);

    let _ = fs::remove_file(path);
}

// ---- Page split ----

#[test]
fn test_sorted_insert_triggers_page_split() {
    let path = "test_si_split.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    // Each tuple = 14 bytes + 8 byte ItemId = 22 bytes per tuple.
    // Page free space = 8192 - 8 = 8184 bytes.
    // Max tuples per page = 8184 / 22 = 372.
    // Insert 373 tuples to force a split.
    let initial_page_count = page_count(&mut file).unwrap();
    assert_eq!(initial_page_count, 2); // header + 1 data page

    for i in 1..=373 {
        let tuple = make_tuple(i, "x");
        sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
    }

    // Verify page count increased
    let final_page_count = page_count(&mut file).unwrap();
    assert!(
        final_page_count > 2,
        "Expected page split to increase page count, got {}",
        final_page_count
    );

    // Verify all tuples are present and sorted
    let tuples = read_all_tuples(&mut file);
    assert_eq!(tuples.len(), 373);

    let ids: Vec<i32> = tuples.iter().map(|t| extract_int(t)).collect();
    for i in 0..ids.len() - 1 {
        assert!(
            ids[i] <= ids[i + 1],
            "Not sorted after split at index {}: {} > {}",
            i,
            ids[i],
            ids[i + 1]
        );
    }

    // Verify cross-page ordering: last tuple of each page <= first tuple of next page
    let total = page_count(&mut file).unwrap();
    for p in 1..total - 1 {
        let mut page_curr = Page::new();
        let mut page_next = Page::new();
        read_page(&mut file, &mut page_curr, p).unwrap();
        read_page(&mut file, &mut page_next, p + 1).unwrap();

        let lower_curr = u32::from_le_bytes(page_curr.data[0..4].try_into().unwrap());
        let num_curr = (lower_curr - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        if num_curr == 0 {
            continue;
        }

        // Last tuple on current page
        let last_base = (PAGE_HEADER_SIZE + (num_curr - 1) * ITEM_ID_SIZE) as usize;
        let last_off =
            u32::from_le_bytes(page_curr.data[last_base..last_base + 4].try_into().unwrap())
                as usize;
        let last_len = u32::from_le_bytes(
            page_curr.data[last_base + 4..last_base + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let last_id = extract_int(&page_curr.data[last_off..last_off + last_len]);

        // First tuple on next page
        let lower_next = u32::from_le_bytes(page_next.data[0..4].try_into().unwrap());
        let num_next = (lower_next - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        if num_next == 0 {
            continue;
        }
        let first_base = PAGE_HEADER_SIZE as usize;
        let first_off = u32::from_le_bytes(
            page_next.data[first_base..first_base + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let first_len = u32::from_le_bytes(
            page_next.data[first_base + 4..first_base + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let first_id = extract_int(&page_next.data[first_off..first_off + first_len]);

        assert!(
            last_id <= first_id,
            "Cross-page ordering violated between page {} and {}: {} > {}",
            p,
            p + 1,
            last_id,
            first_id
        );
    }

    let _ = fs::remove_file(path);
}

#[test]
fn test_sorted_insert_header_updated_after_split() {
    let path = "test_si_hdr_split.dat";
    let mut file = create_empty_ordered_file(path);
    let comparator = TupleComparator::new(schema(), sort_keys_id_asc());

    // Fill page until split
    for i in 1..=373 {
        let tuple = make_tuple(i, "x");
        sorted_insert(&mut file, &tuple, &comparator).expect("sorted_insert failed");
    }

    let header = read_ordered_file_header(&mut file).unwrap();
    assert!(
        header.page_count > 2,
        "Header page_count should have been updated after split"
    );
    assert_eq!(header.file_type, FileType::Ordered);

    let _ = fs::remove_file(path);
}
