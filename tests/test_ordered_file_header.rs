//! Tests for ordered file header: write, read, init_ordered_table.

use std::fs::{OpenOptions, remove_file};
use std::io::Write;

use storage_manager::ordered::ordered_file::{
    FileType, OrderedFileHeader, SortKeyEntry, init_ordered_table, read_ordered_file_header,
    write_ordered_file_header,
};
use storage_manager::page::PAGE_SIZE;

/// Helper: create a fresh test file and write a blank header page.
fn create_test_file(path: &str) -> std::fs::File {
    let _ = remove_file(path);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("Failed to create test file");
    // Write a zeroed header page
    file.write_all(&vec![0u8; PAGE_SIZE])
        .expect("Failed to write header page");
    file
}

#[test]
fn test_write_and_read_header_single_key() {
    let path = "test_hdr_single_key.dat";
    let mut file = create_test_file(path);

    let header = OrderedFileHeader {
        page_count: 5,
        file_type: FileType::Ordered,
        sort_key_count: 1,
        sort_keys: vec![SortKeyEntry {
            column_index: 0,
            direction: 0, // ASC
        }],
    };

    write_ordered_file_header(&mut file, &header).expect("Failed to write header");
    let read_back = read_ordered_file_header(&mut file).expect("Failed to read header");

    assert_eq!(read_back.page_count, 5);
    assert_eq!(read_back.file_type, FileType::Ordered);
    assert_eq!(read_back.sort_key_count, 1);
    assert_eq!(read_back.sort_keys.len(), 1);
    assert_eq!(read_back.sort_keys[0].column_index, 0);
    assert_eq!(read_back.sort_keys[0].direction, 0);

    let _ = remove_file(path);
}

#[test]
fn test_write_and_read_header_two_keys() {
    let path = "test_hdr_two_keys.dat";
    let mut file = create_test_file(path);

    let header = OrderedFileHeader {
        page_count: 10,
        file_type: FileType::Ordered,
        sort_key_count: 2,
        sort_keys: vec![
            SortKeyEntry {
                column_index: 1,
                direction: 0, // ASC
            },
            SortKeyEntry {
                column_index: 0,
                direction: 1, // DESC
            },
        ],
    };

    write_ordered_file_header(&mut file, &header).expect("Failed to write header");
    let read_back = read_ordered_file_header(&mut file).expect("Failed to read header");

    assert_eq!(read_back.page_count, 10);
    assert_eq!(read_back.file_type, FileType::Ordered);
    assert_eq!(read_back.sort_key_count, 2);
    assert_eq!(read_back.sort_keys[0].column_index, 1);
    assert_eq!(read_back.sort_keys[0].direction, 0);
    assert_eq!(read_back.sort_keys[1].column_index, 0);
    assert_eq!(read_back.sort_keys[1].direction, 1);

    let _ = remove_file(path);
}

#[test]
fn test_heap_file_type_roundtrip() {
    let path = "test_hdr_heap_type.dat";
    let mut file = create_test_file(path);

    let header = OrderedFileHeader {
        page_count: 3,
        file_type: FileType::Heap,
        sort_key_count: 0,
        sort_keys: vec![],
    };

    write_ordered_file_header(&mut file, &header).expect("Failed to write header");
    let read_back = read_ordered_file_header(&mut file).expect("Failed to read header");

    assert_eq!(read_back.file_type, FileType::Heap);
    assert_eq!(read_back.sort_key_count, 0);
    assert!(read_back.sort_keys.is_empty());

    let _ = remove_file(path);
}

#[test]
fn test_file_type_conversion() {
    assert_eq!(FileType::Heap.to_u8(), 0);
    assert_eq!(FileType::Ordered.to_u8(), 1);
    assert_eq!(FileType::from_u8(0), FileType::Heap);
    assert_eq!(FileType::from_u8(1), FileType::Ordered);
    assert_eq!(FileType::from_u8(99), FileType::Heap); // unknown defaults to Heap
}

#[test]
fn test_init_ordered_table() {
    let path = "test_init_ordered.dat";
    let _ = remove_file(path);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("Failed to create test file");

    // Write a header page (init needs a file to write into)
    file.write_all(&vec![0u8; PAGE_SIZE])
        .expect("Failed to write initial page");

    let sort_keys = vec![SortKeyEntry {
        column_index: 0,
        direction: 0,
    }];

    init_ordered_table(&mut file, &sort_keys).expect("Failed to init ordered table");

    // Verify header
    let header = read_ordered_file_header(&mut file).expect("Failed to read header");
    assert_eq!(header.page_count, 2); // header + 1 data page
    assert_eq!(header.file_type, FileType::Ordered);
    assert_eq!(header.sort_key_count, 1);
    assert_eq!(header.sort_keys[0].column_index, 0);

    // Verify file size = 2 pages
    let metadata = file.metadata().expect("Failed to get metadata");
    assert_eq!(metadata.len(), 2 * PAGE_SIZE as u64);

    let _ = remove_file(path);
}
