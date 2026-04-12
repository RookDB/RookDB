use std::fs::{OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write};
use storage_manager::disk::create_page;
use storage_manager::page::PAGE_SIZE;
use storage_manager::table::TABLE_HEADER_SIZE;

const TEST_FILE: &str = "test_page_file.bin";

#[test]
fn test_create_page() {
    // Clean up before starting
    let _ = remove_file(TEST_FILE);

    // Create a new test file
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(TEST_FILE)
        .expect("Failed to create file");

    // Initialize table header with 0 pages (so page_count read works correctly)
    file.write_all(&vec![0u8; TABLE_HEADER_SIZE as usize])
        .expect("Failed to write table header");

    // Call API
    let page_num = create_page(&mut file).expect("Failed to create page");

    // Check that the file size == PAGE_HEADER_SIZE + PAGE_SIZE
    let metadata = file.metadata().expect("Failed to get file metadata");
    assert_eq!(
        metadata.len(),
        TABLE_HEADER_SIZE as u64 + PAGE_SIZE as u64,
        "File size should be table header + one page ({} + {} bytes)",
        TABLE_HEADER_SIZE,
        PAGE_SIZE
    );

    // Read back the first page (skip table header)
    let mut buffer = vec![0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(TABLE_HEADER_SIZE as u64))
        .expect("Failed to seek to first page");
    file.read_exact(&mut buffer)
        .expect("Failed to read first page");

    // Verify page header in the first 8 bytes
    let lower = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(buffer[4..8].try_into().unwrap());
    assert_eq!(lower, 8, "Lower offset should be PAGE_HEADER_SIZE (8)");
    assert_eq!(upper, PAGE_SIZE as u32, "Upper offset should be PAGE_SIZE");

    println!(
        "Page {} created correctly with header bytes: {:?}",
        page_num,
        &buffer[0..8]
    );
}
