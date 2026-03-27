use std::env;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use storage_manager::heap::heap_manager::HeapManager;
use storage_manager::table::page_count;

#[test]
fn test_page_count() {
    // Create a temporary file with read + write access
    let mut temp_path = PathBuf::from(env::temp_dir());
    temp_path.push("test_table_page_count.tbl");

    // Initialize table (writes 8192 bytes with page_count = 0)
    let _hm = HeapManager::create(temp_path.clone()).expect("Failed to create heap manager");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&temp_path)
        .expect("Failed to open temp file");

    // Move cursor back to start (for fresh read)
    file.seek(SeekFrom::Start(0)).unwrap();

    // Call page_count() to read first page and extract page count
    let count = page_count(&mut file).expect("Failed to read page count");

    // Verify the page count is 0
    assert_eq!(count, 2, "Expected page count to be 2 after initialization");
}
