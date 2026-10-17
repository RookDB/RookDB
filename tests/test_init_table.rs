use std::fs::{OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom};

use storage_manager::heap::heap_manager::HeapManager;
use storage_manager::page::PAGE_SIZE;

const TEST_FILE: &str = "tests/test_table_file.bin";
/// The table header is page 0, one page long.
const TABLE_HEADER_SIZE: u32 = PAGE_SIZE as u32;

#[test]
fn test_init_table() {
    // Cleanup before test
    let _ = remove_file(TEST_FILE);
    let _ = remove_file(format!("{}.fsm", TEST_FILE));
    /// Best-effort removal of both artifacts when the test ends.
    struct BinCleanup;
    impl Drop for BinCleanup {
        fn drop(&mut self) {
            let _ = remove_file(TEST_FILE);
            let _ = remove_file(format!("{}.fsm", TEST_FILE));
        }
    }
    let _bin_cleanup = BinCleanup;

    // Initialize table
    let _hm = HeapManager::create(std::path::PathBuf::from(TEST_FILE))
        .expect("Failed to create map manager");

    // Open file with read + write
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(TEST_FILE)
        .expect("Failed to open test file");

    // Rewind to start
    file.seek(SeekFrom::Start(0)).expect("Seek failed");

    // ---- Read and verify table header ----
    let mut header = vec![0u8; TABLE_HEADER_SIZE as usize];
    file.read_exact(&mut header)
        .expect("Failed to read table header");

    // First 4 bytes = number of data pages
    let page_count = u32::from_le_bytes(header[0..4].try_into().unwrap());
    assert_eq!(page_count, 2, "Table should start with exactly 1 data page");

    // Then the FSM fork's size in blocks (root, level-1 page, leaf page).
    let fsm_pages = u32::from_le_bytes(header[4..8].try_into().unwrap());
    assert_eq!(fsm_pages, 3, "FSM fork should need 3 blocks");

    // Remaining header bytes should be zero
    assert!(
        header[8..].iter().all(|&b| b == 0),
        "Remaining header bytes should be zero"
    );

    // ---- Verify file size ----
    let metadata = file.metadata().expect("Failed to get file metadata");

    let expected_size = TABLE_HEADER_SIZE as u64 + PAGE_SIZE as u64;

    assert_eq!(
        metadata.len(),
        expected_size,
        "File should contain header + exactly one data page"
    );

    // ---- Verify first data page is readable ----
    file.seek(SeekFrom::Start(TABLE_HEADER_SIZE as u64))
        .expect("Seek to first data page failed");

    let mut page_buf = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut page_buf)
        .expect("Failed to read first data page");
    let _ = remove_file(TEST_FILE);
    let _ = remove_file(format!("{}.fsm", TEST_FILE));
}
