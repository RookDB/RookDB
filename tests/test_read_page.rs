use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use storage_manager::disk::read_page;
use storage_manager::page::{PAGE_SIZE, Page};

#[test]
fn test_read_page() {
    // Create a file with both read + write permissions
    let file_path = "test_page.bin";
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)
        .expect("Failed to create or open file");

    // Prepare one page worth of data (PAGE_SIZE = 8192 bytes typically)
    let original_data = vec![0u8; PAGE_SIZE];

    // Write the single page data to the file
    file.write_all(&original_data)
        .expect("Failed to write to file");
    file.flush().unwrap();

    // Verify file size equals PAGE_SIZE
    let file_size = file.metadata().unwrap().len();
    assert_eq!(
        file_size, PAGE_SIZE as u64,
        "File size mismatch with PAGE_SIZE"
    );

    // Create a blank Page and read the first page (page_num = 0)
    let mut page = Page {
        data: vec![0u8; PAGE_SIZE],
    };

    // Read the first page
    read_page(&mut file, &mut page, 0).expect("Failed to read first page");

    // Verify read data matches what we wrote
    assert_eq!(page.data, original_data, "Page data mismatch after reading");

    // Optional sanity check: cursor should be at PAGE_SIZE offset
    let pos = file.seek(SeekFrom::Current(0)).unwrap();
    assert_eq!(pos, PAGE_SIZE as u64, "File cursor not at expected offset");

    // Cleanup
    std::fs::remove_file(file_path).unwrap();
}
