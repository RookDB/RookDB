use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use storage_manager::disk::write_page;
use storage_manager::page::{PAGE_SIZE, Page};

#[test]
fn test_write_page() {
    // Create a file with both read + write permissions
    let file_path = "test_write_page.bin";
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)
        .expect("Failed to create or open file");

    // Prepare a Page with some identifiable data
    let mut page = Page {
        data: vec![0u8; PAGE_SIZE],
    };
    page.data[..4].copy_from_slice(&[10, 20, 30, 40]);

    // Write the page at page number 0
    write_page(&mut file, &mut page, 0).expect("Failed to write page");

    // Verify file size is at least PAGE_SIZE
    let file_size = file.metadata().unwrap().len();
    assert!(
        file_size >= PAGE_SIZE as u64,
        "File size less than PAGE_SIZE after writing"
    );

    // Read back from the same file to verify written content
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut read_buf = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut read_buf).unwrap();

    // Verify data matches
    assert_eq!(
        &read_buf[..4],
        &[10, 20, 30, 40],
        "Data mismatch after writing page"
    );

    // Cleanup
    std::fs::remove_file(file_path).unwrap();
}
