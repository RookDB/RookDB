use std::fs::OpenOptions;
use storage_manager::disk::{create_page, read_page};
use storage_manager::heap::init_table;
use storage_manager::page::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, page_free_space};
use storage_manager::table::page_count;

#[test]
fn test_page_free_space() {
    // Create a temporary file for testing
    let file_path = "test_page_free_space.bin";
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)
        .expect("Failed to create or open test file");

    // --- Step 0: Initialize the table header (Table metadata region)
    init_table(&mut file).expect("Failed to initialize table header");
    println!("✅ Table initialized successfully.");

    // --- Step 1: Create first page (reserved for table header metadata)
    let _header_page = create_page(&mut file).expect("Failed to create header page");

    // --- Step 2: Create second page (actual data page)
    let data_page_num = create_page(&mut file).expect("Failed to create data page");
    println!("✅ Created data page number: {}", data_page_num);

    // --- Step 3: Verify total page count
    let total_pages = page_count(&mut file).expect("Failed to get page count");
    assert!(
        total_pages >= 2,
        "Expected at least 2 pages (header + data), found {}",
        total_pages
    );
    println!("Total pages after creation: {}", total_pages);

    // --- Step 4: Read back the second (data) page from file
    let mut page = Page {
        data: vec![0u8; PAGE_SIZE],
    };
    read_page(&mut file, &mut page, data_page_num).expect("Failed to read created page");

    // --- Step 5: Compute free space using API
    let free_space = page_free_space(&page).expect("Failed to calculate free space");

    // Extract Lower & Upper from page header for validation
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    println!("LOWER: {}", lower);
    println!("UPPER: {}", upper);
    println!("Calculated Free Space: {}", free_space);

    // --- Step 6: Validate correctness
    assert_eq!(
        free_space,
        upper - lower,
        "Free space mismatch after page creation"
    );

    assert_eq!(
        lower, PAGE_HEADER_SIZE as u32,
        "Expected lower offset = PAGE_HEADER_SIZE after init_page()"
    );

    assert_eq!(
        upper, PAGE_SIZE as u32,
        "Expected upper offset = PAGE_SIZE after init_page()"
    );

    println!("✅ test_page_free_space passed successfully!");

    // --- Step 7: Cleanup
    std::fs::remove_file(file_path).unwrap();
}
