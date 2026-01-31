use storage_manager::page::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, init_page};

#[test]
fn test_init_page() {
    // Create a new page with empty data
    let mut page = Page {
        data: vec![0u8; PAGE_SIZE],
    };

    // Call the existing function
    init_page(&mut page);

    // Verify lower offset
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    assert_eq!(lower, PAGE_HEADER_SIZE);

    // Verify upper offset
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    assert_eq!(upper, PAGE_SIZE as u32);
}
