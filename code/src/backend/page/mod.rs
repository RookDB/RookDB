// Page size in bytes (8 KB)
pub const PAGE_SIZE: usize = 8192;

// Page header size: stores lower & upper pointers
pub const PAGE_HEADER_SIZE: u32 = 8;

// Size of one item slot
pub const ITEM_ID_SIZE: u32 = 8;

// Represents a single database page
pub struct Page {
    // Raw page bytes
    pub data: Vec<u8>,
}

impl Page {
    // Create an empty page
    pub fn new() -> Self {
        Self {
            data: vec![0; PAGE_SIZE],
        }
    }
}

// Initialize page header pointers
pub fn init_page(page: &mut Page) {
    // Lower starts after header
    page.data[0..4].copy_from_slice(&PAGE_HEADER_SIZE.to_le_bytes());

    // Upper starts at page end
    page.data[4..8].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
}

// Return available free space in the page
pub fn page_free_space(page: &Page) -> std::io::Result<u32> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    Ok(upper - lower)
}
