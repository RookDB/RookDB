// Page size in bytes (8 KB)
pub const PAGE_SIZE: usize = 8192;

// Page header size: stores lower & upper pointers
pub const PAGE_HEADER_SIZE: u32 = 8;

// Size of one item slot: [offset: u32][length: u16][flags: u16] = 8 bytes
pub const ITEM_ID_SIZE: u32 = 8;

// Slot flag bit: tuple is soft-deleted (set by DELETE, cleared by compaction)
pub const SLOT_FLAG_DELETED: u16 = 0b0000_0000_0000_0001;

/// Read slot `slot_index` → (offset: u32, length: u16, flags: u16)
pub fn read_slot(page: &Page, slot_index: u32) -> (u32, u16, u16) {
    let base = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
    let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
    let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap());
    let flags  = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
    (offset, length, flags)
}

/// Write slot `slot_index` with (offset, length, flags)
pub fn write_slot(page: &mut Page, slot_index: u32, offset: u32, length: u16, flags: u16) {
    let base = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
    page.data[base..base + 4].copy_from_slice(&offset.to_le_bytes());
    page.data[base + 4..base + 6].copy_from_slice(&length.to_le_bytes());
    page.data[base + 6..base + 8].copy_from_slice(&flags.to_le_bytes());
}

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
