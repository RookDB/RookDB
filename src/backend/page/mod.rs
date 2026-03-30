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

/// Get the number of tuples currently stored in a page.
/// This is calculated as (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE.
/// 
/// # Errors
/// Returns error if reading the page header fails.
pub fn get_tuple_count(page: &Page) -> std::io::Result<u32> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    
    if lower < PAGE_HEADER_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid page lower pointer: {} < {}", lower, PAGE_HEADER_SIZE),
        ));
    }
    
    let tuple_count = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
    println!("[page::get_tuple_count] Computing tuple_count: ({} - {}) / {} = {}", 
             lower, PAGE_HEADER_SIZE, ITEM_ID_SIZE, tuple_count);
    
    Ok(tuple_count)
}

/// Get slot entry (offset, length) for a given slot ID.
/// Returns (offset, length) of the tuple data.
/// 
/// # Arguments
/// * `page` - The phase to read from
/// * `slot_id` - Zero-based slot index
/// 
/// # Returns
/// Result of (offset, length) or error if slot is invalid.
/// 
/// # Errors
/// Returns error if slot_id is out of bounds or read fails.
pub fn get_slot_entry(page: &Page, slot_id: u32) -> std::io::Result<(u32, u32)> {
    let tuple_count = get_tuple_count(page)?;
    
    if slot_id >= tuple_count {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Slot ID {} out of bounds (tuple_count={})", slot_id, tuple_count),
        ));
    }
    
    // Slot entries are stored right after the page header (8 bytes)
    // Each entry is 8 bytes: 4 bytes offset + 4 bytes length
    let slot_offset = PAGE_HEADER_SIZE as usize + (slot_id as usize * ITEM_ID_SIZE as usize);
    
    if slot_offset + ITEM_ID_SIZE as usize > page.data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Slot entry read would exceed page bounds",
        ));
    }
    
    let offset = u32::from_le_bytes(
        page.data[slot_offset..slot_offset + 4]
            .try_into()
            .unwrap(),
    );
    let length = u32::from_le_bytes(
        page.data[slot_offset + 4..slot_offset + 8]
            .try_into()
            .unwrap(),
    );
    
    println!("[page::get_slot_entry] Slot {}: offset={}, length={}", 
             slot_id, offset, length);
    
    Ok((offset, length))
}
