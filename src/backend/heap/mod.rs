use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use crate::disk::{read_page, write_page};
use crate::page::{ITEM_ID_SIZE, Page};
use crate::table::{TABLE_HEADER_SIZE, page_count};

pub mod types;
pub mod heap_manager;

pub use heap_manager::HeapManager;

/// Initialize a new table file with HeaderMetadata.
/// 
/// Creates a new table file with:
/// - Page 0: HeaderMetadata (20 bytes) + padding (remaining 8192 - 20 bytes)
/// - Page 1: First data page (empty slotted page)
/// 
/// This new version uses the improved header format that tracks FSM pages
/// and maintains a tuple counter.
#[deprecated(
    note = "Use HeapManager::create or open with existing file instead"
)]
pub fn init_table(file: &mut File) -> io::Result<()> {
    use crate::backend::heap::types::HeaderMetadata;

    file.seek(SeekFrom::Start(0))?;

    // Initialize header metadata correctly for memory bounds
    let mut header = HeaderMetadata::new();
    header.page_count = 2; // 1 header page + 1 empty slotted page
    let header_bytes = header.serialize()?;

    // Create header page with metadata
    let mut header_page_buf = vec![0u8; TABLE_HEADER_SIZE as usize];
    header_page_buf[0..20].copy_from_slice(&header_bytes);

    file.write_all(&header_page_buf)?;
    file.flush()?;

    // Create first data page dynamically instead of using legacy create_page
    let mut page = Page::new();
    crate::page::init_page(&mut page);
    file.seek(SeekFrom::End(0))?;
    file.write_all(&page.data)?;

    file.flush()?;
    file.sync_all()?;

    log::info!("[init_table] New table initialized with HeaderMetadata and data page");

    Ok(())
}

pub fn insert_tuple(file: &mut File, data: &[u8]) -> io::Result<()> {
    
    let mut total_pages = page_count(file)?;
    // If table is empty (only header), create first data page
    if total_pages == 1 {
        let mut new_page = Page::new();
        crate::page::init_page(&mut new_page);
        
        file.seek(SeekFrom::End(0))?;
        file.write_all(&new_page.data)?;

        // Update header block
        total_pages = 2;
        if let Ok(mut latest_header) = crate::disk::read_header_page(file) {
            latest_header.page_count = 2;
            crate::disk::update_header_page(file, &latest_header)?;
        }
    }
    
    let mut last_page_num = total_pages - 1;

    let mut page = Page::new();
    read_page(file, &mut page, last_page_num)?;

    // Calculate required space: data length + item ID (offset + length)
    let required = data.len() as u32 + ITEM_ID_SIZE;
    
    // Check if current page has enough space
    // We need to actually parse the page to get accurate free space
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    let free_space = upper.saturating_sub(lower);

    if required > free_space {
        total_pages += 1;
        last_page_num = total_pages - 1;
        
        // Initialize new page instead of legacy create_page
        page = Page::new();
        crate::page::init_page(&mut page);
        
        file.seek(SeekFrom::End(0))?;
        file.write_all(&page.data)?;

        if let Ok(mut latest_header) = crate::disk::read_header_page(file) {
            latest_header.page_count = total_pages;
            crate::disk::update_header_page(file, &latest_header)?;
        }
    }

    // Re-read lower/upper because we might have a new page
    let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    // Write tuple data at the end of free space (growing downwards)
    let start = upper - data.len() as u32;
    page.data[start as usize..upper as usize].copy_from_slice(data);

    // Update Item ID (growing upwards)
    // Item ID struct: [offset (4 bytes), length (4 bytes)]
    let item_id_pos = lower as usize;
    page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
    page.data[item_id_pos + 4..item_id_pos + 8]
        .copy_from_slice(&(data.len() as u32).to_le_bytes());

    // Update pointers
    lower += ITEM_ID_SIZE;
    upper = start;

    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());

    write_page(file, &mut page, last_page_num)?;
    
    // Update header metadata (tuple count)
    // Synchronize disk header state
    if let Ok(mut latest_header) = crate::disk::read_header_page(file) {
        latest_header.total_tuples += 1;
        latest_header.page_count = total_pages; 
        crate::disk::update_header_page(file, &latest_header)?;
    }
    
    Ok(())
}
