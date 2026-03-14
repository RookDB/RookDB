use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use crate::disk::{create_page, read_page, write_page};
use crate::page::{ITEM_ID_SIZE, Page, page_free_space};
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

    // Create initial header
    let header = HeaderMetadata::new();
    let header_bytes = header.serialize()?;

    // Create header page with metadata
    let mut header_page_buf = vec![0u8; TABLE_HEADER_SIZE as usize];
    header_page_buf[0..20].copy_from_slice(&header_bytes);

    file.write_all(&header_page_buf)?;
    file.flush()?;
    file.sync_all()?;

    // Create first data page
    create_page(file)?;

    println!("[init_table] New table initialized with HeaderMetadata");

    Ok(())
}

pub fn insert_tuple(file: &mut File, data: &[u8]) -> io::Result<()> {
    let mut total_pages = page_count(file)?;
    let mut last_page_num = total_pages - 1;

    let mut page = Page::new();
    read_page(file, &mut page, last_page_num)?;

    let free_space = page_free_space(&page)?;
    let required = data.len() as u32 + ITEM_ID_SIZE;

    if required > free_space {
        create_page(file)?;
        total_pages += 1;
        last_page_num = total_pages - 1;
        read_page(file, &mut page, last_page_num)?;
    }

    let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    let start = upper - data.len() as u32;
    page.data[start as usize..upper as usize].copy_from_slice(data);

    upper = start;
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());

    page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
    page.data[lower as usize + 4..lower as usize + 8]
        .copy_from_slice(&(data.len() as u32).to_le_bytes());

    lower += ITEM_ID_SIZE;
    page.data[0..4].copy_from_slice(&lower.to_le_bytes());

    write_page(file, &mut page, last_page_num)?;
    Ok(())
}
