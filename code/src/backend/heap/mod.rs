pub mod heap_manager;
pub mod types;
pub mod autovacuum;

pub use heap_manager::{HeapManager, HeapScanIterator};

use std::fs::File;
use std::io::{self, SeekFrom, Seek, Write};

use crate::fsm_manager::{fsm_search_avail, fsm_set_avail};
use crate::page::{Page, page_free_space, ITEM_ID_SIZE, PAGE_HEADER_SIZE, SLOT_FLAG_DELETED};
use crate::disk::{create_page, read_page, write_page};
use crate::backend::page::page_lock::PageWriteLock;
use crate::table::{page_count, TABLE_HEADER_SIZE};
use crate::table::file_identity_from_file;

#[derive(Debug, Clone, Copy)]
pub struct TuplePointer {
    pub page_id: u32,
    pub slot_index: u16,
}

/// Initialize a new table file
pub fn init_table(file: &mut File) -> io::Result<()> {
    // Move cursor to the beginning of the file
    file.seek(SeekFrom::Start(0))?;

    // Allocate 8192 bytes
    let mut zero_buf = vec![0u8; TABLE_HEADER_SIZE as usize];

    //  Write "1" into the first 4 bytes (little-endian u32)
    // This can represent the total number of pages, e.g. 1
    zero_buf[0..4].copy_from_slice(&1u32.to_le_bytes());

    // Write the full buffer (header) to the file
    file.write_all(&zero_buf)?;
    file.flush()?;
    file.sync_all()?;

    create_page(file)?;

    Ok(())
}


fn find_page_with_space(file: &mut File, required: u32, file_identity: u64) -> io::Result<Option<u32>> {
    if let Some(page_id) = fsm_search_avail(file_identity, required) {
        let total_pages = page_count(file)?;
        if page_id > 0 && page_id < total_pages {
            return Ok(Some(page_id));
        }
    }

    let total_pages = page_count(file)?;
    for page_id in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_id)?;
        let free = page_free_space(&page)?;
        fsm_set_avail(file_identity, page_id, free);
        if free >= required {
            return Ok(Some(page_id));
        }
    }

    Ok(None)
}

pub fn insert_tuple(file: &mut File, data: &[u8]) -> io::Result<()> {
    let mut total_pages = page_count(file)?;
    // If table is empty (only header), create first data page
    if total_pages == 1 {
        create_page(file)?;
        total_pages = 2;
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
        create_page(file)?;
        total_pages += 1;
        last_page_num = total_pages - 1;

        // Initialize new page
        page = Page::new();
        crate::page::init_page(&mut page);
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

pub fn soft_delete_tuple_at(file: &mut File, pointer: TuplePointer) -> io::Result<()> {
    // NOTE: Caller must ensure the page is locked (e.g. via PageWriteLock)
    // We don't acquire lock here to avoid deadlock if called from update_tuples which already holds the lock

    let mut page = Page::new();
    read_page(file, &mut page, pointer.page_id)?;

    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;
    if pointer.slot_index as usize >= num_items {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "slot index {} out of bounds for page {}",
                pointer.slot_index, pointer.page_id
            ),
        ));
    }

    let base = PAGE_HEADER_SIZE as usize + pointer.slot_index as usize * ITEM_ID_SIZE as usize;
    let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
    let new_flags = flags | SLOT_FLAG_DELETED;
    page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
    write_page(file, &mut page, pointer.page_id)
}