use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use crate::disk::{create_page, read_page, write_page};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, page_free_space};
use crate::table::{TABLE_HEADER_SIZE, page_count};

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

/// Delete a tuple from a specific page and slot index.
/// Returns the old tuple bytes (for TOAST cleanup) on success.
/// The slot is marked as deleted by zeroing its length field.
pub fn delete_tuple(file: &mut File, page_num: u32, slot_index: u32) -> io::Result<Vec<u8>> {
    let mut page = Page::new();
    read_page(file, &mut page, page_num)?;

    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

    if slot_index >= num_items {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Slot index {} out of range (page has {} tuples)",
                slot_index, num_items
            ),
        ));
    }

    // Read old tuple data from the item-ID slot
    let base = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
    let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
    let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());

    if length == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Tuple at slot {} is already deleted", slot_index),
        ));
    }

    let old_data = page.data[offset as usize..(offset + length) as usize].to_vec();

    // Mark slot as deleted: zero the length field
    page.data[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());

    write_page(file, &mut page, page_num)?;

    Ok(old_data)
}

/// Update a tuple: deletes the old tuple and inserts new data.
/// Returns the old tuple bytes (for TOAST cleanup).
pub fn update_tuple(
    file: &mut File,
    page_num: u32,
    slot_index: u32,
    new_data: &[u8],
) -> io::Result<Vec<u8>> {
    // Step 1: Delete old tuple (get old bytes back)
    let old_bytes = delete_tuple(file, page_num, slot_index)?;

    // Step 2: Insert new tuple (may go into same page or a new one)
    insert_tuple(file, new_data)?;

    Ok(old_bytes)
}
