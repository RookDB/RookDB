use std::fs::File;
use std::io::{self, SeekFrom, Seek, Write};

use crate::page::{Page, page_free_space, ITEM_ID_SIZE};
use crate::disk::{create_page, read_page, write_page};
use crate::table::{page_count, TABLE_HEADER_SIZE};

/// Initialize a new table file
pub fn init_table(file: &mut File) -> io::Result<()> {
    // Move cursor to the beginning of the file
    file.seek(SeekFrom::Start(0))?;

    // Allocate 8192 (TABLE_HEADER_SIZE) + 8192 (PAGE_SIZE) bytes = 16KB
    let mut zero_buf = vec![0u8; TABLE_HEADER_SIZE as usize];

    //  Write "1" into the first 4 bytes (little-endian u32)
    // This can represent the total number of pages, e.g. 1
    zero_buf[0..4].copy_from_slice(&1u32.to_le_bytes());

    // Write the full buffer (header) to the file
    file.write_all(&zero_buf)?;

    // Optionally, flush to ensure write is committed
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