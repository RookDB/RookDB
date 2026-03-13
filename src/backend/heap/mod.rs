use std::fs::File;
use std::io::{self, SeekFrom, Seek, Write};

use crate::catalog::types::Catalog;
use crate::page::{Page, page_free_space, ITEM_ID_SIZE};
use crate::disk::{create_page, read_page, write_page};
use crate::index::{add_tuple_to_all_indexes, remove_tuple_from_all_indexes, RecordId};
use crate::table::{page_count, TABLE_HEADER_SIZE};

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
    insert_tuple_with_rid(file, data).map(|_| ())
}

pub fn insert_tuple_with_rid(file: &mut File, data: &[u8]) -> io::Result<RecordId> {
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

    let item_id = (lower - crate::page::PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

    lower += ITEM_ID_SIZE;
    page.data[0..4].copy_from_slice(&lower.to_le_bytes());

    write_page(file, &mut page, last_page_num)?;
    Ok(RecordId::new(last_page_num, item_id))
}

pub fn insert_tuple_with_index_maintenance(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    data: &[u8],
) -> io::Result<RecordId> {
    let rid = insert_tuple_with_rid(file, data)?;
    add_tuple_to_all_indexes(catalog, db_name, table_name, data, rid.clone())?;
    Ok(rid)
}

pub fn delete_tuple_with_index_maintenance(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    record_id: RecordId,
) -> io::Result<()> {
    let tuple = delete_tuple_raw(file, record_id.clone())?;
    remove_tuple_from_all_indexes(catalog, db_name, table_name, &tuple, record_id)?;
    Ok(())
}

fn delete_tuple_raw(file: &mut File, record_id: RecordId) -> io::Result<Vec<u8>> {
    let mut page = Page::new();
    read_page(file, &mut page, record_id.page_no)?;

    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_items = (lower - crate::page::PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
    if record_id.item_id >= num_items {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "record item {} out of range for page {}",
                record_id.item_id, record_id.page_no
            ),
        ));
    }

    let slot_base = (crate::page::PAGE_HEADER_SIZE + record_id.item_id * ITEM_ID_SIZE) as usize;
    let tuple_offset =
        u32::from_le_bytes(page.data[slot_base..slot_base + 4].try_into().unwrap()) as usize;
    let tuple_len =
        u32::from_le_bytes(page.data[slot_base + 4..slot_base + 8].try_into().unwrap()) as usize;

    if tuple_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "record {}:{} is already deleted",
                record_id.page_no, record_id.item_id
            ),
        ));
    }

    let tuple_end = tuple_offset.checked_add(tuple_len).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "tuple length overflow")
    })?;
    if tuple_end > page.data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "tuple bounds out of page for {}:{}",
                record_id.page_no, record_id.item_id
            ),
        ));
    }

    let tuple = page.data[tuple_offset..tuple_end].to_vec();
    page.data[slot_base + 4..slot_base + 8].copy_from_slice(&0u32.to_le_bytes());

    write_page(file, &mut page, record_id.page_no)?;
    Ok(tuple)
}