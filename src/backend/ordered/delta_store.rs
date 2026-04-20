use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};

use crate::disk::{read_page, write_page};
use crate::page::{ITEM_ID_SIZE, PAGE_SIZE, Page, init_page, page_free_space};

fn delta_path(db_name: &str, table_name: &str) -> String {
    format!("database/base/{}/{}.delta", db_name, table_name)
}

fn ensure_delta_file(db_name: &str, table_name: &str) -> io::Result<File> {
    let path = delta_path(db_name, table_name);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)?;

    let file_len = file.metadata()?.len();
    if file_len == 0 {
        let mut header = vec![0u8; PAGE_SIZE];
        header[0..4].copy_from_slice(&1u32.to_le_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;

        let mut first_page = Page::new();
        init_page(&mut first_page);
        file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;
        file.write_all(&first_page.data)?;

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&2u32.to_le_bytes())?;
        file.flush()?;
    }

    Ok(file)
}

fn page_count(file: &mut File) -> io::Result<u32> {
    use std::io::Read;
    file.seek(SeekFrom::Start(0))?;
    let mut buffer = [0u8; 4];
    file.read_exact(&mut buffer)?;
    Ok(u32::from_le_bytes(buffer))
}

pub fn append_delta_tuple(db_name: &str, table_name: &str, data: &[u8]) -> io::Result<()> {
    let mut file = ensure_delta_file(db_name, table_name)?;
    let mut total_pages = page_count(&mut file)?;
    let mut target_page_num = total_pages - 1;

    let mut page = Page::new();
    read_page(&mut file, &mut page, target_page_num)?;

    let required = data.len() as u32 + ITEM_ID_SIZE;
    let free = page_free_space(&page)?;
    if required > free {
        let mut new_page = Page::new();
        init_page(&mut new_page);
        let offset = total_pages as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&new_page.data)?;
        total_pages += 1;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&total_pages.to_le_bytes())?;
        target_page_num = total_pages - 1;
        page = new_page;
    }

    let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    let start = upper - data.len() as u32;
    page.data[start as usize..upper as usize].copy_from_slice(data);
    page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
    page.data[lower as usize + 4..lower as usize + 8]
        .copy_from_slice(&(data.len() as u32).to_le_bytes());

    lower += ITEM_ID_SIZE;
    upper = start;
    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());

    write_page(&mut file, &mut page, target_page_num)?;
    file.flush()?;
    Ok(())
}

pub fn scan_all_delta_tuples(db_name: &str, table_name: &str) -> io::Result<Vec<Vec<u8>>> {
    let path = delta_path(db_name, table_name);
    let file_exists = std::path::Path::new(&path).exists();
    if !file_exists {
        return Ok(Vec::new());
    }

    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let total_pages = page_count(&mut file)?;
    if total_pages <= 1 {
        return Ok(Vec::new());
    }

    let mut tuples = Vec::new();
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - crate::page::PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (crate::page::PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            tuples.push(page.data[offset..offset + length].to_vec());
        }
    }

    Ok(tuples)
}

pub fn truncate_delta(db_name: &str, table_name: &str) -> io::Result<()> {
    let path = delta_path(db_name, table_name);
    if !std::path::Path::new(&path).exists() {
        return Ok(());
    }

    let mut file = OpenOptions::new().write(true).truncate(true).open(&path)?;
    let mut header = vec![0u8; PAGE_SIZE];
    header[0..4].copy_from_slice(&1u32.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;

    let mut first_page = Page::new();
    init_page(&mut first_page);
    file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;
    file.write_all(&first_page.data)?;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&2u32.to_le_bytes())?;
    file.flush()?;
    Ok(())
}
