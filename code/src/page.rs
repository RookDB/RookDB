use std::fs::{File};
use std::io::{self, Read, Seek, SeekFrom};
use serde_json::Value;

pub const PAGE_SIZE: usize = 8192; // Page size - storing as 8 bytes and as usize only because most pointers(file pointers and otheres requires it to be 8 bytes)
pub const PAGE_HEADER_SIZE: u32 = 8;
pub const ITEM_ID_SIZE: u32  = 8;

use crate::disk::{read_page, write_page};

// pub struct PageHeader {
//     pub lower: u32, // Offset to start of free space - 4 bytes
//     pub upper: u32, // Offset to end of free space - 4 bytes
// }

// pub struct ItemId {
//     pub offset: u32, // Offset of the item or Tuple - 4 bytes
//     pub length: u32, // Length of the item or Tuple - 4 bytes
// }

pub struct Page {
    pub data: Vec<u8>, // Fixed-size buffer holds the raw bytes of a page (PAGE_SIZE = 8KB)
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: vec![0; PAGE_SIZE],
        }
    }
}

pub fn init_page(page: &mut Page) {
    // Lower offset in first 4 bytes
    let lower = PAGE_HEADER_SIZE.to_le_bytes();
    page.data[0..4].copy_from_slice(&lower);

    // Upper offset in next 4 bytes
    let upper = (PAGE_SIZE as u32).to_le_bytes();
    page.data[4..8].copy_from_slice(&upper);
}

pub fn page_count(file: &mut File) -> io::Result<u32> {
    // Move cursor to the start of the file header
    file.seek(SeekFrom::Start(0))?;

    // Read the first 4 bytes of the table header
    let mut buffer = [0u8; 4];
    file.read_exact(&mut buffer)?;

    // Convert bytes to little-endian u32
    let total_pages = u32::from_le_bytes(buffer);

    Ok(total_pages)
}



pub fn page_free_space(page: &Page) -> io::Result<u32> {
    // Get Lower and Upper of page
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    println!("UPPER: {}", upper);
    println!("LOWER: {}", lower);
    // Freespace is equal to upper - lower
    Ok(upper - lower)
}

pub fn insert_tuple(file: &mut File, json_str: &str) -> io::Result<()> {
    // Parse the input JSON string
    let parsed: Value = match serde_json::from_str(json_str) {
        Ok(val) => val,
        Err(_) => {
            println!("Invalid JSON format. Example: {{\"id\": 1, \"name\": \"John\"}}");
            return Ok(());
        }
    };

    // Convert the JSON into a compact string for storage
    let json_compact = serde_json::to_string(&parsed).unwrap();
    let data = json_compact.as_bytes();

    // Get total pages
    let total_pages: u32 = page_count(file)?;
    println!("Total Pages: {}", total_pages);
    let last_page_num: u32 = total_pages;

    println!("Last Page Number: {}", last_page_num - 1);

    // Read last page
    let mut last_page: Page = Page::new();
    read_page(file, &mut last_page, last_page_num)?;

    // Check free space
    let free_space: u32 = page_free_space(&last_page)?;
    println!("Free Space in Last Page: {}", free_space);

    // Total space needed = data + item ID entry
    let total_required = data.len() as u32 + ITEM_ID_SIZE;

    if total_required <= free_space {
        // Current upper and lower
        let mut upper = u32::from_le_bytes(last_page.data[4..8].try_into().unwrap());
        let mut lower = u32::from_le_bytes(last_page.data[0..4].try_into().unwrap());

        // Compute start of data region
        let start = upper - data.len() as u32;

        // Write JSON data into page bytes (store as characters)
        last_page.data[start as usize..upper as usize].copy_from_slice(data);

        // Update upper pointer
        upper = start;
        last_page.data[4..8].copy_from_slice(&upper.to_le_bytes());

        // Update lower pointer
        let item_id_pos = lower as usize;
        lower += ITEM_ID_SIZE;
        last_page.data[0..4].copy_from_slice(&lower.to_le_bytes());

        // Write Item ID: [offset][length]
        last_page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
        last_page.data[item_id_pos + 4..item_id_pos + 8]
            .copy_from_slice(&(data.len() as u32).to_le_bytes());

        // Write updated page to disk
        write_page(file, &mut last_page, last_page_num)?;

        println!(" Tuple inserted successfully on page {}", last_page_num);
        println!("Stored JSON: {}", json_compact);
    } else {
        println!(" Not enough free space in last page. Need a new page.");
    }

    Ok(())
}
