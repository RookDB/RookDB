use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::page::{Page, init_page, page_free_space};
use crate::disk::{read_page, write_page};

pub const EXTENT_SIZE: u32 = 16;   // allocate 16 pages at a time


//-------------------------------------------
// File Header Layout (first 8 bytes):
// bytes[0..4]   = total_pages
// bytes[4..8]   = total_extents
//-------------------------------------------

pub fn read_u32_at(file: &mut File, pos: u64) -> io::Result<u32> {
    file.seek(SeekFrom::Start(pos))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn write_u32_at(file: &mut File, pos: u64, value: u32) -> io::Result<()> {
    file.seek(SeekFrom::Start(pos))?;
    file.write_all(&value.to_le_bytes())?;
    Ok(())
}


//-----------------------------------------------------
// Reads total pages and total extents from the header
//-----------------------------------------------------
pub fn read_total_pages(file: &mut File) -> io::Result<u32> {
    read_u32_at(file, 0)
}

pub fn read_total_extents(file: &mut File) -> io::Result<u32> {
    read_u32_at(file, 4)
}

pub fn write_total_pages(file: &mut File, value: u32) -> io::Result<()> {
    write_u32_at(file, 0, value)
}

pub fn write_total_extents(file: &mut File, value: u32) -> io::Result<()> {
    write_u32_at(file, 4, value)
}


//-----------------------------------------------------
// Create a new extent → allocate 16 new pages
//-----------------------------------------------------
pub fn allocate_extent(file: &mut File) -> io::Result<u32> {
    let mut total_pages = read_total_pages(file)?;
    let mut total_extents = read_total_extents(file)?;

    let new_extent_id = total_extents;

    // Allocate 16 blank pages
    for _ in 0..EXTENT_SIZE {
        let mut page = Page::new();
        init_page(&mut page);
        write_page(file, &mut page, total_pages)?;
        total_pages += 1;
    }

    // Update header
    write_total_pages(file, total_pages)?;
    write_total_extents(file, total_extents + 1)?;

    Ok(new_extent_id)
}


//-----------------------------------------------------
// Finds a page inside the last extent that has free space
// If the extent is completely full → allocate a new one
//-----------------------------------------------------
pub fn find_page_for_insert(file: &mut File, required: u32) -> io::Result<u32> {
    // Read header
    let total_pages = read_total_pages(file)?;
    let total_extents = read_total_extents(file)?;

    // If no extents exist → create first one
    let extent_id = if total_extents == 0 {
        allocate_extent(file)?
    } else {
        total_extents - 1
    };

    // Start page number of this extent
    let start_page = extent_id * EXTENT_SIZE;
    let end_page = start_page + EXTENT_SIZE;

    // Scan pages inside the extent
    for p in start_page..end_page {
        if p >= total_pages {
            break;
        }
        let mut page = Page::new();
        read_page(file, &mut page, p)?;

        let free = page_free_space(&page)?;
        if free >= required {
            return Ok(p);
        }
    }

    // All 16 pages full → allocate next extent
    let new_extent = allocate_extent(file)?;
    Ok(new_extent * EXTENT_SIZE)   // first page of new extent
}
