use std::fs::File;
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};

use crate::page::{PAGE_SIZE, Page, init_page};
use crate::table::page_count;

// Create a new page on disk and return its page number
pub fn create_page(file: &mut File) -> io::Result<u32> {
    // Create a zeroed page buffer
    let mut page = Page::new();

    // Initialize page header (lower & upper pointers)
    init_page(&mut page);

    // Read current number of pages from table header
    let mut page_count = page_count(file)?;

    // New page ID is the current page count
    let page_num = page_count;

    // Append page at the end of the file
    file.seek(SeekFrom::End(0))?;
    file.write_all(&page.data)?;

    // Update page count in table header (page 0)
    page_count += 1;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&page_count.to_le_bytes())?;

    Ok(page_num)
}

// Read a page from disk into the provided page buffer
pub fn read_page(file: &mut File, page: &mut Page, page_num: u32) -> io::Result<()> {
    // Calculate byte offset for the page
    let offset = page_num * PAGE_SIZE as u32;

    // Get total file size
    let file_size = file.metadata()?.len();

    // Validate page existence
    if offset > file_size as u32 {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        ));
    }

    // Seek to page offset
    file.seek(SeekFrom::Start(offset as u64))?;

    // Read full page data
    file.read_exact(&mut page.data)?;

    Ok(())
}

// Write a page buffer to disk at the given page number
pub fn write_page(file: &mut File, page: &mut Page, page_num: u32) -> io::Result<()> {
    // Calculate byte offset for the page
    let offset = page_num as u64 * PAGE_SIZE as u64;

    // Get total file size
    let file_size = file.metadata()?.len();

    // Validate page existence
    if offset > file_size {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        ));
    }

    // Seek to page offset
    file.seek(SeekFrom::Start(offset))?;

    // Write page data to disk
    file.write_all(&page.data)?;

    Ok(())
}
