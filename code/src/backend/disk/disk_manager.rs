use std::fs::File;
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write};

use crate::page::{PAGE_SIZE, Page, init_page};
use crate::table::page_count;
use crate::heap::types::HeaderMetadata;
use crate::backend::instrumentation::HEAP_METRICS;
use std::sync::atomic::Ordering;

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
    HEAP_METRICS.read_page_calls.fetch_add(1, Ordering::Relaxed);
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
    HEAP_METRICS.write_page_calls.fetch_add(1, Ordering::Relaxed);
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

/// Update the header page (Page 0) with HeaderMetadata.
pub fn update_header_page(file: &mut File, header: &HeaderMetadata) -> io::Result<()> {
    log::trace!("[disk::update_header_page] Writing header metadata to page 0");

    // Serialize the header
    let header_bytes = header.serialize()?;

    // Seek to page 0, offset 0
    file.seek(SeekFrom::Start(0))?;

    // Write the 20-byte header
    file.write_all(&header_bytes)?;

    log::trace!("[disk::update_header_page] Header successfully written");

    Ok(())
}

/// Read and deserialize the header page (Page 0) into HeaderMetadata.
pub fn read_header_page(file: &mut File) -> io::Result<HeaderMetadata> {
    log::trace!("[disk::read_header_page] Reading header metadata from page 0");

    // Seek to page 0, offset 0
    file.seek(SeekFrom::Start(0))?;

    // Read 20 bytes
    let mut buf = [0u8; 20];
    file.read_exact(&mut buf)?;

    // Deserialize
    HeaderMetadata::deserialize(&buf)
}

/// Read all pages (header + data) from the file into memory.
pub fn read_all_pages(file: &mut File) -> io::Result<Vec<Page>> {
    let metadata = file.metadata()?;
    let file_size = metadata.len();

    if file_size == 0 {
        return Ok(Vec::new());
    }

    let total_pages = (file_size / PAGE_SIZE as u64) as usize;
    let mut pages = Vec::with_capacity(total_pages);

    file.seek(SeekFrom::Start(0))?;

    for _ in 0..total_pages {
        let mut page = Page::new();
        match file.read_exact(&mut page.data) {
            Ok(_) => pages.push(page),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(pages)
}