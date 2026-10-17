use crate::backend::error::{RookError, RookResult};
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};

use crate::backend::instrumentation::HEAP_METRICS;
use crate::heap::types::HeaderMetadata;
use crate::page::{PAGE_SIZE, Page};
use std::sync::atomic::Ordering;

// Read a page from disk into the provided page buffer
pub fn read_page(file: &mut File, page: &mut Page, page_num: u32) -> RookResult<()> {
    HEAP_METRICS.read_page_calls.fetch_add(1, Ordering::Relaxed);
    // Calculate byte offset for the page
    // 64-bit arithmetic: a u32 byte offset wraps at page 524,288 (4 GiB).
    let offset = page_num as u64 * PAGE_SIZE as u64;

    // Get total file size
    let file_size = file.metadata()?.len();

    // Validate page existence
    if offset > file_size {
        return Err(RookError::from(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        )));
    }

    // Seek to page offset
    file.seek(SeekFrom::Start(offset))?;

    // Read full page data
    file.read_exact(&mut page.data)?;

    Ok(())
}

// Write a page buffer to disk at the given page number
pub fn write_page(file: &mut File, page: &mut Page, page_num: u32) -> RookResult<()> {
    HEAP_METRICS
        .write_page_calls
        .fetch_add(1, Ordering::Relaxed);
    // Calculate byte offset for the page
    let offset = page_num as u64 * PAGE_SIZE as u64;

    // Get total file size
    let file_size = file.metadata()?.len();

    // Validate page existence
    if offset > file_size {
        return Err(RookError::from(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        )));
    }

    // Seek to page offset
    file.seek(SeekFrom::Start(offset))?;

    // Write page data to disk
    file.write_all(&page.data)?;

    Ok(())
}

/// Update the header page (Page 0) with HeaderMetadata.
pub fn update_header_page(file: &mut File, header: &HeaderMetadata) -> RookResult<()> {
    // Serialize the header
    let header_bytes = header.serialize()?;

    // Seek to page 0, offset 0
    file.seek(SeekFrom::Start(0))?;

    // Write the 24-byte header
    file.write_all(&header_bytes)?;

    Ok(())
}

/// Read and deserialize the header page (Page 0) into HeaderMetadata.
pub fn read_header_page(file: &mut File) -> RookResult<HeaderMetadata> {
    // Seek to page 0, offset 0
    file.seek(SeekFrom::Start(0))?;

    // Read 24 bytes
    let mut buf = [0u8; 24];
    file.read_exact(&mut buf)?;

    // Deserialize
    HeaderMetadata::deserialize(&buf)
}

/// Read all pages (header + data) from the file into memory.
pub fn read_all_pages(file: &mut File) -> RookResult<Vec<Page>> {
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
            Err(e) => return Err(RookError::from(e)),
        }
    }

    Ok(pages)
}
