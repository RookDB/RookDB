use std::fs::File;
use std::io::{self, Seek, SeekFrom, Read, ErrorKind, Error, Write};

use crate::page::{Page, PAGE_SIZE, init_page, page_count};
// use crate::table::{TABLE_HEADER_SIZE};

// Create Page 
pub fn create_page(file: &mut File) -> io::Result<u32> {
    // Create an empty page (all zeros) - In Memory
    let mut page = Page::new();
    println!("Initializing a In Memory Page with Page Headers...");
    // Initialise Page Header
    init_page(&mut page);

    // Print the first 8 bytes (page header: lower + upper offsets)
    println!("Created Page. Page Data: {:?}", &page.data[0..8]);

    // --- Step 1: Read existing page_count from File Header (first 4 bytes)
    let mut page_count =  page_count(file)?; // total pages currently in file

    println!("Page count: {}", page_count);
    
    // --- Step 2: The new page number = current page_count
    let page_num = page_count;

    // --- Step 3: Move to end of file and append new page
    file.seek(SeekFrom::End(0))?;
    file.write_all(&page.data)?;

    // --- Step 4: Update page_count in file header
    page_count += 1;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&page_count.to_le_bytes())?;

    println!(
        "Created new Page with Id: {} (total pages = {})",
        page_num, page_count
    );

    Ok(page_num)
}

// Read page from disk
pub fn read_page(file: &mut File, page: &mut Page, page_num: u32) -> io::Result<()> {   // Page Number or Page Id - as offset. (For Contiguous - PageNum * offset is ok but pageId requires more)
    
    // calculating the offset
    let offset = (page_num) * PAGE_SIZE as u32;


    // get file size
    let file_size = file.metadata()?.len();
    println!("File Size: {}", file_size);
    println!("OFFSET: {}", offset);

    if offset > file_size as u32 {
        // Return an error if the page doesn't exist
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        ));
    }

    // move the file cursor
    file.seek(SeekFrom::Start(offset as u64))?;

    // read the page data
    file.read_exact(&mut page.data)?;
    // println!("READING PAGE OK");
    Ok(())
}

// Write Page into Disk
pub fn write_page(file: &mut File, page: &mut Page, page_num: u32) -> io::Result<()> {   // Page Number or Page Id - as offset. (For Contiguous - PageNum * offset is ok but pageId requires more)
    // calculating the offset
    let offset = (page_num) as u64 * PAGE_SIZE as u64;  // as is required because not compiling - pageNum is 4 byte but offset requries 8 bytes.

    // get file size
    let file_size = file.metadata()?.len();
    println!("File Size: {}", file_size);

    if offset > file_size {
        // Return an error if the page doesn't exist
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            format!("Page {} does not exist in the file", page_num),
        ));
    }

    // move the file cursor
    file.seek(SeekFrom::Start(offset))?;

    // Write the page data into the file at that offset
    file.write_all(&page.data)?;

    Ok(())
}