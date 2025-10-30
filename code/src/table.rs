use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};

use crate::disk::create_page;
pub const TABLE_HEADER_SIZE: u32 = 8192;

pub struct Table {
    pub data: Vec<u8>, // Fixed-size buffer holds the raw bytes of a table.
}

pub struct TableHeader {
    pub page_count: u32, // Total Number of Pages in a Table
}

// pub struct Table {
//     pub table_header: TableHeader;
// }


impl Table {
    pub fn new() -> Self {
        Self {
            data: vec![0; TABLE_HEADER_SIZE as usize], // 8192 bytes initialized to 0 in Memory
        }
    }
}

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
