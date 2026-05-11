use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

/// Page ID reserved for table metadata
pub const TABLE_HEADER_PAGE_ID: u32 = 0;

// Size of a single page
pub const TABLE_HEADER_SIZE: u32 = 8192;

// Physical page buffer
pub struct Table {
    // Raw page bytes
    pub data: Vec<u8>,
}

impl Table {
    // Create an empty page buffer
    pub fn new() -> Self {
        Self {
            data: vec![0; TABLE_HEADER_SIZE as usize],
        }
    }
}

// Read page count from page 0
pub fn page_count(file: &mut File) -> io::Result<u32> {
    // Page 0 starts at offset 0
    file.seek(SeekFrom::Start(0))?;

    // First 4 bytes store page count
    let mut buffer = [0u8; 4];
    file.read_exact(&mut buffer)?;

    Ok(u32::from_le_bytes(buffer))
}
