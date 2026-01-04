use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

/// Page 0 is the table header
pub const TABLE_HEADER_PAGE_ID: u32 = 0;

pub const TABLE_HEADER_SIZE: u32 = 8192;

pub struct Table {
    pub data: Vec<u8>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            data: vec![0; TABLE_HEADER_SIZE as usize], // 8192 bytes initialized to 0 in Memory
        }
    }
}

/// Physical table header metadata
pub struct TableHeader {
    pub page_count: u32,
}

/// Read total number of pages in the table file
pub fn page_count(file: &mut File) -> io::Result<u32> {
    file.seek(SeekFrom::Start(0))?;

    let mut buffer = [0u8; 4];
    file.read_exact(&mut buffer)?;

    Ok(u32::from_le_bytes(buffer))
}