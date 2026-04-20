//! Ordered file header management.
//!
//! Extends the existing table header page (page 0, 8192 bytes) to store
//! sort metadata. The header layout is:
//!
//! | Bytes      | Field            | Type       |
//! |------------|------------------|------------|
//! | [0..4]     | page_count       | u32 LE     |
//! | [4..5]     | file_type        | u8         |
//! | [5..9]     | sort_key_count   | u32 LE     |
//! | [9..9+N*5] | sort_key_entries | array      |
//!
//! Each sort key entry is 5 bytes: column_index (u32 LE) + direction (u8).

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::disk::create_page;
use crate::page::PAGE_SIZE;

/// File type discriminator stored in the header.
#[derive(Clone, Debug, PartialEq)]
pub enum FileType {
    Heap,    // 0
    Ordered, // 1
}

impl FileType {
    pub fn to_u8(&self) -> u8 {
        match self {
            FileType::Heap => 0,
            FileType::Ordered => 1,
        }
    }

    pub fn from_u8(val: u8) -> Self {
        match val {
            1 => FileType::Ordered,
            _ => FileType::Heap,
        }
    }
}

/// On-disk representation of a sort key (5 bytes each).
#[derive(Clone, Debug)]
pub struct SortKeyEntry {
    pub column_index: u32, // 4 bytes
    pub direction: u8,     // 1 byte: 0 = ASC, 1 = DESC
}

/// Metadata stored in the table header page (page 0) for ordered files.
#[derive(Clone, Debug)]
pub struct OrderedFileHeader {
    pub page_count: u32,
    pub file_type: FileType,
    pub sort_key_count: u32,
    pub sort_keys: Vec<SortKeyEntry>,
}

/// Reads and parses the extended header page of a table file.
pub fn read_ordered_file_header(file: &mut File) -> io::Result<OrderedFileHeader> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut buf)?;

    let page_count = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let file_type = FileType::from_u8(buf[4]);
    let sort_key_count = u32::from_le_bytes(buf[5..9].try_into().unwrap());

    let mut sort_keys = Vec::with_capacity(sort_key_count as usize);
    for i in 0..sort_key_count as usize {
        let base = 9 + i * 5;
        let column_index = u32::from_le_bytes(buf[base..base + 4].try_into().unwrap());
        let direction = buf[base + 4];
        sort_keys.push(SortKeyEntry {
            column_index,
            direction,
        });
    }

    Ok(OrderedFileHeader {
        page_count,
        file_type,
        sort_key_count,
        sort_keys,
    })
}

/// Writes the extended header metadata to page 0 of a table file.
pub fn write_ordered_file_header(file: &mut File, header: &OrderedFileHeader) -> io::Result<()> {
    let mut buf = vec![0u8; PAGE_SIZE];

    // page_count
    buf[0..4].copy_from_slice(&header.page_count.to_le_bytes());
    // file_type
    buf[4] = header.file_type.to_u8();
    // sort_key_count
    buf[5..9].copy_from_slice(&header.sort_key_count.to_le_bytes());
    // sort key entries
    for (i, key) in header.sort_keys.iter().enumerate() {
        let base = 9 + i * 5;
        buf[base..base + 4].copy_from_slice(&key.column_index.to_le_bytes());
        buf[base + 4] = key.direction;
    }

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&buf)?;
    file.flush()?;

    Ok(())
}

/// Creates and initializes a new ordered table file with sort metadata in the header page.
pub fn init_ordered_table(file: &mut File, sort_keys: &[SortKeyEntry]) -> io::Result<()> {
    // Create the first data page (page 1) via create_page.
    // create_page reads page_count from byte 0..4, appends a page, then
    // writes the incremented page_count back. So we must ensure page_count
    // starts at 1 (header page only) before calling it.

    // Write a full temporary header page with page_count = 1 so create_page works correctly
    let mut temp_header = vec![0u8; PAGE_SIZE];
    temp_header[0..4].copy_from_slice(&1u32.to_le_bytes());
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&temp_header)?;
    file.flush()?;

    create_page(file)?;
    // Now page_count in file = 2

    // Overwrite the full header page with ordered metadata (page_count = 2)
    let header = OrderedFileHeader {
        page_count: 2, // header page + first data page
        file_type: FileType::Ordered,
        sort_key_count: sort_keys.len() as u32,
        sort_keys: sort_keys.to_vec(),
    };

    write_ordered_file_header(file, &header)?;

    Ok(())
}
