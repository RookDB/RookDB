use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

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

pub fn file_identity_from_file(file: &File) -> io::Result<u64> {
    let metadata = file.metadata()?;
    Ok(file_identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn file_identity_from_metadata(metadata: &std::fs::Metadata) -> u64 {
    (metadata.dev() << 32) ^ metadata.ino()
}

#[cfg(not(unix))]
fn file_identity_from_metadata(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
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

/// Read the dead-tuple counter from the table header (bytes 4..8).
pub fn read_dead_tuple_count(file: &mut File) -> io::Result<u32> {
    file.seek(SeekFrom::Start(20))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Overwrite the dead-tuple counter in the table header with `count`.
pub fn write_dead_tuple_count(file: &mut File, count: u32) -> io::Result<()> {
    file.seek(SeekFrom::Start(20))?;
    file.write_all(&count.to_le_bytes())?;
    file.flush()?;
    Ok(())
}

/// Add `delta` to the stored dead-tuple counter (saturating at u32::MAX).
pub fn increment_dead_tuple_count(file: &mut File, delta: u32) -> io::Result<()> {
    let current = read_dead_tuple_count(file)?;
    let new_count = current.saturating_add(delta);
    write_dead_tuple_count(file, new_count)
}