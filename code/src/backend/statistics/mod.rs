use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::page::PAGE_SIZE;

/// Print total number of pages in a table using header page metadata
pub fn print_table_page_count(
    db_name: &str,
    table_name: &str,
) -> io::Result<()> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = File::open(&table_path)?;

    // Read header page (page 0)
    let mut header_page = vec![0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_page)?;

    // First 4 bytes store total page count
    let total_pages =
        u32::from_le_bytes(header_page[0..4].try_into().unwrap());

    println!(
        "Table '{}' has {} total pages.",
        table_name, total_pages
    );

    Ok(())
}
