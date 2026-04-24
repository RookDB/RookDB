use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::page::PAGE_SIZE;
use crate::catalog::types::Catalog;

/// Print total number of pages in a table using header page metadata
pub fn print_table_page_count(db_name: &str, table_name: &str) -> io::Result<()> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = File::open(&table_path)?;

    // Read header page (page 0)
    let mut header_page = vec![0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_page)?;

    // First 4 bytes store total page count
    let bytes: [u8; 4] = header_page[0..4].try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "malformed table header"))?;
    let total_pages = u32::from_le_bytes(bytes);

    println!("Table '{}' has {} total pages.", table_name, total_pages);

    Ok(())
}

/// Get actual page count for a table from disk
pub fn get_page_count(db_name: &str, table_name: &str) -> io::Result<u64> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = File::open(&table_path)?;

    // Read header page (page 0)
    let mut header_page = vec![0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_page)?;

    // First 4 bytes store total page count
    let bytes: [u8; 4] = header_page[0..4].try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "malformed table header"))?;
    let total_pages = u32::from_le_bytes(bytes);

    Ok(total_pages as u64)
}

/// Estimate row count based on page count and average row size
/// Assumes 80% page utilization (20% overhead for header/free space)
pub fn estimate_row_count(page_count: u64, avg_row_size: usize) -> u64 {
    if avg_row_size == 0 || page_count == 0 {
        return 0;
    }

    let usable_space_per_page = (PAGE_SIZE as f64 * 0.8) as usize; // 80% utilization
    let rows_per_page = usable_space_per_page / avg_row_size;
    (page_count as usize * rows_per_page) as u64
}

/// Update table statistics in catalog by reading actual data from disk
pub fn update_catalog_statistics(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
    avg_row_size: usize,
) -> io::Result<()> {
    if let Some(database) = catalog.databases.get_mut(db_name)
        && let Some(table) = database.tables.get_mut(table_name)
    {
        // Get actual page count from disk
        let page_count = get_page_count(db_name, table_name)?;

        // Estimate row count from pages
        let row_count = estimate_row_count(page_count, avg_row_size);

        // Update statistics
        table.page_count = page_count;
        table.row_count = row_count;
        table.avg_row_size = avg_row_size;
    }

    Ok(())
}
