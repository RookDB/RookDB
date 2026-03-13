//! In-memory sorting for tables that fit entirely in the buffer pool.
//!
//! Loads all tuples from a heap table into memory, sorts them using
//! the TupleComparator, and rewrites them as an ordered file.

use std::fs::File;
use std::io;

use crate::catalog::save_catalog;
use crate::catalog::types::{Catalog, Column, SortDirection, SortKey};
use crate::disk::{read_page, write_page};
use crate::ordered::ordered_file::{
    write_ordered_file_header, FileType, OrderedFileHeader, SortKeyEntry,
};
use crate::page::{init_page, Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::sorting::comparator::TupleComparator;
use crate::table::page_count;

/// Sorts a table that fits entirely in the buffer pool without creating
/// temporary files.
///
/// Loads all tuples from the table file, sorts them in memory, and
/// rewrites the file as an ordered file with sorted ItemId arrays
/// and cross-page sort invariant maintained.
pub fn in_memory_sort(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
    sort_keys: Vec<SortKey>,
    file: &mut File,
) -> io::Result<()> {
    // 1. Get the schema from catalog
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db_name),
        )
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", table_name),
        )
    })?;
    let columns: Vec<Column> = table.columns.clone();

    // 2. Create comparator
    let comparator = TupleComparator::new(columns.clone(), sort_keys.clone());

    // 3. Read total pages and extract all tuples
    let total_pages = page_count(file)?;
    let mut all_tuples: Vec<Vec<u8>> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_data = page.data[offset..offset + length].to_vec();
            all_tuples.push(tuple_data);
        }
    }

    // 4. Sort tuples in memory
    all_tuples.sort_by(|a, b| comparator.compare(a, b));

    // 5. Rewrite pages with sorted tuples
    let mut pages: Vec<Page> = Vec::new();
    let mut current_page = Page::new();
    init_page(&mut current_page);

    for tuple in &all_tuples {
        let tuple_len = tuple.len() as u32;
        let required = tuple_len + ITEM_ID_SIZE;
        let free = {
            let lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
            let upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());
            upper - lower
        };

        if required > free {
            // Current page is full, start a new one
            pages.push(current_page);
            current_page = Page::new();
            init_page(&mut current_page);
        }

        // Insert tuple into current page
        let mut lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());

        let start = upper - tuple_len;
        current_page.data[start as usize..upper as usize].copy_from_slice(tuple);

        // Write ItemId entry
        current_page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        current_page.data[lower as usize + 4..lower as usize + 8]
            .copy_from_slice(&tuple_len.to_le_bytes());

        lower += ITEM_ID_SIZE;
        upper = start;

        current_page.data[0..4].copy_from_slice(&lower.to_le_bytes());
        current_page.data[4..8].copy_from_slice(&upper.to_le_bytes());
    }

    // Push the last page (even if partially filled)
    pages.push(current_page);

    // 6. Write the ordered file header
    let total_page_count = (pages.len() + 1) as u32; // +1 for header page
    let sort_key_entries: Vec<SortKeyEntry> = sort_keys
        .iter()
        .map(|sk| SortKeyEntry {
            column_index: sk.column_index,
            direction: match sk.direction {
                SortDirection::Ascending => 0,
                SortDirection::Descending => 1,
            },
        })
        .collect();

    let header = OrderedFileHeader {
        page_count: total_page_count,
        file_type: FileType::Ordered,
        sort_key_count: sort_key_entries.len() as u32,
        sort_keys: sort_key_entries,
    };

    write_ordered_file_header(file, &header)?;

    // 7. Write data pages to disk
    // If we have fewer pages than before, we truncate.
    // If we have more, we need to extend the file.
    let file_needs_pages = pages.len() as u32;
    let old_data_pages = if total_pages > 1 { total_pages - 1 } else { 0 };

    // Write existing page slots
    for (i, mut page) in pages.into_iter().enumerate() {
        let page_num = (i + 1) as u32; // data pages start at page 1
        if page_num < total_pages {
            // Overwrite existing page
            write_page(file, &mut page, page_num)?;
        } else {
            // Need to extend the file - write at end
            // The file should already be the right size due to create_page,
            // but we can use write_page if the file is large enough.
            // For safety, seek to the correct position and write.
            use std::io::{Seek, SeekFrom, Write};
            let offset = page_num as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&page.data)?;
        }
    }

    // Truncate file if we have fewer pages now
    if file_needs_pages < old_data_pages {
        let new_file_size = total_page_count as u64 * PAGE_SIZE as u64;
        file.set_len(new_file_size)?;
    }

    // 8. Update catalog metadata
    let db = catalog.databases.get_mut(db_name).unwrap();
    let table = db.tables.get_mut(table_name).unwrap();
    table.sort_keys = Some(sort_keys);
    table.file_type = Some("ordered".to_string());
    save_catalog(catalog);

    println!(
        "Table '{}' sorted in-memory. {} tuples across {} data pages.",
        table_name,
        all_tuples.len(),
        file_needs_pages
    );

    Ok(())
}
