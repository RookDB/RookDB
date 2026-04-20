use std::fs::OpenOptions;
use std::io;

use crate::catalog::{Catalog, SortDirection, save_catalog};
use crate::ordered::delta_store::{scan_all_delta_tuples, truncate_delta};
use crate::ordered::ordered_file::{
    FileType, OrderedFileHeader, SortKeyEntry, write_ordered_file_header,
};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, init_page};
use crate::sorting::comparator::TupleComparator;

fn read_base_tuples(file: &mut std::fs::File) -> io::Result<Vec<Vec<u8>>> {
    use crate::disk::read_page;
    use crate::table::page_count;

    let total_pages = page_count(file)?;
    let mut tuples = Vec::new();

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
            tuples.push(page.data[offset..offset + length].to_vec());
        }
    }

    Ok(tuples)
}

fn rewrite_ordered_file(
    file: &mut std::fs::File,
    tuples: &[Vec<u8>],
    sort_keys: &[crate::catalog::SortKey],
) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut pages: Vec<Page> = Vec::new();
    let mut current_page = Page::new();
    init_page(&mut current_page);

    for tuple in tuples {
        let tuple_len = tuple.len() as u32;
        let required = tuple_len + ITEM_ID_SIZE;
        let free = {
            let lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
            let upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());
            upper - lower
        };

        if required > free {
            pages.push(current_page);
            current_page = Page::new();
            init_page(&mut current_page);
        }

        let mut lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());

        let start = upper - tuple_len;
        current_page.data[start as usize..upper as usize].copy_from_slice(tuple);
        current_page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        current_page.data[lower as usize + 4..lower as usize + 8]
            .copy_from_slice(&tuple_len.to_le_bytes());

        lower += ITEM_ID_SIZE;
        upper = start;
        current_page.data[0..4].copy_from_slice(&lower.to_le_bytes());
        current_page.data[4..8].copy_from_slice(&upper.to_le_bytes());
    }

    pages.push(current_page);

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

    let total_page_count = (pages.len() + 1) as u32;
    file.set_len(total_page_count as u64 * PAGE_SIZE as u64)?;

    let header = OrderedFileHeader {
        page_count: total_page_count,
        file_type: FileType::Ordered,
        sort_key_count: sort_key_entries.len() as u32,
        sort_keys: sort_key_entries,
    };
    write_ordered_file_header(file, &header)?;

    for (i, page) in pages.iter().enumerate() {
        let offset = (i as u32 + 1) as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page.data)?;
    }

    file.flush()?;
    Ok(())
}

pub fn merge_delta_into_base(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<()> {
    let (columns, sort_keys) = {
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

        let sort_keys = table
            .sort_keys
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Table has no sort keys"))?;

        (table.columns.clone(), sort_keys.clone())
    };

    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)?;

    let mut tuples = read_base_tuples(&mut file)?;
    let mut delta_tuples = scan_all_delta_tuples(db_name, table_name)?;

    if delta_tuples.is_empty() {
        return Ok(());
    }

    tuples.append(&mut delta_tuples);
    let comparator = TupleComparator::new(columns, sort_keys.clone());
    tuples.sort_by(|a, b| comparator.compare(a, b));

    rewrite_ordered_file(&mut file, &tuples, &sort_keys)?;
    truncate_delta(db_name, table_name)?;

    if let Some(db) = catalog.databases.get_mut(db_name) {
        if let Some(table) = db.tables.get_mut(table_name) {
            table.delta_current_tuples = Some(0);
        }
    }
    save_catalog(catalog);

    Ok(())
}

pub fn merge_if_needed(catalog: &mut Catalog, db_name: &str, table_name: &str) -> io::Result<bool> {
    let (enabled, current, threshold) = {
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

        (
            table.delta_enabled.unwrap_or(false),
            table.delta_current_tuples.unwrap_or(0),
            table.delta_merge_threshold_tuples.unwrap_or(500),
        )
    };

    if !enabled || current < threshold {
        return Ok(false);
    }

    merge_delta_into_base(catalog, db_name, table_name)?;
    Ok(true)
}
