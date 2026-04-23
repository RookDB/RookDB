use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::index::{AnyIndex, IndexKey, index_file_path, secondary_index_file_path};
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE};

/// Fetch tuples from a table using a secondary index.
pub fn index_scan(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    key: &IndexKey,
) -> io::Result<Vec<Vec<u8>>> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    let entry = table
        .indexes
        .iter()
        .find(|idx| idx.index_name == index_name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "index '{}' not found on table '{}.{}'",
                    index_name, db_name, table_name
                ),
            )
        })?;

    let index_path = if entry.is_secondary() {
        secondary_index_file_path(db_name, table_name, index_name)
    } else {
        index_file_path(db_name, table_name, index_name)
    };
    let mut record_ids = AnyIndex::search_on_disk(&index_path, &entry.algorithm, key)?;

    // Clustered indexes should be read in physical RID order for page locality.
    if entry.is_clustered {
        record_ids.sort_by_key(|rid| (rid.page_no, rid.item_id));
    }

    if record_ids.is_empty() {
        return Ok(Vec::new());
    }

    let table_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);
    let mut file = OpenOptions::new().read(true).open(&table_path)?;

    let mut tuples = Vec::new();
    let mut page_cache: HashMap<u32, Page> = HashMap::new();

    for rid in record_ids {
        let page = if let Some(page) = page_cache.get(&rid.page_no) {
            page
        } else {
            let mut page = Page::new();
            read_page(&mut file, &mut page, rid.page_no)?;
            page_cache.insert(rid.page_no, page);
            page_cache.get(&rid.page_no).unwrap()
        };

        if let Some(tuple) = read_tuple_from_page(page, rid.item_id)? {
            tuples.push(tuple);
        }
    }

    Ok(tuples)
}

/// Fetch tuples by probing a secondary index for the given column.
///
/// If multiple indexes exist on the same column, a clustered index is preferred.
pub fn index_scan_by_column(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    column_name: &str,
    key: &IndexKey,
) -> io::Result<Vec<Vec<u8>>> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    let entry = table
        .indexes
        .iter()
        .filter(|idx| idx.column_name.len() == 1 && idx.column_name[0] == column_name)
        .max_by_key(|idx| idx.is_clustered)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no index found on column '{}' for table '{}.{}'",
                    column_name, db_name, table_name
                ),
            )
        })?;

    index_scan(catalog, db_name, table_name, &entry.index_name, key)
}

fn read_tuple_from_page(page: &Page, item_id: u32) -> io::Result<Option<Vec<u8>>> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

    if item_id >= num_items {
        return Ok(None);
    }

    let slot_base = (PAGE_HEADER_SIZE + item_id * ITEM_ID_SIZE) as usize;
    let tuple_offset =
        u32::from_le_bytes(page.data[slot_base..slot_base + 4].try_into().unwrap()) as usize;
    let tuple_len =
        u32::from_le_bytes(page.data[slot_base + 4..slot_base + 8].try_into().unwrap()) as usize;

    if tuple_len == 0 {
        return Ok(None);
    }

    let tuple_end = tuple_offset.checked_add(tuple_len).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "tuple length overflow")
    })?;

    if tuple_end > page.data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tuple bounds out of page",
        ));
    }

    Ok(Some(page.data[tuple_offset..tuple_end].to_vec()))
}
