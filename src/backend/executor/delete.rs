//! DELETE execution helpers.
//!
//! Row *selection* lives in `row_select` (Volcano engine); this module
//! provides the pointer-based mutation (`delete_by_pointers`) plus page
//! compaction. Rows are soft-deleted via `SLOT_FLAG_DELETED`; space is
//! reclaimed by VACUUM.

use std::io;

use crate::backend::executor::compaction_api::rebuild_table_fsm;
use crate::backend::executor::create_index::update_index_on_delete;
use crate::backend::log::operation_log::{current_timestamp_iso, log_compaction, log_delete};
use crate::backend::page::page_lock::PageWriteLock;
use crate::backend::visibility_map::{vm_clear_page, vm_is_visible, vm_set_page};
use crate::catalog::types::Catalog;
use crate::disk::{read_page, write_page};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, SLOT_FLAG_DELETED};
use crate::table::{increment_dead_tuple_count, page_count, write_dead_tuple_count};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A column value that can appear on the right-hand side of a condition.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int(i32),
    Text(String),
    /// Used by IN / NOT IN: list of literal values.
    List(Vec<ColumnValue>),
}

/// Result returned by the DELETE entry points.
pub struct DeleteResult {
    /// How many rows were deleted.
    pub deleted_count: usize,
    /// The deleted rows (only populated when `returning = true`).
    pub returning_rows: Vec<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

use crate::types::datatype::DataType;
use crate::types::row::deserialize_nullable_row;
use crate::types::value::DataValue;

fn decode_tuple(
    tuple_data: &[u8],
    columns: &[crate::catalog::types::Column],
) -> Vec<(String, ColumnValue)> {
    let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let decoded = match deserialize_nullable_row(&schema, tuple_data) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let mut result = Vec::new();

    for (col, value) in columns.iter().zip(decoded.iter()) {
        let converted = match value {
            Some(DataValue::Int(v)) => ColumnValue::Int(*v),

            Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => ColumnValue::Text(s.clone()),

            Some(other) => ColumnValue::Text(format!("{}", other)),

            None => ColumnValue::Text("NULL".to_string()),
        };

        result.push((col.name.clone(), converted));
    }

    result
}

fn delete_log_details(deleted_count: Option<usize>, error: Option<&str>) -> Value {
    json!({
        "timestamp": current_timestamp_iso(),
        "deleted_count": deleted_count,
        "error": error,
    })
}

fn compaction_log_details(pages_compacted: Option<usize>, error: Option<&str>) -> Value {
    json!({
        "timestamp": current_timestamp_iso(),
        "pages_compacted": pages_compacted,
        "error": error,
    })
}

/// Physically compact a page: remove all slots whose DELETED flag is set.
/// Called only by `compaction_table()`.
///
/// Slot layout: [offset: u32][length: u16][flags: u16]
fn compact_page(page: &mut Page, num_items: usize) {
    // Collect surviving tuple data (live slots only)
    let mut surviving: Vec<Vec<u8>> = Vec::new();

    for i in 0..num_items {
        let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
        let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap());
        let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

        // Skip empty or soft-deleted slots
        if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
            continue;
        }

        surviving.push(page.data[offset as usize..(offset as usize + length as usize)].to_vec());
    }

    // Zero out the entire page
    page.data.iter_mut().for_each(|b| *b = 0);

    // Re-initialise header pointers
    let mut lower = PAGE_HEADER_SIZE;
    let mut upper = PAGE_SIZE as u32;

    // Re-insert surviving tuples with fresh (unflagged) slots
    for tuple in &surviving {
        let start = upper - tuple.len() as u32;

        // Write tuple data
        page.data[start as usize..upper as usize].copy_from_slice(tuple);
        upper = start;

        // Write slot entry: [offset: u32][length: u16][flags: u16 = 0]
        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 6]
            .copy_from_slice(&(tuple.len() as u16).to_le_bytes());
        page.data[lower as usize + 6..lower as usize + 8].copy_from_slice(&0u16.to_le_bytes()); // flags = 0 (live)

        lower += ITEM_ID_SIZE;
    }

    // Persist updated lower and upper pointers
    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// Physically remove all soft-deleted slots from every data page in a table.
///
/// Call this periodically (e.g. via menu option) rather than on every DELETE.
/// Returns the number of pages that were actually rewritten.
pub fn compaction_table(db_name: &str, table_name: &str) -> io::Result<usize> {
    use std::fs::OpenOptions;

    let result = (|| -> io::Result<usize> {
        let path = format!("database/base/{}/{}.dat", db_name, table_name);
        // Compaction rewrites pages via direct I/O — flush cached state first.
        crate::backend::cache::quiesce_for_direct_io(std::path::Path::new(&path))?;
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_identity = crate::table::file_identity_from_file(&file)?;

        let total_pages = page_count(&mut file)?;
        let mut pages_compacted = 0usize;

        for page_num in 1..total_pages {
            // ── Visibility-map shortcut ───────────────────────────────────────
            // vm_clear_page is called by DELETE/UPDATE before modifying any
            // slot on this page, so a set VM bit means zero dead tuples →
            // nothing to compact here.  This is the whole point of tracking
            // the VM: we skip reading, locking, and scanning clean pages.
            if vm_is_visible(db_name, table_name, page_num) {
                continue;
            }

            // Acquire exclusive write lock on this page before reading and potentially compacting
            let _page_lock = PageWriteLock::acquire(file_identity, page_num);

            let mut page = Page::new();
            read_page(&mut file, &mut page, page_num)?;

            let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

            let has_deleted = (0..num_items).any(|i| {
                let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                flags & SLOT_FLAG_DELETED != 0
            });

            if has_deleted {
                compact_page(&mut page, num_items);

                write_page(&mut file, &mut page, page_num)?;
                pages_compacted += 1;
            }

            // Page is now clean (either had no dead tuples, or just compacted).
            // Mark all-visible so the next compaction can skip it entirely.
            let _ = vm_set_page(db_name, table_name, page_num);
            // Lock is automatically released here when _page_lock is dropped
        }

        // Rebuild the FSM and reset dead-tuple counter after a successful compaction.
        // rebuild_table_fsm scans actual on-disk free space and writes a fresh .fsm fork,
        // which is more accurate than per-page fsm_set_avail calls and removes the need
        // for a global fsm_manager singleton.
        if pages_compacted > 0 {
            rebuild_table_fsm(db_name, table_name)?;
            write_dead_tuple_count(&mut file, 0)?;
        }

        Ok(pages_compacted)
    })();

    match &result {
        Ok(pages_compacted) => {
            let details = compaction_log_details(Some(*pages_compacted), None);
            let _ = log_compaction(db_name, table_name, details, "success");
        }
        Err(err) => {
            let details = compaction_log_details(None, Some(&err.to_string()));
            let _ = log_compaction(db_name, table_name, details, "failed");
        }
    }

    result
}

//
// The user types the full WHERE expression as a single string, e.g.:
//   "price > 10"
//   "dept = HR AND salary < 50000"
//   "(dept = HR AND salary < 50000) OR (dept = Sales AND salary < 30000)"
//   "(c1 = 1 AND c2 = 2) AND (c3 = 3 OR c4 = 4)"
//
// Grammar (AND binds tighter than OR, same as SQL):
/// A heap page kept locked in memory while consecutive rows on it are deleted.
struct HeldPage {
    page_num: u32,
    page: Page,
    dirty: bool,
    _lock: PageWriteLock,
}

/// Write the held page back if any slot on it was deleted, then release it.
fn flush_held_page(
    file: &mut std::fs::File,
    held: &mut Option<HeldPage>,
    db_name: &str,
    table_name: &str,
) -> io::Result<()> {
    if let Some(mut h) = held.take()
        && h.dirty
    {
        write_page(file, &mut h.page, h.page_num)?;
        let _ = vm_clear_page(db_name, table_name, h.page_num);
    }
    Ok(())
}

/// Delete rows identified by explicit heap pointers (page_id, slot_id).
///
/// This is the Volcano-aware DELETE path: instead of scanning the heap and
/// evaluating WHERE conditions here, the caller (Volcano engine) has already
/// identified the matching rows. This function deletes the rows at the given
/// pointers using standard soft-delete semantics (`SLOT_FLAG_DELETED`).
pub fn delete_by_pointers(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    pointers: &[(u32, u32)],
) -> io::Result<DeleteResult> {
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
    let columns = &table.columns;

    let path = format!("database/base/{}/{}.dat", db_name, table_name);
    // DELETE rewrites pages via direct I/O — flush cached state first.
    crate::backend::cache::quiesce_for_direct_io(std::path::Path::new(&path))
        .map_err(|e| io::Error::other(format!("Failed to flush cache: {}", e)))?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| io::Error::other(format!("Failed to open table file: {}", e)))?;
    let file_identity = crate::table::file_identity_from_file(&file)?;

    let mut deleted_count = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();

    // Consecutive pointers usually share a heap page. When no table
    // references this one, deleting a row cannot cascade back into this
    // file, so the page is held across those rows and written once instead
    // of being read and rewritten for every row.
    let hold_pages = crate::backend::cache::referencing_fks(db_name, table_name).is_empty();
    let mut held: Option<HeldPage> = None;

    // A row still referenced through a RESTRICT foreign key fails the whole
    // statement: check every target row before deleting the first one.
    if !hold_pages {
        let mut page = Page::new();
        let mut loaded: Option<u32> = None;
        for &(page_num, slot_idx) in pointers {
            if loaded != Some(page_num) {
                read_page(&mut file, &mut page, page_num)?;
                loaded = Some(page_num);
            }
            let base = (PAGE_HEADER_SIZE + slot_idx * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length =
                u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
            let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
            if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                continue;
            }
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];
            let decoded = decode_tuple(tuple_data, columns);
            crate::backend::constraint::check_row_delete_restrictions(
                db_name, table_name, &decoded,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        }
    }

    for &(page_num, slot_idx) in pointers {
        let slot_index = slot_idx as u16;

        if !hold_pages || held.as_ref().map(|h| h.page_num) != Some(page_num) {
            flush_held_page(&mut file, &mut held, db_name, table_name)?;

            // Acquire exclusive write lock on this page
            let lock = PageWriteLock::acquire(file_identity, page_num);
            let mut page = Page::new();
            read_page(&mut file, &mut page, page_num)?;
            held = Some(HeldPage {
                page_num,
                page,
                dirty: false,
                _lock: lock,
            });
        }
        let held_page = held.as_mut().expect("page loaded above");
        let page = &mut held_page.page;

        let base = (PAGE_HEADER_SIZE + slot_index as u32 * ITEM_ID_SIZE) as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
        let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
        let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

        if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
            continue;
        }

        let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();
        let decoded = decode_tuple(&tuple_data, columns);

        // FOREIGN KEY constraint check
        if let Err(e) =
            crate::backend::constraint::validate_row_delete(catalog, db_name, table_name, &decoded)
        {
            log::warn!(
                "[DeleteByPointers] Skipping row due to FOREIGN KEY constraint: {}",
                e
            );
            continue;
        }

        // Re-read the page in case validate_row_delete triggered CASCADE
        if !hold_pages && let Err(e) = read_page(&mut file, page, page_num) {
            log::error!("Failed to re-read page after validation: {}", e);
        }

        // Update any existing B+ Tree index
        if let Err(e) = update_index_on_delete(
            db_name,
            table_name,
            columns,
            &tuple_data,
            page_num,
            slot_idx,
        ) {
            log::warn!("Failed to update index for deleted tuple: {}", e);
        }

        // Soft-delete the slot
        let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
        let new_flags = flags | SLOT_FLAG_DELETED;
        page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
        held_page.dirty = true;

        log::info!(
            "[Delete] Soft-deleted slot (page={}, slot={}), updated index and cleared VM",
            page_num,
            slot_idx
        );

        returning_rows.push(
            decoded
                .iter()
                .map(|(col, val)| {
                    let s = match val {
                        ColumnValue::Int(n) => n.to_string(),
                        ColumnValue::Text(t) => t.clone(),
                        ColumnValue::List(_) => String::from("[list]"),
                    };
                    (col.clone(), s)
                })
                .collect(),
        );

        deleted_count += 1;
    }

    flush_held_page(&mut file, &mut held, db_name, table_name)?;

    if deleted_count > 0 {
        increment_dead_tuple_count(&mut file, deleted_count as u32)?;
    }

    let result = DeleteResult {
        deleted_count,
        returning_rows,
    };
    let details = delete_log_details(Some(result.deleted_count), None);
    let _ = log_delete(db_name, table_name, details, "success");

    Ok(result)
}
