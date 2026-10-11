//! DELETE execution helpers.
//!
//! Row *selection* lives in `row_select` (Volcano engine); this module
//! provides the pointer-based mutation (`delete_by_pointers`) plus page
//! compaction. Rows are soft-deleted via `SLOT_FLAG_DELETED`; space is
//! reclaimed by VACUUM.

use std::io;

use crate::backend::error::{RookError, RookResult};

use crate::backend::executor::compaction_api::rebuild_table_fsm;
use crate::backend::executor::create_index::update_index_on_delete;
use crate::backend::heap::HeapFile;
use crate::backend::log::operation_log::{current_timestamp_iso, log_compaction, log_delete};
use crate::backend::page::page_lock::PageWriteLock;
use crate::backend::visibility_map::{vm_clear_page, vm_is_visible, vm_set_page};
use crate::catalog::types::Catalog;
use crate::page::Page;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

/// Decode a stored row of the table with `columns`.
pub(crate) fn decode_row(
    tuple_data: &[u8],
    columns: &[crate::catalog::types::Column],
) -> io::Result<Vec<Option<DataValue>>> {
    let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
    deserialize_nullable_row(&schema, tuple_data).map_err(io::Error::other)
}

/// A row as `(column, text)` pairs for RETURNING output: NULL as `NULL`,
/// strings without quotes.
pub(crate) fn display_row(
    columns: &[crate::catalog::types::Column],
    values: &[Option<DataValue>],
) -> Vec<(String, String)> {
    columns
        .iter()
        .zip(values)
        .map(|(col, value)| {
            let text = match value {
                None => "NULL".to_string(),
                Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => s.clone(),
                Some(other) => other.to_string(),
            };
            (col.name.clone(), text)
        })
        .collect()
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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// Physically remove dead slots and the bytes of replaced rows from every
/// data page in a table.
///
/// Call this periodically (e.g. via menu option) rather than on every DELETE.
/// Returns the number of pages that were actually rewritten.
pub fn compaction_table(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> io::Result<usize> {
    let result = (|| -> io::Result<usize> {
        let path = engine.paths().heap(db_name, table_name);
        let file = HeapFile::open(engine, std::path::Path::new(&path))?;
        let file_identity = file.identity();

        let total_pages = file.page_count();
        let mut pages_compacted = 0usize;

        for page_num in 1..total_pages {
            // ── Visibility-map shortcut ───────────────────────────────────────
            // vm_clear_page is called by DELETE/UPDATE before modifying any
            // slot on this page, so a set VM bit means zero dead tuples →
            // nothing to compact here.  This is the whole point of tracking
            // the VM: we skip reading, locking, and scanning clean pages.
            if vm_is_visible(engine, db_name, table_name, page_num) {
                continue;
            }

            // Acquire exclusive write lock on this page before reading and potentially compacting
            let _page_lock = PageWriteLock::acquire(file_identity, page_num);

            let mut page = Page::new();
            file.read_page(&mut page, page_num)?;

            if page.has_garbage() {
                page.compact();

                file.write_page(&page, page_num)?;
                pages_compacted += 1;
            }

            // Page is now clean (either had no dead tuples, or just compacted).
            // Mark all-visible so the next compaction can skip it entirely.
            let _ = vm_set_page(engine, db_name, table_name, page_num);
            // Lock is automatically released here when _page_lock is dropped
        }

        // Rebuild the FSM and reset dead-tuple counter after a successful compaction.
        // rebuild_table_fsm scans actual on-disk free space and writes a fresh .fsm fork,
        // which is more accurate than per-page fsm_set_avail calls and removes the need
        // for a global fsm_manager singleton.
        if pages_compacted > 0 {
            file.reset_dead_tuples()?;
            drop(file);
            // The rebuild reads the heap file and replaces the FSM fork, so
            // the compacted pages go to the file and the cached manager,
            // which holds the old fork, is dropped first.
            crate::backend::cache::evict_heap(engine, std::path::Path::new(&path))?;
            rebuild_table_fsm(engine, db_name, table_name)?;
        }

        Ok(pages_compacted)
    })();

    match &result {
        Ok(pages_compacted) => {
            let details = compaction_log_details(Some(*pages_compacted), None);
            let _ = log_compaction(engine, db_name, table_name, details, "success");
        }
        Err(err) => {
            let details = compaction_log_details(None, Some(&err.to_string()));
            let _ = log_compaction(engine, db_name, table_name, details, "failed");
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
    engine: &crate::backend::engine::Engine,
    file: &HeapFile,
    held: &mut Option<HeldPage>,
    db_name: &str,
    table_name: &str,
) -> io::Result<()> {
    if let Some(h) = held.take()
        && h.dirty
    {
        file.write_page(&h.page, h.page_num)?;
        let _ = vm_clear_page(engine, db_name, table_name, h.page_num);
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
) -> RookResult<DeleteResult> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db_name.to_string(),
        })?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Table",
            name: table_name.to_string(),
        })?;
    let columns = &table.columns;

    let path = catalog.engine().paths().heap(db_name, table_name);
    let file = HeapFile::open(catalog.engine(), std::path::Path::new(&path))
        .map_err(|e| RookError::Io(e).with_context("Failed to open table file"))?;
    let file_identity = file.identity();

    let mut deleted_count = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();

    // Consecutive pointers usually share a heap page. When no table
    // references this one, deleting a row cannot cascade back into this
    // file, so the page is held across those rows and written once instead
    // of being read and rewritten for every row.
    let hold_pages =
        crate::backend::cache::referencing_fks(catalog.engine(), db_name, table_name).is_empty();
    let mut held: Option<HeldPage> = None;

    // A row still referenced through a RESTRICT foreign key fails the whole
    // statement: check every target row before deleting the first one.
    if !hold_pages {
        let mut page = Page::new();
        let mut loaded: Option<u32> = None;
        for &(page_num, slot_idx) in pointers {
            if loaded != Some(page_num) {
                file.read_page(&mut page, page_num)?;
                loaded = Some(page_num);
            }
            let Some(tuple_data) = page.tuple(slot_idx) else {
                continue;
            };
            let decoded = decode_row(tuple_data, columns)?;
            crate::backend::constraint::check_row_delete_restrictions(
                catalog, db_name, table_name, &decoded,
            )?;
        }
    }

    // The first index failure; the indexes are rebuilt once the rows are
    // written, rather than maintained row by row after that.
    let mut index_error: Option<String> = None;
    for &(page_num, slot_idx) in pointers {
        if !hold_pages || held.as_ref().map(|h| h.page_num) != Some(page_num) {
            flush_held_page(catalog.engine(), &file, &mut held, db_name, table_name)?;

            // Acquire exclusive write lock on this page
            let lock = PageWriteLock::acquire(file_identity, page_num);
            let mut page = Page::new();
            file.read_page(&mut page, page_num)?;
            held = Some(HeldPage {
                page_num,
                page,
                dirty: false,
                _lock: lock,
            });
        }
        let held_page = held.as_mut().expect("page loaded above");
        let page = &mut held_page.page;

        let Some(tuple_data) = page.tuple(slot_idx).map(<[u8]>::to_vec) else {
            continue;
        };
        let decoded = decode_row(&tuple_data, columns)?;

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
        if !hold_pages && let Err(e) = file.read_page(page, page_num) {
            log::error!("Failed to re-read page after validation: {}", e);
        }

        // Update any existing B+ Tree index
        if index_error.is_none()
            && let Err(e) = update_index_on_delete(
                catalog.engine(),
                db_name,
                table_name,
                columns,
                &tuple_data,
                page_num,
                slot_idx,
            )
        {
            index_error = Some(e.to_string());
        }

        page.mark_deleted(slot_idx);
        held_page.dirty = true;

        log::info!(
            "[Delete] Soft-deleted slot (page={}, slot={}), updated index and cleared VM",
            page_num,
            slot_idx
        );

        returning_rows.push(display_row(columns, &decoded));

        deleted_count += 1;
    }

    flush_held_page(catalog.engine(), &file, &mut held, db_name, table_name)?;

    if let Some(cause) = index_error {
        crate::backend::executor::create_index::repair_indexes(
            catalog,
            db_name,
            table_name,
            &format!("Index maintenance failed for a deleted row: {}", cause),
        )?;
    }

    if deleted_count > 0 {
        file.record_dead_tuples(deleted_count as u32)?;
    }

    let result = DeleteResult {
        deleted_count,
        returning_rows,
    };
    let details = delete_log_details(Some(result.deleted_count), None);
    let _ = log_delete(catalog.engine(), db_name, table_name, details, "success");

    Ok(result)
}
