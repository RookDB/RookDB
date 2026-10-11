//! Implements UPDATE with SET assignments and optional WHERE conditions.
//!
//! Supports:
//!   UPDATE table SET col = val;
//!   UPDATE table SET col = val WHERE other = x;
//!   UPDATE table SET col1 = val1, col2 = val2 WHERE id > 5;
//!   UPDATE table SET col = val WHERE (a = 1 AND b = 2) OR c = 3;
//!
//! ("Print updated rows?" is an interactive frontend prompt, not SQL RETURNING.)
//!
//! UPDATE is implemented using delete + insert semantics for latest-version wins.
//! Rows with the SLOT_FLAG_DELETED bit set are invisible and are never updated.

use std::io;

use crate::backend::log::operation_log::current_timestamp_iso;
use crate::backend::page::page_lock::PageWriteLock;
use crate::catalog::types::Catalog;
use crate::page::Page;
use crate::types::datatype::DataType;
use crate::types::row::deserialize_nullable_row;
use crate::types::row::serialize_nullable_typed_row;
use crate::types::value::DataValue;

use super::physical::expr::{Expr, expr_from_ast};
use super::physical::tuple::{ColumnInfo, Tuple};
use serde_json::{Value, json};

/// (page_num, slot_index) pair that uniquely identifies a stored tuple.
#[derive(Debug, Clone, Copy)]
struct TuplePointer {
    page_id: u32,
    slot_index: u16,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// One `col = <expr>` assignment from the SET clause.
pub use rook_ast::SetAssignment;

/// Result returned by the UPDATE entry points.
pub struct UpdateResult {
    /// How many rows were modified.
    pub updated_count: usize,
    /// The rows **after** update (only populated when `returning = true`).
    pub returning_rows: Vec<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Evaluate every SET expression against the row as it was before the
/// statement, and store each result in its column converted to the column's
/// type (see [`crate::types::functions::assign_cast`]).
fn apply_assignments(
    columns: &[crate::catalog::types::Column],
    schema: &[ColumnInfo],
    compiled: &[(usize, Expr)],
    old: &[Option<DataValue>],
) -> Result<Vec<Option<DataValue>>, String> {
    let row = Tuple::new(old.to_vec());
    let mut new_values = old.to_vec();
    for (idx, expr) in compiled {
        let column = &columns[*idx];
        new_values[*idx] = match expr.evaluate(&row, schema)? {
            None => None,
            Some(v) => Some(
                crate::types::functions::assign_cast(&v, &column.data_type)
                    .map_err(|e| format!("column '{}': {}", column.name, e))?,
            ),
        };
    }
    Ok(new_values)
}

struct PendingUpdate {
    pointer: TuplePointer,
    /// The raw bytes of the OLD tuple (before update), for index key extraction.
    old_tuple_data: Vec<u8>,
    /// The raw bytes of the NEW tuple (after update).
    new_bytes: Vec<u8>,
    /// The row BEFORE the update, for FK cascade propagation.
    old_values: Vec<Option<DataValue>>,
    /// The updated typed values, used for intra-batch constraint validation.
    updated_values: Vec<Option<DataValue>>,
}

fn update_log_details(updated_count: Option<usize>, error: Option<&str>) -> Value {
    json!({
        "timestamp": current_timestamp_iso(),
        "updated_count": updated_count,
        "error": error,
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Update rows identified by explicit heap pointers (page_id, slot_id).
///
/// This is the Volcano-aware UPDATE path: instead of scanning the heap and
/// evaluating WHERE conditions here, the caller (Volcano engine) has already
/// identified the matching rows. This function applies the SET assignments to
/// the rows at the given pointers (soft-delete + rewrite).
pub fn update_by_pointers(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    pointers: &[(u32, u32)],
    assignments: &[SetAssignment],
) -> io::Result<UpdateResult> {
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

    // Bind each SET expression to the table's columns once for the statement.
    let row_schema: Vec<ColumnInfo> = columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            table: Some(table_name.to_string()),
        })
        .collect();
    let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut compiled: Vec<(usize, Expr)> = Vec::with_capacity(assignments.len());
    for asgn in assignments {
        let idx = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&asgn.column))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Assignment column '{}' not found in table '{}'",
                        asgn.column, table_name
                    ),
                )
            })?;
        let expr = expr_from_ast(&asgn.value, &names).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("SET {}: {}", asgn.column, e),
            )
        })?;
        compiled.push((idx, expr));
    }

    let path = crate::backend::paths::heap(db_name, table_name);
    let file = crate::backend::heap::HeapFile::open(std::path::Path::new(&path))
        .map_err(|e| io::Error::other(format!("Failed to open table file: {}", e)))?;
    let file_identity = file.identity();

    let mut updated_count = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();
    let mut pending_updates: Vec<PendingUpdate> = Vec::new();

    // Consecutive pointers usually share a heap page; nothing in this pass
    // writes to the heap, so a page is read once for all of its rows.
    let mut page = Page::new();
    let mut loaded_page: Option<u32> = None;

    for &(page_num, slot_idx) in pointers {
        let slot_index = slot_idx as u16;

        if loaded_page != Some(page_num) {
            file.read_page(&mut page, page_num)?;
            loaded_page = Some(page_num);
        }

        let Some(tuple_data) = page.tuple(slot_idx).map(<[u8]>::to_vec) else {
            continue;
        };
        let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
        let decoded = match deserialize_nullable_row(&schema, &tuple_data) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let updated_values = match apply_assignments(columns, &row_schema, &compiled, &decoded) {
            Ok(v) => v,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("assignment error: {}", e),
                ));
            }
        };
        let new_bytes = match serialize_nullable_typed_row(&schema, &updated_values) {
            Ok(b) => b,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("serialization error: {}", e),
                ));
            }
        };

        // Constraint validation against disk, excluding all pointers in this batch
        if let Err(e) = crate::backend::constraint::validate_row_update_with_excludes(
            catalog,
            db_name,
            table_name,
            &updated_values,
            pointers,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("constraint violation: {}", e),
            ));
        }

        if let Err(e) = crate::backend::constraint::validate_update_restrict(
            db_name,
            table_name,
            columns,
            &decoded,
            &updated_values,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("FOREIGN KEY RESTRICT constraint: {}", e),
            ));
        }

        log::info!(
            "[Update] Validated row at (page={}, slot={}): new values={:?}",
            page_num,
            slot_idx,
            updated_values
        );

        pending_updates.push(PendingUpdate {
            pointer: TuplePointer {
                page_id: page_num,
                slot_index,
            },
            old_tuple_data: tuple_data,
            new_bytes,
            old_values: decoded,
            updated_values,
        });

        updated_count += 1;
    }

    // Intra-batch uniqueness validation: ensure no two updated rows in the same
    // statement produce duplicate keys for any UNIQUE constraint or index.
    let meta = crate::backend::cache::metadata(db_name, table_name);
    let mut has_intra_batch_conflict = false;

    // Check column-level UNIQUE constraints
    for (col_idx, col) in columns.iter().enumerate() {
        if !col.constraints.unique {
            continue;
        }
        let mut seen = std::collections::HashSet::new();
        for pu in &pending_updates {
            if let Some(Some(dv)) = pu.updated_values.get(col_idx) {
                let val_str = dv.to_string();
                if !seen.insert(val_str) {
                    has_intra_batch_conflict = true;
                    log::warn!(
                        "[UpdateByPointers] Batch UNIQUE violation on column '{}': duplicate value '{}'",
                        col.name,
                        dv
                    );
                    break;
                }
            }
        }
        if has_intra_batch_conflict {
            break;
        }
    }

    // Check unique indexes (including composite)
    if !has_intra_batch_conflict && let Some(ref m) = meta {
        for (idx_name, idx_cols) in &m.unique_indexes {
            let col_indices: Vec<usize> = idx_cols
                .iter()
                .filter_map(|c| {
                    columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if col_indices.len() != idx_cols.len() {
                continue;
            }

            let mut seen = std::collections::HashSet::new();
            for pu in &pending_updates {
                let mut has_null = false;
                let mut key_parts = Vec::with_capacity(col_indices.len());
                for &idx in &col_indices {
                    match pu.updated_values.get(idx) {
                        Some(Some(dv)) => key_parts.push(dv.to_string()),
                        _ => {
                            has_null = true;
                            break;
                        }
                    }
                }
                if !has_null {
                    let composite_key = key_parts.join("|||");
                    if !seen.insert(composite_key) {
                        has_intra_batch_conflict = true;
                        log::warn!(
                            "[UpdateByPointers] Batch UNIQUE violation on index '{}': duplicate key ({})",
                            idx_name,
                            key_parts.join(", ")
                        );
                        break;
                    }
                }
            }
            if has_intra_batch_conflict {
                break;
            }
        }
    }

    if has_intra_batch_conflict {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "UNIQUE constraint violated: the update gives two rows the same key".to_string(),
        ));
    }

    // An index rejects keys above its size limit. Check before Phase 2: once
    // rows are rewritten, a failed index update would leave them invisible
    // to index scans.
    if let Some(ref m) = meta {
        for (idx_name, idx_cols, _) in &m.named_indexes {
            let col_indices: Vec<usize> = idx_cols
                .iter()
                .filter_map(|c| {
                    columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if col_indices.len() != idx_cols.len() {
                continue;
            }
            for pu in &pending_updates {
                let key: Option<Vec<DataValue>> = col_indices
                    .iter()
                    .map(|&i| pu.updated_values.get(i).cloned().flatten())
                    .collect();
                // A NULL in the key is not indexed.
                let Some(key) = key else { continue };
                let len = crate::backend::index::btree::encoded_key_len(&key)?;
                if len > crate::backend::index::btree::MAX_ENCODED_KEY_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "index key of {} bytes for index '{}' exceeds the maximum of {} bytes",
                            len,
                            idx_name,
                            crate::backend::index::btree::MAX_ENCODED_KEY_SIZE
                        ),
                    ));
                }
            }
        }
    }

    // Phase 2: apply every pending update.
    //
    // A new row image of the same length is written over the old one: the row
    // keeps its (page, slot), no dead tuple is left behind, and indexes whose
    // key did not change need no maintenance. Any other row is replaced by
    // delete+insert as before.
    let mut in_place = vec![false; pending_updates.len()];
    if !pending_updates.is_empty() {
        let mut moved = 0u32;
        // Each run of updates on one page is applied to a single page image:
        // one read, one write and one lock per run rather than per row.
        let mut start = 0;
        while start < pending_updates.len() {
            let page_id = pending_updates[start].pointer.page_id;
            let mut end = start + 1;
            while end < pending_updates.len() && pending_updates[end].pointer.page_id == page_id {
                end += 1;
            }

            let _page_lock = PageWriteLock::acquire(file_identity, page_id);
            let mut page = Page::new();
            file.read_page(&mut page, page_id)?;
            let mut moved_on_page = false;

            for (i, update) in pending_updates.iter().enumerate().take(end).skip(start) {
                let slot = update.pointer.slot_index as u32;
                if update.new_bytes.len() == update.old_tuple_data.len()
                    && page
                        .tuple(slot)
                        .is_some_and(|t| t.len() == update.new_bytes.len())
                    && page.overwrite(slot, &update.new_bytes)
                {
                    in_place[i] = true;
                    continue;
                }
                page.mark_deleted(slot);
                moved_on_page = true;
                moved += 1;
            }

            file.write_page(&page, page_id)?;
            if moved_on_page {
                let _ = crate::backend::visibility_map::vm_clear_page(db_name, table_name, page_id);
            }
            start = end;
        }
        if moved > 0 {
            file.record_dead_tuples(moved)?;
        }
    }

    if !pending_updates.is_empty() {
        for (i, update) in pending_updates.iter().enumerate() {
            let (new_page_id, new_slot_id) = if in_place[i] {
                (update.pointer.page_id, update.pointer.slot_index as u32)
            } else {
                crate::backend::executor::compaction_api::insert_raw_tuple(
                    db_name,
                    table_name,
                    &update.new_bytes,
                )?
            };

            log::info!(
                "[Update] Row at (page={}, slot={}) now at (page={}, slot={})",
                update.pointer.page_id,
                update.pointer.slot_index,
                new_page_id,
                new_slot_id
            );

            if let Err(e) = crate::backend::executor::create_index::update_index_on_update(
                db_name,
                table_name,
                columns,
                &update.old_tuple_data,
                &update.new_bytes,
                update.pointer.page_id,
                update.pointer.slot_index as u32,
                new_page_id,
                new_slot_id,
            ) {
                log::warn!("Failed to update index for updated tuple: {}", e);
            }

            returning_rows.push(super::delete::display_row(columns, &update.updated_values));
        }
    }

    // FK cascade propagation
    if !pending_updates.is_empty() {
        for update in &pending_updates {
            if let Err(e) = crate::backend::constraint::propagate_update_to_children(
                catalog,
                db_name,
                table_name,
                columns,
                &update.old_values,
                &update.updated_values,
            ) {
                log::warn!("[UpdateByPointers] Failed to propagate FK cascade: {}", e);
            }
        }
    }

    let result = UpdateResult {
        updated_count,
        returning_rows,
    };
    let details = update_log_details(Some(result.updated_count), None);
    let _ = crate::backend::log::operation_log::log_update(db_name, table_name, details, "success");

    Ok(result)
}
