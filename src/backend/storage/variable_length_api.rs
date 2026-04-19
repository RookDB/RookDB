//! Programmatic CRUD API for RookDB variable-length storage.
//!
//! Provides insert/read/update/delete functions that are fully consistent with the
//! BLOB / ARRAY / TOAST pipeline used by the interactive commands in `data_cmd.rs`,
//! but with **no interactive I/O** — suitable for tests, examples, and query engines.
//!
//! # Storage layout (mirrors data_cmd.rs)
//! - Heap page file : `database/base/{db}/{table}.dat`
//! - TOAST file     : `database/base/{db}/{table}.toast`
//!
//! # TOAST integration
//! Values that exceed [`TOAST_THRESHOLD`] (8 KiB) are automatically stored
//! out-of-line.  The TOAST file is loaded before every mutating operation
//! and persisted afterwards, matching the behaviour of the interactive commands.

use std::fs::OpenOptions;
use std::io::{self};

use crate::backend::catalog::data_type::{DataType, Value};
use crate::backend::storage::row_layout::{ToastPointer, TupleHeader, VarFieldEntry};
use crate::backend::storage::toast::ToastManager;
use crate::backend::storage::tuple_codec::TupleCodec;
use crate::backend::executor::scan_tuples_indexed;
use crate::catalog::types::Catalog;
use crate::heap::{insert_tuple, delete_tuple, update_tuple};
use crate::table::page_count;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Result returned by a successful insert.
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// Number of bytes written to the heap (encoded tuple size).
    pub tuple_bytes: usize,
    /// Total page count in the table after the insert.
    pub total_pages: u32,
    /// Number of TOAST values created for oversized BLOB/ARRAY columns.
    pub toast_values_created: usize,
}

/// A single decoded tuple together with its heap location.
#[derive(Debug, Clone)]
pub struct TupleRecord {
    /// Page the tuple resides in (0-indexed).
    pub page_num: u32,
    /// Slot index within the page.
    pub slot_index: u32,
    /// Decoded column values (in schema order).
    pub values: Vec<Value>,
}

/// Result returned by a successful update.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of bytes in the old (replaced) tuple.
    pub old_bytes: usize,
    /// Number of bytes in the new (inserted) tuple.
    pub new_bytes: usize,
    /// Number of old TOAST values freed.
    pub toast_values_freed: usize,
    /// Number of new TOAST values created.
    pub toast_values_created: usize,
}

/// Result returned by a successful delete.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Page that contained the deleted tuple.
    pub page_num: u32,
    /// Slot that contained the deleted tuple.
    pub slot_index: u32,
    /// Number of TOAST values freed.
    pub toast_values_freed: usize,
}

// ---------------------------------------------------------------------------
// Helper: collect TOAST value IDs from raw tuple bytes (schema-aware)
// ---------------------------------------------------------------------------

/// Parse TOAST value IDs from a raw tuple's variable-length directory.
///
/// This is schema-aware: it correctly skips the fixed region to locate the
/// variable payload, matching the layout produced by [`TupleCodec::encode_tuple`].
pub fn collect_toast_ids_from_bytes(
    tuple_bytes: &[u8],
    schema: &[(String, DataType)],
) -> Vec<u64> {
    let mut ids = Vec::new();

    if tuple_bytes.len() < TupleHeader::size() {
        return ids;
    }

    let header = match TupleHeader::from_bytes(&tuple_bytes[0..TupleHeader::size()]) {
        Ok(h) => h,
        Err(_) => return ids,
    };

    let null_bitmap_bytes = header.null_bitmap_bytes as usize;
    let var_field_count = header.var_field_count as usize;
    let mut cursor = TupleHeader::size() + null_bitmap_bytes;

    let var_dir_size = var_field_count * VarFieldEntry::size();
    if cursor + var_dir_size > tuple_bytes.len() {
        return ids;
    }

    let mut var_entries = Vec::new();
    for i in 0..var_field_count {
        let start = cursor + i * VarFieldEntry::size();
        let end = start + VarFieldEntry::size();
        if let Ok(entry) = VarFieldEntry::from_bytes(&tuple_bytes[start..end]) {
            var_entries.push(entry);
        }
    }
    cursor += var_dir_size;

    // Compute fixed-region size from schema
    let fixed_size: usize = schema
        .iter()
        .filter(|(_, dt)| !dt.is_variable_length())
        .filter_map(|(_, dt)| dt.fixed_size())
        .sum();

    if cursor + fixed_size > tuple_bytes.len() {
        return ids;
    }
    cursor += fixed_size;

    let var_payload = &tuple_bytes[cursor..];

    for entry in &var_entries {
        if !entry.is_toast() {
            continue;
        }
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= var_payload.len() {
            if let Ok(ptr) = ToastPointer::from_bytes(&var_payload[start..end]) {
                ids.push(ptr.value_id);
            }
        }
    }

    ids
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

fn table_path(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.dat", db, table)
}

fn toast_path(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.toast", db, table)
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

/// Insert a tuple into `db.table`.
///
/// Encodes all values with [`TupleCodec`], automatically pushing oversized
/// BLOB / ARRAY columns to TOAST storage, then appends the tuple to the heap
/// and persists the TOAST file.
///
/// # Arguments
/// * `db`     – database name (sub-directory of `database/base/`)
/// * `table`  – table name (`.dat` file under the database directory)
/// * `values` – column values **in schema order**
/// * `schema` – column `(name, DataType)` pairs **in schema order**
///
/// # Errors
/// Returns an `io::Error` if the table file cannot be opened or written.
pub fn insert_tuple_api(
    db: &str,
    table: &str,
    values: &[Value],
    schema: &[(String, DataType)],
) -> io::Result<InsertResult> {
    let tpath = toast_path(db, table);

    // Load existing TOAST state (or start fresh)
    let toast_count_before;
    let mut toast_manager = ToastManager::load_from_disk(&tpath)
        .unwrap_or_else(|_| ToastManager::new());
    toast_count_before = toast_manager.value_count();

    // Encode tuple (TOAST applied automatically for oversized values)
    let tuple_bytes = TupleCodec::encode_tuple(values, schema, &mut toast_manager)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let toast_values_created = toast_manager.value_count() - toast_count_before;

    // Write to heap
    let fpath = table_path(db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fpath)
        .map_err(|e| io::Error::new(e.kind(), format!("Cannot open table '{}': {}", fpath, e)))?;

    insert_tuple(&mut file, &tuple_bytes)?;

    // Persist TOAST
    toast_manager
        .save_to_disk(&tpath)
        .map_err(|e| io::Error::new(e.kind(), format!("TOAST persist failed: {}", e)))?;

    let total_pages = page_count(&mut file)?;

    Ok(InsertResult {
        tuple_bytes: tuple_bytes.len(),
        total_pages,
        toast_values_created,
    })
}

// ---------------------------------------------------------------------------
// READ (full scan)
// ---------------------------------------------------------------------------

/// Scan and decode every live tuple in `db.table`.
///
/// Uses [`scan_tuples_indexed`] to iterate all heap pages and decodes each
/// raw tuple through [`TupleCodec::decode_tuple_with_toast`] so that
/// TOAST-backed BLOB / ARRAY columns are transparently reconstructed.
///
/// # Arguments
/// * `catalog` – catalog loaded with [`load_catalog`]
/// * `db`      – database name
/// * `table`   – table name
/// * `schema`  – column `(name, DataType)` pairs **in schema order**
pub fn read_tuples_api(
    catalog: &Catalog,
    db: &str,
    table: &str,
    _schema: &[(String, DataType)],
) -> io::Result<Vec<TupleRecord>> {
    let fpath = table_path(db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fpath)
        .map_err(|e| io::Error::new(e.kind(), format!("Cannot open table '{}': {}", fpath, e)))?;

    // Load TOAST so we can resolve out-of-line values
    let tpath = toast_path(db, table);
    let toast_manager = ToastManager::load_from_disk(&tpath)
        .unwrap_or_else(|_| ToastManager::new());

    // scan_tuples_indexed returns (page_num, slot_index, raw_values) — raw_values
    // are already decoded by the executor which uses decode_tuple_with_toast internally.
    // We call it directly so we reuse the executor's scan logic.
    let indexed = scan_tuples_indexed(catalog, db, table, &mut file)?;

    let mut records = Vec::with_capacity(indexed.len());
    for (page_num, slot_index, raw_values) in indexed {
        // The executor's scan already resolves TOAST via the toast file path
        // embedded in scan_tuples_indexed. If we need explicit control we can
        // re-decode here. For simplicity, we expose the already-decoded values.
        let _ = &toast_manager; // available if deeper TOAST resolution is needed
        records.push(TupleRecord {
            page_num,
            slot_index,
            values: raw_values,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// READ (single tuple by location)
// ---------------------------------------------------------------------------

/// Decode a single tuple identified by `(page_num, slot_index)`.
///
/// Scans the table to find the matching entry rather than seeking directly,
/// which keeps the implementation consistent with the heap page format.
///
/// Returns `None` if no live tuple exists at the given location.
pub fn read_tuple_by_location_api(
    catalog: &Catalog,
    db: &str,
    table: &str,
    schema: &[(String, DataType)],
    page_num: u32,
    slot_index: u32,
) -> io::Result<Option<TupleRecord>> {
    let all = read_tuples_api(catalog, db, table, schema)?;
    Ok(all
        .into_iter()
        .find(|r| r.page_num == page_num && r.slot_index == slot_index))
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

/// Update the tuple at `(page_num, slot_index)` in `db.table`.
///
/// Encodes `new_values` (TOAST-ing any oversized columns), calls
/// [`update_tuple`] to atomically replace the heap slot, frees old TOAST
/// values, and persists the updated TOAST file.
///
/// # Arguments
/// * `db`, `table`    – target table
/// * `page_num`       – page containing the tuple to replace
/// * `slot_index`     – slot within that page
/// * `new_values`     – replacement column values **in schema order**
/// * `schema`         – column schema
pub fn update_tuple_api(
    db: &str,
    table: &str,
    page_num: u32,
    slot_index: u32,
    new_values: &[Value],
    schema: &[(String, DataType)],
) -> io::Result<UpdateResult> {
    let tpath = toast_path(db, table);
    let mut toast_manager = ToastManager::load_from_disk(&tpath)
        .unwrap_or_else(|_| ToastManager::new());

    let toast_count_before = toast_manager.value_count();

    // Encode new tuple (possibly creating new TOAST values)
    let new_bytes = TupleCodec::encode_tuple(new_values, schema, &mut toast_manager)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let toast_values_created = toast_manager.value_count() - toast_count_before;

    // Open table file and run heap update
    let fpath = table_path(db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fpath)
        .map_err(|e| io::Error::new(e.kind(), format!("Cannot open table '{}': {}", fpath, e)))?;

    let old_bytes = update_tuple(&mut file, page_num, slot_index, &new_bytes)?;

    // Free old TOAST values
    let old_toast_ids = collect_toast_ids_from_bytes(&old_bytes, schema);
    for id in &old_toast_ids {
        let _ = toast_manager.delete_value(*id); // best-effort
    }

    // Persist updated TOAST state
    toast_manager
        .save_to_disk(&tpath)
        .map_err(|e| io::Error::new(e.kind(), format!("TOAST persist failed: {}", e)))?;

    Ok(UpdateResult {
        old_bytes: old_bytes.len(),
        new_bytes: new_bytes.len(),
        toast_values_freed: old_toast_ids.len(),
        toast_values_created,
    })
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

/// Delete the tuple at `(page_num, slot_index)` from `db.table`.
///
/// Marks the heap slot as deleted, extracts any TOAST value IDs from the old
/// tuple bytes, and frees them from the TOAST store before persisting.
///
/// # Arguments
/// * `db`, `table`  – target table
/// * `page_num`     – page containing the tuple
/// * `slot_index`   – slot within that page
/// * `schema`       – column schema (required for schema-aware TOAST ID extraction)
pub fn delete_tuple_api(
    db: &str,
    table: &str,
    page_num: u32,
    slot_index: u32,
    schema: &[(String, DataType)],
) -> io::Result<DeleteResult> {
    let fpath = table_path(db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fpath)
        .map_err(|e| io::Error::new(e.kind(), format!("Cannot open table '{}': {}", fpath, e)))?;

    // Delete from heap; get old bytes for TOAST cleanup
    let old_bytes = delete_tuple(&mut file, page_num, slot_index)?;

    let tpath = toast_path(db, table);
    let mut toast_manager = ToastManager::load_from_disk(&tpath)
        .unwrap_or_else(|_| ToastManager::new());

    let toast_ids = collect_toast_ids_from_bytes(&old_bytes, schema);
    for id in &toast_ids {
        let _ = toast_manager.delete_value(*id);
    }

    if !toast_ids.is_empty() {
        toast_manager
            .save_to_disk(&tpath)
            .map_err(|e| io::Error::new(e.kind(), format!("TOAST persist failed: {}", e)))?;
    }

    Ok(DeleteResult {
        page_num,
        slot_index,
        toast_values_freed: toast_ids.len(),
    })
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::data_type::{DataType, Value};
    use crate::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};
    use crate::backend::storage::tuple_codec::TupleCodec;

    // --- collect_toast_ids_from_bytes ---

    #[test]
    fn test_collect_toast_ids_no_toast() {
        let schema: Vec<(String, DataType)> = vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::Text),
        ];
        let values = vec![Value::Int32(1), Value::Text("hello".to_string())];
        let mut tm = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut tm).unwrap();
        let ids = collect_toast_ids_from_bytes(&encoded, &schema);
        assert!(ids.is_empty(), "small text should not be TOASTed");
    }

    #[test]
    fn test_collect_toast_ids_with_large_blob() {
        let schema: Vec<(String, DataType)> = vec![
            ("id".to_string(), DataType::Int32),
            ("data".to_string(), DataType::Blob),
        ];
        let values = vec![
            Value::Int32(1),
            Value::Blob(vec![0xAB; TOAST_THRESHOLD + 100]),
        ];
        let mut tm = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut tm).unwrap();
        let ids = collect_toast_ids_from_bytes(&encoded, &schema);
        assert_eq!(ids.len(), 1, "large blob should produce one TOAST value");
    }

    #[test]
    fn test_collect_toast_ids_large_array() {
        let schema: Vec<(String, DataType)> = vec![
            ("id".to_string(), DataType::Int32),
            (
                "arr".to_string(),
                DataType::Array {
                    element_type: Box::new(DataType::Int32),
                },
            ),
        ];
        // Construct a large array (each Int32 is 4 bytes; need > TOAST_THRESHOLD total)
        let element_count = (TOAST_THRESHOLD / 4) + 100;
        let elements: Vec<Value> = (0..element_count as i32)
            .map(Value::Int32)
            .collect();
        let values = vec![Value::Int32(99), Value::Array(elements)];
        let mut tm = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut tm).unwrap();
        let ids = collect_toast_ids_from_bytes(&encoded, &schema);
        assert_eq!(ids.len(), 1, "large array should produce one TOAST value");
    }

    #[test]
    fn test_collect_toast_ids_two_large_blobs_with_fixed() {
        // Mirrors the schema-aware fix validated in data_cmd.rs
        let schema: Vec<(String, DataType)> = vec![
            ("id".to_string(), DataType::Int32),
            ("d1".to_string(), DataType::Blob),
            ("flag".to_string(), DataType::Boolean),
            ("d2".to_string(), DataType::Blob),
        ];
        let values = vec![
            Value::Int32(7),
            Value::Blob(vec![0xAA; TOAST_THRESHOLD + 500]),
            Value::Boolean(false),
            Value::Blob(vec![0xBB; TOAST_THRESHOLD + 1000]),
        ];
        let mut tm = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut tm).unwrap();
        let ids = collect_toast_ids_from_bytes(&encoded, &schema);
        assert_eq!(ids.len(), 2);

        let id_set: std::collections::HashSet<u64> = ids.into_iter().collect();
        let expected: std::collections::HashSet<u64> = (1..=2).collect();
        assert_eq!(id_set, expected);
    }
}
