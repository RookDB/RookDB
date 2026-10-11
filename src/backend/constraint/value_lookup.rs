//! Value lookup helpers for constraint checking.
//!
//! Provides functions to look up whether a value exists in a table column,
//! using B+ Tree index lookups (fast path) or sequential heap scans (fallback).

use super::loaders;
use crate::types::{DataType, DataValue};

/// Helper: try all index files (named + legacy) for a table and search for a value.
/// Returns `Some(true)` if found, `Some(false)` if not found (in any index),
/// or `None` if no indexes exist (caller should fall back to heap scan).
fn try_index_for_value(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    column_name: &str,
    col_type: &DataType,
    key_value: &DataValue,
) -> Option<bool> {
    // Try named indexes: load all indexes from sys_indexes for this table
    if let Ok(indexes) =
        crate::backend::executor::create_index::load_table_indexes(engine, db_name, table_name)
    {
        for (idx_name, idx_col, _is_unique) in &indexes {
            if !idx_col.eq_ignore_ascii_case(column_name) {
                continue;
            }
            let idx_path =
                std::path::PathBuf::from(engine.paths().index(db_name, table_name, idx_name));
            if !idx_path.exists() {
                continue;
            }
            let search_res = crate::backend::cache::with_btree(
                engine,
                &idx_path,
                || -> std::io::Result<crate::backend::index::btree::BTree> {
                    let mut bt = crate::backend::index::btree::BTree::open(idx_path.clone())?;
                    bt.set_key_type(col_type.clone());
                    Ok(bt)
                },
                |bt| {
                    bt.set_key_type(col_type.clone());
                    bt.search(key_value).map_err(std::io::Error::other)
                },
            );
            match search_res {
                Ok(Some(_)) => return Some(true),
                Ok(None) => return Some(false),
                Err(e) => log::warn!(
                    "[Constraint] Cached BTree search error for FK lookup: {}",
                    e
                ),
            }
        }
    }

    // Fallback to legacy single-index file
    let legacy_idx = std::path::PathBuf::from(engine.paths().legacy_index(db_name, table_name));
    if legacy_idx.exists() {
        let search_res = crate::backend::cache::with_btree(
            engine,
            &legacy_idx,
            || -> std::io::Result<crate::backend::index::btree::BTree> {
                let mut bt = crate::backend::index::btree::BTree::open(legacy_idx.clone())?;
                bt.set_key_type(col_type.clone());
                Ok(bt)
            },
            |bt| {
                bt.set_key_type(col_type.clone());
                bt.search(key_value).map_err(std::io::Error::other)
            },
        );
        match search_res {
            Ok(Some(_)) => return Some(true),
            Ok(None) => return Some(false),
            Err(e) => log::warn!(
                "[Constraint] Cached legacy BTree search error for FK lookup: {}",
                e
            ),
        }
    }

    None // No index found — caller should fall back to heap scan
}

/// Check whether a specific value exists in a table column.
///
/// `key_value` must already have the column's type. First tries a B+ Tree
/// index lookup (for speed), falling back to a sequential heap scan.
pub(crate) fn value_exists_in_table(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    column_name: &str,
    col_type: &DataType,
    key_value: &DataValue,
) -> Result<bool, String> {
    // Try all B+ Tree indexes first (named + legacy)
    if let Some(result) = try_index_for_value(
        engine,
        db_name,
        table_name,
        column_name,
        col_type,
        key_value,
    ) {
        return Ok(result);
    }

    // Fallback: sequential heap scan
    let schema = match loaders::load_table_schema(engine, db_name, table_name) {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(false),
        Err(e) => {
            log::warn!("[Constraint] Error loading schema: {}", e);
            return Ok(false);
        }
    };
    let Some(col_pos) = schema
        .iter()
        .position(|(name, _)| name.eq_ignore_ascii_case(column_name))
    else {
        log::warn!(
            "[Constraint] Column '{}' not found in table schema for FK lookup",
            column_name
        );
        return Ok(false);
    };
    let schema_types: Vec<DataType> = schema.into_iter().map(|(_, dt)| dt).collect();
    scan_heap_for_value(
        engine,
        db_name,
        table_name,
        &schema_types,
        col_pos,
        key_value,
    )
}

/// Check whether a child table has any row whose given column matches the value.
///
/// Used by parent DELETE validation to detect referencing rows.
pub(crate) fn child_has_referencing_row(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    value: &DataValue,
) -> Result<bool, String> {
    // Load the child table's column schema from sys_columns to get the correct type
    let (schema, col_pos, col_type) =
        match loaders::load_table_schema_for_column(engine, db_name, child_table, child_col) {
            Ok(Some(result)) => result,
            Ok(None) => {
                // Table not in system tables: fall back to matching the key's text.
                let heap_path = std::path::PathBuf::from(engine.paths().heap(db_name, child_table));
                return scan_raw_heap_for_string(&heap_path, &super::fk_actions::key_text(value));
            }
            Err(e) => {
                log::warn!(
                    "[Constraint] Error loading schema for child table FK lookup: {}",
                    e
                );
                return Ok(false);
            }
        };

    // The parent key in the child column's type; a key the column cannot
    // hold is referenced by no child row.
    let key_value = match crate::types::functions::assign_cast(value, &col_type) {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "[Constraint] Failed to convert value for FK child lookup: {}",
                e
            );
            return Ok(false);
        }
    };

    // Try all B+ Tree indexes (named + legacy)
    if let Some(result) = try_index_for_value(
        engine,
        db_name,
        child_table,
        child_col,
        &col_type,
        &key_value,
    ) {
        return Ok(result);
    }

    scan_heap_for_value(engine, db_name, child_table, &schema, col_pos, &key_value)
}

/// Scan a table heap for a row whose column `col_pos` equals `key_value`.
fn scan_heap_for_value(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    schema: &[DataType],
    col_pos: usize,
    key_value: &DataValue,
) -> Result<bool, String> {
    use crate::types::Comparable;

    let heap_path = std::path::PathBuf::from(engine.paths().heap(db_name, table_name));
    if !heap_path.exists() {
        return Ok(false);
    }

    let heap = match crate::backend::heap::HeapManager::open(heap_path) {
        Ok(h) => h,
        Err(e) => {
            log::warn!("[Constraint] Failed to open heap for FK scan: {}", e);
            return Ok(false);
        }
    };

    for result in heap.scan() {
        let Ok((_page_id, _slot_id, raw_bytes)) = result else {
            continue;
        };
        let Ok(decoded) = crate::types::deserialize_nullable_row(schema, &raw_bytes) else {
            continue;
        };
        if let Some(Some(dv)) = decoded.get(col_pos)
            && matches!(dv.compare(key_value), Ok(std::cmp::Ordering::Equal))
        {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Fallback raw heap scan using string matching (for tables not in system catalog).
fn scan_raw_heap_for_string(heap_path: &std::path::Path, value: &str) -> Result<bool, String> {
    if !heap_path.exists() {
        return Ok(false);
    }

    let heap = match crate::backend::heap::HeapManager::open(heap_path.to_path_buf()) {
        Ok(h) => h,
        Err(e) => {
            log::warn!("[Constraint] Failed to open heap for raw scan: {}", e);
            return Ok(false);
        }
    };

    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) = match result {
            Ok(triple) => triple,
            Err(_) => continue,
        };

        let raw_str = String::from_utf8_lossy(&raw_bytes);
        if raw_str.contains(value) {
            return Ok(true);
        }
    }

    Ok(false)
}
