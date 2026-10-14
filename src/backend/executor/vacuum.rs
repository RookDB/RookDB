//! VACUUM — space reclamation for tables with soft-deleted rows.
//!
//! DELETE only flags slots as soft-deleted; the space stays reserved until
//! a VACUUM pass physically rewrites the affected pages. This module wires
//! together the existing building blocks into one safe, logged operation:
//!
//! 1. **Counter snapshot** — `dead_tuple_count` is read for the report.
//! 2. **Compaction** — `compaction_table()` rewrites every page holding
//!    dead slots or bytes of replaced rows (renumbering surviving slots),
//!    then rebuilds the FSM and resets `dead_tuple_count`.
//! 3. **Index rebuild** — slot renumbering invalidates the `(page_id,
//!    slot_id)` row pointers stored in B+Tree leaves. Every index on the
//!    table is therefore bulk-rebuilt from the post-compaction heap.
//! 4. **Header stamping** — `last_vacuum` is recorded in the table header.

use std::io;
use std::path::PathBuf;

use crate::backend::executor::create_index::{index_file_path, load_table_indexes_multi};
use crate::backend::index::btree::BTree;
use crate::catalog::Catalog;
use crate::types::{DataType, deserialize_nullable_row};

/// Summary of one VACUUM run, for reporting and tests.
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStats {
    /// Pages that contained dead slots and were rewritten.
    pub pages_compacted: usize,
    /// Dead-tuple counter value observed before the reset.
    pub dead_tuples_before: u32,
    /// Number of indexes bulk-rebuilt after compaction.
    pub indexes_rebuilt: usize,
}

/// Reclaim space occupied by soft-deleted rows in `db.table`.
///
/// Safe to call at any time: tables with zero dead rows compact zero pages
/// (the visibility map short-circuits clean pages) and index rebuilds are
/// skipped entirely when no compaction happened.
pub fn vacuum_table(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> Result<VacuumStats, String> {
    // Path-safety before touching the filesystem.
    crate::backend::name_validation::validate_database_name(db_name).map_err(|e| e.to_string())?;
    crate::backend::name_validation::validate_table_name(table_name).map_err(|e| e.to_string())?;

    // Table must exist in the catalog.
    catalog
        .databases
        .get(db_name)
        .ok_or_else(|| format!("Database '{}' not found", db_name))?
        .tables
        .get(table_name)
        .ok_or_else(|| format!("Table '{}.{}' not found", db_name, table_name))?;

    let heap_path = catalog.engine().paths().heap(db_name, table_name);
    if !PathBuf::from(&heap_path).exists() {
        return Err(format!("Heap file not found: {}", heap_path.display()));
    }

    // 1. Snapshot the dead-tuple counter before compaction resets it.
    let dead_before =
        read_dead_tuple_count(catalog.engine(), &heap_path).map_err(|e| e.to_string())?;

    // 2. Compact pages with dead slots (+ FSM rebuild + counter reset).
    let pages_compacted = super::delete::compaction_table(catalog.engine(), db_name, table_name)
        .map_err(|e| e.to_string())?;

    let mut indexes_rebuilt = 0usize;
    if pages_compacted > 0 {
        // 4. Slot renumbering invalidated every stored row pointer → rebuild
        //    all indexes from the fresh heap.
        indexes_rebuilt = rebuild_indexes(catalog, db_name, table_name)?;
    }

    // 5. Stamp last_vacuum through a FRESH manager (post-invalidation).
    stamp_last_vacuum(catalog.engine(), &heap_path)?;

    log::info!(
        "[Vacuum] {}.{} done: {} page(s) compacted, {} dead tuple(s) reclaimed, {} index(es) rebuilt",
        db_name,
        table_name,
        pages_compacted,
        dead_before,
        indexes_rebuilt
    );

    Ok(VacuumStats {
        pages_compacted,
        dead_tuples_before: dead_before,
        indexes_rebuilt,
    })
}

/// Read the header's `dead_tuple_count` field (bytes 20..24 of page 0).
fn read_dead_tuple_count(
    engine: &crate::backend::engine::Engine,
    heap_path: &std::path::Path,
) -> io::Result<u32> {
    let file = crate::backend::heap::HeapFile::open(engine, heap_path)?;
    Ok(file.header()?.dead_tuple_count)
}

/// Bulk-rebuild every named index on a table from current heap contents.
///
/// Only the `.idx` B+Tree files are regenerated; `sys_indexes` is untouched.
fn rebuild_indexes(catalog: &Catalog, db_name: &str, table_name: &str) -> Result<usize, String> {
    let engine = catalog.engine();
    let indexes = load_table_indexes_multi(engine, db_name, table_name)?;
    if indexes.is_empty() {
        return Ok(0);
    }

    let schema_types: Vec<DataType> = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .map(|t| t.columns.iter().map(|c| c.data_type.clone()).collect())
        .ok_or_else(|| format!("Table '{}.{}' not found in catalog", db_name, table_name))?;

    let heap_path = engine.paths().heap(db_name, table_name);
    let heap_manager = crate::backend::cache::lease_heap(engine, &heap_path)
        .map_err(|e| format!("Failed to open heap: {}", e))?;

    let mut rebuilt = 0usize;
    for (idx_name, col_names, _is_unique) in &indexes {
        // Resolve every key column's position + type from the schema.
        let mut col_positions = Vec::with_capacity(col_names.len());
        let mut key_types = Vec::with_capacity(col_names.len());
        let catalog_table = catalog
            .databases
            .get(db_name)
            .and_then(|db| db.tables.get(table_name));
        let Some(t) = catalog_table else {
            return Err(format!(
                "Table '{}.{}' not found in catalog",
                db_name, table_name
            ));
        };
        for cname in col_names {
            match t
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(cname))
            {
                Some(pos) => {
                    key_types.push(t.columns[pos].data_type.clone());
                    col_positions.push(pos);
                }
                None => {
                    log::warn!(
                        "[Vacuum] Index '{}' references unknown column '{}'; skipping rebuild",
                        idx_name,
                        cname
                    );
                    col_positions.clear();
                    break;
                }
            }
        }
        if col_positions.is_empty() {
            continue;
        }

        let idx_path = index_file_path(engine, db_name, table_name, idx_name);
        if !idx_path.exists() {
            continue;
        }

        // Drop any stale cached handle before truncating/recreating the index file.
        crate::backend::cache::evict_btree(catalog.engine(), &idx_path);

        // Fresh tree over the post-compaction heap.
        let mut btree = BTree::create_composite(idx_path.clone(), key_types)
            .map_err(|e| format!("Failed to recreate index '{}': {}", idx_name, e))?;

        let mut entries = 0usize;
        for result in heap_manager.scan() {
            let (page_id, slot_id, raw_bytes) = match result {
                Ok(t) => t,
                Err(e) => return Err(format!("Heap scan failed during index rebuild: {}", e)),
            };
            let values = deserialize_nullable_row(&schema_types, &raw_bytes)
                .map_err(|e| format!("Tuple decode failed during index rebuild: {}", e))?;
            // Composite key: gather every component; skip rows with any NULL.
            let mut key: Vec<crate::types::DataValue> = Vec::with_capacity(col_positions.len());
            let mut has_null = false;
            for &pos in &col_positions {
                match values.get(pos) {
                    Some(Some(dv)) => key.push(dv.clone()),
                    _ => {
                        has_null = true;
                        break;
                    }
                }
            }
            if !has_null {
                btree
                    .insert_keys(&key, page_id, slot_id)
                    .map_err(|e| format!("Failed to insert into rebuilt index: {}", e))?;
                entries += 1;
            }
        }
        btree
            .sync()
            .map_err(|e| format!("Failed to sync rebuilt index '{}': {}", idx_name, e))?;
        crate::backend::cache::evict_btree(catalog.engine(), &idx_path);
        log::info!(
            "[Vacuum] Rebuilt index '{}' with {} entries",
            idx_name,
            entries
        );
        rebuilt += 1;
    }

    Ok(rebuilt)
}

/// Record the current time in the header's `last_vacuum` field.
///
/// Takes the engine's handle AFTER cache invalidation (compaction evicts it),
/// so the write lands on the post-compaction file state.
fn stamp_last_vacuum(
    engine: &crate::backend::engine::Engine,
    heap_path: &std::path::Path,
) -> Result<(), String> {
    let mut manager =
        crate::backend::cache::lease_heap(engine, heap_path).map_err(|e| e.to_string())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);
    manager
        .update_header(|h| h.last_vacuum = now)
        .map_err(|e| e.to_string())?;
    manager.flush().map_err(|e| e.to_string())
}
