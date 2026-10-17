//! VACUUM — space reclamation for tables with soft-deleted rows.
//!
//! DELETE only flags slots as soft-deleted; the space stays reserved until
//! a VACUUM pass physically rewrites the affected pages. This module wires
//! together the existing building blocks into one safe, logged operation:
//!
//! 1. **Counter snapshot** — `dead_tuple_count` is read for the report.
//! 2. **Compaction** — `compaction_table()` rewrites every page holding
//!    dead slots or bytes of replaced rows, then rebuilds the FSM and resets
//!    `dead_tuple_count`. Every row keeps its slot, so the `(page_id,
//!    slot_id)` pointers in the indexes stay valid and need no rebuild, and
//!    a VACUUM that stops partway leaves the indexes correct.
//! 3. **Header stamping** — `last_vacuum` is recorded in the table header.

use std::path::PathBuf;

use crate::backend::error::{RookError, RookResult};
use crate::catalog::Catalog;

/// Summary of one VACUUM run, for reporting and tests.
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStats {
    /// Pages that contained dead slots and were rewritten.
    pub pages_compacted: usize,
    /// Dead-tuple counter value observed before the reset.
    pub dead_tuples_before: u32,
}

/// Reclaim space occupied by soft-deleted rows in `db.table`.
///
/// Safe to call at any time: tables with zero dead rows compact zero pages
/// (the visibility map short-circuits clean pages).
pub fn vacuum_table(catalog: &Catalog, db_name: &str, table_name: &str) -> RookResult<VacuumStats> {
    // Path-safety before touching the filesystem.
    crate::backend::name_validation::validate_database_name(db_name)?;
    crate::backend::name_validation::validate_table_name(table_name)?;

    // Table must exist in the catalog.
    catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db_name.to_string(),
        })?
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Table",
            name: format!("{}.{}", db_name, table_name),
        })?;

    let heap_path = catalog.engine().paths().heap(db_name, table_name);
    if !PathBuf::from(&heap_path).exists() {
        return Err(RookError::Internal(format!(
            "Heap file not found: {}",
            heap_path.display()
        )));
    }

    // 1. Snapshot the dead-tuple counter before compaction resets it.
    let dead_before =
        read_dead_tuple_count(catalog.engine(), &heap_path).map_err(RookError::storage_bare)?;

    // 2. Compact pages with dead slots (+ FSM rebuild + counter reset).
    let pages_compacted = super::delete::compaction_table(catalog.engine(), db_name, table_name)
        .map_err(RookError::storage_bare)?;

    // 3. Stamp last_vacuum through a FRESH manager (post-invalidation).
    stamp_last_vacuum(catalog.engine(), &heap_path)?;

    log::info!(
        "[Vacuum] {}.{} done: {} page(s) compacted, {} dead tuple(s) reclaimed",
        db_name,
        table_name,
        pages_compacted,
        dead_before
    );

    Ok(VacuumStats {
        pages_compacted,
        dead_tuples_before: dead_before,
    })
}

/// Read the header's `dead_tuple_count` field (bytes 20..24 of page 0).
fn read_dead_tuple_count(
    engine: &crate::backend::engine::Engine,
    heap_path: &std::path::Path,
) -> RookResult<u32> {
    let file = crate::backend::heap::HeapFile::open(engine, heap_path)?;
    Ok(file.header()?.dead_tuple_count)
}

/// Record the current time in the header's `last_vacuum` field.
///
/// Takes the engine's handle AFTER cache invalidation (compaction evicts it),
/// so the write lands on the post-compaction file state.
fn stamp_last_vacuum(
    engine: &crate::backend::engine::Engine,
    heap_path: &std::path::Path,
) -> RookResult<()> {
    let mut manager =
        crate::backend::cache::lease_heap(engine, heap_path).map_err(RookError::storage_bare)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0);
    manager
        .update_header(|h| h.last_vacuum = now)
        .map_err(RookError::storage_bare)?;
    manager.flush().map_err(RookError::storage_bare)
}
