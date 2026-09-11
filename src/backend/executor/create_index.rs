//! CREATE INDEX — Build a B+ Tree index from a heap scan.
//!
//! Scans all tuples in a table's heap file, extracts the indexed column's value
//! from each tuple, and inserts it into a new B+ Tree index file at
//! `database/base/{db}/{table}.{index_name}.idx`.
//!
//! Also stores index metadata in a companion `.idx.meta` file so that DML
//! operations (INSERT, DELETE, UPDATE) can update the index automatically.
//!
//! # Multiple indexes per table
//!
//! Each index gets its own file pair:
//!   `database/base/{db}/{table}.{index_name}.idx`
//!   `database/base/{db}/{table}.{index_name}.idx.meta`
//!
//! The `update_index_on_*` functions query `sys_indexes` to discover all
//! indexes defined on a table and maintain each one individually.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::backend::heap::heap_manager::HeapManager;
use crate::backend::index::btree::BTree;
use crate::types::value::{DataValue, OrderedF32, OrderedF64};
use crate::types::DataType;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Construct the `.idx` file path for a named index.
pub(crate) fn index_file_path(db_name: &str, table_name: &str, index_name: &str) -> PathBuf {
    PathBuf::from(format!(
        "database/base/{}/{}.{}.idx",
        db_name, table_name, index_name
    ))
}

/// Construct the `.idx.meta` file path for a named index.
fn index_meta_file_path(db_name: &str, table_name: &str, index_name: &str) -> PathBuf {
    PathBuf::from(format!(
        "database/base/{}/{}.{}.idx.meta",
        db_name, table_name, index_name
    ))
}

/// Construct the legacy `.idx` file path (single-index fallback).
fn legacy_index_file_path(db_name: &str, table_name: &str) -> PathBuf {
    PathBuf::from(format!("database/base/{}/{}.idx", db_name, table_name))
}

/// Construct the legacy `.idx.meta` file path (single-index fallback).
fn legacy_index_meta_file_path(db_name: &str, table_name: &str) -> PathBuf {
    PathBuf::from(format!("database/base/{}/{}.idx.meta", db_name, table_name))
}

/// Metadata about an index, stored alongside the `.idx` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexMeta {
    /// The name of the indexed column (for human reference).
    ///
    /// Legacy single-column field; kept for backward compatibility with
    /// meta files written before composite indexes. When present alongside
    /// `column_names`, it mirrors the first entry.
    #[serde(default)]
    column_name: String,
    /// The ordinal position of the indexed column in the table schema.
    #[serde(default)]
    column_idx: usize,
    /// String representation of the key type (e.g. "INT", "VARCHAR(100)").
    #[serde(default)]
    key_type: String,
    /// Indexed columns in key order — composite indexes have >1 entry.
    #[serde(default)]
    column_names: Vec<String>,
    /// Ordinal position of each indexed column, aligned with `column_names`.
    #[serde(default)]
    column_idxs: Vec<usize>,
    /// Key type per segment, aligned with `column_names`.
    #[serde(default)]
    key_types: Vec<String>,
    /// Whether this index enforces uniqueness (feeds `sys_indexes.is_unique`,
    /// which in turn feeds the UNIQUE checker's index fast path).
    ///
    /// Defaults to `false` when absent (older `.idx.meta` files): a missing
    /// flag degrades UNIQUE enforcement to the heap-scan fallback — correct,
    /// just slower. Defaulting to `true` instead would wrongly let plain
    /// indexes reject duplicate values after a catalog rebuild.
    #[serde(default)]
    is_unique: bool,
    /// Whether this index was created for a PRIMARY KEY (informational;
    /// surfaced through `information_schema.indexes`).
    #[serde(default)]
    is_primary: bool,
}

impl IndexMeta {
    /// Indexed column names, normalising legacy single-column files.
    fn columns(&self) -> Vec<String> {
        if !self.column_names.is_empty() {
            self.column_names.clone()
        } else {
            vec![self.column_name.clone()]
        }
    }

    /// Column ordinal positions, normalising legacy files.
    fn idxs(&self) -> Vec<usize> {
        if !self.column_idxs.is_empty() {
            self.column_idxs.clone()
        } else {
            vec![self.column_idx]
        }
    }

    /// Key types as strings, normalising legacy files.
    fn types(&self) -> Vec<String> {
        if !self.key_types.is_empty() {
            self.key_types.clone()
        } else {
            vec![self.key_type.clone()]
        }
    }
}

/// Load index metadata from an `.idx.meta` file.
fn load_index_meta(meta_path: &PathBuf) -> Result<IndexMeta, String> {
    let meta_json = std::fs::read_to_string(meta_path)
        .map_err(|e| format!("Failed to read index metadata: {}", e))?;
    serde_json::from_str(&meta_json)
        .map_err(|e| format!("Failed to parse index metadata: {}", e))
}

/// Load all indexes defined for a table from `sys_indexes`.
///
/// # Dependency
///
/// This function reads from the `sys_indexes` system table, which requires:
/// - `sys_databases` and `sys_tables` to have the database/table registered
/// - `sys_indexes` to be populated (i.e., `save_catalog()` has been called at
///   least once so the system tables contain this table's metadata).
///
/// During initial catalog bootstrap — before `save_catalog()` has persisted
/// the table's metadata — this function returns an empty list because the
/// system tables have not yet been populated.  Callers should fall back to
/// legacy `{table}.idx` file scanning when this function returns `Vec::new()`.
///
/// Returns `Vec<(index_name, column_name, is_unique)>`.
///
/// For composite indexes only the FIRST column is reported; use
/// [`load_table_indexes_multi`] when the full key layout matters.
pub fn load_table_indexes(db_name: &str, table_name: &str) -> Result<Vec<(String, String, bool)>, String> {
    Ok(load_table_indexes_multi(db_name, table_name)?
        .into_iter()
        .map(|(name, mut cols, unique)| {
            let first = cols.drain(..).next().unwrap_or_default();
            (name, first, unique)
        })
        .collect())
}

/// Load all indexes defined for a table from `sys_indexes`, preserving every
/// key column of composite indexes.
///
/// The `sys_indexes.columns` field stores a comma-separated column list.
///
/// Returns `Vec<(index_name, Vec<column_name>, is_unique)>`.
pub fn load_table_indexes_multi(db_name: &str, table_name: &str) -> Result<Vec<(String, Vec<String>, bool)>, String> {
    // Hot path: serve from the process-cached table metadata (populated once
    // per (db, table), invalidated on DDL). Physical planning calls this for
    // every query — a full sys_indexes scan per SELECT was the dominant
    // fixed cost of point lookups.
    if let Some(meta) = crate::backend::cache::metadata(db_name, table_name) {
        return Ok(meta.named_indexes.clone());
    }

    use crate::backend::system_table::SYS_INDEXES_SCHEMA;

    let (table_id, _) = match crate::backend::system_table::resolve_table_id(db_name, table_name) {
        Ok(ids) => ids,
        Err(_) => return Ok(Vec::new()),
    };

    let idx_path = PathBuf::from(format!("{}/indexes.dat", crate::layout::SYSTEM_DIR));
    if !idx_path.exists() {
        return Ok(Vec::new());
    }

    let heap = HeapManager::open(idx_path)
        .map_err(|e| format!("Failed to open sys_indexes: {}", e))?;

    let mut indexes = Vec::new();
    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) = result
            .map_err(|e| format!("Error scanning sys_indexes: {}", e))?;
        let decoded = crate::types::deserialize_nullable_row(SYS_INDEXES_SCHEMA, &raw_bytes)
            .map_err(|e| format!("Error deserializing sys_indexes: {}", e))?;

        // SYS_INDEXES_SCHEMA: index_id, table_id, name, is_unique, is_primary, columns
        if decoded.len() < 6 {
            continue;
        }

        let row_table_id = match &decoded[1] {
            Some(DataValue::Int(id)) => *id,
            _ => continue,
        };
        if row_table_id != table_id {
            continue;
        }

        let index_name = match &decoded[2] {
            Some(DataValue::Varchar(n)) => n.clone(),
            Some(DataValue::Char(n)) => n.clone(),
            _ => continue,
        };
        let columns_field = match &decoded[5] {
            Some(DataValue::Varchar(c)) => c.clone(),
            Some(DataValue::Char(c)) => c.clone(),
            _ => continue,
        };
        let columns: Vec<String> = columns_field
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let is_unique = match &decoded[3] {
            Some(DataValue::Bool(v)) => *v,
            _ => false,
        };

        indexes.push((index_name, columns, is_unique));
    }

    Ok(indexes)
}

/// Check if a legacy single-index file exists for a table.
pub fn legacy_index_exists(db_name: &str, table_name: &str) -> bool {
    legacy_index_file_path(db_name, table_name).exists()
}

// ── Create Index ──────────────────────────────────────────────────────────────

/// Build a B+ Tree index on the specified column(s) of a table.
///
/// Each index gets its own file pair:
///   `database/base/{db}/{table}.{index_name}.idx`
///   `database/base/{db}/{table}.{index_name}.idx.meta`
///
/// Multiple columns form a composite key: entries whose key is the
/// concatenation of the column values in the given order.
///
/// # Arguments
/// * `catalog` - The catalog (used to resolve the table schema).
/// * `db_name` - The current database name.
/// * `table_name` - The table to index.
/// * `index_name` - The name of the index (used in the file name).
/// * `column_names` - The column(s) to index, in key order.
///
/// # Returns
/// * `Ok(tuple_count)` on success, with the number of indexed tuples.
/// * `Err(message)` on failure.
pub fn create_index(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    column_names: &[String],
) -> crate::backend::error::RookResult<usize> {
    create_index_with_flags(catalog, db_name, table_name, index_name, column_names, false, false)
}

/// Build a B+ Tree index with explicit constraint flags.
///
/// Identical to [`create_index`] except that the index is registered as
/// unique and/or primary in both `.idx.meta` and `sys_indexes`. The unique
/// flag is what lets the UNIQUE checker (`constraint::validation`) use the
/// index as its fast path instead of falling back to an O(n) heap scan per
/// row.
///
/// # Arguments
/// * `is_unique` — enforce uniqueness through this index.
/// * `is_primary` — the index backs a PRIMARY KEY constraint.
pub fn create_index_with_flags(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    column_names: &[String],
    is_unique: bool,
    is_primary: bool,
) -> crate::backend::error::RookResult<usize> {
    log::info!(
        "[CreateIndex] Creating index '{}.{}.{}' on columns {:?} (unique={}, primary={})",
        db_name, table_name, index_name, column_names, is_unique, is_primary
    );

    if column_names.is_empty() {
        return Err("CREATE INDEX requires at least one column".to_string().into());
    }

    // 1. Resolve the table schema from the catalog
    let db = catalog.databases.get(db_name)
        .ok_or_else(|| format!("Database '{}' not found", db_name))?;
    let table = db.tables.get(table_name)
        .ok_or_else(|| format!("Table '{}' not found in database '{}'", table_name, db_name))?;

    // 2. Resolve every indexed column
    let mut indexed_cols: Vec<&crate::catalog::types::Column> = Vec::new();
    let mut indexed_idxs: Vec<usize> = Vec::new();
    for col_name in column_names {
        let idx = table.columns.iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| format!("Column '{}' not found in table '{}'", col_name, table_name))?;
        indexed_idxs.push(idx);
        indexed_cols.push(&table.columns[idx]);
    }

    let schema_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
    let key_types: Vec<DataType> = indexed_cols.iter().map(|c| c.data_type.clone()).collect();

    // 3. Determine the actual index name (auto-generate if empty)
    let resolved_name = if index_name.is_empty() {
        format!("idx_{}_{}", table_name, column_names.join("_"))
    } else {
        index_name.to_string()
    };

    // 4. Build paths — use index-name-based files
    let heap_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    let idx_path = index_file_path(db_name, table_name, &resolved_name);
    let meta_path = index_meta_file_path(db_name, table_name, &resolved_name);

    if !heap_path.exists() {
        return Err(format!("Heap file not found: {:?}", heap_path).into());
    }

    // 5. Open the heap file for scanning
    let heap_manager = HeapManager::open(heap_path)
        .map_err(|e| format!("Failed to open heap for table '{}': {}", table_name, e))?;

    // 6. Create the B+ Tree index file (composite-aware)
    let mut btree = BTree::create_composite(idx_path.clone(), key_types.clone())
        .map_err(|e| format!("Failed to create index file: {}", e))?;

    let mut stat_collectors: Vec<crate::backend::statistics::column_stats::ColumnStatisticsCollector> = indexed_cols
        .iter()
        .map(|c| crate::backend::statistics::column_stats::ColumnStatisticsCollector::new(table_name.to_string(), c.name.clone()))
        .collect();

    // 7. Scan all tuples and insert into the B+ Tree
    let mut inserted_count = 0usize;
    let scan_iter = heap_manager.scan();
    for result in scan_iter {
        let (page_id, slot_id, raw_bytes) = match result {
            Ok(triple) => triple,
            Err(e) => return Err(format!("Scan error: {}", e).into()),
        };

        // Deserialize the tuple
        let values = crate::types::deserialize_nullable_row(&schema_types, &raw_bytes)
            .map_err(|e| format!("Failed to deserialize tuple: {}", e))?;

        // Feed column statistics for each indexed column
        for (i, &ci) in indexed_idxs.iter().enumerate() {
            let val = values.get(ci).and_then(|opt| opt.as_ref());
            stat_collectors[i].add(val);
        }

        // Assemble the key from every indexed column; skip rows where ANY
        // component is NULL (NULLs are not indexed).
        let mut key_values: Vec<DataValue> = Vec::with_capacity(indexed_idxs.len());
        let mut has_null = false;
        for &ci in &indexed_idxs {
            match values.get(ci) {
                Some(Some(dv)) => key_values.push(dv.clone()),
                _ => {
                    has_null = true;
                    break;
                }
            }
        }
        if has_null {
            log::trace!(
                "[CreateIndex] Skipping NULL key at (page={}, slot={})",
                page_id, slot_id
            );
            continue;
        }

        // Insert into the B+ Tree: key=column values, value=(page_id, slot_id)
        if let Err(e) = btree.insert_keys(&key_values, page_id, slot_id) {
            return Err(format!(
                "Failed to insert into index at (page={}, slot={}): {}",
                page_id, slot_id, e
            ).into());
        }

        inserted_count += 1;
    }

    // 8. Sync the index to disk
    btree.sync()
        .map_err(|e| format!("Failed to sync index: {}", e))?;

    // 9. Write index metadata file for DML integration
    let meta = IndexMeta {
        column_name: indexed_cols[0].name.clone(),
        column_idx: indexed_idxs[0],
        key_type: format!("{}", indexed_cols[0].data_type),
        column_names: indexed_cols.iter().map(|c| c.name.clone()).collect(),
        column_idxs: indexed_idxs.clone(),
        key_types: key_types.iter().map(|t| format!("{}", t)).collect(),
        is_unique,
        is_primary,
    };
    let meta_json = serde_json::to_string_pretty(&meta)
        .map_err(|e| format!("Failed to serialize index metadata: {}", e))?;
    std::fs::write(&meta_path, meta_json)
        .map_err(|e| format!("Failed to write index metadata: {}", e))?;

    // 10. Save index metadata to sys_indexes system table (comma-separated
    //     column list for composite indexes)
    if let Err(e) = crate::backend::system_table::insert_index_metadata(
        db_name,
        table_name,
        &resolved_name,
        &indexed_cols.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(","),
        is_unique,
        is_primary,
    ) {
        log::warn!(
            "[CreateIndex] Failed to save index metadata to sys_indexes: {}",
            e
        );
    }

    // 11. Persist column statistics for the indexed column(s) into sys_statistics
    let col_stats: Vec<_> = stat_collectors.into_iter().map(|c| c.finalize()).collect();
    if let Err(e) = crate::backend::system_table::save_column_statistics(db_name, table_name, &col_stats) {
        log::warn!("[CreateIndex] Failed to save column statistics to sys_statistics: {}", e);
    }

    // DDL landed: forget memoised metadata/discovery for this table and any
    // stale handle for the freshly rewritten index file.
    invalidate_discovery(db_name, Some(table_name));
    crate::backend::cache::invalidate_metadata();
    crate::backend::cache::evict_btree(&idx_path);

    log::info!(
        "[CreateIndex] Index '{}' created with {} entries on {}.{}({})",
        resolved_name, inserted_count, db_name, table_name, column_names.join(",")
    );

    Ok(inserted_count)
}

// ── Auto-indexing for constraint targets ─────────────────────────────────────

/// True when some index on `(db, table)` has `column` as its FIRST key
/// column — the only position usable by single-key equality probes
/// (FK existence checks, per-column UNIQUE checks).
fn column_is_indexed(db_name: &str, table_name: &str, column: &str) -> bool {
    load_table_indexes_multi(db_name, table_name)
        .unwrap_or_default()
        .iter()
        .any(|(_name, cols, _u)| {
            cols.first()
                .map(|c| c.eq_ignore_ascii_case(column))
                .unwrap_or(false)
        })
}

/// Create an index on a parent-side FK target column if it does not already
/// have one.
///
/// FK enforcement checks child inserts against the PARENT table
/// (`value_exists_in_table`). Without an index on the referenced column,
/// every child insert pays a full parent-table heap scan — O(n) per row.
/// The CLI calls this (a) when a child table declares `REFERENCES t(c)` and
/// (b) after creating a table, so forward references (child created before
/// parent) are covered too.
///
/// The created index is named `fkp_{table}_{column}` and is marked unique
/// only when the column itself declares UNIQUE/PK (otherwise a plain index:
/// accelerating the probe must never change what the table accepts).
///
/// Returns `true` if an index was created.
pub fn ensure_parent_column_index(
    catalog: &Catalog,
    db_name: &str,
    parent_table: &str,
    parent_col: &str,
) -> bool {
    // The parent must exist in the catalog with the referenced column.
    let Some(col) = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(parent_table))
        .and_then(|t| {
            t.columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(parent_col))
        })
    else {
        // Parent table/column not present (forward reference) — the
        // deferred pass (`ensure_fk_parent_indexes`) covers it later.
        log::info!(
            "[AutoIndex] FK parent '{}.{}' not found yet; deferring parent-side index",
            db_name,
            parent_table
        );
        return false;
    };

    if column_is_indexed(db_name, parent_table, parent_col) {
        return false; // PK/UNIQUE/user index already covers the probe
    }

    let heap_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, parent_table));
    if !heap_path.exists() {
        log::warn!(
            "[AutoIndex] FK parent heap missing: {:?}; skipping parent-side index",
            heap_path
        );
        return false;
    }

    let index_name = format!("fkp_{}_{}", parent_table, col.name);
    let is_unique = col.constraints.unique;
    match create_index_with_flags(
        catalog,
        db_name,
        parent_table,
        &index_name,
        std::slice::from_ref(&col.name),
        is_unique,
        false,
    ) {
        Ok(count) => {
            log::info!(
                "[AutoIndex] Created parent-side FK index '{}.{}' ({} entries)",
                parent_table,
                col.name,
                count
            );
            true
        }
        Err(e) => {
            log::warn!(
                "[AutoIndex] Parent-side FK index on '{}.{}' failed: {} (FK checks fall back to heap scan)",
                parent_table,
                col.name,
                e
            );
            false
        }
    }
}

/// Index every column of `table_name` that other tables' FOREIGN KEYs
/// reference.
///
/// Called after CREATE TABLE so that a parent created AFTER its children
/// (`CREATE TABLE orders ... REFERENCES staff(id)` before `staff` exists)
/// still ends up with indexed FK targets — the deferred half of
/// [`ensure_parent_column_index`]. Safe to call any time: columns that
/// already have a leading index are skipped.
///
/// Returns the number of indexes created.
pub fn ensure_fk_parent_indexes(catalog: &Catalog, db_name: &str, table_name: &str) -> usize {
    let referencing =
        match crate::backend::constraint::loaders::load_referencing_foreign_keys(db_name, table_name) {
            Ok(fks) => fks,
            Err(e) => {
                log::warn!(
                    "[AutoIndex] Could not load referencing FKs for '{}.{}': {}",
                    db_name,
                    table_name,
                    e
                );
                return 0;
            }
        };

    let mut created = 0usize;
    // ReferencingFk = (child_table, child_col, parent_col, parent_table, action)
    for (_child_table, _child_col, parent_col, parent_table, _action) in &referencing {
        if ensure_parent_column_index(catalog, db_name, parent_table, parent_col) {
            created += 1;
        }
    }
    created
}

// ── DML Index Update Helpers ──────────────────────────────────────────────────

/// For a given table, discover all index files (named and legacy) and return
/// `Vec<(meta_path, idx_path, IndexMeta)>`.
fn discover_indexes_for_table(
    db_name: &str,
    table_name: &str,
) -> Vec<(PathBuf, PathBuf, IndexMeta)> {
    let mut discovered = Vec::new();

    // 1. Try loading all named indexes from sys_indexes
    if let Ok(indexes) = load_table_indexes(db_name, table_name) {
        for (idx_name, _col_name, _is_unique) in &indexes {
            let idx_path = index_file_path(db_name, table_name, idx_name);
            let meta_path = index_meta_file_path(db_name, table_name, idx_name);
            if idx_path.exists() && meta_path.exists() {
                if let Ok(meta) = load_index_meta(&meta_path) {
                    discovered.push((meta_path, idx_path, meta));
                } else {
                    log::warn!(
                        "[IndexDiscover] Failed to parse metadata for named index '{}'",
                        idx_name
                    );
                }
            }
        }
    }

    // 2. Fallback: check for legacy single-index file
    let legacy_idx = legacy_index_file_path(db_name, table_name);
    let legacy_meta = legacy_index_meta_file_path(db_name, table_name);
    if legacy_idx.exists() && legacy_meta.exists() {
        // Only add if not already covered by a named index (avoid duplicates)
        if !discovered.iter().any(|(_, p, _)| *p == legacy_idx)
            && let Ok(meta) = load_index_meta(&legacy_meta) {
                log::info!(
                    "[IndexDiscover] Found legacy index for {}.{} at {:?}",
                    db_name, table_name, legacy_idx
                );
                discovered.push((legacy_meta, legacy_idx, meta));
            }
    }

    discovered
}

type IndexList = std::sync::Arc<Vec<(PathBuf, PathBuf, IndexMeta)>>;
type DiscoveryCacheMap = std::sync::Mutex<std::collections::HashMap<(String, String), IndexList>>;

/// Memoised [`discover_indexes_for_table`] — one sys_indexes scan per
/// `(db, table)` instead of one per row. Invalidated on CREATE INDEX and
/// wherever system tables are rewritten (DDL).
fn discovery_cache() -> &'static DiscoveryCacheMap {
    static CACHE: std::sync::OnceLock<DiscoveryCacheMap> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn discover_cached(
    db_name: &str,
    table_name: &str,
) -> std::sync::Arc<Vec<(PathBuf, PathBuf, IndexMeta)>> {
    let key = (db_name.to_string(), table_name.to_string());
    if let Ok(map) = discovery_cache().lock()
        && let Some(hit) = map.get(&key) {
            return std::sync::Arc::clone(hit);
        }

    let discovered = std::sync::Arc::new(discover_indexes_for_table(db_name, table_name));
    if let Ok(mut map) = discovery_cache().lock() {
        map.insert(key, std::sync::Arc::clone(&discovered));
    }
    discovered
}

/// Forget memoised index discovery for a table (or all tables when `None`).
///
/// Call after any DDL that changes a table's index set.
pub fn invalidate_discovery(db_name: &str, table_name: Option<&str>) {
    let mut cleared_all = false;
    if let Ok(mut map) = discovery_cache().lock() {
        match table_name {
            Some(t) => {
                map.remove(&(db_name.to_string(), t.to_string()));
            }
            None => {
                map.clear();
                cleared_all = true;
            }
        }
    }
    let _ = cleared_all;
}

/// After a tuple is inserted into the heap, update all existing B+ Tree indexes
/// for the table.
///
/// Index discovery is memoised per `(db, table)` and B+ Tree handles are
/// process-cached with fsync batching (`backend::cache`), so the per-row cost
/// is just the key build + tree descent.
///
/// Returns `Ok(())` on success or if no indexes exist (no-op).
pub fn update_index_on_insert(
    db_name: &str,
    table_name: &str,
    values: &[&str],
    page_id: u32,
    slot_id: u32,
) -> crate::backend::error::RookResult<()> {
    let indexes = discover_cached(db_name, table_name);
    if indexes.is_empty() {
        return Ok(()); // No indexes to update
    }

    for (_meta_path, idx_path, meta) in indexes.iter() {
        // Build the (possibly composite) key from the raw string values.
        // Rows where ANY key component is NULL are not indexed.
        let Some(key_values) = build_key_from_strings(meta, values) else {
            log::trace!(
                "[IndexInsert] Skipping NULL/partial key for {}.{}({})",
                db_name, table_name, meta.columns().join(",")
            );
            continue;
        };

        let res = crate::backend::cache::with_btree(
            idx_path,
            || {
                open_btree_for_meta(idx_path, meta)
                    .ok_or_else(|| format!("Failed to open index {:?}", idx_path))
            },
            |bt| {
                bt.insert_keys(&key_values, page_id, slot_id)
                    .map_err(|e| format!("{}", e))
            },
        );

        if let Err(e) = res {
            log::warn!(
                "[IndexInsert] Failed to insert into index {:?}: {}, skipping",
                idx_path, e
            );
            continue;
        }

        log::trace!(
            "[IndexInsert] Inserted key {:?} → (page={}, slot={}) into index on {}.{}({})",
            key_values, page_id, slot_id, db_name, table_name, meta.columns().join(",")
        );
    }

    Ok(())
}

/// Build a composite key from a row's raw string values using `meta`'s column
/// layout. Returns `None` when any component is missing or NULL.
fn build_key_from_strings(meta: &IndexMeta, values: &[&str]) -> Option<Vec<DataValue>> {
    let types = meta.types();
    let idxs = meta.idxs();
    let mut key = Vec::with_capacity(idxs.len());
    for (i, &ci) in idxs.iter().enumerate() {
        let raw = values.get(ci)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
            return None;
        }
        let ty: DataType = types.get(i).and_then(|t| t.parse().ok())?;
        key.push(parse_string_to_value(&ty, trimmed).ok()?);
    }
    Some(key)
}

/// Build a composite key from an already-decoded row. Returns `None` when any
/// component is missing or NULL.
fn build_key_from_decoded(
    meta: &IndexMeta,
    decoded: &[Option<DataValue>],
) -> Option<Vec<DataValue>> {
    let idxs = meta.idxs();
    let mut key = Vec::with_capacity(idxs.len());
    for &ci in &idxs {
        match decoded.get(ci) {
            Some(Some(dv)) => key.push(dv.clone()),
            _ => return None,
        }
    }
    Some(key)
}

/// Open the B+Tree for `meta`, configuring its key types (composite-aware).
fn open_btree_for_meta(idx_path: &Path, meta: &IndexMeta) -> Option<BTree> {
    let mut btree = BTree::open(idx_path.to_path_buf()).ok()?;
    let cols = meta.columns();
    let types: Vec<DataType> = meta
        .types()
        .iter()
        .filter_map(|t| t.parse().ok())
        .collect();
    if !cols.is_empty() && types.len() == cols.len() {
        btree.set_key_types(types);
    } else {
        let t: DataType = meta.key_type.parse().ok()?;
        btree.set_key_type(t);
    }
    Some(btree)
}

/// After a tuple is deleted from the heap, update all existing B+ Tree indexes
/// for the table.
///
/// Discovers all indexes (named and legacy) via `sys_indexes` and file scanning,
/// then removes the specific entry matching `(page_id, slot_id)` from each one.
///
/// Returns `Ok(true)` if at least one index was updated, `Ok(false)` if no
/// indexes exist, or `Err` on failure.
pub fn update_index_on_delete(
    db_name: &str,
    table_name: &str,
    columns: &[crate::catalog::types::Column],
    tuple_data: &[u8],
    page_id: u32,
    slot_id: u32,
) -> crate::backend::error::RookResult<bool> {
    let indexes = discover_cached(db_name, table_name);
    if indexes.is_empty() {
        return Ok(false); // No indexes to update
    }

    // Pre-deserialize the tuple once for all indexes
    let schema_types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
    let decoded = crate::types::deserialize_nullable_row(&schema_types, tuple_data)
        .map_err(|e| format!("Failed to deserialize tuple for index update: {}", e))?;

    let mut any_updated = false;

    for (_meta_path, idx_path, meta) in indexes.iter() {
        // Build the (possibly composite) key from the deleted row. Rows with
        // ANY NULL key component were never indexed.
        let Some(key_values) = build_key_from_decoded(meta, &decoded) else {
            log::trace!(
                "[IndexDelete] Skipping NULL/partial key for {}.{}({})",
                db_name, table_name, meta.columns().join(",")
            );
            continue;
        };

        let del = crate::backend::cache::with_btree(
            idx_path,
            || {
                open_btree_for_meta(idx_path, meta)
                    .ok_or_else(|| format!("Failed to open index {:?}", idx_path))
            },
            |bt| {
                bt.delete_keys(&key_values, page_id, slot_id)
                    .map_err(|e| format!("{}", e))
            },
        );

        match del {
            Ok(true) => {
                any_updated = true;
                log::trace!(
                    "[IndexDelete] Removed key {:?} at (page={}, slot={}) from index on {}.{}({})",
                    key_values, page_id, slot_id, db_name, table_name, meta.columns().join(",")
                );
            }
            Ok(false) => {
                log::warn!(
                    "[IndexDelete] Key {:?} at (page={}, slot={}) not found in index on {}.{}({})",
                    key_values, page_id, slot_id, db_name, table_name, meta.columns().join(",")
                );
            }
            Err(e) => {
                log::warn!(
                    "[IndexDelete] Failed to delete from index {:?}: {}, skipping",
                    idx_path, e
                );
                continue;
            }
        }
    }

    Ok(any_updated)
}

/// After a tuple is updated, update all existing B+ Tree indexes for the table.
///
/// Discovers all indexes (named and legacy) via `sys_indexes` and file scanning,
/// then for each index:
///   - Deletes the old key matching `(old_page_id, old_slot_id)`
///   - Inserts the new key at `(new_page_id, new_slot_id)`
#[allow(clippy::too_many_arguments)]
pub fn update_index_on_update(
    db_name: &str,
    table_name: &str,
    columns: &[crate::catalog::types::Column],
    old_tuple_data: &[u8],
    new_tuple_data: &[u8],
    old_page_id: u32,
    old_slot_id: u32,
    new_page_id: u32,
    new_slot_id: u32,
) -> crate::backend::error::RookResult<()> {
    let indexes = discover_cached(db_name, table_name);
    if indexes.is_empty() {
        return Ok(()); // No indexes to update
    }

    // Pre-deserialize both tuples once
    let schema_types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let old_decoded = crate::types::deserialize_nullable_row(&schema_types, old_tuple_data)
        .map_err(|e| format!("Failed to deserialize old tuple: {}", e))?;
    let new_decoded = crate::types::deserialize_nullable_row(&schema_types, new_tuple_data)
        .map_err(|e| format!("Failed to deserialize new tuple: {}", e))?;

    for (_meta_path, idx_path, meta) in indexes.iter() {
        let old_key = build_key_from_decoded(meta, &old_decoded);
        let new_key = build_key_from_decoded(meta, &new_decoded);

        // If both keys are None (NULL / partial), skip this index
        if old_key.is_none() && new_key.is_none() {
            log::trace!(
                "[IndexUpdate] Both old and new keys are NULL for {}.{}({}), skipping index.",
                db_name, table_name, meta.columns().join(",")
            );
            continue;
        }

        let res = crate::backend::cache::with_btree(
            idx_path,
            || {
                open_btree_for_meta(idx_path, meta)
                    .ok_or_else(|| format!("Failed to open index {:?}", idx_path))
            },
            |btree| -> Result<(), String> {
                // Delete old key (if it was indexed), matching by the OLD heap location
                if let Some(ref old) = old_key {
                    btree.delete_keys(old, old_page_id, old_slot_id)
                        .map_err(|e| format!("Failed to delete old key from index: {}", e))?;
                    log::trace!(
                        "[IndexUpdate] Deleted old key {:?} at (page={}, slot={}) from index on {}.{}({})",
                        old, old_page_id, old_slot_id, db_name, table_name, meta.columns().join(",")
                    );
                }

                // Insert new key (if it's not NULL)
                if let Some(ref new) = new_key {
                    btree.insert_keys(new, new_page_id, new_slot_id)
                        .map_err(|e| format!("Failed to insert new key into index: {}", e))?;
                    log::trace!(
                        "[IndexUpdate] Inserted new key {:?} → (page={}, slot={}) into index on {}.{}({})",
                        new, new_page_id, new_slot_id, db_name, table_name, meta.columns().join(",")
                    );
                }
                Ok(())
            },
        );

        if let Err(e) = res {
            log::warn!("[IndexUpdate] Failed to update index {:?}: {}", idx_path, e);
        }
    }

    Ok(())
}

/// Parse a raw string value into a `DataValue` for the given `DataType`.
/// This mirrors the DataValue construction logic in `DataValue::parse_and_encode`
/// but returns a `DataValue` instead of encoded bytes.
pub fn parse_string_to_value(ty: &DataType, input: &str) -> Result<DataValue, String> {
    let input = input.trim().trim_matches('"').trim_matches('\'');

    match ty {
        DataType::SmallInt => input
            .parse::<i16>()
            .map(DataValue::SmallInt)
            .map_err(|e| format!("Invalid SMALLINT '{}': {}", input, e)),
        DataType::Int => input
            .parse::<i32>()
            .map(DataValue::Int)
            .map_err(|e| format!("Invalid INT '{}': {}", input, e)),
        DataType::BigInt => input
            .parse::<i64>()
            .map(DataValue::BigInt)
            .map_err(|e| format!("Invalid BIGINT '{}': {}", input, e)),
        DataType::Real => input
            .parse::<f32>()
            .map(|v| DataValue::Real(OrderedF32(v)))
            .map_err(|e| format!("Invalid REAL '{}': {}", input, e)),
        DataType::DoublePrecision => input
            .parse::<f64>()
            .map(|v| DataValue::DoublePrecision(OrderedF64(v)))
            .map_err(|e| format!("Invalid DOUBLE '{}': {}", input, e)),
        DataType::Bool => match input.to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(DataValue::Bool(true)),
            "false" | "f" | "0" => Ok(DataValue::Bool(false)),
            _ => Err(format!("Invalid BOOLEAN '{}': expected true/false", input)),
        },
        DataType::Char(_) | DataType::Character(_) => {
            Ok(DataValue::Char(input.to_string()))
        }
        DataType::Varchar(_) => {
            Ok(DataValue::Varchar(input.to_string()))
        }
        DataType::Date => {
            use chrono::NaiveDate;
            NaiveDate::parse_from_str(input, "%Y-%m-%d")
                .map(DataValue::Date)
                .map_err(|e| format!("Invalid DATE '{}': {}", input, e))
        }
        DataType::Time => {
            use chrono::NaiveTime;
            NaiveTime::parse_from_str(input, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(input, "%H:%M:%S"))
                .map(DataValue::Time)
                .map_err(|e| format!("Invalid TIME '{}': {}", input, e))
        }
        DataType::Timestamp => {
            use chrono::NaiveDateTime;
            NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S"))
                .map(DataValue::Timestamp)
                .map_err(|e| format!("Invalid TIMESTAMP '{}': {}", input, e))
        }
        DataType::Bit(_) => {
            let bits = crate::types::bit_utils::normalize_bit_literal(input);
            Ok(DataValue::Bit(bits))
        }
        DataType::Numeric { .. } | DataType::Decimal { .. } => {
            Err("Index maintenance for NUMERIC/DECIMAL types not yet supported".to_string())
        }
    }
}
