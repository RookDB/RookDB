//! System Tables — Physical catalog stored as standard heap files.
//!
//! Step 8 of the SQL-99 roadmap replaces `database/global/catalog.json` with
//! proper slotted-page heap files under `database/system/`. Each system table
//! is a normal `.dat` file with an `.fsm` fork, managed via `HeapManager`.
//!
//! Module layout (split from the original single 2000-line file):
//! - `mod.rs`        — schemas, bootstrap, load, shared scan/insert helpers
//! - `save.rs`       — `save_catalog_to_system` + populate passes
//! - `metadata.rs`   — constraint/index metadata writers and deleters
//! - `info_schema.rs`— INFORMATION_SCHEMA view schemas
//!
//! Public paths are unchanged: everything is re-exported from `mod.rs`.

use crate::backend::error::{RookError, RookResult};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::catalog::types::{Catalog, Column, Constraints, Database, Table};
use crate::types::DataType;

use save::strip_check_wrapper;

mod info_schema;
mod metadata;
mod save;

pub use info_schema::*;
pub use metadata::*;
pub use save::*;

pub(crate) fn sys_path(engine: &crate::backend::engine::Engine, table: &str) -> PathBuf {
    engine.paths().system_table(table)
}

// ── Column schemas for each system table ──────────────────────────────────────
// These match the physical column order used for serialisation.
/// Schema for `sys_databases`: db_id:INT, name:VARCHAR(255)
pub const SYS_DATABASES_SCHEMA: &[DataType] = &[DataType::Int, DataType::Varchar(255)];
/// Schema for `sys_tables`: table_id:INT, db_id:INT, name:VARCHAR(255), file_path:VARCHAR(512)
pub const SYS_TABLES_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(255),
    DataType::Varchar(512),
];
/// Schema for `sys_columns`: col_id:INT, table_id:INT, name:VARCHAR(255),
/// data_type:VARCHAR(100), ordinal:INT, nullable:BOOL, has_default:BOOL,
/// default_value:VARCHAR(255)
pub const SYS_COLUMNS_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(255),
    DataType::Varchar(100),
    DataType::Int,
    DataType::Bool,
    DataType::Bool,
    DataType::Varchar(255),
];
/// Schema for `sys_constraints`: constraint_id:INT, table_id:INT,
/// constraint_type:VARCHAR(50), columns:TEXT, ref_table:VARCHAR(255),
/// ref_columns:TEXT
pub const SYS_CONSTRAINTS_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(50),
    DataType::Varchar(1024),
    DataType::Varchar(255),
    DataType::Varchar(1024),
];
/// Schema for `sys_indexes`: index_id:INT, table_id:INT, name:VARCHAR(255),
/// is_unique:BOOL, is_primary:BOOL, columns:VARCHAR(255)
pub const SYS_INDEXES_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(255),
    DataType::Bool,
    DataType::Bool,
    DataType::Varchar(255),
];
/// Schema for `sys_views`: view_id:INT, db_id:INT, name:VARCHAR(255),
/// query_json:TEXT
pub const SYS_VIEWS_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(255),
    DataType::Varchar(8192),
];

// ── String helpers ────────────────────────────────────────────────────────────
/// The columns of a stored column list (`columns` / `ref_columns` of
/// `sys_constraints`). A lone column is just its name, so rows written for
/// single-column keys read back unchanged.
pub fn split_column_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect()
}

/// The stored form of a column list: names joined by `", "`.
pub fn join_column_list(columns: &[String]) -> String {
    columns.join(", ")
}

/// Convert a `DataValue` to its string representation for storage in system
/// table VARCHAR columns.
fn value_to_string(dv: &crate::types::DataValue) -> String {
    match dv {
        crate::types::DataValue::SmallInt(v) => v.to_string(),
        crate::types::DataValue::Int(v) => v.to_string(),
        crate::types::DataValue::BigInt(v) => v.to_string(),
        crate::types::DataValue::Real(v) => v.0.to_string(),
        crate::types::DataValue::DoublePrecision(v) => v.0.to_string(),
        crate::types::DataValue::Bool(v) => v.to_string(),
        crate::types::DataValue::Char(v) | crate::types::DataValue::Varchar(v) => v.clone(),
        crate::types::DataValue::Date(_) => format!("{}", dv),
        crate::types::DataValue::Time(_) => format!("{}", dv),
        crate::types::DataValue::Timestamp(_) => format!("{}", dv),
        crate::types::DataValue::Numeric(_) => format!("{}", dv),
        crate::types::DataValue::Bit(_) => format!("{}", dv),
    }
}
/// Format a `DataType` to its canonical string representation.
fn datatype_to_string(dt: &DataType) -> String {
    format!("{}", dt)
}

// ── Public API ────────────────────────────────────────────────────────────────
/// Load the full `Catalog` from system table heap files.
///
/// Scans each system table via direct disk reads and reconstructs the
/// in-memory `Catalog` hierarchy. An unreadable system table is an error,
/// not an empty catalog: the next DDL would save that empty catalog over
/// the real one and orphan every table file.
pub fn load_catalog_from_system(
    engine: &std::sync::Arc<crate::backend::engine::Engine>,
) -> RookResult<Catalog> {
    recover_system_dir(engine.paths())?;
    let mut catalog = Catalog::new(std::sync::Arc::clone(engine));

    let databases = scan_system_table(engine, "databases", SYS_DATABASES_SCHEMA)?;
    let tables = scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?;
    let columns = scan_system_table(engine, "columns", SYS_COLUMNS_SCHEMA)?;
    let views_data = scan_system_table(engine, "views", SYS_VIEWS_SCHEMA)?;
    // Per-column flags (NOT NULL / UNIQUE / CHECK) are restored from here.
    let constraints = scan_system_table(engine, "constraints", SYS_CONSTRAINTS_SCHEMA)?;
    // table_id → [(constraint_type, columns_field)]
    let mut constraints_by_table: std::collections::HashMap<i32, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for row in &constraints {
        if row.len() < 4 {
            continue;
        }
        let table_id = match &row[1] {
            Some(crate::types::DataValue::Int(id)) => *id,
            _ => continue,
        };
        let constr_type = match &row[2] {
            Some(crate::types::DataValue::Varchar(s)) => s.to_ascii_uppercase(),
            Some(crate::types::DataValue::Char(s)) => s.to_ascii_uppercase(),
            _ => continue,
        };
        let columns_field = match &row[3] {
            Some(crate::types::DataValue::Varchar(s)) => s.clone(),
            Some(crate::types::DataValue::Char(s)) => s.clone(),
            _ => continue,
        };
        // FOREIGN KEY rows are loaded live by the constraint module and are
        // not represented as column flags — skip them here.
        if constr_type.contains("FOREIGN KEY") {
            continue;
        }
        constraints_by_table
            .entry(table_id)
            .or_default()
            .push((constr_type, columns_field));
    }

    // Build the in-memory catalog
    for db_row in &databases {
        // db_row columns: db_id, name
        let db_name = match &db_row.get(1) {
            Some(Some(crate::types::DataValue::Varchar(name))) => name.clone(),
            Some(Some(crate::types::DataValue::Char(name))) => name.clone(),
            _ => continue,
        };

        let db_id = match &db_row.first() {
            Some(Some(crate::types::DataValue::Int(id))) => *id,
            _ => continue,
        };

        let mut database = Database {
            tables: HashMap::new(),
            views: HashMap::new(),
        };

        // Find all tables belonging to this database
        for tbl_row in &tables {
            let tbl_db_id = match &tbl_row.get(1) {
                Some(Some(crate::types::DataValue::Int(id))) => *id,
                _ => continue,
            };

            if tbl_db_id != db_id {
                continue;
            }

            let tbl_name = match &tbl_row.get(2) {
                Some(Some(crate::types::DataValue::Varchar(name))) => name.clone(),
                Some(Some(crate::types::DataValue::Char(name))) => name.clone(),
                _ => continue,
            };

            let table_id = match &tbl_row.first() {
                Some(Some(crate::types::DataValue::Int(id))) => *id,
                _ => continue,
            };

            // Collect columns with their ordinal positions, then sort
            let mut col_with_ordinals: Vec<(i32, Column)> = Vec::new();
            for col_row in &columns {
                let col_table_id = match &col_row.get(1) {
                    Some(Some(crate::types::DataValue::Int(id))) => *id,
                    _ => continue,
                };
                if col_table_id != table_id {
                    continue;
                }
                let col_ordinal = match &col_row.get(4) {
                    Some(Some(crate::types::DataValue::Int(o))) => *o,
                    _ => continue,
                };
                let col_name = match &col_row.get(2) {
                    Some(Some(crate::types::DataValue::Varchar(n))) => n.clone(),
                    Some(Some(crate::types::DataValue::Char(n))) => n.clone(),
                    _ => continue,
                };
                let col_type_str = match &col_row.get(3) {
                    Some(Some(crate::types::DataValue::Varchar(s))) => s.clone(),
                    Some(Some(crate::types::DataValue::Char(s))) => s.clone(),
                    _ => continue,
                };
                let col_type = col_type_str
                    .parse::<DataType>()
                    .unwrap_or(DataType::Varchar(255));
                let col_nullable = match &col_row.get(5) {
                    Some(Some(crate::types::DataValue::Bool(v))) => *v,
                    _ => true,
                };

                // Restore DEFAULT metadata (sys_columns idx 6/7 are written
                // during save but were never read back on load).
                let has_default = match &col_row.get(6) {
                    Some(Some(crate::types::DataValue::Bool(v))) => *v,
                    _ => false,
                };
                let default_value_str = match &col_row.get(7) {
                    Some(Some(crate::types::DataValue::Varchar(s))) => s.clone(),
                    Some(Some(crate::types::DataValue::Char(s))) => s.clone(),
                    _ => String::new(),
                };
                let parsed_default = if has_default && !default_value_str.is_empty() {
                    crate::backend::executor::create_index::parse_string_to_value(
                        &col_type,
                        &default_value_str,
                    )
                    .ok()
                } else {
                    None
                };

                // Restore per-column constraint flags from sys_constraints.
                let mut constraints = Constraints {
                    not_null: false,
                    unique: false,
                    default: parsed_default,
                    check: None,
                };
                if let Some(rows) = constraints_by_table.get(&table_id) {
                    for (constr_type, columns_field) in rows {
                        // NOT NULL / UNIQUE / PRIMARY KEY store the column
                        // name (possibly comma-separated) in the columns
                        // field; match this column case-insensitively.
                        let applies = columns_field
                            .split(',')
                            .map(|s| s.trim())
                            .any(|c| c.eq_ignore_ascii_case(&col_name));
                        if !applies {
                            continue;
                        }
                        if constr_type.contains("PRIMARY KEY") {
                            constraints.not_null = true;
                            if !columns_field.contains(',') {
                                constraints.unique = true;
                            }
                        } else if constr_type.contains("NOT NULL") {
                            constraints.not_null = true;
                        } else if constr_type == "UNIQUE" && !columns_field.contains(',') {
                            constraints.unique = true;
                        }
                    }
                }

                // NOT NULL is enforced via the `nullable` flag; keep both in
                // sync so validation works identically before/after reload.
                let effective_nullable = col_nullable && !constraints.not_null;

                col_with_ordinals.push((
                    col_ordinal,
                    Column {
                        name: col_name,
                        data_type: col_type,
                        nullable: effective_nullable,
                        constraints,
                    },
                ));
            }
            col_with_ordinals.sort_by_key(|(ord, _)| *ord);

            // Attach CHECK constraints to their owning column.
            //
            // The stored format ("CHECK(expr)") does not record which column
            // the constraint was declared on, so re-attach each expression to
            // the first column whose name appears in it (falling back to the
            // table's first column).  `Constraints.check` is only used to
            // re-emit sys_constraints rows on save — actual CHECK validation
            // reads sys_constraints live — so the attachment choice is stable
            // across save/load cycles.
            if let Some(rows) = constraints_by_table.get(&table_id) {
                for (constr_type, columns_field) in rows {
                    if constr_type != "CHECK" {
                        continue;
                    }
                    let Some(expr) = strip_check_wrapper(columns_field) else {
                        continue;
                    };
                    let owner = col_with_ordinals
                        .iter()
                        .position(|(_, c)| expr.contains(&c.name))
                        .unwrap_or(0);
                    if let Some((_, col)) = col_with_ordinals.get_mut(owner)
                        && col.constraints.check.is_none()
                    {
                        col.constraints.check = Some(expr);
                    }
                }
            }

            let table_cols: Vec<Column> = col_with_ordinals.into_iter().map(|(_, c)| c).collect();

            database.tables.insert(
                tbl_name,
                Table {
                    columns: table_cols,
                },
            );
        }

        // Load views for this database
        for view_row in &views_data {
            let view_db_id = match &view_row.get(1) {
                Some(Some(crate::types::DataValue::Int(id))) => *id,
                _ => continue,
            };
            if view_db_id != db_id {
                continue;
            }
            let view_name = match &view_row.get(2) {
                Some(Some(crate::types::DataValue::Varchar(n))) => n,
                Some(Some(crate::types::DataValue::Char(n))) => n,
                _ => continue,
            };
            let view_query_json = match &view_row.get(3) {
                Some(Some(crate::types::DataValue::Varchar(q))) => q,
                Some(Some(crate::types::DataValue::Char(q))) => q,
                _ => continue,
            };
            database.views.insert(
                view_name.clone(),
                crate::catalog::types::ViewDef {
                    query_json: view_query_json.clone(),
                },
            );
        }

        catalog.databases.insert(db_name, database);
    }

    Ok(catalog)
}
/// Internal helper to resolve (db_name, table_name) → (table_id, db_id).
pub fn resolve_table_id(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> RookResult<(i32, i32)> {
    let db_rows = scan_system_table(engine, "databases", SYS_DATABASES_SCHEMA)?;
    let db_id = db_rows
        .iter()
        .find_map(|row| {
            let name = match row.get(1) {
                Some(Some(crate::types::DataValue::Varchar(n))) => n,
                Some(Some(crate::types::DataValue::Char(n))) => n,
                _ => return None,
            };
            if name.eq_ignore_ascii_case(db_name)
                && let Some(Some(crate::types::DataValue::Int(id))) = row.first()
            {
                return Some(*id);
            }
            None
        })
        .ok_or_else(|| {
            RookError::Internal(format!("Database '{}' not found in sys_databases", db_name))
        })?;

    let tbl_rows = scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?;
    let table_id = tbl_rows
        .iter()
        .find_map(|row| {
            let tbl_db_id = match row.get(1) {
                Some(Some(crate::types::DataValue::Int(id))) => *id,
                _ => return None,
            };
            if tbl_db_id != db_id {
                return None;
            }
            let name = match row.get(2) {
                Some(Some(crate::types::DataValue::Varchar(n))) => n,
                Some(Some(crate::types::DataValue::Char(n))) => n,
                _ => return None,
            };
            if name.eq_ignore_ascii_case(table_name)
                && let Some(Some(crate::types::DataValue::Int(id))) = row.first()
            {
                return Some(*id);
            }
            None
        })
        .ok_or_else(|| {
            RookError::Internal(format!(
                "Table '{}.{}' not found in sys_tables",
                db_name, table_name
            ))
        })?;

    Ok((table_id, db_id))
}

// ── Internal helpers ──────────────────────────────────────────────────────────
/// Every system table, by file stem.
const SYSTEM_TABLES: [&str; 6] = [
    "databases",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
];

/// Create empty system table heap files (with HeapManager).
fn create_system_table_files(engine: &crate::backend::engine::Engine) -> RookResult<()> {
    std::fs::create_dir_all(engine.paths().system_dir())?;

    for name in SYSTEM_TABLES {
        let path = sys_path(engine, name);
        if !path.exists() {
            crate::backend::heap::HeapManager::create(path.clone())
                .and_then(|mut hm| hm.flush())
                .map_err(|e| e.with_context(format!("Failed to create system table {}", name)))?;
        }
    }
    Ok(())
}
/// Public wrapper: ensure all system table files exist, creating any that are
/// missing. Called during startup to support incremental schema additions
/// (e.g. adding `sys_views` to an existing system catalog).
pub fn ensure_system_tables(engine: &crate::backend::engine::Engine) -> RookResult<()> {
    recover_system_dir(engine.paths())?;
    create_system_table_files(engine)
}

/// fsync a directory, making the creates and renames inside it durable.
fn sync_dir(dir: &std::path::Path) -> RookResult<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Finish or roll back a catalog save that a crash interrupted.
///
/// A save builds `system.new`, fsyncs it, then renames `system` to
/// `system.old` and `system.new` to `system`. A save only starts once
/// `system` exists, so:
/// - `system` present: it is the committed catalog; any `system.new`
///   is unfinished or was never swapped in, and any `system.old` is stale.
/// - `system` missing: the crash fell between the two renames, so
///   `system.new` is complete and is the catalog. Without one, `system.old`
///   goes back.
fn recover_system_dir(paths: &crate::backend::paths::Paths) -> RookResult<()> {
    let system = paths.system_dir();
    let staging = paths.system_staging_dir();
    let old = paths.system_old_dir();
    if !system.exists() {
        let committed = if staging.exists() { &staging } else { &old };
        if !committed.exists() {
            return Ok(());
        }
        log::warn!(
            "[SystemCatalog] recovering an interrupted catalog save from {}",
            committed.display()
        );
        std::fs::rename(committed, &system)?;
        sync_dir(&paths.data_dir())?;
    }
    for leftover in [&staging, &old] {
        if leftover.exists() {
            std::fs::remove_dir_all(leftover)?;
        }
    }
    Ok(())
}

/// Write `rows` as a fresh, fsynced system table `name` under `dir`.
///
/// The file bypasses the engine's handle cache: it is about to be renamed,
/// and a cached handle would outlive its path.
fn write_system_table(
    dir: &std::path::Path,
    name: &str,
    schema: &[DataType],
    rows: &[Vec<Option<String>>],
) -> RookResult<()> {
    let path = dir.join(format!("{}.dat", name));
    let write = || -> RookResult<()> {
        let mut heap = crate::backend::heap::HeapManager::create(path.clone())?;
        for row in rows {
            let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
            let tuple_bytes = crate::types::serialize_nullable_row(schema, &str_refs)?;
            heap.insert_tuple(&tuple_bytes)?;
        }
        heap.flush()
    };
    let written = write();
    crate::backend::buffer_manager::shared_pool::invalidate(&path);
    written.map_err(|e| e.with_context(format!("Failed to write sys_{}", name)))
}
/// Scan all rows from a system table and return decoded values.
fn scan_system_table(
    engine: &crate::backend::engine::Engine,
    name: &str,
    schema: &[DataType],
) -> RookResult<Vec<Vec<Option<crate::types::DataValue>>>> {
    let path = sys_path(engine, name);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let heap = crate::backend::cache::lease_heap(engine, &path)
        .map_err(|e| RookError::storage(format!("Failed to open {}", name), e))?;

    let mut rows = Vec::new();
    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) = match result {
            Ok(triple) => triple,
            Err(e) => return Err(RookError::storage(format!("Scan error on {}", name), e)),
        };

        let decoded = crate::types::deserialize_nullable_row(schema, &raw_bytes)
            .map_err(|e| e.with_context(format!("Deserialize error on {}", name)))?;

        rows.push(decoded);
    }

    Ok(rows)
}
/// Insert multiple rows into a system table in a single batch.
///
/// Opens the HeapManager once, inserts all rows, and closes.
fn insert_system_rows(
    engine: &crate::backend::engine::Engine,
    name: &str,
    schema: &[DataType],
    rows: &[Vec<Option<String>>],
) -> RookResult<()> {
    let path = sys_path(engine, name);
    let mut heap = crate::backend::cache::lease_heap(engine, &path)?;

    for row in rows {
        // Convert Vec<Option<String>> into Vec<Option<&str>> for serialisation
        let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
        let tuple_bytes = crate::types::serialize_nullable_row(schema, &str_refs)?;
        heap.insert_tuple(&tuple_bytes)?;
    }

    heap.flush()?;
    Ok(())
}
/// Iterate a catalog's databases in deterministic (sorted-by-name) order.
///
/// HashMap iteration order is randomised per process; sorting by name keeps
/// generated IDs stable across save/load cycles and process restarts.
fn sorted_databases(catalog: &Catalog) -> Vec<(&String, &Database)> {
    let mut entries: Vec<(&String, &Database)> = catalog.databases.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
}
/// Return the physical `DataType` schema for a system table by name.
///
/// Used by the physical planner's `plan_table_scan` to correctly deserialise
/// system table heap file tuples using the actual on-disk column types rather
/// than the VARCHAR-heavy INFORMATION_SCHEMA view schema.
pub fn system_table_schema(name: &str) -> &'static [DataType] {
    match name {
        "databases" => SYS_DATABASES_SCHEMA,
        "tables" => SYS_TABLES_SCHEMA,
        "columns" => SYS_COLUMNS_SCHEMA,
        "constraints" => SYS_CONSTRAINTS_SCHEMA,
        "indexes" => SYS_INDEXES_SCHEMA,
        "views" => SYS_VIEWS_SCHEMA,
        // Fallback: treat all columns as VARCHAR(255)
        _ => &[DataType::Varchar(255)],
    }
}
