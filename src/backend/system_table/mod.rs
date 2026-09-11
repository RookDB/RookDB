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

use std::collections::HashMap;
use std::path::PathBuf;

use crate::catalog::types::{Catalog, Column, Constraints, Database, Table};
use crate::types::DataType;


use save::{populate_system_tables, strip_check_wrapper};

mod metadata;
mod save;
mod info_schema;
mod statistics;

pub use metadata::*;
pub use save::*;
pub use info_schema::*;
pub use statistics::*;

pub(crate) const SYS_DIR: &str = crate::layout::SYSTEM_DIR;
pub(crate) fn sys_path(table: &str) -> PathBuf {
    PathBuf::from(format!("{}/{}.dat", SYS_DIR, table))
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
/// Schema for `sys_statistics`: stat_id:INT, table_id:INT, column_name:VARCHAR(255),
/// total_count:BIGINT, null_count:BIGINT, distinct_count:BIGINT,
/// min_val:VARCHAR(255), max_val:VARCHAR(255), histogram_json:VARCHAR(8192)
pub const SYS_STATISTICS_SCHEMA: &[DataType] = &[
    DataType::Int,
    DataType::Int,
    DataType::Varchar(255),
    DataType::BigInt,
    DataType::BigInt,
    DataType::BigInt,
    DataType::Varchar(255),
    DataType::Varchar(255),
    DataType::Varchar(8192),
];

// ── String helpers ────────────────────────────────────────────────────────────
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
        crate::types::DataValue::Date(_) => format!("{:?}", dv),
        crate::types::DataValue::Time(_) => format!("{:?}", dv),
        crate::types::DataValue::Timestamp(_) => format!("{:?}", dv),
        crate::types::DataValue::Numeric(_) => format!("{:?}", dv),
        crate::types::DataValue::Bit(_) => format!("{:?}", dv),
    }
}
/// Format a `DataType` to its canonical string representation.
fn datatype_to_string(dt: &DataType) -> String {
    format!("{}", dt)
}

// ── Public API ────────────────────────────────────────────────────────────────
/// Bootstrap the system catalog from `catalog.json` if needed.
///
/// Returns `true` if a migration happened, `false` if system tables already
/// existed (or if no JSON catalog was found).
pub fn bootstrap_system_catalog() -> bool {
    let sys_dir = std::path::Path::new(SYS_DIR);
    let catalog_json = std::path::Path::new(crate::layout::CATALOG_FILE);

    // If system directory already exists with at least one table file, skip.
    if sys_dir.exists() && sys_path("databases").exists() {
        return false;
    }

    // No JSON catalog to bootstrap from — just create empty system tables.
    if !catalog_json.exists() {
        log::info!("[SystemCatalog] No catalog.json found; creating empty system tables.");
        create_system_table_files();
        return false;
    }

    log::info!("[SystemCatalog] Migrating catalog.json → system tables ...");

    // Read the existing JSON catalog.
    let catalog = crate::catalog::load_catalog();

    // Create system directory and heap files.
    std::fs::create_dir_all(SYS_DIR).ok();
    create_system_table_files();

    // Populate system tables from the in-memory catalog.
    populate_system_tables(&catalog, Vec::new(), Vec::new());

    // Rename catalog.json so we don't migrate again.
    let migrated_str = format!("{}.migrated", crate::layout::CATALOG_FILE);
    let _ = std::fs::rename(catalog_json, &migrated_str);

    log::info!("[SystemCatalog] Migration complete. catalog.json → system tables.");
    true
}
/// Load the full `Catalog` from system table heap files.
///
/// Scans each system table via direct disk reads and reconstructs the
/// in-memory `Catalog` hierarchy.
pub fn load_catalog_from_system() -> Catalog {
    let mut catalog = Catalog {
        databases: HashMap::new(),
    };

    let databases = match scan_system_table("databases", SYS_DATABASES_SCHEMA) {
        Ok(rows) => rows,
        Err(e) => {
            log::error!("[SystemCatalog] Failed to read sys_databases: {}", e);
            return catalog;
        }
    };

    let tables = match scan_system_table("tables", SYS_TABLES_SCHEMA) {
        Ok(rows) => rows,
        Err(e) => {
            log::error!("[SystemCatalog] Failed to read sys_tables: {}", e);
            return catalog;
        }
    };

    let columns = match scan_system_table("columns", SYS_COLUMNS_SCHEMA) {
        Ok(rows) => rows,
        Err(e) => {
            log::error!("[SystemCatalog] Failed to read sys_columns: {}", e);
            return catalog;
        }
    };

    let views_data = match scan_system_table("views", SYS_VIEWS_SCHEMA) {
        Ok(rows) => rows,
        Err(e) => {
            log::error!("[SystemCatalog] Failed to read sys_views: {}", e);
            return catalog;
        }
    };

    // Scan sys_constraints so per-column flags (NOT NULL / UNIQUE / CHECK)
    // can be restored onto the rebuilt `Column` structs below.
    //
    // CRITICAL FIX (see ANALYSIS.md Tier 1 #1): without this mapping every
    // Column was rebuilt with `Constraints::default()`, silently disabling
    // NOT NULL/UNIQUE validation after any process restart.
    let constraints = match scan_system_table("constraints", SYS_CONSTRAINTS_SCHEMA) {
        Ok(rows) => rows,
        Err(e) => {
            log::error!("[SystemCatalog] Failed to read sys_constraints: {}", e);
            Vec::new()
        }
    };
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
                let col_type = col_type_str.parse::<DataType>().unwrap_or(DataType::Varchar(255));
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
                            constraints.unique = true;
                        } else if constr_type.contains("NOT NULL") {
                            constraints.not_null = true;
                        } else if constr_type == "UNIQUE" {
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
                        && col.constraints.check.is_none() {
                            col.constraints.check = Some(expr);
                        }
                }
            }

            let table_cols: Vec<Column> = col_with_ordinals.into_iter().map(|(_, c)| c).collect();

            database
                .tables
                .insert(tbl_name, Table { columns: table_cols });
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
            database.views.insert(view_name.clone(), crate::catalog::types::ViewDef {
                query_json: view_query_json.clone(),
            });
        }

        catalog.databases.insert(db_name, database);
    }

    catalog
}
/// Internal helper to resolve (db_name, table_name) → (table_id, db_id).
pub fn resolve_table_id(db_name: &str, table_name: &str) -> std::io::Result<(i32, i32)> {
    let db_rows = scan_system_table("databases", SYS_DATABASES_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let db_id = db_rows.iter().find_map(|row| {
        let name = match row.get(1) {
            Some(Some(crate::types::DataValue::Varchar(n))) => n,
            Some(Some(crate::types::DataValue::Char(n))) => n,
            _ => return None,
        };
        if name.eq_ignore_ascii_case(db_name)
            && let Some(Some(crate::types::DataValue::Int(id))) = row.first() {
                return Some(*id);
            }
        None
    }).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound,
            format!("Database '{}' not found in sys_databases", db_name))
    })?;

    let tbl_rows = scan_system_table("tables", SYS_TABLES_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let table_id = tbl_rows.iter().find_map(|row| {
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
            && let Some(Some(crate::types::DataValue::Int(id))) = row.first() {
                return Some(*id);
            }
        None
    }).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound,
            format!("Table '{}.{}' not found in sys_tables", db_name, table_name))
    })?;

    Ok((table_id, db_id))
}

/// Check if the system tables exist and are accessible.
pub fn system_tables_exist() -> bool {
    let sys_dir = std::path::Path::new(SYS_DIR);
    sys_dir.exists()
        && sys_path("databases").exists()
        && sys_path("tables").exists()
        && sys_path("columns").exists()
        && sys_path("constraints").exists()
        && sys_path("indexes").exists()
        && sys_path("views").exists()
        && sys_path("statistics").exists()
}

// ── Internal helpers ──────────────────────────────────────────────────────────
/// Create empty system table heap files (with HeapManager).
fn create_system_table_files() {
    let _ = std::fs::create_dir_all(SYS_DIR);

    for name in &["databases", "tables", "columns", "constraints", "indexes", "views", "statistics"] {
        let path = sys_path(name);
        if !path.exists() {
            match crate::backend::heap::HeapManager::create(path.clone()) {
                Ok(mut hm) => {
                    if let Err(e) = hm.flush() {
                        log::warn!("[SystemCatalog] Failed to flush {}: {}", name, e);
                    }
                }
                Err(e) => {
                    log::error!("[SystemCatalog] Failed to create system table {}: {}", name, e);
                }
            }
        }
    }
}
/// Public wrapper: ensure all system table files exist, creating any that are
/// missing. Called during startup to support incremental schema additions
/// (e.g. adding `sys_views` or `sys_statistics` to an existing system catalog).
pub fn ensure_system_tables() {
    create_system_table_files();
}
/// Scan all rows from a system table and return decoded values.
pub(crate) fn scan_system_table(name: &str, schema: &[DataType]) -> Result<Vec<Vec<Option<crate::types::DataValue>>>, String> {
    let path = sys_path(name);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let heap = crate::backend::heap::HeapManager::open(path)
        .map_err(|e| format!("Failed to open {}: {}", name, e))?;

    let mut rows = Vec::new();
    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) = match result {
            Ok(triple) => triple,
            Err(e) => return Err(format!("Scan error on {}: {}", name, e)),
        };

        let decoded = crate::types::deserialize_nullable_row(schema, &raw_bytes)
            .map_err(|e| format!("Deserialize error on {}: {}", name, e))?;

        rows.push(decoded);
    }

    Ok(rows)
}
/// Insert multiple rows into a system table in a single batch.
///
/// Opens the HeapManager once, inserts all rows, and closes.
pub(crate) fn insert_system_rows(
    name: &str,
    schema: &[DataType],
    rows: &[Vec<Option<String>>],
) -> std::io::Result<()> {
    let path = sys_path(name);
    let mut heap = crate::backend::heap::HeapManager::open(path)?;

    for row in rows {
        // Convert Vec<Option<String>> into Vec<Option<&str>> for serialisation
        let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
        let tuple_bytes = crate::types::serialize_nullable_row(schema, &str_refs)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
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
        "statistics" => SYS_STATISTICS_SCHEMA,
        // Fallback: treat all columns as VARCHAR(255)
        _ => &[DataType::Varchar(255)],
    }
}