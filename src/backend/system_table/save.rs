//! Split of `system_table` — see `mod.rs` for the module overview.

use crate::catalog::types::Catalog;

use super::*;


/// Persist the in-memory catalog to system table heap files.
///
/// Deletes and recreates all system table files from scratch to prevent
/// duplicate rows on subsequent saves.
pub fn save_catalog_to_system(catalog: &Catalog) -> std::io::Result<()> {
    // 1. Load existing table_id → table_name mapping before deleting sys_tables
    let mut tbl_id_to_name: std::collections::HashMap<i32, String> = std::collections::HashMap::new();
    let tbl_path = sys_path("tables");
    if tbl_path.exists()
        && let Ok(heap) = crate::backend::heap::HeapManager::open(tbl_path) {
            for result in heap.scan() {
                if let Ok((_, _, raw_bytes)) = result
                    && let Ok(decoded) = crate::types::deserialize_nullable_row(SYS_TABLES_SCHEMA, &raw_bytes)
                        && decoded.len() >= 3
                            && let Some(Some(crate::types::DataValue::Int(tid))) = decoded.first() {
                                let name_opt = match decoded.get(2) {
                                    Some(Some(crate::types::DataValue::Varchar(name))) => Some(name.clone()),
                                    Some(Some(crate::types::DataValue::Char(name))) => Some(name.clone()),
                                    _ => None,
                                };
                                if let Some(name) = name_opt {
                                    tbl_id_to_name.insert(*tid, name);
                                }
                            }
            }
        }

    // 2. Load existing FOREIGN KEY constraints from sys_constraints before deleting it.
    let mut fk_rows = Vec::new();
    let constr_path = sys_path("constraints");
    if constr_path.exists() {
        if let Ok(heap) = crate::backend::heap::HeapManager::open(constr_path.clone()) {
            for result in heap.scan() {
                if let Ok((_, _, raw_bytes)) = result
                    && let Ok(decoded) = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)
                        && decoded.len() >= 6 {
                            let constr_type = match &decoded[2] {
                                Some(crate::types::DataValue::Varchar(s)) => s.as_str(),
                                Some(crate::types::DataValue::Char(s)) => s.as_str(),
                                _ => "",
                            };
                            if constr_type.to_uppercase().contains("FOREIGN KEY") {
                                // Resolve numeric table_id → table name for the CHILD table
                                let old_table_id = match &decoded[1] {
                                    Some(crate::types::DataValue::Int(id)) => *id,
                                    _ => 0,
                                };
                                let child_table_name = tbl_id_to_name.get(&old_table_id)
                                    .cloned()
                                    .unwrap_or_else(|| format!("<table_id={}>", old_table_id));

                                let constraint_type = decoded[2].as_ref().map(value_to_string);
                                let columns = decoded[3].as_ref().map(value_to_string);
                                let ref_table = decoded[4].as_ref().map(value_to_string);
                                let ref_columns = decoded[5].as_ref().map(value_to_string);

                                fk_rows.push(vec![
                                    None,                                      // constraint_id (auto-assigned)
                                    Some(format!("TABLE_NAME:{}", child_table_name)),  // resolved by name
                                    constraint_type,
                                    columns,
                                    ref_table,
                                    ref_columns,
                                ]);
                            }
                        }
            }
        }
    }

    // 3. Load existing statistics from sys_statistics before deleting it.
    let mut stat_rows = Vec::new();
    let stat_path = sys_path("statistics");
    if stat_path.exists()
        && let Ok(heap) = crate::backend::heap::HeapManager::open(stat_path.clone()) {
            for result in heap.scan() {
                if let Ok((_, _, raw_bytes)) = result
                    && let Ok(decoded) = crate::types::deserialize_nullable_row(SYS_STATISTICS_SCHEMA, &raw_bytes)
                        && decoded.len() >= 9 {
                            let old_table_id = match &decoded[1] {
                                Some(crate::types::DataValue::Int(id)) => *id,
                                _ => 0,
                            };
                            let tbl_name = tbl_id_to_name.get(&old_table_id)
                                .cloned()
                                .unwrap_or_else(|| format!("<table_id={}>", old_table_id));

                            stat_rows.push(vec![
                                decoded[0].as_ref().map(value_to_string),
                                Some(format!("TABLE_NAME:{}", tbl_name)),
                                decoded[2].as_ref().map(value_to_string),
                                decoded[3].as_ref().map(value_to_string),
                                decoded[4].as_ref().map(value_to_string),
                                decoded[5].as_ref().map(value_to_string),
                                decoded[6].as_ref().map(value_to_string),
                                decoded[7].as_ref().map(value_to_string),
                                decoded[8].as_ref().map(value_to_string),
                            ]);
                        }
            }
        }

    // Delete existing system table files to start fresh
    for name in &["databases", "tables", "columns", "constraints", "indexes", "views", "statistics"] {
        let path = sys_path(name);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        let fsm_path_str = format!("{}.fsm", path.to_string_lossy());
        let fsm_path = std::path::Path::new(&fsm_path_str);
        if fsm_path.exists() {
            std::fs::remove_file(fsm_path)?;
        }
    }
    // Create fresh files and populate
    create_system_table_files();
    populate_system_tables(catalog, fk_rows, stat_rows);
    // System tables rewritten wholesale: table_ids may have changed and
    // constraint/index rows were rebuilt — forget all memoised metadata.
    crate::backend::cache::invalidate_metadata();
    crate::backend::executor::create_index::invalidate_discovery("", None);
    Ok(())
}

/// Populate all system tables from an in-memory Catalog.
///
/// Writes databases, tables, columns, constraints, and (where possible)
/// indexes to their respective heap files.
pub(crate) fn populate_system_tables(
    catalog: &Catalog,
    mut constr_rows: Vec<Vec<Option<String>>>,
    mut stat_rows: Vec<Vec<Option<String>>>,
) {
    // Sequence counters for auto-generated IDs
    let mut next_db_id = 1i32;
    let mut next_table_id = 1i32;
    let mut next_col_id = 1i32;

    // Build a table_name → table_id mapping BEFORE processing FK rows.
    // This is populated during the table iteration below, then used to
    // resolve "TABLE_NAME:xxx" entries in constr_rows.
    let mut table_name_to_new_id: std::collections::HashMap<String, i32> = std::collections::HashMap::new();

    // First pass: collect table names and assign table_ids (needed for FK
    // name resolution).  The iteration order matches the main loop below
    // because no HashMap mutations happen between passes.
    //
    // DETERMINISM: databases and tables are iterated in sorted-name order so
    // that db_id/table_id assignment is stable across processes.  HashMap
    // iteration order is randomised per process, which previously produced
    // nondeterministic IDs (breaking FK resolution and reproducibility).
    for (_db_name, database) in sorted_databases(catalog) {
        let mut tbl_names: Vec<&String> = database.tables.keys().collect();
        tbl_names.sort();
        for tbl_name in tbl_names {
            let table_id = next_table_id;
            table_name_to_new_id.insert(tbl_name.clone(), table_id);
            next_table_id += 1;
        }
    }
    // Reset counter — the real table iteration below will re-assign the same IDs
    next_table_id = 1i32;

    // Compute the next available constraint_id from existing rows
    let mut next_constr_id = constr_rows.iter().fold(1i32, |max_id, row| {
        if let Some(Some(id_str)) = row.first()
            && let Ok(id) = id_str.parse::<i32>() {
                return std::cmp::max(max_id, id + 1);
            }
        max_id
    });

    // Assign real constraint_ids to FK rows that were preserved with None
    // (they were loaded from the previous save cycle with constraint_id=None
    //  because the numeric ID was tied to the old table iteration order).
    for row in &mut constr_rows {
        if !row.is_empty() && row[0].is_none() {
            row[0] = Some(next_constr_id.to_string());
            next_constr_id += 1;
        }
    }

    // Batch collections per system table
    let mut db_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut tbl_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut col_rows: Vec<Vec<Option<String>>> = Vec::new();

    for (db_name, database) in sorted_databases(catalog) {
        let db_id = next_db_id;
        next_db_id += 1;

        db_rows.push(vec![
            Some(db_id.to_string()),
            Some(db_name.clone()),
        ]);

        let mut tbl_names: Vec<&String> = database.tables.keys().collect();
        tbl_names.sort();
        for tbl_name in tbl_names {
            let table = &database.tables[tbl_name];
            let table_id = next_table_id;
            next_table_id += 1;

            let file_path = format!("database/base/{}/{}.dat", db_name, tbl_name);

            tbl_rows.push(vec![
                Some(table_id.to_string()),
                Some(db_id.to_string()),
                Some(tbl_name.clone()),
                Some(file_path),
            ]);

            for (ordinal, col) in table.columns.iter().enumerate() {
                let col_id = next_col_id;
                next_col_id += 1;

                let data_type_str = datatype_to_string(&col.data_type);
                let has_default = col.constraints.default.is_some();
                let default_value = col
                    .constraints
                    .default
                    .as_ref()
                    .map(value_to_string)
                    .unwrap_or_default();
                let nullable_str = if col.nullable { "true" } else { "false" };
                let has_default_str = if has_default { "true" } else { "false" };

                col_rows.push(vec![
                    Some(col_id.to_string()),
                    Some(table_id.to_string()),
                    Some(col.name.clone()),
                    Some(data_type_str),
                    Some(ordinal.to_string()),
                    Some(nullable_str.to_string()),
                    Some(has_default_str.to_string()),
                    Some(default_value),
                ]);

                // ── Emit constraint rows from per-column constraints ──
                if col.constraints.not_null {
                    constr_rows.push(vec![
                        Some(next_constr_id.to_string()),
                        Some(table_id.to_string()),
                        Some("NOT NULL".to_string()),
                        Some(col.name.clone()),
                        None,
                        None,
                    ]);
                    next_constr_id += 1;
                }
                if col.constraints.unique {
                    constr_rows.push(vec![
                        Some(next_constr_id.to_string()),
                        Some(table_id.to_string()),
                        Some("UNIQUE".to_string()),
                        Some(col.name.clone()),
                        None,
                        None,
                    ]);
                    next_constr_id += 1;
                }
                if let Some(ref check_expr) = col.constraints.check {
                    constr_rows.push(vec![
                        Some(next_constr_id.to_string()),
                        Some(table_id.to_string()),
                        Some("CHECK".to_string()),
                        Some(format!("CHECK({})", check_expr)),
                        None,
                        None,
                    ]);
                    next_constr_id += 1;
                }
            }
        }
    }

    // ── Resolve "TABLE_NAME:xxx" FK entries ────────────────────────────
    // FK rows preserved from the previous save cycle use the child table's
    // NAME (prefixed with "TABLE_NAME:") instead of a numeric table_id.  We
    // resolve them to the newly-assigned numeric table_ids here.
    for row in &mut constr_rows {
        if row.len() < 6 {
            continue;
        }
        if let Some(Some(table_id_str)) = &row.get(1).cloned()
            && let Some(table_name) = table_id_str.strip_prefix("TABLE_NAME:") {
                let resolved_id = table_name_to_new_id.get(table_name)
                    .cloned()
                    .unwrap_or(0);
                row[1] = if resolved_id != 0 {
                    Some(resolved_id.to_string())
                } else {
                    log::warn!(
                        "[SystemCatalog] Could not resolve table name '{}' for FK constraint, using table_id=0",
                        table_name
                    );
                    Some("0".to_string())
                };
            }
    }

    // ── Resolve "TABLE_NAME:xxx" statistics entries ───────────────────
    for row in &mut stat_rows {
        if row.len() < 9 {
            continue;
        }
        if let Some(Some(table_id_str)) = &row.get(1).cloned()
            && let Some(table_name) = table_id_str.strip_prefix("TABLE_NAME:") {
                let resolved_id = table_name_to_new_id.get(table_name)
                    .cloned()
                    .unwrap_or(0);
                row[1] = if resolved_id != 0 {
                    Some(resolved_id.to_string())
                } else {
                    Some("0".to_string())
                };
            }
    }

    // Batch-write each system table
    if let Err(e) = insert_system_rows("databases", SYS_DATABASES_SCHEMA, &db_rows) {
        log::error!("[SystemCatalog] Failed to write sys_databases: {}", e);
    }
    if let Err(e) = insert_system_rows("tables", SYS_TABLES_SCHEMA, &tbl_rows) {
        log::error!("[SystemCatalog] Failed to write sys_tables: {}", e);
    }
    if let Err(e) = insert_system_rows("columns", SYS_COLUMNS_SCHEMA, &col_rows) {
        log::error!("[SystemCatalog] Failed to write sys_columns: {}", e);
    }
    if let Err(e) = insert_system_rows("constraints", SYS_CONSTRAINTS_SCHEMA, &constr_rows) {
        log::error!("[SystemCatalog] Failed to write sys_constraints: {}", e);
    }
    if !stat_rows.is_empty()
        && let Err(e) = insert_system_rows("statistics", SYS_STATISTICS_SCHEMA, &stat_rows) {
            log::error!("[SystemCatalog] Failed to write sys_statistics: {}", e);
        }

    // ── Populate sys_indexes by scanning .idx.meta files on disk ────────
    // Build a table_name → table_id mapping for index rows.
    let mut table_name_to_id: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
    for row in &tbl_rows {
        // tbl_rows: [table_id, db_id, name, file_path]
        if let (Some(table_id_str), Some(table_name)) = (&row[0], &row[2])
            && let Ok(tid) = table_id_str.parse::<i32>() {
                table_name_to_id.insert(table_name.clone(), tid);
            }
    }
    // ── Populate sys_views ──────────────────────────────────────────────
    let mut view_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut next_view_id = 1i32;
    for (db_name, database) in sorted_databases(catalog) {
        let db_id = db_rows.iter().find_map(|row| {
            match (&row[0], &row[1]) {
                (Some(id_str), Some(name)) if name == db_name => id_str.parse::<i32>().ok(),
                _ => None,
            }
        }).unwrap_or(1);

        let mut view_names: Vec<&String> = database.views.keys().collect();
        view_names.sort();
        for view_name in view_names {
            let view_def = &database.views[view_name];
            view_rows.push(vec![
                Some(next_view_id.to_string()), // view_id
                Some(db_id.to_string()),        // db_id
                Some(view_name.clone()),        // name
                Some(view_def.query_json.clone()),   // query_json
            ]);
            next_view_id += 1;
        }
    }
    if let Err(e) = insert_system_rows("views", SYS_VIEWS_SCHEMA, &view_rows) {
        log::error!("[SystemCatalog] Failed to write sys_views: {}", e);
    }

    populate_indexes_from_meta(&table_name_to_id);

    let view_count: usize = catalog.databases.values().map(|d| d.views.len()).sum();
    log::info!(
        "[SystemCatalog] Populated system tables: {} databases, {} tables, {} constraints, {} views",
        catalog.databases.len(),
        {
            let mut count = 0;
            for db in catalog.databases.values() {
                count += db.tables.len();
            }
            count
        },
        next_constr_id - 1,
        view_count
    );
}

/// Scan `database/base/**/*.idx.meta` files and populate `sys_indexes`.
///
/// `table_name_to_id` maps table names to their assigned table IDs (from
/// `sys_tables`) so the index rows can reference the correct table.
fn populate_indexes_from_meta(table_name_to_id: &std::collections::HashMap<String, i32>) {
    let base_dir = std::path::Path::new(crate::layout::DATABASE_DIR);
    if !base_dir.exists() {
        return;
    }

    let mut idx_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut next_idx_id = 1i32;

    // Walk database/base/*/ looking for .idx.meta files
    let walker = match std::fs::read_dir(base_dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    for entry in walker.flatten() {
        let entry_path = entry.path();
        if !entry_path.is_dir() {
            continue;
        }

        // Look for *.idx.meta files in this database directory
        let meta_files = match std::fs::read_dir(&entry_path) {
            Ok(r) => r,
            Err(_) => continue,
        };

        for meta_entry in meta_files.flatten() {
            let meta_path = meta_entry.path();
            let fname = meta_path.to_string_lossy();
            if !fname.ends_with(".idx.meta") {
                continue;
            }

            // Extract table name and index name from the .idx.meta filename.
            //
            // Named format:  {table}.{index_name}.idx.meta
            //   e.g. "users.idx_users_name.idx.meta" → stem = "users.idx_users_name.idx"
            //   strip ".idx" → "users.idx_users_name" → split on first '.' → table="users", index="idx_users_name"
            //
            // Legacy format: {table}.idx.meta
            //   e.g. "users.idx.meta" → stem = "users.idx" → strip ".idx" → "users"
            //   no '.' in the result → legacy, index_name = "idx_{table}_{column}"
            let filename = match meta_path.file_stem() {
                Some(s) => s.to_string_lossy().to_string(),
                None => continue,
            };
            let stripped = filename.strip_suffix(".idx").unwrap_or(&filename).to_string();

            // Determine if this is a named or legacy format by checking for extra dots
            let (table_name, index_name) = if stripped.contains('.') {
                // Named format: "table.index_name" → split at first dot
                if let Some(dot_pos) = stripped.find('.') {
                    let tbl = stripped[..dot_pos].to_string();
                    let idx = stripped[dot_pos + 1..].to_string();
                    (tbl, idx)
                } else {
                    (stripped.clone(), format!("idx_{}", stripped))
                }
            } else {
                // Legacy format: just "table" → index_name = "idx_{table}_{column}"
                // We'll read the meta to get the column name later
                (stripped.clone(), String::new())
            };

            // Resolve table_id from the mapping
            let table_id = match table_name_to_id.get(&table_name) {
                Some(id) => *id,
                None => {
                    log::warn!("[SystemCatalog] Index meta references unknown table '{}', skipping", table_name);
                    continue;
                }
            };

            // Read and parse the metadata JSON
            let meta_content = match std::fs::read_to_string(&meta_path) {
                Ok(c) => c,
                Err(_) => continue,
            };

            // Parse IndexMeta from the metadata file.
            //
            // The flags (`is_unique` / `is_primary`) and the full composite
            // `column_names` list are optional: older .idx.meta files predate
            // them. Missing flags default to `false` — a plain index must NOT
            // enforce uniqueness, so defaulting to `true` (the old behaviour)
            // wrongly made every rebuilt index a UNIQUE constraint.
            #[derive(serde::Deserialize)]
            #[allow(dead_code)]
            struct IndexMeta {
                column_name: String,
                #[serde(default)]
                column_idx: usize,
                #[serde(default)]
                key_type: String,
                #[serde(default)]
                column_names: Vec<String>,
                #[serde(default)]
                is_unique: bool,
                #[serde(default)]
                is_primary: bool,
            }

            let meta: IndexMeta = match serde_json::from_str(&meta_content) {
                Ok(m) => m,
                Err(_) => continue,
            };

            // Full key layout, normalising legacy single-column files.
            let mut columns = if meta.column_names.is_empty() {
                vec![meta.column_name.clone()]
            } else {
                meta.column_names.clone()
            };
            // sys_indexes.columns is VARCHAR(255): truncate instead of failing
            // the whole save if a very wide composite key does not fit.
            let columns_field = {
                let joined = columns.join(",");
                if joined.len() > 255 {
                    log::warn!(
                        "[SystemCatalog] Index column list {:?} exceeds 255 bytes; truncating",
                        joined
                    );
                    columns.truncate(1);
                    columns[0].clone()
                } else {
                    joined
                }
            };

            // Compute the index name: use parsed name if available, otherwise generate from column
            let idx_name = if !index_name.is_empty() {
                index_name
            } else {
                format!("idx_{}_{}", table_name, meta.column_name)
            };

            // SYS_INDEXES_SCHEMA: [index_id:INT, table_id:INT, name:VARCHAR(255),
            //                     is_unique:BOOL, is_primary:BOOL, columns:VARCHAR(255)]
            idx_rows.push(vec![
                Some(next_idx_id.to_string()),           // index_id
                Some(table_id.to_string()),              // table_id
                Some(idx_name),                          // name
                Some(meta.is_unique.to_string()),        // is_unique (persisted in .idx.meta)
                Some(meta.is_primary.to_string()),       // is_primary
                Some(columns_field),                     // columns (full composite list)
            ]);
            next_idx_id += 1;
        }
    }

    if !idx_rows.is_empty()
        && let Err(e) = insert_system_rows("indexes", SYS_INDEXES_SCHEMA, &idx_rows) {
            log::error!("[SystemCatalog] Failed to write sys_indexes: {}", e);
        }
}

/// Strip the `CHECK(...)` wrapper from a sys_constraints `columns` field.
///
/// Handles both `"CHECK(expr)"` and bare `"expr"` formats (matching the
/// parser used by the constraint loaders).
pub(crate) fn strip_check_wrapper(columns_field: &str) -> Option<String> {
    if let Some(inner) = columns_field.strip_prefix("CHECK(") {
        if let Some(end) = inner.rfind(')') {
            return Some(inner[..end].to_string());
        }
        return Some(inner.to_string());
    }
    if columns_field.is_empty() {
        None
    } else {
        Some(columns_field.to_string())
    }
}
