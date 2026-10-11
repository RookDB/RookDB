//! Split of `system_table` — see `mod.rs` for the module overview.

use crate::catalog::types::Catalog;

use super::*;

/// Persist the in-memory catalog to system table heap files.
///
/// Deletes and recreates all system table files from scratch to prevent
/// duplicate rows on subsequent saves.
pub fn save_catalog_to_system(catalog: &Catalog) -> std::io::Result<()> {
    // 1. Load existing FOREIGN KEY constraints from sys_constraints before deleting it.
    //
    // CRITICAL: FK rows store the child table's numeric `table_id`.  After system tables
    // are rebuilt below, `populate_system_tables()` assigns new table_ids by iterating
    // a HashMap — whose iteration order is NOT deterministic (SwissTable random seed).
    // To prevent FK rows from pointing to wrong tables, we resolve the numeric table_id
    // to the table NAME before deletion, then re-resolve to the new table_id after rebuild.
    let mut fk_rows = Vec::new();
    let constr_path = sys_path(catalog.engine(), "constraints");
    if constr_path.exists() {
        // Also load sys_tables to resolve table_id → table_name
        let mut tbl_id_to_name: std::collections::HashMap<i32, String> =
            std::collections::HashMap::new();
        let tbl_path = sys_path(catalog.engine(), "tables");
        if tbl_path.exists()
            && let Ok(heap) = crate::backend::heap::HeapManager::open(tbl_path)
        {
            for result in heap.scan() {
                if let Ok((_, _, raw_bytes)) = result
                    && let Ok(decoded) =
                        crate::types::deserialize_nullable_row(SYS_TABLES_SCHEMA, &raw_bytes)
                    && decoded.len() >= 3
                    && let Some(Some(crate::types::DataValue::Int(tid))) = decoded.first()
                {
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

        if let Ok(heap) = crate::backend::heap::HeapManager::open(constr_path.clone()) {
            for result in heap.scan() {
                if let Ok((_, _, raw_bytes)) = result
                    && let Ok(decoded) =
                        crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)
                    && decoded.len() >= 6
                {
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
                        let child_table_name = tbl_id_to_name
                            .get(&old_table_id)
                            .cloned()
                            .unwrap_or_else(|| format!("<table_id={}>", old_table_id));

                        let constraint_type = decoded[2].as_ref().map(value_to_string);
                        let columns = decoded[3].as_ref().map(value_to_string);
                        let ref_table = decoded[4].as_ref().map(value_to_string);
                        let ref_columns = decoded[5].as_ref().map(value_to_string);

                        // Store CHILD TABLE NAME as the "table_id" field.
                        // After rebuilding, populate_system_tables will resolve
                        // this name back to a numeric table_id. We use a special
                        // "NAME:" prefix to distinguish name-based entries from
                        // legacy numeric entries loaded by older code paths.
                        fk_rows.push(vec![
                            None,                                             // constraint_id (auto-assigned)
                            Some(format!("TABLE_NAME:{}", child_table_name)), // resolved by name
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

    // Index rows only live in sys_indexes: read them before it is rebuilt.
    let saved_indexes = load_saved_indexes(catalog.engine());

    // Delete existing system table files to start fresh
    for name in &[
        "databases",
        "tables",
        "columns",
        "constraints",
        "indexes",
        "views",
    ] {
        let path = sys_path(catalog.engine(), name);
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
    create_system_table_files(catalog.engine());
    populate_system_tables(catalog, fk_rows, Some(saved_indexes));
    // System tables rewritten wholesale: table_ids may have changed and
    // constraint/index rows were rebuilt — forget all memoised metadata.
    catalog.engine().invalidate_metadata();
    Ok(())
}

/// Populate all system tables from an in-memory Catalog.
///
/// Writes databases, tables, columns, constraints, and (where possible)
/// indexes to their respective heap files.
///
/// `indexes` are the `sys_indexes` rows to write back. `None` (the one-time
/// migration from `catalog.json`) recovers them from `.idx.meta` files.
pub(crate) fn populate_system_tables(
    catalog: &Catalog,
    mut constr_rows: Vec<Vec<Option<String>>>,
    indexes: Option<Vec<SavedIndex>>,
) {
    // Sequence counters for auto-generated IDs
    let mut next_db_id = 1i32;
    let mut next_table_id = 1i32;
    let mut next_col_id = 1i32;

    // Build a table_name → table_id mapping BEFORE processing FK rows.
    // This is populated during the table iteration below, then used to
    // resolve "TABLE_NAME:xxx" entries in constr_rows.
    let mut table_name_to_new_id: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();

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
            && let Ok(id) = id_str.parse::<i32>()
        {
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

        db_rows.push(vec![Some(db_id.to_string()), Some(db_name.clone())]);

        let mut tbl_names: Vec<&String> = database.tables.keys().collect();
        tbl_names.sort();
        for tbl_name in tbl_names {
            let table = &database.tables[tbl_name];
            let table_id = next_table_id;
            next_table_id += 1;

            let file_path = crate::backend::paths::relative_heap(db_name, tbl_name);

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
            && let Some(table_name) = table_id_str.strip_prefix("TABLE_NAME:")
        {
            let resolved_id = table_name_to_new_id.get(table_name).cloned().unwrap_or(0);
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

    // Batch-write each system table
    if let Err(e) = insert_system_rows(
        catalog.engine(),
        "databases",
        SYS_DATABASES_SCHEMA,
        &db_rows,
    ) {
        log::error!("[SystemCatalog] Failed to write sys_databases: {}", e);
    }
    if let Err(e) = insert_system_rows(catalog.engine(), "tables", SYS_TABLES_SCHEMA, &tbl_rows) {
        log::error!("[SystemCatalog] Failed to write sys_tables: {}", e);
    }
    if let Err(e) = insert_system_rows(catalog.engine(), "columns", SYS_COLUMNS_SCHEMA, &col_rows) {
        log::error!("[SystemCatalog] Failed to write sys_columns: {}", e);
    }
    if let Err(e) = insert_system_rows(
        catalog.engine(),
        "constraints",
        SYS_CONSTRAINTS_SCHEMA,
        &constr_rows,
    ) {
        log::error!("[SystemCatalog] Failed to write sys_constraints: {}", e);
    }

    // ── Populate sys_indexes by scanning .idx.meta files on disk ────────
    // Build a table_name → table_id mapping for index rows.
    let mut table_name_to_id: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();
    for row in &tbl_rows {
        // tbl_rows: [table_id, db_id, name, file_path]
        if let (Some(table_id_str), Some(table_name)) = (&row[0], &row[2])
            && let Ok(tid) = table_id_str.parse::<i32>()
        {
            table_name_to_id.insert(table_name.clone(), tid);
        }
    }
    // ── Populate sys_views ──────────────────────────────────────────────
    let mut view_rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut next_view_id = 1i32;
    for (db_name, database) in sorted_databases(catalog) {
        let db_id = db_rows
            .iter()
            .find_map(|row| match (&row[0], &row[1]) {
                (Some(id_str), Some(name)) if name == db_name => id_str.parse::<i32>().ok(),
                _ => None,
            })
            .unwrap_or(1);

        let mut view_names: Vec<&String> = database.views.keys().collect();
        view_names.sort();
        for view_name in view_names {
            let view_def = &database.views[view_name];
            view_rows.push(vec![
                Some(next_view_id.to_string()),    // view_id
                Some(db_id.to_string()),           // db_id
                Some(view_name.clone()),           // name
                Some(view_def.query_json.clone()), // query_json
            ]);
            next_view_id += 1;
        }
    }
    if let Err(e) = insert_system_rows(catalog.engine(), "views", SYS_VIEWS_SCHEMA, &view_rows) {
        log::error!("[SystemCatalog] Failed to write sys_views: {}", e);
    }

    match indexes {
        Some(saved) => write_saved_indexes(catalog.engine(), &saved, &db_rows, &tbl_rows),
        None => populate_indexes_from_meta(catalog.engine(), &table_name_to_id),
    }

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

/// One `sys_indexes` row, identified by database and table name so it
/// survives the table ids changing when the system tables are rebuilt.
pub(crate) struct SavedIndex {
    db: String,
    table: String,
    pub(crate) name: String,
    pub(crate) is_unique: bool,
    pub(crate) is_primary: bool,
    /// Comma-separated key columns.
    pub(crate) columns: String,
}

impl SavedIndex {
    /// The key columns, in order.
    pub(crate) fn column_list(&self) -> Vec<String> {
        self.columns
            .split(',')
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect()
    }
}

/// The `sys_indexes` rows of one table.
pub(crate) fn table_indexes(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
) -> Vec<SavedIndex> {
    load_saved_indexes(engine)
        .into_iter()
        .filter(|ix| ix.db.eq_ignore_ascii_case(db) && ix.table.eq_ignore_ascii_case(table))
        .collect()
}

/// Read every `sys_indexes` row with its table's database and name.
fn load_saved_indexes(engine: &crate::backend::engine::Engine) -> Vec<SavedIndex> {
    use crate::types::DataValue;
    let text = |v: &Option<DataValue>| match v {
        Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => Some(s.clone()),
        _ => None,
    };
    let int = |v: &Option<DataValue>| match v {
        Some(DataValue::Int(n)) => Some(*n),
        _ => None,
    };
    let flag = |v: &Option<DataValue>| matches!(v, Some(DataValue::Bool(true)));

    // SYS_DATABASES_SCHEMA: db_id, name
    let databases: std::collections::HashMap<i32, String> =
        scan_system_table(engine, "databases", SYS_DATABASES_SCHEMA)
            .unwrap_or_default()
            .iter()
            .filter_map(|r| Some((int(r.first()?)?, text(r.get(1)?)?)))
            .collect();
    // SYS_TABLES_SCHEMA: table_id, db_id, name, file_path
    let tables: std::collections::HashMap<i32, (String, String)> =
        scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)
            .unwrap_or_default()
            .iter()
            .filter_map(|r| {
                let db = databases.get(&int(r.get(1)?)?)?.clone();
                Some((int(r.first()?)?, (db, text(r.get(2)?)?)))
            })
            .collect();
    // SYS_INDEXES_SCHEMA: index_id, table_id, name, is_unique, is_primary, columns
    scan_system_table(engine, "indexes", SYS_INDEXES_SCHEMA)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| {
            let (db, table) = tables.get(&int(r.get(1)?)?)?.clone();
            Some(SavedIndex {
                db,
                table,
                name: text(r.get(2)?)?,
                is_unique: flag(r.get(3)?),
                is_primary: flag(r.get(4)?),
                columns: text(r.get(5)?)?,
            })
        })
        .collect()
}

/// Write `saved` back to `sys_indexes` against the new table ids, dropping
/// rows whose table or index file no longer exists.
fn write_saved_indexes(
    engine: &crate::backend::engine::Engine,
    saved: &[SavedIndex],
    db_rows: &[Vec<Option<String>>],
    tbl_rows: &[Vec<Option<String>>],
) {
    // db_rows: [db_id, name]; tbl_rows: [table_id, db_id, name, file_path]
    let db_names: std::collections::HashMap<&str, &str> = db_rows
        .iter()
        .filter_map(|r| Some((r[0].as_deref()?, r[1].as_deref()?)))
        .collect();
    let table_ids: std::collections::HashMap<(&str, &str), &str> = tbl_rows
        .iter()
        .filter_map(|r| {
            let db = *db_names.get(r[1].as_deref()?)?;
            Some(((db, r[2].as_deref()?), r[0].as_deref()?))
        })
        .collect();

    let idx_rows: Vec<Vec<Option<String>>> = saved
        .iter()
        .filter(|ix| {
            engine.paths().index(&ix.db, &ix.table, &ix.name).exists()
                || engine.paths().legacy_index(&ix.db, &ix.table).exists()
        })
        .filter_map(|ix| {
            let table_id = table_ids.get(&(ix.db.as_str(), ix.table.as_str()))?;
            Some((table_id.to_string(), ix))
        })
        .enumerate()
        .map(|(i, (table_id, ix))| {
            vec![
                Some((i + 1).to_string()),
                Some(table_id),
                Some(ix.name.clone()),
                Some(ix.is_unique.to_string()),
                Some(ix.is_primary.to_string()),
                Some(ix.columns.clone()),
            ]
        })
        .collect();

    if !idx_rows.is_empty()
        && let Err(e) = insert_system_rows(engine, "indexes", SYS_INDEXES_SCHEMA, &idx_rows)
    {
        log::error!("[SystemCatalog] Failed to write sys_indexes: {}", e);
    }
}

/// Scan `database/base/**/*.idx.meta` files and populate `sys_indexes`.
///
/// Only used when migrating from `catalog.json`; afterwards `sys_indexes`
/// is the only record of which indexes exist.
///
/// `table_name_to_id` maps table names to their assigned table IDs (from
/// `sys_tables`) so the index rows can reference the correct table.
fn populate_indexes_from_meta(
    engine: &crate::backend::engine::Engine,
    table_name_to_id: &std::collections::HashMap<String, i32>,
) {
    let base_dir = engine.paths().base_dir();
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
            let stripped = filename
                .strip_suffix(".idx")
                .unwrap_or(&filename)
                .to_string();

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
                    log::warn!(
                        "[SystemCatalog] Index meta references unknown table '{}', skipping",
                        table_name
                    );
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
                Some(next_idx_id.to_string()),     // index_id
                Some(table_id.to_string()),        // table_id
                Some(idx_name),                    // name
                Some(meta.is_unique.to_string()),  // is_unique (persisted in .idx.meta)
                Some(meta.is_primary.to_string()), // is_primary
                Some(columns_field),               // columns (full composite list)
            ]);
            next_idx_id += 1;
        }
    }

    if !idx_rows.is_empty()
        && let Err(e) = insert_system_rows(engine, "indexes", SYS_INDEXES_SCHEMA, &idx_rows)
    {
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
