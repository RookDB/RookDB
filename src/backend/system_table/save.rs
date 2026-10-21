//! Split of `system_table` — see `mod.rs` for the module overview.

use crate::catalog::types::Catalog;

use super::*;

/// Persist the in-memory catalog to system table heap files.
///
/// The system tables are rebuilt from scratch, which keeps saves free of
/// duplicate rows, and must change together: their rows refer to each other
/// by table id. So the new files are built and fsynced in `system.new`, then
/// swapped in by two renames; [`recover_system_dir`] settles a crash between
/// them. A crash at any point leaves the old catalog or the new one.
pub fn save_catalog_to_system(catalog: &Catalog) -> RookResult<()> {
    // Recovery relies on `system` existing whenever a save starts. A save
    // inside a catalog change must not run recovery, which would roll the
    // change back.
    create_system_table_files(catalog.engine())?;

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
        let db_id_to_name = load_database_names(catalog.engine());
        let tbl_path = sys_path(catalog.engine(), "tables");
        if tbl_path.exists()
            && let Ok(heap) = crate::backend::cache::lease_heap(catalog.engine(), &tbl_path)
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
                    let db_name = match decoded.get(1) {
                        Some(Some(crate::types::DataValue::Int(id))) => db_id_to_name.get(id),
                        _ => None,
                    };
                    if let (Some(name), Some(db_name)) = (name_opt, db_name) {
                        tbl_id_to_name.insert(*tid, table_key(db_name, &name));
                    }
                }
            }
        }

        if let Ok(heap) = crate::backend::cache::lease_heap(catalog.engine(), &constr_path) {
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
                    // Rows the catalog cannot regenerate: foreign keys, and
                    // the table-level PRIMARY KEY / UNIQUE / CHECK ones
                    // (column-level NOT NULL / UNIQUE / CHECK are re-emitted
                    // from the in-memory columns below).
                    let constr_upper = constr_type.to_uppercase();
                    if constr_upper.contains("FOREIGN KEY")
                        || matches!(constr_upper.as_str(), "PRIMARY KEY" | "UNIQUE" | "CHECK")
                    {
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

                        // Store the owning table's key as the "table_id" field.
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

    let paths = catalog.engine().paths();
    let system = paths.system_dir();
    let staging = paths.system_staging_dir();
    let old = paths.system_old_dir();
    for leftover in [&staging, &old] {
        if leftover.exists() {
            std::fs::remove_dir_all(leftover)?;
        }
    }
    std::fs::create_dir_all(&staging)?;
    populate_system_tables(catalog, fk_rows, saved_indexes, &staging)?;
    sync_dir(&staging)?;

    // Cached handles hold the live files open: write them out and drop them
    // before the files move.
    for name in SYSTEM_TABLES {
        crate::backend::cache::release_file(&sys_path(catalog.engine(), name))?;
    }
    std::fs::rename(&system, &old)?;
    std::fs::rename(&staging, &system)?;
    sync_dir(&paths.data_dir())?;
    // The new catalog is committed; a leftover `system.old` is removed by
    // the next recovery if this fails.
    if let Err(e) = std::fs::remove_dir_all(&old) {
        log::warn!("[SystemCatalog] could not remove {}: {}", old.display(), e);
    }
    // System tables rewritten wholesale: table_ids may have changed and
    // constraint/index rows were rebuilt — forget all memoised metadata.
    catalog.engine().invalidate_metadata();
    Ok(())
}

/// Identifies a table across databases when constraint rows are carried over
/// a rebuild, by name, since table_ids are reassigned.
fn table_key(db: &str, table: &str) -> String {
    format!("{}\u{1f}{}", db, table)
}

fn load_database_names(
    engine: &crate::backend::engine::Engine,
) -> std::collections::HashMap<i32, String> {
    let mut names = std::collections::HashMap::new();
    let path = sys_path(engine, "databases");
    if path.exists()
        && let Ok(heap) = crate::backend::cache::lease_heap(engine, &path)
    {
        for result in heap.scan() {
            if let Ok((_, _, raw)) = result
                && let Ok(row) = crate::types::deserialize_nullable_row(SYS_DATABASES_SCHEMA, &raw)
                && let (Some(Some(crate::types::DataValue::Int(id))), Some(Some(name))) =
                    (row.first(), row.get(1))
            {
                names.insert(*id, value_to_string(name));
            }
        }
    }
    names
}

/// A CHECK row's expression, whether stored bare or as `CHECK(expr)`.
fn check_expr(field: &str) -> &str {
    field
        .strip_prefix("CHECK(")
        .and_then(|inner| inner.strip_suffix(')'))
        .unwrap_or(field)
        .trim()
}

/// Populate all system tables from an in-memory Catalog.
///
/// Writes databases, tables, columns, constraints, and (where possible)
/// indexes to their respective heap files.
///
/// `indexes` are the `sys_indexes` rows to write back.
pub(crate) fn populate_system_tables(
    catalog: &Catalog,
    mut constr_rows: Vec<Vec<Option<String>>>,
    indexes: Vec<SavedIndex>,
    dir: &std::path::Path,
) -> RookResult<()> {
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
    for (db_name, database) in sorted_databases(catalog) {
        let mut tbl_names: Vec<&String> = database.tables.keys().collect();
        tbl_names.sort();
        for tbl_name in tbl_names {
            let table_id = next_table_id;
            table_name_to_new_id.insert(table_key(db_name, tbl_name), table_id);
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

    // Carried-over UNIQUE / CHECK rows, so the same constraint is not also
    // re-emitted from the column that carries it in memory.
    let kept: std::collections::HashSet<(String, String, String)> = constr_rows
        .iter()
        .filter_map(|row| {
            let table = row.get(1)?.as_deref()?.strip_prefix("TABLE_NAME:")?;
            let kind = row.get(2)?.as_deref()?;
            let cols = row.get(3)?.as_deref()?;
            match kind {
                "UNIQUE" => Some((table.to_string(), kind.to_string(), cols.to_lowercase())),
                "CHECK" => Some((
                    table.to_string(),
                    kind.to_string(),
                    check_expr(cols).to_string(),
                )),
                _ => None,
            }
        })
        .collect();

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
                let key = table_key(db_name, tbl_name);
                if col.constraints.unique
                    && !kept.contains(&(key.clone(), "UNIQUE".into(), col.name.to_lowercase()))
                {
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
                if let Some(ref check_expr) = col.constraints.check
                    && !kept.contains(&(key.clone(), "CHECK".into(), check_expr.trim().to_string()))
                {
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
            // A carried-over PRIMARY KEY / UNIQUE / CHECK row of a table that
            // no longer exists is dropped, not parked under table_id 0.
            let is_fk = row[2].as_deref().is_some_and(|t| t.contains("FOREIGN KEY"));
            if resolved_id == 0 && !is_fk {
                row.clear();
                continue;
            }
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

    constr_rows.retain(|row| !row.is_empty());

    // Batch-write each system table
    write_system_table(dir, "databases", SYS_DATABASES_SCHEMA, &db_rows)?;
    write_system_table(dir, "tables", SYS_TABLES_SCHEMA, &tbl_rows)?;
    write_system_table(dir, "columns", SYS_COLUMNS_SCHEMA, &col_rows)?;
    write_system_table(dir, "constraints", SYS_CONSTRAINTS_SCHEMA, &constr_rows)?;

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
    write_system_table(dir, "views", SYS_VIEWS_SCHEMA, &view_rows)?;

    write_saved_indexes(catalog.engine(), &indexes, &db_rows, &tbl_rows, dir)?;

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
    Ok(())
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
        super::split_column_list(&self.columns)
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

/// The table of database `db` that owns an index called `name` (compared
/// case-insensitively), if any. Index names are unique per database.
pub(crate) fn index_owner(
    engine: &crate::backend::engine::Engine,
    db: &str,
    name: &str,
) -> Option<String> {
    load_saved_indexes(engine)
        .into_iter()
        .find(|ix| ix.db.eq_ignore_ascii_case(db) && ix.name.eq_ignore_ascii_case(name))
        .map(|ix| ix.table)
}

/// `base` if no index of `db` has that name, else `base_2`, `base_3`, ...
/// For the indexes a constraint creates, which must not take a user's name.
pub(crate) fn free_index_name(
    engine: &crate::backend::engine::Engine,
    db: &str,
    base: &str,
) -> String {
    let taken: Vec<String> = load_saved_indexes(engine)
        .into_iter()
        .filter(|ix| ix.db.eq_ignore_ascii_case(db))
        .map(|ix| ix.name)
        .collect();
    let is_taken = |n: &str| taken.iter().any(|t| t.eq_ignore_ascii_case(n));
    if !is_taken(base) {
        return base.to_string();
    }
    (2..)
        .map(|i| format!("{}_{}", base, i))
        .find(|n| !is_taken(n))
        .expect("an unused suffix exists")
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
    dir: &std::path::Path,
) -> RookResult<()> {
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
        .filter(|ix| engine.paths().index(&ix.db, &ix.table, &ix.name).exists())
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

    write_system_table(dir, "indexes", SYS_INDEXES_SCHEMA, &idx_rows)
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
