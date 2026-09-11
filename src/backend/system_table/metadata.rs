//! Split of `system_table` — see `mod.rs` for the module overview.

use crate::types::DataType;

use super::*;


/// Delete all system table metadata (constraints, columns, indexes) for a specific table.
///
/// Scans `sys_constraints`, `sys_columns`, and `sys_indexes` for rows whose
/// `table_id` (column index 1) matches the resolved table_id for
/// `(db_name, table_name)`, and deletes them via `HeapManager::delete_tuple`.
///
/// Returns the total number of deleted rows, or an error if the table can't
/// be found in `sys_tables`.
pub fn delete_table_metadata(db_name: &str, table_name: &str) -> std::io::Result<usize> {
    // Resolve table_id from sys_tables
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

    let mut total = 0usize;
    total += delete_rows_by_table_id("constraints", SYS_CONSTRAINTS_SCHEMA, table_id)?;
    total += delete_rows_by_table_id("columns", SYS_COLUMNS_SCHEMA, table_id)?;
    total += delete_rows_by_table_id("indexes", SYS_INDEXES_SCHEMA, table_id)?;
    total += delete_rows_by_table_id("statistics", SYS_STATISTICS_SCHEMA, table_id)?;

    log::info!(
        "[SystemCatalog] Deleted {} metadata rows for table '{}.{}' (table_id={})",
        total, db_name, table_name, table_id
    );
    Ok(total)
}

/// Insert a single constraint row into `sys_constraints`.
///
/// Resolves the `table_id` by scanning `sys_databases` and `sys_tables`,
/// then inserts a row with the appropriate `constraint_id` (auto-incremented).
///
/// Returns `Ok(())` on success, or an error if the table isn't found.
pub fn insert_constraint_metadata(
    db_name: &str,
    table_name: &str,
    constraint_type: &str,      // e.g. "NOT NULL", "UNIQUE", "PRIMARY KEY", "CHECK", "FOREIGN KEY"
    columns: &str,               // e.g. "id" or "id,name"
    ref_table: Option<&str>,     // for FOREIGN KEY
    ref_columns: Option<&str>,   // for FOREIGN KEY
) -> std::io::Result<()> {
    // 1. Resolve db_name → db_id
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

    // 2. Resolve (db_id, table_name) → table_id
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

    // 3. Compute next constraint_id by scanning sys_constraints
    let constr_rows = scan_system_table("constraints", SYS_CONSTRAINTS_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let next_constr_id = constr_rows.iter().fold(1i32, |max_id, row| {
        match row.first() {
            Some(Some(crate::types::DataValue::Int(id))) => std::cmp::max(max_id, *id + 1),
            _ => max_id,
        }
    });

    // 4. Insert the new constraint row
    // SYS_CONSTRAINTS_SCHEMA: [constraint_id:INT, table_id:INT, constraint_type:VARCHAR(50),
    //                         columns:TEXT, ref_table:VARCHAR(255), ref_columns:TEXT]
    let constr_row = vec![
        Some(next_constr_id.to_string()),
        Some(table_id.to_string()),
        Some(constraint_type.to_string()),
        Some(columns.to_string()),
        Some(ref_table.unwrap_or("").to_string()),
        Some(ref_columns.unwrap_or("").to_string()),
    ];
    let res = insert_system_rows("constraints", SYS_CONSTRAINTS_SCHEMA, &[constr_row]);
    if res.is_ok() {
        crate::backend::cache::invalidate_metadata();
    }
    res
}

/// Insert metadata for a newly created index into `sys_indexes`.
///
/// Resolves the `table_id` by scanning `sys_databases` and `sys_tables`,
/// then inserts a row with the appropriate `index_id` (auto-incremented).
///
/// Returns `Ok(())` on success, or an error if any system table can't be
/// read or the table isn't found.
pub fn insert_index_metadata(
    db_name: &str,
    table_name: &str,
    index_name: &str,
    column_name: &str,
    is_unique: bool,
    is_primary: bool,
) -> std::io::Result<()> {
    // 1. Resolve db_name → db_id from sys_databases
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

    // 2. Resolve (db_id, table_name) → table_id from sys_tables
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

    // 3. Compute next index_id by scanning sys_indexes
    let idx_rows = scan_system_table("indexes", SYS_INDEXES_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let next_idx_id = idx_rows.iter().fold(1i32, |max_id, row| {
        match row.first() {
            Some(Some(crate::types::DataValue::Int(id))) => std::cmp::max(max_id, *id + 1),
            _ => max_id,
        }
    });

    // 4. Insert the new index row
    let idx_row = vec![
        Some(next_idx_id.to_string()),           // index_id
        Some(table_id.to_string()),              // table_id
        Some(index_name.to_string()),            // name
        Some(if is_unique { "true" } else { "false" }.to_string()),  // is_unique
        Some(if is_primary { "true" } else { "false" }.to_string()), // is_primary
        Some(column_name.to_string()),           // columns
    ];
    let res = insert_system_rows("indexes", SYS_INDEXES_SCHEMA, &[idx_row]);
    if res.is_ok() {
        crate::backend::cache::invalidate_metadata();
        crate::backend::executor::create_index::invalidate_discovery(db_name, Some(table_name));
    }
    res
}

/// Delete a specific index entry from sys_indexes by index name and table.
///
/// Scans sys_databases and sys_tables to resolve table_id, then scans
/// sys_indexes to find a matching row by name, and deletes it.
///
/// Returns the number of deleted rows (0 or 1).
pub fn delete_index_metadata(
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> std::io::Result<usize> {
    // Resolve db_name → db_id
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

    // Resolve (db_id, table_name) → table_id
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

    // Scan sys_indexes to find matching row
    let idx_path = sys_path("indexes");
    if !idx_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::heap::HeapManager::open(idx_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_INDEXES_SCHEMA, &raw_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // SYS_INDEXES_SCHEMA: index_id, table_id, name, is_unique, is_primary, columns
        if decoded.len() >= 3 {
            let row_table_id = match &decoded[1] {
                Some(crate::types::DataValue::Int(id)) => *id,
                _ => continue,
            };
            if row_table_id != table_id {
                continue;
            }
            let row_name = match &decoded[2] {
                Some(crate::types::DataValue::Varchar(n)) => n,
                Some(crate::types::DataValue::Char(n)) => n,
                _ => continue,
            };
            if row_name.eq_ignore_ascii_case(index_name) {
                to_delete.push((page_id, slot_id));
            }
        }
    }

    let count = to_delete.len();
    for (page_id, slot_id) in &to_delete {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    heap.flush()?;

    log::info!(
        "[SystemCatalog] Deleted {} index row(s) for '{}.{}[{})'",
        count, db_name, table_name, index_name
    );
    Ok(count)
}

/// Delete constraint rows from sys_constraints for a specific column name.
///
/// Scans sys_databases and sys_tables to resolve table_id, then scans
/// sys_constraints for rows whose `columns` field matches `column_name`
/// and whose `table_id` matches, and deletes them.
///
/// Used when dropping a column: removes NOT NULL, UNIQUE, CHECK, FOREIGN KEY
/// constraints that reference the dropped column.
pub fn delete_column_constraints(
    db_name: &str,
    table_name: &str,
    column_name: &str,
) -> std::io::Result<usize> {
    // Resolve db_name → db_id, (db_id, table_name) → table_id
    let (table_id, _db_id) = resolve_table_id(db_name, table_name)?;

    let constr_path = sys_path("constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::heap::HeapManager::open(constr_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // SYS_CONSTRAINTS_SCHEMA: constraint_id, table_id, constraint_type, columns, ref_table, ref_columns
        if decoded.len() >= 4 {
            let row_table_id = match &decoded[1] {
                Some(crate::types::DataValue::Int(id)) => *id,
                _ => continue,
            };
            if row_table_id != table_id {
                continue;
            }
            let row_columns = match &decoded[3] {
                Some(crate::types::DataValue::Varchar(c)) => c,
                Some(crate::types::DataValue::Char(c)) => c,
                _ => continue,
            };
            // Check if the columns field references the dropped column
            // (could be exact match or comma-separated)
            let col_refs: Vec<&str> = row_columns.split(',').map(|s| s.trim()).collect();
            if col_refs.iter().any(|c| c.eq_ignore_ascii_case(column_name)) {
                to_delete.push((page_id, slot_id));
            }
        }
    }

    let count = to_delete.len();
    for (page_id, slot_id) in &to_delete {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    heap.flush()?;

    log::info!(
        "[SystemCatalog] Deleted {} constraint row(s) for column '{}.{}.{}'",
        count, db_name, table_name, column_name
    );
    Ok(count)
}

/// Update the column name in sys_constraints for a renamed column.
///
/// Scans sys_constraints for rows matching the table_id and whose `columns`
/// field contains the old column name, then updates the field to the new name.
/// For composite constraint entries (e.g. "id,name"), only the matching part
/// is replaced.
pub fn rename_column_in_constraints(
    db_name: &str,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> std::io::Result<usize> {
    let (table_id, _db_id) = resolve_table_id(db_name, table_name)?;

    let constr_path = sys_path("constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::heap::HeapManager::open(constr_path.clone())?;
    let mut updates: Vec<(u32, u32, Vec<Option<String>>)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        if decoded.len() < 4 {
            continue;
        }

        let row_table_id = match &decoded[1] {
            Some(crate::types::DataValue::Int(id)) => *id,
            _ => continue,
        };
        if row_table_id != table_id {
            continue;
        }

        let row_columns = match &decoded[3] {
            Some(crate::types::DataValue::Varchar(c)) => c.clone(),
            Some(crate::types::DataValue::Char(c)) => c.clone(),
            _ => continue,
        };

        // Check if columns field contains the old name
        let parts: Vec<&str> = row_columns.split(',').map(|s| s.trim()).collect();
        let has_old = parts.iter().any(|c| c.eq_ignore_ascii_case(old_name));
        if !has_old {
            continue;
        }

        // Replace old_name with new_name (case-insensitive match, case-preserving replace)
        let new_columns = parts.iter().map(|c| {
            if c.eq_ignore_ascii_case(old_name) { new_name.to_string() } else { c.to_string() }
        }).collect::<Vec<_>>().join(", ");

        // Reconstruct a row with the updated columns field
        let updated_row = vec![
            decoded.first().and_then(|v| v.as_ref().map(value_to_string)),
            decoded.get(1).and_then(|v| v.as_ref().map(value_to_string)),
            decoded.get(2).and_then(|v| v.as_ref().map(value_to_string)),
            Some(new_columns),
            decoded.get(4).and_then(|v| v.as_ref().map(value_to_string)),
            decoded.get(5).and_then(|v| v.as_ref().map(value_to_string)),
        ];
        updates.push((page_id, slot_id, updated_row));
    }

    let count = updates.len();
    // Delete old rows and insert updated ones
    for (page_id, slot_id, _row) in &updates {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    for (_page_id, _slot_id, row) in &updates {
        let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
        let tuple_bytes = crate::types::serialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &str_refs)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        heap.insert_tuple(&tuple_bytes)?;
    }
    heap.flush()?;

    log::info!(
        "[SystemCatalog] Renamed column in {} constraint row(s) for '{}.{}': '{}' → '{}'",
        count, db_name, table_name, old_name, new_name
    );
    Ok(count)
}

/// Delete all system table metadata for an entire database.
///
/// Deletes the database entry from `sys_databases`, all its tables from
/// `sys_tables`, and all associated columns, constraints, and indexes.
///
/// Returns the total number of deleted rows, or an error if the database
/// can't be found in `sys_databases`.
pub fn delete_database_metadata(db_name: &str) -> std::io::Result<usize> {
    // 1. Resolve db_name → db_id
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

    // 2. Find all tables belonging to this database
    let tbl_rows = scan_system_table("tables", SYS_TABLES_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let table_ids: Vec<i32> = tbl_rows.iter().filter_map(|row| {
        let tbl_db_id = match row.get(1) {
            Some(Some(crate::types::DataValue::Int(id))) => *id,
            _ => return None,
        };
        if tbl_db_id == db_id
            && let Some(Some(crate::types::DataValue::Int(id))) = row.first() {
                return Some(*id);
            }
        None
    }).collect();

    let mut total = 0usize;

    // 3. Delete metadata for each table (columns, constraints, indexes, statistics)
    for table_id in &table_ids {
        total += delete_rows_by_table_id("constraints", SYS_CONSTRAINTS_SCHEMA, *table_id)?;
        total += delete_rows_by_table_id("columns", SYS_COLUMNS_SCHEMA, *table_id)?;
        total += delete_rows_by_table_id("indexes", SYS_INDEXES_SCHEMA, *table_id)?;
        total += delete_rows_by_table_id("statistics", SYS_STATISTICS_SCHEMA, *table_id)?;
    }

    // 4. Delete table entries from sys_tables (db_id is at column index 1)
    total += delete_rows_by_column("tables", SYS_TABLES_SCHEMA, 1, db_id)?;

    // 5. Delete view entries from sys_views (db_id is at column index 1)
    total += delete_rows_by_column("views", SYS_VIEWS_SCHEMA, 1, db_id)?;

    // 6. Delete the database entry from sys_databases (db_id is at column index 0)
    total += delete_rows_by_column("databases", SYS_DATABASES_SCHEMA, 0, db_id)?;

    log::info!(
        "[SystemCatalog] Deleted {} metadata rows for database '{}' (db_id={})",
        total, db_name, db_id
    );
    Ok(total)
}

/// Internal helper: scan a system table and delete all rows where the
/// column at `column_idx` matches `target_id`.
///
/// Used by `delete_database_metadata` to delete from `sys_databases` (col 0)
/// and `sys_tables` (col 1), and by `delete_table_metadata` via the
/// more specific `delete_rows_by_table_id` wrapper.
fn delete_rows_by_column(name: &str, schema: &[DataType], column_idx: usize, target_id: i32) -> std::io::Result<usize> {
    let path = sys_path(name);
    if !path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::heap::HeapManager::open(path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(schema, &raw_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(Some(crate::types::DataValue::Int(tid))) = decoded.get(column_idx)
            && *tid == target_id {
                to_delete.push((page_id, slot_id));
            }
    }

    let count = to_delete.len();
    for (page_id, slot_id) in &to_delete {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    heap.flush()?;

    log::trace!(
        "[SystemCatalog] delete_rows_by_column({}, col={}): removed {} rows for id={}",
        name, column_idx, count, target_id
    );
    Ok(count)
}

/// Internal helper: scan a system table and delete all rows where the
/// `table_id` column (index 1) matches `target_table_id`.
///
/// Uses `HeapManager::delete_tuple` to mark each matching slot as deleted.
pub(crate) fn delete_rows_by_table_id(name: &str, schema: &[DataType], target_table_id: i32) -> std::io::Result<usize> {
    delete_rows_by_column(name, schema, 1, target_table_id)
}

/// Scan `sys_constraints` and delete all FOREIGN KEY rows referencing `parent_table_name`.
pub fn delete_referencing_foreign_keys(
    db_name: &str,
    parent_table_name: &str,
) -> std::io::Result<usize> {
    // Resolve db_id
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

    // Load sys_tables for the db to create a table_id → db_id mapping
    let tbl_rows = scan_system_table("tables", SYS_TABLES_SCHEMA)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut table_id_to_db_id = std::collections::HashMap::new();
    for row in &tbl_rows {
        if let (Some(Some(crate::types::DataValue::Int(tid))), Some(Some(crate::types::DataValue::Int(t_db_id)))) = (row.first(), row.get(1)) {
            table_id_to_db_id.insert(*tid, *t_db_id);
        }
    }

    let constr_path = sys_path("constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::heap::HeapManager::open(constr_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        if decoded.len() >= 6 {
            let row_table_id = match &decoded[1] {
                Some(crate::types::DataValue::Int(id)) => *id,
                _ => continue,
            };
            // Ensure this constraint belongs to a table in the target database
            if table_id_to_db_id.get(&row_table_id) != Some(&db_id) {
                continue;
            }
            let constraint_type = match &decoded[2] {
                Some(crate::types::DataValue::Varchar(t)) => t,
                Some(crate::types::DataValue::Char(t)) => t,
                _ => continue,
            };
            if !constraint_type.eq_ignore_ascii_case("FOREIGN KEY") {
                continue;
            }
            let ref_table = match &decoded[4] {
                Some(crate::types::DataValue::Varchar(t)) => t,
                Some(crate::types::DataValue::Char(t)) => t,
                _ => continue,
            };
            if ref_table.eq_ignore_ascii_case(parent_table_name) {
                to_delete.push((page_id, slot_id));
            }
        }
    }

    let count = to_delete.len();
    for (page_id, slot_id) in &to_delete {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    heap.flush()?;
    Ok(count)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::*;

    #[allow(dead_code)]
    fn make_test_catalog() -> Catalog {
        let mut catalog = Catalog {
            databases: HashMap::new(),
        };

        let users = Table {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                    constraints: Constraints::default(),
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(100),
                    nullable: true,
                    constraints: Constraints::default(),
                },
            ],
        };

        let mut tables = HashMap::new();
        tables.insert("users".to_string(), users);

        catalog
            .databases
            .insert("test_db".to_string(), Database { tables, views: HashMap::new() });

        catalog
    }

    #[test]
    fn test_schema_constants_are_valid() {
        // Just verify schemas compile and have expected column counts
        assert_eq!(SYS_DATABASES_SCHEMA.len(), 2);
        assert_eq!(SYS_TABLES_SCHEMA.len(), 4);
        assert_eq!(SYS_COLUMNS_SCHEMA.len(), 8);
        assert_eq!(SYS_CONSTRAINTS_SCHEMA.len(), 6);
        assert_eq!(SYS_INDEXES_SCHEMA.len(), 6);
        assert_eq!(SYS_VIEWS_SCHEMA.len(), 4);
    }

    #[test]
    fn test_schema_serialization_roundtrip() {
        // Verify that the system table schemas produce valid serialization
        let schema = vec![DataType::Int, DataType::Varchar(255)];
        let result = crate::types::serialize_nullable_row(
            &schema,
            &[Some("42"), Some("hello")],
        );
        assert!(result.is_ok());

        let bytes = result.unwrap();
        let decoded = crate::types::deserialize_nullable_row(&schema, &bytes)
            .expect("Roundtrip deserialization failed");
        assert_eq!(decoded.len(), 2);

        // Verify values roundtrip
        match &decoded[0] {
            Some(crate::types::DataValue::Int(v)) => assert_eq!(*v, 42),
            _ => panic!("Expected Int(42)"),
        }
        match &decoded[1] {
            Some(crate::types::DataValue::Varchar(v)) => assert_eq!(v, "hello"),
            _ => panic!("Expected Varchar(hello)"),
        }
    }
}
