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
pub fn delete_table_metadata(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> RookResult<usize> {
    // Resolve table_id from sys_tables
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

    let mut total = 0usize;
    total += delete_rows_by_table_id(engine, "constraints", SYS_CONSTRAINTS_SCHEMA, table_id)?;
    total += delete_rows_by_table_id(engine, "columns", SYS_COLUMNS_SCHEMA, table_id)?;
    total += delete_rows_by_table_id(engine, "indexes", SYS_INDEXES_SCHEMA, table_id)?;

    log::info!(
        "[SystemCatalog] Deleted {} metadata rows for table '{}.{}' (table_id={})",
        total,
        db_name,
        table_name,
        table_id
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
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    constraint_type: &str, // e.g. "NOT NULL", "UNIQUE", "PRIMARY KEY", "CHECK", "FOREIGN KEY"
    columns: &str,         // e.g. "id" or "id,name"
    ref_table: Option<&str>, // for FOREIGN KEY
    ref_columns: Option<&str>, // for FOREIGN KEY
) -> RookResult<()> {
    // 1. Resolve db_name → db_id
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

    // 2. Resolve (db_id, table_name) → table_id
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

    // 3. Compute next constraint_id by scanning sys_constraints
    let constr_rows = scan_system_table(engine, "constraints", SYS_CONSTRAINTS_SCHEMA)?;
    let next_constr_id = constr_rows
        .iter()
        .fold(1i32, |max_id, row| match row.first() {
            Some(Some(crate::types::DataValue::Int(id))) => std::cmp::max(max_id, *id + 1),
            _ => max_id,
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
    let res = insert_system_rows(engine, "constraints", SYS_CONSTRAINTS_SCHEMA, &[constr_row]);
    if res.is_ok() {
        engine.invalidate_metadata();
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
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    column_name: &str,
    is_unique: bool,
    is_primary: bool,
) -> RookResult<()> {
    // 1. Resolve db_name → db_id from sys_databases
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

    // 2. Resolve (db_id, table_name) → table_id from sys_tables
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

    // 3. Compute next index_id by scanning sys_indexes
    let idx_rows = scan_system_table(engine, "indexes", SYS_INDEXES_SCHEMA)?;
    let next_idx_id = idx_rows.iter().fold(1i32, |max_id, row| match row.first() {
        Some(Some(crate::types::DataValue::Int(id))) => std::cmp::max(max_id, *id + 1),
        _ => max_id,
    });

    // 4. Insert the new index row
    let idx_row = vec![
        Some(next_idx_id.to_string()),                               // index_id
        Some(table_id.to_string()),                                  // table_id
        Some(index_name.to_string()),                                // name
        Some(if is_unique { "true" } else { "false" }.to_string()),  // is_unique
        Some(if is_primary { "true" } else { "false" }.to_string()), // is_primary
        Some(column_name.to_string()),                               // columns
    ];
    let res = insert_system_rows(engine, "indexes", SYS_INDEXES_SCHEMA, &[idx_row]);
    if res.is_ok() {
        engine.invalidate_metadata();
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
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> RookResult<usize> {
    // Resolve db_name → db_id
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

    // Resolve (db_id, table_name) → table_id
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

    // Scan sys_indexes to find matching row
    let idx_path = sys_path(engine, "indexes");
    if !idx_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::cache::lease_heap(engine, &idx_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_INDEXES_SCHEMA, &raw_bytes)?;
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
        count,
        db_name,
        table_name,
        index_name
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
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    column_name: &str,
) -> RookResult<usize> {
    // Resolve db_name → db_id, (db_id, table_name) → table_id
    let (table_id, _db_id) = resolve_table_id(engine, db_name, table_name)?;

    let constr_path = sys_path(engine, "constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::cache::lease_heap(engine, &constr_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)?;
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
            let is_check = matches!(
                &decoded[2],
                Some(crate::types::DataValue::Varchar(t)) | Some(crate::types::DataValue::Char(t))
                    if t.trim().eq_ignore_ascii_case("CHECK")
            );
            // A CHECK row stores its expression: it goes if it mentions the column.
            let mentions = if is_check {
                rename_identifier_in_expr(row_columns, column_name, "\u{0}") != *row_columns
            } else {
                let lists = |list: &str| {
                    split_column_list(list)
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(column_name))
                };
                // A self-referencing foreign key also names the column on the
                // parent side.
                let ref_mentions = matches!(
                    (&decoded[2], decoded.get(4), decoded.get(5)),
                    (
                        Some(crate::types::DataValue::Varchar(t)) | Some(crate::types::DataValue::Char(t)),
                        Some(Some(crate::types::DataValue::Varchar(rt)) | Some(crate::types::DataValue::Char(rt))),
                        Some(Some(crate::types::DataValue::Varchar(rc)) | Some(crate::types::DataValue::Char(rc))),
                    ) if t.to_uppercase().starts_with("FOREIGN KEY")
                        && rt.eq_ignore_ascii_case(table_name)
                        && lists(rc)
                );
                lists(row_columns) || ref_mentions
            };
            if mentions {
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
        count,
        db_name,
        table_name,
        column_name
    );
    Ok(count)
}

/// Replace every occurrence of the identifier `old_name` in a SQL expression
/// with `new_name`, leaving string literals and longer identifiers untouched.
pub fn rename_identifier_in_expr(expr: &str, old_name: &str, new_name: &str) -> String {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.char_indices().peekable();
    while let Some((start, c)) = chars.next() {
        if c == '\'' {
            // Copy a string literal verbatim ('' is an escaped quote).
            out.push(c);
            while let Some((_, q)) = chars.next() {
                out.push(q);
                if q == '\'' {
                    if matches!(chars.peek(), Some((_, '\''))) {
                        out.push('\'');
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        } else if is_ident(c) {
            let mut end = start + c.len_utf8();
            while let Some(&(i, n)) = chars.peek() {
                if !is_ident(n) {
                    break;
                }
                end = i + n.len_utf8();
                chars.next();
            }
            let word = &expr[start..end];
            if word.eq_ignore_ascii_case(old_name) {
                out.push_str(new_name);
            } else {
                out.push_str(word);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Update the column name in sys_constraints for a renamed column.
///
/// Scans sys_constraints for rows matching the table_id and whose `columns`
/// field contains the old column name, then updates the field to the new name.
/// For composite constraint entries (e.g. "id,name"), only the matching part
/// is replaced.
pub fn rename_column_in_constraints(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    old_name: &str,
    new_name: &str,
) -> RookResult<usize> {
    let (table_id, _db_id) = resolve_table_id(engine, db_name, table_name)?;

    let constr_path = sys_path(engine, "constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::cache::lease_heap(engine, &constr_path)?;
    let mut updates: Vec<(u32, u32, Vec<Option<String>>)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)?;

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

        let is_check = matches!(
            &decoded[2],
            Some(crate::types::DataValue::Varchar(t)) | Some(crate::types::DataValue::Char(t))
                if t.trim().eq_ignore_ascii_case("CHECK")
        );

        let new_columns = if is_check {
            // A CHECK row stores its expression here: rename the identifier
            // inside it, or every later insert fails to compile the check.
            let renamed = rename_identifier_in_expr(&row_columns, old_name, new_name);
            if renamed == row_columns {
                continue;
            }
            renamed
        } else {
            // Check if columns field contains the old name
            let parts: Vec<&str> = row_columns.split(',').map(|s| s.trim()).collect();
            let has_old = parts.iter().any(|c| c.eq_ignore_ascii_case(old_name));
            if !has_old {
                continue;
            }

            // Replace old_name with new_name (case-insensitive match, case-preserving replace)
            parts
                .iter()
                .map(|c| {
                    if c.eq_ignore_ascii_case(old_name) {
                        new_name.to_string()
                    } else {
                        c.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Reconstruct a row with the updated columns field
        let updated_row = vec![
            decoded
                .first()
                .and_then(|v| v.as_ref().map(value_to_string)),
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
        let tuple_bytes = crate::types::serialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &str_refs)?;
        heap.insert_tuple(&tuple_bytes)?;
    }
    heap.flush()?;

    log::info!(
        "[SystemCatalog] Renamed column in {} constraint row(s) for '{}.{}': '{}' → '{}'",
        count,
        db_name,
        table_name,
        old_name,
        new_name
    );
    Ok(count)
}

/// Point every FOREIGN KEY row of `child_tables` that references `old_name`
/// at `new_name`. Called after a table rename so its children keep finding
/// their parent. Returns the number of rows rewritten.
pub fn rename_referenced_table_in_constraints(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    child_tables: &[String],
    old_name: &str,
    new_name: &str,
) -> RookResult<usize> {
    let constr_path = sys_path(engine, "constraints");
    if !constr_path.exists() {
        return Ok(0);
    }
    let child_ids: Vec<i32> = child_tables
        .iter()
        .filter_map(|t| resolve_table_id(engine, db_name, t).ok().map(|(id, _)| id))
        .collect();

    let mut heap = crate::backend::cache::lease_heap(engine, &constr_path)?;
    let mut updates: Vec<(u32, u32, Vec<Option<String>>)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)?;
        if decoded.len() < 6 {
            continue;
        }
        let owned_by_child = matches!(
            &decoded[1],
            Some(crate::types::DataValue::Int(id)) if child_ids.contains(id)
        );
        let references_old = matches!(
            &decoded[4],
            Some(crate::types::DataValue::Varchar(t)) | Some(crate::types::DataValue::Char(t))
                if t.trim().eq_ignore_ascii_case(old_name)
        );
        if !owned_by_child || !references_old {
            continue;
        }
        let mut row: Vec<Option<String>> = decoded
            .iter()
            .map(|v| v.as_ref().map(value_to_string))
            .collect();
        row[4] = Some(new_name.to_string());
        updates.push((page_id, slot_id, row));
    }

    for (page_id, slot_id, _) in &updates {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    for (_, _, row) in &updates {
        let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
        let tuple_bytes = crate::types::serialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &str_refs)?;
        heap.insert_tuple(&tuple_bytes)?;
    }
    heap.flush()?;
    Ok(updates.len())
}

/// Point the `ref_columns` of every FOREIGN KEY that references
/// `parent_table`'s column `old_name` at `new_name`. Called after a column
/// rename so the children keep finding the column they reference. Returns the
/// number of rows rewritten.
pub fn rename_referenced_column_in_constraints(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    parent_table: &str,
    old_name: &str,
    new_name: &str,
) -> RookResult<usize> {
    let constr_path = sys_path(engine, "constraints");
    if !constr_path.exists() {
        return Ok(0);
    }
    let (_, db_id) = resolve_table_id(engine, db_name, parent_table)?;
    // Only tables of this database can reference it.
    let db_tables: std::collections::HashSet<i32> =
        scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?
            .iter()
            .filter_map(|row| match (row.first(), row.get(1)) {
                (
                    Some(Some(crate::types::DataValue::Int(tid))),
                    Some(Some(crate::types::DataValue::Int(did))),
                ) if *did == db_id => Some(*tid),
                _ => None,
            })
            .collect();

    let mut heap = crate::backend::cache::lease_heap(engine, &constr_path)?;
    let mut updates: Vec<(u32, u32, Vec<Option<String>>)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)?;
        if decoded.len() < 6 {
            continue;
        }
        let text = |i: usize| match &decoded[i] {
            Some(crate::types::DataValue::Varchar(t)) | Some(crate::types::DataValue::Char(t)) => {
                Some(t.as_str())
            }
            _ => None,
        };
        let in_db = matches!(
            &decoded[1],
            Some(crate::types::DataValue::Int(id)) if db_tables.contains(id)
        );
        if !in_db
            || !text(2).is_some_and(|t| t.to_uppercase().starts_with("FOREIGN KEY"))
            || !text(4).is_some_and(|t| t.trim().eq_ignore_ascii_case(parent_table))
        {
            continue;
        }
        let Some(ref_columns) = text(5) else {
            continue;
        };
        let parts = split_column_list(ref_columns);
        if !parts.iter().any(|c| c.eq_ignore_ascii_case(old_name)) {
            continue;
        }
        let renamed: Vec<String> = parts
            .into_iter()
            .map(|c| {
                if c.eq_ignore_ascii_case(old_name) {
                    new_name.to_string()
                } else {
                    c
                }
            })
            .collect();
        let mut row: Vec<Option<String>> = decoded
            .iter()
            .map(|v| v.as_ref().map(value_to_string))
            .collect();
        row[5] = Some(join_column_list(&renamed));
        updates.push((page_id, slot_id, row));
    }

    for (page_id, slot_id, _) in &updates {
        heap.delete_tuple(*page_id, *slot_id)?;
    }
    for (_, _, row) in &updates {
        let str_refs: Vec<Option<&str>> = row.iter().map(|opt| opt.as_deref()).collect();
        let tuple_bytes = crate::types::serialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &str_refs)?;
        heap.insert_tuple(&tuple_bytes)?;
    }
    heap.flush()?;
    Ok(updates.len())
}

/// Delete all system table metadata for an entire database.
///
/// Deletes the database entry from `sys_databases`, all its tables from
/// `sys_tables`, and all associated columns, constraints, and indexes.
///
/// Returns the total number of deleted rows, or an error if the database
/// can't be found in `sys_databases`.
pub fn delete_database_metadata(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
) -> RookResult<usize> {
    // 1. Resolve db_name → db_id
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

    // 2. Find all tables belonging to this database
    let tbl_rows = scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?;
    let table_ids: Vec<i32> = tbl_rows
        .iter()
        .filter_map(|row| {
            let tbl_db_id = match row.get(1) {
                Some(Some(crate::types::DataValue::Int(id))) => *id,
                _ => return None,
            };
            if tbl_db_id == db_id
                && let Some(Some(crate::types::DataValue::Int(id))) = row.first()
            {
                return Some(*id);
            }
            None
        })
        .collect();

    let mut total = 0usize;

    // 3. Delete metadata for each table (columns, constraints, indexes)
    for table_id in &table_ids {
        total += delete_rows_by_table_id(engine, "constraints", SYS_CONSTRAINTS_SCHEMA, *table_id)?;
        total += delete_rows_by_table_id(engine, "columns", SYS_COLUMNS_SCHEMA, *table_id)?;
        total += delete_rows_by_table_id(engine, "indexes", SYS_INDEXES_SCHEMA, *table_id)?;
    }

    // 4. Delete table entries from sys_tables (db_id is at column index 1)
    total += delete_rows_by_column(engine, "tables", SYS_TABLES_SCHEMA, 1, db_id)?;

    // 5. Delete view entries from sys_views (db_id is at column index 1)
    total += delete_rows_by_column(engine, "views", SYS_VIEWS_SCHEMA, 1, db_id)?;

    // 6. Delete the database entry from sys_databases (db_id is at column index 0)
    total += delete_rows_by_column(engine, "databases", SYS_DATABASES_SCHEMA, 0, db_id)?;

    log::info!(
        "[SystemCatalog] Deleted {} metadata rows for database '{}' (db_id={})",
        total,
        db_name,
        db_id
    );
    Ok(total)
}

/// Internal helper: scan a system table and delete all rows where the
/// column at `column_idx` matches `target_id`.
///
/// Used by `delete_database_metadata` to delete from `sys_databases` (col 0)
/// and `sys_tables` (col 1), and by `delete_table_metadata` via the
/// more specific `delete_rows_by_table_id` wrapper.
fn delete_rows_by_column(
    engine: &crate::backend::engine::Engine,
    name: &str,
    schema: &[DataType],
    column_idx: usize,
    target_id: i32,
) -> RookResult<usize> {
    let path = sys_path(engine, name);
    if !path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::cache::lease_heap(engine, &path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(schema, &raw_bytes)?;
        if let Some(Some(crate::types::DataValue::Int(tid))) = decoded.get(column_idx)
            && *tid == target_id
        {
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
        name,
        column_idx,
        count,
        target_id
    );
    Ok(count)
}

/// Internal helper: scan a system table and delete all rows where the
/// `table_id` column (index 1) matches `target_table_id`.
///
/// Uses `HeapManager::delete_tuple` to mark each matching slot as deleted.
fn delete_rows_by_table_id(
    engine: &crate::backend::engine::Engine,
    name: &str,
    schema: &[DataType],
    target_table_id: i32,
) -> RookResult<usize> {
    delete_rows_by_column(engine, name, schema, 1, target_table_id)
}

/// Scan `sys_constraints` and delete all FOREIGN KEY rows referencing `parent_table_name`.
pub fn delete_referencing_foreign_keys(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    parent_table_name: &str,
) -> RookResult<usize> {
    // Resolve db_id
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

    // Load sys_tables for the db to create a table_id → db_id mapping
    let tbl_rows = scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?;
    let mut table_id_to_db_id = std::collections::HashMap::new();
    for row in &tbl_rows {
        if let (
            Some(Some(crate::types::DataValue::Int(tid))),
            Some(Some(crate::types::DataValue::Int(t_db_id))),
        ) = (row.first(), row.get(1))
        {
            table_id_to_db_id.insert(*tid, *t_db_id);
        }
    }

    let constr_path = sys_path(engine, "constraints");
    if !constr_path.exists() {
        return Ok(0);
    }

    let mut heap = crate::backend::cache::lease_heap(engine, &constr_path)?;
    let mut to_delete: Vec<(u32, u32)> = Vec::new();

    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_CONSTRAINTS_SCHEMA, &raw_bytes)?;

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
            if !constraint_type.to_uppercase().starts_with("FOREIGN KEY") {
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

/// Rename a table's row in `sys_tables`.
///
/// A catalog save carries constraint rows over by table name, read from
/// `sys_tables`; renaming the row first lets a renamed table keep its own
/// PRIMARY KEY, UNIQUE, CHECK and FOREIGN KEY rows.
pub fn rename_table_in_system(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    old_name: &str,
    new_name: &str,
) -> RookResult<()> {
    let (table_id, _db_id) = resolve_table_id(engine, db_name, old_name)?;
    let path = sys_path(engine, "tables");
    let mut heap = crate::backend::cache::lease_heap(engine, &path)?;
    let mut found = None;
    for result in heap.scan() {
        let (page_id, slot_id, raw_bytes) = result?;
        let decoded = crate::types::deserialize_nullable_row(SYS_TABLES_SCHEMA, &raw_bytes)?;
        if matches!(decoded.first(), Some(Some(crate::types::DataValue::Int(id))) if *id == table_id)
        {
            found = Some((page_id, slot_id, decoded));
            break;
        }
    }
    let Some((page_id, slot_id, decoded)) = found else {
        return Ok(());
    };
    heap.delete_tuple(page_id, slot_id)?;
    let file_path = crate::backend::paths::relative_heap(db_name, new_name);
    let row = [
        decoded
            .first()
            .and_then(|v| v.as_ref().map(value_to_string)),
        decoded.get(1).and_then(|v| v.as_ref().map(value_to_string)),
        Some(new_name.to_string()),
        Some(file_path),
    ];
    let refs: Vec<Option<&str>> = row.iter().map(|o| o.as_deref()).collect();
    let bytes = crate::types::serialize_nullable_row(SYS_TABLES_SCHEMA, &refs)?;
    heap.insert_tuple(&bytes)?;
    Ok(heap.flush()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::*;

    #[allow(dead_code)]
    fn make_test_catalog() -> Catalog {
        let mut catalog = Catalog::new(crate::backend::engine::Engine::current_dir());

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

        catalog.databases.insert(
            "test_db".to_string(),
            Database {
                tables,
                views: HashMap::new(),
            },
        );

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
        let result = crate::types::serialize_nullable_row(&schema, &[Some("42"), Some("hello")]);
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
