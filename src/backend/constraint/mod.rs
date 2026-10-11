//! Constraint Validator — Unified constraint enforcement for DML operations (Step 10).
//!
//! Validates NOT NULL, UNIQUE, and CHECK constraints at INSERT/UPDATE time before
//! data is written to the heap.  Loads constraint metadata from `sys_constraints`
//! and performs B+ Tree lookups for unique checks.
//!
//! # Usage
//!
//! ```ignore
//! use crate::backend::constraint::validate_row_insert;
//!
//! validate_row_insert(catalog, db, table, &str_values)?;
//! ```

pub mod fk_actions;
pub mod loaders;
pub mod validation;
pub mod value_lookup;

#[cfg(test)]
pub mod tests;

pub use crate::backend::error::{ConstraintKind, RookError, RookResult};

/// Validate all constraints before inserting a new row.
///
/// Checks (in order):
/// 1. NOT NULL — every `nullable = false` column must have a non-NULL value.
/// 2. UNIQUE — if a B+ Tree index exists for the column, the value must not
///    already exist in the index.
/// 3. FOREIGN KEY — referenced key must exist in the parent table.
/// 4. CHECK — any CHECK constraints registered in `sys_constraints` must
///    evaluate to `true` against the new row.
///
/// Returns `Ok(())` if all constraints pass, or [`RookError`] (typically
/// `RookError::ConstraintViolation`) describing the first violation found.
pub fn validate_row_insert(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> Result<(), RookError> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db_name.to_string(),
        })?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Table",
            name: format!("{}.{}", db_name, table_name),
        })?;

    let columns = &table.columns;

    // 1. NOT NULL
    validation::check_not_null(table_name, columns, values)?;

    // Load constraint/index metadata once (process-cached; a miss does one
    // pass over sys_tables/sys_indexes/sys_constraints instead of five).
    let meta = crate::backend::cache::metadata(db_name, table_name);

    // 2. UNIQUE (via B+ Tree if index exists) — inserts have no self-row
    validation::check_unique_insert_meta(
        db_name,
        table_name,
        columns,
        values,
        &[],
        meta.as_deref(),
    )?;

    // 3. FOREIGN KEY (parent key must exist)
    validation::check_foreign_key_insert_meta(
        catalog,
        db_name,
        table_name,
        columns,
        values,
        meta.as_deref(),
    )?;

    // 4. CHECK constraints
    validation::check_constraints_meta(db_name, table_name, columns, values, meta.as_deref())?;

    Ok(())
}

/// Validate FOREIGN KEY constraints before deleting from a parent table.
///
/// For each row being deleted, checks whether any child table has a row
/// referencing the deleted key value.
///
/// - RESTRICT mode (default): blocks the DELETE if child rows exist.
/// - CASCADE mode: automatically deletes all referencing child rows.
///
/// `column_values` is the decoded tuple as `Vec<(String, ColumnValue)>`
/// from the table being deleted from.
pub fn validate_row_delete(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    column_values: &[(String, crate::backend::executor::delete::ColumnValue)],
) -> Result<(), RookError> {
    // Every restricting FK is checked before any CASCADE / SET NULL action
    // runs, so a blocked delete leaves the child tables untouched.
    check_row_delete_restrictions(catalog, db_name, table_name, column_values)?;

    // Load FK constraints where OUR table is the parent (process-cached).
    let foreign_keys = crate::backend::cache::referencing_fks(db_name, table_name);

    for fk in foreign_keys.iter() {
        // fk: (child_table, child_col, parent_col, parent_table, action_type)
        let child_table = &fk.0;
        let child_col = &fk.1;
        let Some(parent_value_str) = parent_key_value(column_values, &fk.2) else {
            continue;
        };

        match delete_action(&fk.4) {
            DeleteAction::Cascade => {
                // CASCADE: delete all child rows that reference this parent value
                let mut visited: std::collections::HashSet<(String, String)> =
                    std::collections::HashSet::new();
                visited.insert((child_table.clone(), parent_value_str.clone()));
                fk_actions::cascade_delete_child_rows(
                    catalog,
                    db_name,
                    child_table,
                    child_col,
                    &parent_value_str,
                    &mut visited,
                )?;
            }
            DeleteAction::SetNull => {
                // SET NULL: set the FK column to NULL in all referencing child rows
                let mut visited: std::collections::HashSet<(String, String)> =
                    std::collections::HashSet::new();
                visited.insert((child_table.clone(), parent_value_str.clone()));
                fk_actions::set_null_child_rows(
                    catalog,
                    db_name,
                    child_table,
                    child_col,
                    &parent_value_str,
                    &mut visited,
                )?;
            }
            DeleteAction::Restrict => {}
        }
    }

    Ok(())
}

/// What a foreign key does to its child rows when the parent row is deleted.
enum DeleteAction {
    Cascade,
    SetNull,
    Restrict,
}

fn delete_action(action_type: &str) -> DeleteAction {
    let action_upper = action_type.to_uppercase();
    if action_upper.contains("ON DELETE CASCADE") || action_upper == "FOREIGN KEY CASCADE" {
        DeleteAction::Cascade
    } else if action_upper.contains("ON DELETE SET NULL") || action_upper == "FOREIGN KEY SET NULL"
    {
        DeleteAction::SetNull
    } else {
        DeleteAction::Restrict
    }
}

/// The value of `parent_col` in a decoded row, or `None` when it is NULL (a
/// NULL key is referenced by nothing).
fn parent_key_value(
    column_values: &[(String, crate::backend::executor::delete::ColumnValue)],
    parent_col: &str,
) -> Option<String> {
    let value = column_values
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(parent_col))
        .map(|(_, val)| match val {
            crate::backend::executor::delete::ColumnValue::Int(n) => n.to_string(),
            crate::backend::executor::delete::ColumnValue::Text(s) => s.clone(),
            crate::backend::executor::delete::ColumnValue::List(_) => String::new(),
        })
        .unwrap_or_default();
    (!value.is_empty() && !value.eq_ignore_ascii_case("null")).then_some(value)
}

/// Check, without changing anything, whether a RESTRICT (default) foreign key
/// blocks deleting this row. DELETE runs this over every target row before it
/// deletes the first one, so a blocked statement deletes nothing.
///
/// The check follows ON DELETE CASCADE keys: a child row that the delete
/// would cascade to must itself be free of restricting references.
pub fn check_row_delete_restrictions(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    column_values: &[(String, crate::backend::executor::delete::ColumnValue)],
) -> Result<(), RookError> {
    let mut visited = std::collections::HashSet::new();
    check_delete_restrictions_from(catalog, db_name, table_name, column_values, &mut visited)
}

fn check_delete_restrictions_from(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    column_values: &[(String, crate::backend::executor::delete::ColumnValue)],
    visited: &mut std::collections::HashSet<(String, String, String)>,
) -> Result<(), RookError> {
    let foreign_keys = crate::backend::cache::referencing_fks(db_name, table_name);

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;
        let Some(parent_value_str) = parent_key_value(column_values, parent_col) else {
            continue;
        };

        match delete_action(&fk.4) {
            DeleteAction::Restrict => {
                if value_lookup::child_has_referencing_row(
                    db_name,
                    child_table,
                    child_col,
                    &parent_value_str,
                )? {
                    return Err(RookError::constraint(
                        ConstraintKind::ForeignKey,
                        table_name,
                        Some(parent_col),
                        format!(
                            "FOREIGN KEY constraint violated: cannot delete from '{}' because value '{}' is referenced by '{}' (column '{}' referencing '{}.{}')",
                            table_name,
                            parent_value_str,
                            child_table,
                            child_col,
                            table_name,
                            parent_col
                        ),
                    ));
                }
            }
            DeleteAction::Cascade => {
                // Each (table, column, value) is expanded once; this also
                // ends the walk on a reference cycle.
                if !visited.insert((
                    child_table.clone(),
                    child_col.clone(),
                    parent_value_str.clone(),
                )) {
                    continue;
                }
                for child_row in child_rows_referencing(
                    catalog,
                    db_name,
                    child_table,
                    child_col,
                    &parent_value_str,
                )? {
                    check_delete_restrictions_from(
                        catalog,
                        db_name,
                        child_table,
                        &child_row,
                        visited,
                    )?;
                }
            }
            // SET NULL keeps the child row, so nothing below it is affected.
            DeleteAction::SetNull => {}
        }
    }

    Ok(())
}

/// Decoded rows of `child_table` whose `child_col` equals `value`.
fn child_rows_referencing(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    value: &str,
) -> Result<Vec<Vec<(String, crate::backend::executor::delete::ColumnValue)>>, RookError> {
    let Some(table) = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    else {
        return Ok(Vec::new());
    };
    let heap_path =
        std::path::PathBuf::from(format!("database/base/{}/{}.dat", db_name, child_table));
    if !heap_path.exists() {
        return Ok(Vec::new());
    }
    let heap = crate::backend::heap::HeapManager::open(heap_path).map_err(RookError::Io)?;
    let mut rows = Vec::new();
    for item in heap.scan() {
        let (_, _, bytes) = item.map_err(RookError::Io)?;
        let decoded = crate::backend::executor::delete::decode_tuple(&bytes, &table.columns);
        if parent_key_value(&decoded, child_col).as_deref() == Some(value) {
            rows.push(decoded);
        }
    }
    Ok(rows)
}

/// Propagate an UPDATE to child rows via ON UPDATE CASCADE / SET NULL.
///
/// Called after an UPDATE has been committed on a parent table.  Compares
/// old and new decoded tuples, and for each changed column that is referenced
/// by a FOREIGN KEY with an ON UPDATE action:
///
/// - **CASCADE**: updates child rows' FK column from the old value to the new value.
/// - **SET NULL**: sets child rows' FK column to NULL.
/// - **RESTRICT** (default): returns `Err` if any child row references the old value.
pub fn propagate_update_to_children(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    old_decoded: &[(String, crate::backend::executor::delete::ColumnValue)],
    new_decoded: &[(String, crate::backend::executor::delete::ColumnValue)],
) -> Result<(), RookError> {
    let foreign_keys = crate::backend::cache::referencing_fks(db_name, table_name);

    if foreign_keys.is_empty() {
        return Ok(());
    }

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;
        let action_type = &fk.4;

        // Find the OLD value of the parent column
        let old_val = old_decoded
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(parent_col))
            .map(|(_, v)| match v {
                crate::backend::executor::delete::ColumnValue::Int(n) => n.to_string(),
                crate::backend::executor::delete::ColumnValue::Text(s) => s.clone(),
                _ => String::new(),
            })
            .unwrap_or_default();

        if old_val.is_empty()
            || old_val.eq_ignore_ascii_case("null")
            || old_val.eq_ignore_ascii_case("NULL")
        {
            continue;
        }

        // Find the NEW value of the parent column
        let new_val = new_decoded
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(parent_col))
            .map(|(_, v)| match v {
                crate::backend::executor::delete::ColumnValue::Int(n) => n.to_string(),
                crate::backend::executor::delete::ColumnValue::Text(s) => s.clone(),
                _ => String::new(),
            })
            .unwrap_or_default();

        // Skip if the value didn't change
        if old_val == new_val {
            continue;
        }

        let action_upper = action_type.to_uppercase();

        if action_upper.contains("ON UPDATE CASCADE") {
            if !new_val.is_empty()
                && !new_val.eq_ignore_ascii_case("null")
                && !new_val.eq_ignore_ascii_case("NULL")
            {
                let mut visited: std::collections::HashSet<(String, String)> =
                    std::collections::HashSet::new();
                visited.insert((child_table.clone(), old_val.clone()));
                fk_actions::update_child_rows_fk(
                    catalog,
                    db_name,
                    child_table,
                    child_col,
                    &old_val,
                    &new_val,
                    &mut visited,
                )?;
            }
        } else if action_upper.contains("ON UPDATE SET NULL") {
            let mut visited: std::collections::HashSet<(String, String)> =
                std::collections::HashSet::new();
            visited.insert((child_table.clone(), old_val.clone()));
            fk_actions::set_null_child_rows(
                catalog,
                db_name,
                child_table,
                child_col,
                &old_val,
                &mut visited,
            )?;
        } else {
            // RESTRICT (default): block the UPDATE if child rows reference the old value
            if value_lookup::child_has_referencing_row(db_name, child_table, child_col, &old_val)? {
                return Err(RookError::constraint(
                    ConstraintKind::ForeignKey,
                    table_name,
                    Some(parent_col),
                    format!(
                        "FOREIGN KEY constraint violated: cannot update '{}' column '{}' because value '{}' is referenced by '{}' (column '{}')",
                        table_name, parent_col, old_val, child_table, child_col
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Validate referencing FOREIGN KEY constraints with RESTRICT action before updating a row.
///
/// Returns Err if any child table references the old value of a changed column.
pub fn validate_update_restrict(
    db_name: &str,
    table_name: &str,
    old_decoded: &[(String, crate::backend::executor::delete::ColumnValue)],
    new_decoded: &[(String, crate::backend::executor::delete::ColumnValue)],
) -> Result<(), RookError> {
    let foreign_keys = crate::backend::cache::referencing_fks(db_name, table_name);

    if foreign_keys.is_empty() {
        return Ok(());
    }

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;
        let action_type = &fk.4;

        let action_upper = action_type.to_uppercase();
        if action_upper.contains("ON UPDATE CASCADE") || action_upper.contains("ON UPDATE SET NULL")
        {
            continue;
        }

        // Find the OLD value of the parent column
        let old_val = old_decoded
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(parent_col))
            .map(|(_, v)| match v {
                crate::backend::executor::delete::ColumnValue::Int(n) => n.to_string(),
                crate::backend::executor::delete::ColumnValue::Text(s) => s.clone(),
                _ => String::new(),
            })
            .unwrap_or_default();

        if old_val.is_empty() || old_val.eq_ignore_ascii_case("null") {
            continue;
        }

        // Find the NEW value of the parent column
        let new_val = new_decoded
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(parent_col))
            .map(|(_, v)| match v {
                crate::backend::executor::delete::ColumnValue::Int(n) => n.to_string(),
                crate::backend::executor::delete::ColumnValue::Text(s) => s.clone(),
                _ => String::new(),
            })
            .unwrap_or_default();

        // Skip if the value didn't change
        if old_val == new_val {
            continue;
        }

        // RESTRICT (default): block the UPDATE if child rows reference the old value
        if value_lookup::child_has_referencing_row(db_name, child_table, child_col, &old_val)? {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                table_name,
                Some(parent_col),
                format!(
                    "FOREIGN KEY constraint violated: cannot update '{}' column '{}' because value '{}' is referenced by '{}' (column '{}')",
                    table_name, parent_col, old_val, child_table, child_col
                ),
            ));
        }
    }

    Ok(())
}

/// Validate all constraints before updating a row.
///
/// Similar to `validate_row_insert`, but the UNIQUE check excludes the
/// row's own previous heap location `exclude = Some((page_id, slot_id))` —
/// a row always trivially equals its own UNIQUE values, and an UPDATE that
/// leaves a UNIQUE column untouched (or rewrites it to the same value) must
/// not collide with itself.
///
/// Returns `Ok(())` if all constraints pass, or [`RookError`].
pub fn validate_row_update(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    new_values: &[&str],
    exclude: Option<(u32, u32)>,
) -> Result<(), RookError> {
    let exclude_slice: &[(u32, u32)] = match &exclude {
        Some(ptr) => std::slice::from_ref(ptr),
        None => &[],
    };
    validate_row_update_with_excludes(catalog, db_name, table_name, new_values, exclude_slice)
}

/// Validate row update with multiple excluded pointers (for batch updates).
pub fn validate_row_update_with_excludes(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    new_values: &[&str],
    exclude_ptrs: &[(u32, u32)],
) -> Result<(), RookError> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db_name.to_string(),
        })?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Table",
            name: format!("{}.{}", db_name, table_name),
        })?;

    let columns = &table.columns;

    // Load constraint/index metadata once (process-cached).
    let meta = crate::backend::cache::metadata(db_name, table_name);

    // 1. NOT NULL (new values must not violate)
    validation::check_not_null(table_name, columns, new_values)?;

    // 2. UNIQUE — self/batch matches excluded via `exclude_ptrs`
    validation::check_unique_insert_meta(
        db_name,
        table_name,
        columns,
        new_values,
        exclude_ptrs,
        meta.as_deref(),
    )?;

    // 3. FOREIGN KEY (parent key must exist)
    validation::check_foreign_key_insert_meta(
        catalog,
        db_name,
        table_name,
        columns,
        new_values,
        meta.as_deref(),
    )?;

    // 4. CHECK constraints
    validation::check_constraints_meta(db_name, table_name, columns, new_values, meta.as_deref())?;

    Ok(())
}
