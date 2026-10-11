//! Constraint Validator — Unified constraint enforcement for DML operations (Step 10).
//!
//! Validates NOT NULL, UNIQUE, and CHECK constraints at INSERT/UPDATE time before
//! data is written to the heap.  Loads constraint metadata from `sys_constraints`
//! and performs B+ Tree lookups for unique checks.
//!
//! Rows are typed: one `Option<DataValue>` per column, `None` for SQL NULL,
//! already converted to the column's type.
//!
//! # Usage
//!
//! ```ignore
//! use crate::backend::constraint::validate_row_insert;
//!
//! validate_row_insert(catalog, db, table, &row)?;
//! ```

pub mod fk_actions;
pub mod loaders;
pub mod validation;
pub mod value_lookup;

#[cfg(test)]
pub mod tests;

pub use crate::backend::error::{ConstraintKind, RookError, RookResult};

use crate::catalog::types::{Catalog, Column};
use crate::types::DataValue;

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
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[Option<DataValue>],
) -> Result<(), RookError> {
    validate_row_update_with_excludes(catalog, db_name, table_name, values, &[])
}

/// Validate FOREIGN KEY constraints before deleting from a parent table.
///
/// For each row being deleted, checks whether any child table has a row
/// referencing the deleted key value.
///
/// - RESTRICT mode (default): blocks the DELETE if child rows exist.
/// - CASCADE mode: automatically deletes all referencing child rows.
pub fn validate_row_delete(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    row: &[Option<DataValue>],
) -> Result<(), RookError> {
    // Every restricting FK is checked before any CASCADE / SET NULL action
    // runs, so a blocked delete leaves the child tables untouched.
    check_row_delete_restrictions(catalog, db_name, table_name, row)?;

    let Some(columns) = table_columns(catalog, db_name, table_name) else {
        return Ok(());
    };
    // Load FK constraints where OUR table is the parent (process-cached).
    let foreign_keys =
        crate::backend::cache::referencing_fks(catalog.engine(), db_name, table_name);

    for fk in foreign_keys.iter() {
        // fk: (child_table, child_col, parent_col, parent_table, action_type)
        let child_table = &fk.0;
        let child_col = &fk.1;
        let Some(parent_value) = column_value(columns, row, &fk.2) else {
            continue;
        };
        let mut visited = std::collections::HashSet::new();
        visited.insert((child_table.clone(), fk_actions::key_text(parent_value)));

        match delete_action(&fk.4) {
            DeleteAction::Cascade => fk_actions::cascade_delete_child_rows(
                catalog,
                db_name,
                child_table,
                child_col,
                parent_value,
                &mut visited,
            )?,
            DeleteAction::SetNull => fk_actions::set_null_child_rows(
                catalog,
                db_name,
                child_table,
                child_col,
                parent_value,
                &mut visited,
            )?,
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

fn table_columns<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
) -> Option<&'a [Column]> {
    catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .map(|t| t.columns.as_slice())
}

/// The value of column `name` in `row`, or `None` when it is NULL (a NULL key
/// is referenced by nothing) or the column does not exist.
fn column_value<'a>(
    columns: &[Column],
    row: &'a [Option<DataValue>],
    name: &str,
) -> Option<&'a DataValue> {
    let pos = columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))?;
    row.get(pos)?.as_ref()
}

fn same_value(a: Option<&DataValue>, b: Option<&DataValue>) -> bool {
    use crate::types::Comparable;
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => matches!(x.compare(y), Ok(std::cmp::Ordering::Equal)),
        _ => false,
    }
}

/// Check, without changing anything, whether a RESTRICT (default) foreign key
/// blocks deleting this row. DELETE runs this over every target row before it
/// deletes the first one, so a blocked statement deletes nothing.
///
/// The check follows ON DELETE CASCADE keys: a child row that the delete
/// would cascade to must itself be free of restricting references.
pub fn check_row_delete_restrictions(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    row: &[Option<DataValue>],
) -> Result<(), RookError> {
    let mut visited = std::collections::HashSet::new();
    check_delete_restrictions_from(catalog, db_name, table_name, row, &mut visited)
}

fn check_delete_restrictions_from(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    row: &[Option<DataValue>],
    visited: &mut std::collections::HashSet<(String, String, String)>,
) -> Result<(), RookError> {
    let Some(columns) = table_columns(catalog, db_name, table_name) else {
        return Ok(());
    };
    let foreign_keys =
        crate::backend::cache::referencing_fks(catalog.engine(), db_name, table_name);

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;
        let Some(parent_value) = column_value(columns, row, parent_col) else {
            continue;
        };

        match delete_action(&fk.4) {
            DeleteAction::Restrict => {
                if value_lookup::child_has_referencing_row(
                    catalog.engine(),
                    db_name,
                    child_table,
                    child_col,
                    parent_value,
                )? {
                    return Err(RookError::constraint(
                        ConstraintKind::ForeignKey,
                        table_name,
                        Some(parent_col),
                        format!(
                            "FOREIGN KEY constraint violated: cannot delete from '{}' because value '{}' is referenced by '{}' (column '{}' referencing '{}.{}')",
                            table_name,
                            fk_actions::key_text(parent_value),
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
                    fk_actions::key_text(parent_value),
                )) {
                    continue;
                }
                for child_row in
                    child_rows_referencing(catalog, db_name, child_table, child_col, parent_value)?
                {
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
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    value: &DataValue,
) -> Result<Vec<Vec<Option<DataValue>>>, RookError> {
    let Some(columns) = table_columns(catalog, db_name, child_table) else {
        return Ok(Vec::new());
    };
    let heap_path = std::path::PathBuf::from(catalog.engine().paths().heap(db_name, child_table));
    if !heap_path.exists() {
        return Ok(Vec::new());
    }
    let schema: Vec<crate::types::DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
    let heap = crate::backend::heap::HeapManager::open(heap_path).map_err(RookError::Io)?;
    let mut rows = Vec::new();
    for item in heap.scan() {
        let (_, _, bytes) = item.map_err(RookError::Io)?;
        let Ok(row) = crate::types::deserialize_nullable_row(&schema, &bytes) else {
            continue;
        };
        if same_value(column_value(columns, &row, child_col), Some(value)) {
            rows.push(row);
        }
    }
    Ok(rows)
}

/// Propagate an UPDATE to child rows via ON UPDATE CASCADE / SET NULL.
///
/// Called after an UPDATE has been committed on a parent table.  Compares
/// old and new rows, and for each changed column that is referenced
/// by a FOREIGN KEY with an ON UPDATE action:
///
/// - **CASCADE**: updates child rows' FK column from the old value to the new value.
/// - **SET NULL**: sets child rows' FK column to NULL.
/// - **RESTRICT** (default): returns `Err` if any child row references the old value.
pub fn propagate_update_to_children(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    old_row: &[Option<DataValue>],
    new_row: &[Option<DataValue>],
) -> Result<(), RookError> {
    let foreign_keys =
        crate::backend::cache::referencing_fks(catalog.engine(), db_name, table_name);

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;
        let action_upper = fk.4.to_uppercase();

        let Some(old_val) = column_value(columns, old_row, parent_col) else {
            continue;
        };
        let new_val = column_value(columns, new_row, parent_col);
        if same_value(Some(old_val), new_val) {
            continue;
        }

        let mut visited = std::collections::HashSet::new();
        visited.insert((child_table.clone(), fk_actions::key_text(old_val)));
        if action_upper.contains("ON UPDATE CASCADE") {
            if let Some(new_val) = new_val {
                fk_actions::update_child_rows_fk(
                    catalog,
                    db_name,
                    child_table,
                    child_col,
                    old_val,
                    new_val,
                    &mut visited,
                )?;
            }
        } else if action_upper.contains("ON UPDATE SET NULL") {
            fk_actions::set_null_child_rows(
                catalog,
                db_name,
                child_table,
                child_col,
                old_val,
                &mut visited,
            )?;
        } else {
            restrict_update(
                catalog.engine(),
                table_name,
                parent_col,
                child_table,
                child_col,
                db_name,
                old_val,
            )?;
        }
    }

    Ok(())
}

/// Validate referencing FOREIGN KEY constraints with RESTRICT action before updating a row.
///
/// Returns Err if any child table references the old value of a changed column.
pub fn validate_update_restrict(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    old_row: &[Option<DataValue>],
    new_row: &[Option<DataValue>],
) -> Result<(), RookError> {
    let foreign_keys = crate::backend::cache::referencing_fks(engine, db_name, table_name);

    for fk in foreign_keys.iter() {
        let child_table = &fk.0;
        let child_col = &fk.1;
        let parent_col = &fk.2;

        let action_upper = fk.4.to_uppercase();
        if action_upper.contains("ON UPDATE CASCADE") || action_upper.contains("ON UPDATE SET NULL")
        {
            continue;
        }

        let Some(old_val) = column_value(columns, old_row, parent_col) else {
            continue;
        };
        if same_value(Some(old_val), column_value(columns, new_row, parent_col)) {
            continue;
        }
        restrict_update(
            engine,
            table_name,
            parent_col,
            child_table,
            child_col,
            db_name,
            old_val,
        )?;
    }

    Ok(())
}

/// Fail when a child row still references `old_val`, the key being changed.
fn restrict_update(
    engine: &crate::backend::engine::Engine,
    table_name: &str,
    parent_col: &str,
    child_table: &str,
    child_col: &str,
    db_name: &str,
    old_val: &DataValue,
) -> Result<(), RookError> {
    if value_lookup::child_has_referencing_row(engine, db_name, child_table, child_col, old_val)? {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            table_name,
            Some(parent_col),
            format!(
                "FOREIGN KEY constraint violated: cannot update '{}' column '{}' because value '{}' is referenced by '{}' (column '{}')",
                table_name,
                parent_col,
                fk_actions::key_text(old_val),
                child_table,
                child_col
            ),
        ));
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
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    new_values: &[Option<DataValue>],
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
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    new_values: &[Option<DataValue>],
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

    // Load constraint/index metadata once (process-cached; a miss does one
    // pass over sys_tables/sys_indexes/sys_constraints instead of five).
    let meta = crate::backend::cache::metadata(catalog.engine(), db_name, table_name);

    // 1. NOT NULL
    validation::check_not_null(table_name, columns, new_values)?;

    // 2. UNIQUE — self/batch matches excluded via `exclude_ptrs`
    validation::check_unique_insert_meta(
        catalog.engine(),
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
    validation::check_constraints_meta(table_name, columns, new_values, meta.as_deref())?;

    Ok(())
}
