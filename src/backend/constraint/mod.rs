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
        let Some(parent_key) = row_key(columns, row, &fk.parent_cols) else {
            continue;
        };
        let mut visited = std::collections::HashSet::new();
        visited.insert(fk_actions::visit_key(
            &fk.child_table,
            &fk.child_cols,
            &parent_key,
        ));

        match delete_action(&fk.action) {
            DeleteAction::Cascade => fk_actions::cascade_delete_child_rows(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &parent_key,
                &mut visited,
            )?,
            DeleteAction::SetNull => fk_actions::set_null_child_rows(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &parent_key,
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

/// The values of the columns `names` in `row`, or `None` when one is NULL
/// (a key with a NULL part is referenced by nothing) or does not exist.
fn row_key(
    columns: &[Column],
    row: &[Option<DataValue>],
    names: &[String],
) -> Option<Vec<DataValue>> {
    names
        .iter()
        .map(|name| {
            let pos = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))?;
            row.get(pos)?.clone()
        })
        .collect()
}

/// The values of the columns `names` in `row`, NULLs included.
fn row_values(
    columns: &[Column],
    row: &[Option<DataValue>],
    names: &[String],
) -> Vec<Option<DataValue>> {
    names
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .and_then(|pos| row.get(pos).cloned().flatten())
        })
        .collect()
}

fn same_value(a: Option<&DataValue>, b: Option<&DataValue>) -> bool {
    use crate::types::Comparable;
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => matches!(x.compare(y), Ok(std::cmp::Ordering::Equal)),
        _ => false,
    }
}

/// Whether `old` and `new` hold the same key, NULLs equal to each other.
fn same_key(old: &[Option<DataValue>], new: &[Option<DataValue>]) -> bool {
    old.len() == new.len()
        && old
            .iter()
            .zip(new)
            .all(|(a, b)| same_value(a.as_ref(), b.as_ref()))
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
    visited: &mut std::collections::HashSet<fk_actions::Visit>,
) -> Result<(), RookError> {
    let Some(columns) = table_columns(catalog, db_name, table_name) else {
        return Ok(());
    };
    let foreign_keys =
        crate::backend::cache::referencing_fks(catalog.engine(), db_name, table_name);

    for fk in foreign_keys.iter() {
        let Some(parent_key) = row_key(columns, row, &fk.parent_cols) else {
            continue;
        };

        match delete_action(&fk.action) {
            DeleteAction::Restrict => {
                if value_lookup::child_has_referencing_row(
                    catalog,
                    db_name,
                    &fk.child_table,
                    &fk.child_cols,
                    &parent_key,
                )? {
                    return Err(RookError::constraint(
                        ConstraintKind::ForeignKey,
                        table_name,
                        Some(&fk.parent_cols.join(", ")),
                        format!(
                            "FOREIGN KEY constraint violated: cannot delete from '{}' because value '{}' is referenced by '{}' (column '{}' referencing '{}.{}')",
                            table_name,
                            fk_actions::tuple_text(&parent_key),
                            fk.child_table,
                            fk.child_cols.join(", "),
                            table_name,
                            fk.parent_cols.join(", ")
                        ),
                    ));
                }
            }
            DeleteAction::Cascade => {
                // Each (table, columns, key) is expanded once; this also
                // ends the walk on a reference cycle.
                if !visited.insert(fk_actions::visit_key(
                    &fk.child_table,
                    &fk.child_cols,
                    &parent_key,
                )) {
                    continue;
                }
                for child_row in value_lookup::child_rows_referencing(
                    catalog,
                    db_name,
                    &fk.child_table,
                    &fk.child_cols,
                    &parent_key,
                )? {
                    check_delete_restrictions_from(
                        catalog,
                        db_name,
                        &fk.child_table,
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

/// Propagate an UPDATE to child rows via ON UPDATE CASCADE / SET NULL.
///
/// Called after an UPDATE has been committed on a parent table.  Compares
/// old and new rows, and for each changed column that is referenced
/// by a FOREIGN KEY with an ON UPDATE action:
///
/// - **CASCADE**: updates child rows' FK column from the old value to the new value.
/// - **SET NULL**: sets child rows' FK column to NULL.
/// - **RESTRICT** (default): returns `Err` if any child row references the old value.
///
/// `foreign_keys` are the ones referencing the table
/// (`cache::referencing_fks`), looked up once for the statement.
pub fn propagate_update_to_children(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    old_row: &[Option<DataValue>],
    new_row: &[Option<DataValue>],
    foreign_keys: &[loaders::ReferencingFk],
) -> Result<(), RookError> {
    for fk in foreign_keys {
        let action_upper = fk.action.to_uppercase();

        let Some(old_key) = row_key(columns, old_row, &fk.parent_cols) else {
            continue;
        };
        let new_values = row_values(columns, new_row, &fk.parent_cols);
        let old_values: Vec<Option<DataValue>> = old_key.iter().cloned().map(Some).collect();
        if same_key(&old_values, &new_values) {
            continue;
        }

        let mut visited = std::collections::HashSet::new();
        visited.insert(fk_actions::visit_key(
            &fk.child_table,
            &fk.child_cols,
            &old_key,
        ));
        if action_upper.contains("ON UPDATE CASCADE") {
            fk_actions::update_child_rows_fk(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                &new_values,
                &mut visited,
            )?;
        } else if action_upper.contains("ON UPDATE SET NULL") {
            fk_actions::set_null_child_rows(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                &mut visited,
            )?;
        } else {
            restrict_update(catalog, table_name, fk, db_name, &old_key)?;
        }
    }

    Ok(())
}

/// Validate referencing FOREIGN KEY constraints with RESTRICT action before updating a row.
///
/// Returns Err if any child table references the old value of a changed key.
/// `foreign_keys` are the ones referencing the table, as for
/// [`propagate_update_to_children`].
pub fn validate_update_restrict(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    old_row: &[Option<DataValue>],
    new_row: &[Option<DataValue>],
    foreign_keys: &[loaders::ReferencingFk],
) -> Result<(), RookError> {
    for fk in foreign_keys {
        let action_upper = fk.action.to_uppercase();
        if action_upper.contains("ON UPDATE CASCADE") || action_upper.contains("ON UPDATE SET NULL")
        {
            continue;
        }

        let Some(old_key) = row_key(columns, old_row, &fk.parent_cols) else {
            continue;
        };
        let old_values: Vec<Option<DataValue>> = old_key.iter().cloned().map(Some).collect();
        if same_key(&old_values, &row_values(columns, new_row, &fk.parent_cols)) {
            continue;
        }
        restrict_update(catalog, table_name, fk, db_name, &old_key)?;
    }

    Ok(())
}

/// Fail when a child row still references `old_key`, the key being changed.
fn restrict_update(
    catalog: &Catalog,
    table_name: &str,
    fk: &loaders::ReferencingFk,
    db_name: &str,
    old_key: &[DataValue],
) -> Result<(), RookError> {
    if value_lookup::child_has_referencing_row(
        catalog,
        db_name,
        &fk.child_table,
        &fk.child_cols,
        old_key,
    )? {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            table_name,
            Some(&fk.parent_cols.join(", ")),
            format!(
                "FOREIGN KEY constraint violated: cannot update '{}' column '{}' because value '{}' is referenced by '{}' (column '{}')",
                table_name,
                fk.parent_cols.join(", "),
                fk_actions::tuple_text(old_key),
                fk.child_table,
                fk.child_cols.join(", ")
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
    RowChecks::new(catalog, db_name, table_name).check(
        catalog,
        &table.columns,
        new_values,
        exclude_ptrs,
    )
}

/// The constraints of one table, resolved once for a statement that checks
/// many rows: the table's constraint metadata, and what each UNIQUE and
/// FOREIGN KEY check finds out about its index the first time a row needs it.
pub struct RowChecks {
    db_name: String,
    table_name: String,
    meta: Option<std::sync::Arc<crate::backend::cache::TableMeta>>,
    /// Per `meta.unique_indexes`: set once the index file is known to exist.
    unique_ready: Vec<std::cell::OnceCell<()>>,
    /// Per `meta.foreign_keys`: the lookup into the parent table's index.
    parent_probes: Vec<std::cell::OnceCell<value_lookup::KeyProbe>>,
}

impl RowChecks {
    pub fn new(catalog: &Catalog, db_name: &str, table_name: &str) -> Self {
        // Process-cached; a miss does one pass over sys_tables, sys_indexes
        // and sys_constraints.
        let meta = crate::backend::cache::metadata(catalog.engine(), db_name, table_name);
        let (uniques, fks) = meta
            .as_ref()
            .map_or((0, 0), |m| (m.unique_indexes.len(), m.foreign_keys.len()));
        RowChecks {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            meta,
            unique_ready: (0..uniques).map(|_| Default::default()).collect(),
            parent_probes: (0..fks).map(|_| Default::default()).collect(),
        }
    }

    pub(crate) fn meta(&self) -> Option<&crate::backend::cache::TableMeta> {
        self.meta.as_deref()
    }

    /// Check `values`, a row of the table with `columns`, against every
    /// constraint; UNIQUE ignores the rows at `exclude_ptrs`.
    pub fn check(
        &self,
        catalog: &Catalog,
        columns: &[Column],
        values: &[Option<DataValue>],
        exclude_ptrs: &[(u32, u32)],
    ) -> Result<(), RookError> {
        let (db_name, table_name) = (self.db_name.as_str(), self.table_name.as_str());
        let meta = self.meta();

        // 1. NOT NULL
        validation::check_not_null(table_name, columns, values)?;

        // 2. UNIQUE — self/batch matches excluded via `exclude_ptrs`
        validation::check_unique_insert_meta(
            catalog,
            db_name,
            table_name,
            columns,
            values,
            exclude_ptrs,
            meta,
            &self.unique_ready,
        )?;

        // 3. FOREIGN KEY (parent key must exist)
        validation::check_foreign_key_insert_meta(
            catalog,
            db_name,
            table_name,
            columns,
            values,
            meta,
            &self.parent_probes,
        )?;

        // 4. CHECK constraints
        validation::check_constraints_meta(table_name, columns, values, meta)?;

        Ok(())
    }
}
