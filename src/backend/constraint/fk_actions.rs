//! FOREIGN KEY action handlers.
//!
//! Implements `ON DELETE CASCADE`, `ON DELETE SET NULL`, `ON UPDATE CASCADE`,
//! and recursive propagation into grandchild tables with cycle detection.
//! All three actions share one pass over the child rows,
//! [`apply_to_child_rows`], which finds them through the child column's
//! index; they differ only in what they do to a matching row and where they
//! recurse next.

use std::collections::HashSet;

use crate::backend::error::{RookError, RookResult};
use crate::catalog::types::Catalog;
use crate::types::functions::assign_cast;
use crate::types::{DataType, DataValue};

use super::loaders::{self, ReferencingFk};
use super::row_key;

/// What a foreign-key action does to each matching child row.
enum ChildAction<'a> {
    Delete,
    SetNull,
    /// Set the FK columns to these values (a NULL stays NULL).
    SetTo(&'a [Option<DataValue>]),
}

/// A foreign key's place in the cycle guard: the child table, its FK columns
/// and the parent key being acted on.
pub(crate) type Visit = (String, String, String);

/// The cycle-guard entry for acting on `key` in `child_table`'s `child_cols`.
pub(crate) fn visit_key(child_table: &str, child_cols: &[String], key: &[DataValue]) -> Visit {
    (
        child_table.to_string(),
        child_cols.join(", "),
        tuple_text(key),
    )
}

/// Apply `action` to every row of `child_table` whose `child_cols` equal
/// `parent_key` (one value per column), and return those rows as they were
/// before the change.
///
/// The rows come from the index on `child_cols` (built on first use if the
/// table lacks it), so the cost follows the number of matches, not the size
/// of the child table. Rows are changed in place when they still fit their
/// slot. A row that grows is marked deleted and inserted again after the
/// pass, with its index entries moved to the new location.
fn apply_to_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
    action: ChildAction,
    label: &str,
) -> RookResult<Vec<Vec<Option<DataValue>>>> {
    use crate::backend::executor::compaction_api::insert_raw_tuple;
    use crate::backend::executor::create_index::{update_index_on_delete, update_index_on_update};
    use crate::backend::heap::HeapFile;
    use crate::page::Page;
    // The first index failure; the child's indexes are rebuilt once its rows
    // are written, rather than maintained row by row after that.
    let mut index_error: Option<String> = None;

    let Some(table) = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    else {
        log::warn!(
            "[Constraint] Child table '{}' not found in catalog for {}",
            child_table,
            label
        );
        return Ok(Vec::new());
    };
    let columns = &table.columns;
    let col_positions: Option<Vec<usize>> = child_cols
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        })
        .collect();
    let Some(col_positions) = col_positions else {
        return Ok(Vec::new());
    };

    let new_values = new_fk_values(columns, &col_positions, &action, child_table, label)?;

    let heap_path = catalog.engine().paths().heap(db_name, child_table);
    if !heap_path.exists() {
        return Ok(Vec::new());
    }
    let file = HeapFile::open(catalog.engine(), &heap_path)
        .map_err(|e| RookError::storage(format!("Failed to open child heap for {}", label), e))?;
    let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let pointers = super::value_lookup::child_row_pointers(
        catalog,
        db_name,
        child_table,
        child_cols,
        parent_key,
    )?;
    if pointers.is_empty() {
        return Ok(Vec::new());
    }
    // Every changed row must keep the child's constraints; nothing is
    // written unless all of them do.
    if let Some(values) = &new_values {
        let _ = check_new_child_rows(
            catalog,
            db_name,
            child_table,
            child_cols,
            &file,
            &pointers,
            &col_positions,
            values,
            label,
        )?;
    }

    let mut matched = Vec::new();
    let mut dead = 0u32;
    let mut relocations: Vec<(Vec<u8>, Vec<u8>, u32, u32)> = Vec::new();

    // Pointers are sorted, so each page is read and written once.
    for page_pointers in pointers.chunk_by(|a, b| a.0 == b.0) {
        let page_num = page_pointers[0].0;
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(RookError::storage_bare)?;
        let mut modified = false;

        for &(_, slot) in page_pointers {
            let Some(old_bytes) = page.tuple(slot).map(<[u8]>::to_vec) else {
                continue;
            };
            let Ok(row) = crate::types::deserialize_nullable_row(&schema, &old_bytes) else {
                continue;
            };

            match &new_values {
                None => {
                    if index_error.is_none()
                        && let Err(e) = update_index_on_delete(
                            catalog.engine(),
                            db_name,
                            child_table,
                            columns,
                            &old_bytes,
                            page_num,
                            slot,
                        )
                    {
                        index_error = Some(e.to_string());
                    }
                    page.mark_deleted(slot);
                    dead += 1;
                }
                Some(values) => {
                    let mut new_row = row.clone();
                    for (&pos, value) in col_positions.iter().zip(values) {
                        new_row[pos] = value.clone();
                    }
                    let new_bytes =
                        match crate::types::serialize_nullable_typed_row(&schema, &new_row) {
                            Ok(b) => b,
                            Err(e) => {
                                log::warn!("[{}] Failed to re-serialize tuple: {}", label, e);
                                continue;
                            }
                        };
                    if page.overwrite(slot, &new_bytes) {
                        if index_error.is_none()
                            && let Err(e) = update_index_on_update(
                                catalog.engine(),
                                db_name,
                                child_table,
                                columns,
                                &old_bytes,
                                &new_bytes,
                                page_num,
                                slot,
                                page_num,
                                slot,
                            )
                        {
                            index_error = Some(e.to_string());
                        }
                    } else {
                        log::warn!(
                            "[{}] New tuple larger than old ({} > {}); relocating slot",
                            label,
                            new_bytes.len(),
                            old_bytes.len()
                        );
                        page.mark_deleted(slot);
                        dead += 1;
                        relocations.push((old_bytes, new_bytes, page_num, slot));
                    }
                }
            }
            matched.push(row);
            modified = true;
        }

        if modified {
            file.write_page(&page, page_num)
                .map_err(RookError::storage_bare)?;
            let _ = crate::backend::visibility_map::vm_clear_page(
                catalog.engine(),
                db_name,
                child_table,
                page_num,
            );
        }
    }

    if dead > 0
        && let Err(e) = file.record_dead_tuples(dead)
    {
        log::warn!("[{}] Failed to increment dead tuple count: {}", label, e);
    }

    for (old_bytes, new_bytes, old_page, old_slot) in relocations {
        match insert_raw_tuple(catalog.engine(), db_name, child_table, &new_bytes) {
            Ok((new_page, new_slot)) => {
                if index_error.is_none()
                    && let Err(e) = update_index_on_update(
                        catalog.engine(),
                        db_name,
                        child_table,
                        columns,
                        &old_bytes,
                        &new_bytes,
                        old_page,
                        old_slot,
                        new_page,
                        new_slot,
                    )
                {
                    index_error = Some(e.to_string());
                }
            }
            // The old image is already marked deleted: the row is lost
            // unless the statement fails loudly.
            Err(e) => {
                return Err(RookError::storage(
                    format!(
                        "[{}] Failed to write back a grown row of '{}'",
                        label, child_table
                    ),
                    e,
                ));
            }
        }
    }

    if let Some(cause) = index_error {
        crate::backend::executor::create_index::repair_indexes(
            catalog,
            db_name,
            child_table,
            &format!(
                "[{}] Index maintenance failed for a child row: {}",
                label, cause
            ),
        )?;
    }

    Ok(matched)
}

/// The values `action` gives the FK columns at `col_positions` of
/// `columns`, converted to their types; `None` for a delete.
fn new_fk_values(
    columns: &[crate::catalog::types::Column],
    col_positions: &[usize],
    action: &ChildAction,
    child_table: &str,
    label: &str,
) -> RookResult<Option<Vec<Option<DataValue>>>> {
    Ok(match action {
        ChildAction::Delete => None,
        ChildAction::SetNull => Some(vec![None; col_positions.len()]),
        ChildAction::SetTo(values) => {
            let mut converted = Vec::with_capacity(values.len());
            for (value, &pos) in values.iter().zip(col_positions) {
                converted.push(match value {
                    None => None,
                    Some(v) => Some(assign_cast(v, &columns[pos].data_type).map_err(|e| {
                        RookError::TypeMismatch(format!(
                            "{}: new key '{}' does not fit column '{}.{}': {}",
                            label,
                            key_text(v),
                            child_table,
                            columns[pos].name,
                            e
                        ))
                    })?),
                });
            }
            Some(converted)
        }
    })
}

/// Fail unless every row of `child_table` at `pointers`, with `values` in
/// its FK columns (`col_positions`), keeps the child's constraints: NOT
/// NULL, UNIQUE, CHECK and its other foreign keys. The foreign key being
/// acted on is not checked, as its new parent key may not be written yet.
/// Returns the rows as they are now.
#[allow(clippy::too_many_arguments)]
fn check_new_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    file: &crate::backend::heap::HeapFile,
    pointers: &[(u32, u32)],
    col_positions: &[usize],
    values: &[Option<DataValue>],
    label: &str,
) -> RookResult<Vec<Vec<Option<DataValue>>>> {
    let Some(columns) = child_columns(catalog, db_name, child_table) else {
        return Ok(Vec::new());
    };
    let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
    let checks = super::RowChecks::new(catalog, db_name, child_table);
    let mut old_rows = Vec::with_capacity(pointers.len());
    let mut page = crate::page::Page::new();
    for page_pointers in pointers.chunk_by(|a, b| a.0 == b.0) {
        file.read_page(&mut page, page_pointers[0].0)
            .map_err(RookError::storage_bare)?;
        for &(_, slot) in page_pointers {
            let Some(bytes) = page.tuple(slot) else {
                continue;
            };
            let Ok(old_row) = crate::types::deserialize_nullable_row(&schema, bytes) else {
                continue;
            };
            let mut row = old_row.clone();
            for (&pos, value) in col_positions.iter().zip(values) {
                row[pos] = value.clone();
            }
            // The rows being changed are excluded: two of them never share
            // a key afterwards unless they already did before.
            checks
                .check_except_fk(catalog, columns, &row, pointers, child_cols)
                .map_err(|e| {
                    e.with_context(format!(
                        "{} would break a constraint of '{}'",
                        label, child_table
                    ))
                })?;
            old_rows.push(old_row);
        }
    }
    Ok(old_rows)
}

/// Check, without writing anything, that `action` on the rows of
/// `child_table` whose `child_cols` equal `parent_key` keeps every one of
/// them within the child's constraints, and that what the change does in
/// turn to the rows referencing them is allowed too, at every depth. For a
/// parent's UPDATE or DELETE to call before it changes its own rows.
#[allow(clippy::too_many_arguments)]
fn check_child_action(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
    action: ChildAction,
    label: &str,
    visited: &mut HashSet<Visit>,
) -> RookResult<()> {
    let Some(columns) = child_columns(catalog, db_name, child_table) else {
        return Ok(());
    };
    let Some(col_positions) = child_cols
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        })
        .collect::<Option<Vec<usize>>>()
    else {
        return Ok(());
    };
    let Some(values) = new_fk_values(columns, &col_positions, &action, child_table, label)? else {
        return Ok(());
    };
    let heap_path = catalog.engine().paths().heap(db_name, child_table);
    if !heap_path.exists() {
        return Ok(());
    }
    let pointers = super::value_lookup::child_row_pointers(
        catalog,
        db_name,
        child_table,
        child_cols,
        parent_key,
    )?;
    if pointers.is_empty() {
        return Ok(());
    }
    let file = crate::backend::heap::HeapFile::open(catalog.engine(), &heap_path)
        .map_err(|e| RookError::storage(format!("Failed to open child heap for {}", label), e))?;
    let old_rows = check_new_child_rows(
        catalog,
        db_name,
        child_table,
        child_cols,
        &file,
        &pointers,
        &col_positions,
        &values,
        label,
    )?;

    for (fk, old_key, new_key) in key_changes(
        catalog,
        db_name,
        child_table,
        child_cols,
        &values,
        &old_rows,
    ) {
        if !visited.insert(visit_key(&fk.child_table, &fk.child_cols, &old_key)) {
            continue;
        }
        match on_update(&fk.action) {
            OnUpdate::Cascade => check_child_action(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                ChildAction::SetTo(&new_key),
                "UPDATE CASCADE",
                visited,
            )?,
            OnUpdate::SetNull => check_child_action(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                ChildAction::SetNull,
                "SET NULL",
                visited,
            )?,
            OnUpdate::Restrict => {
                super::restrict_update(catalog, child_table, &fk, db_name, &old_key)?
            }
        }
    }
    Ok(())
}

/// [`check_child_action`] for ON DELETE / ON UPDATE SET NULL.
pub(crate) fn check_set_null(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
) -> RookResult<()> {
    check_child_action(
        catalog,
        db_name,
        child_table,
        child_cols,
        parent_key,
        ChildAction::SetNull,
        "SET NULL",
        &mut HashSet::new(),
    )
}

/// [`check_child_action`] for ON UPDATE CASCADE to `new_key`.
pub(crate) fn check_update_cascade(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    old_key: &[DataValue],
    new_key: &[Option<DataValue>],
) -> RookResult<()> {
    check_child_action(
        catalog,
        db_name,
        child_table,
        child_cols,
        old_key,
        ChildAction::SetTo(new_key),
        "UPDATE CASCADE",
        &mut HashSet::new(),
    )
}

/// What a foreign key does when the key it references is updated.
enum OnUpdate {
    Cascade,
    SetNull,
    Restrict,
}

fn on_update(action: &str) -> OnUpdate {
    let upper = action.to_uppercase();
    if upper.contains("ON UPDATE CASCADE") {
        OnUpdate::Cascade
    } else if upper.contains("ON UPDATE SET NULL") {
        OnUpdate::SetNull
    } else {
        OnUpdate::Restrict
    }
}

/// The rows `old_rows` of `table` get `values` in their columns `changed`.
/// For every foreign key referencing a changed column: each distinct old
/// key those rows held, and the key they hold after. Keys that stay the
/// same, or that held a NULL, are left out.
fn key_changes(
    catalog: &Catalog,
    db_name: &str,
    table: &str,
    changed: &[String],
    values: &[Option<DataValue>],
    old_rows: &[Vec<Option<DataValue>>],
) -> Vec<(ReferencingFk, Vec<DataValue>, Vec<Option<DataValue>>)> {
    let Some(columns) = child_columns(catalog, db_name, table) else {
        return Vec::new();
    };
    let positions: Vec<usize> = changed
        .iter()
        .filter_map(|n| columns.iter().position(|c| c.name.eq_ignore_ascii_case(n)))
        .collect();
    let fks = crate::backend::cache::referencing_fks(catalog.engine(), db_name, table);
    let mut seen: HashSet<Visit> = HashSet::new();
    let mut out = Vec::new();
    for fk in fks.iter().filter(|fk| references_any(fk, changed)) {
        for old_row in old_rows {
            let Some(old_key) = row_key(columns, old_row, &fk.parent_cols) else {
                continue;
            };
            let mut new_row = old_row.clone();
            for (&pos, value) in positions.iter().zip(values) {
                new_row[pos] = value.clone();
            }
            let new_key = super::row_values(columns, &new_row, &fk.parent_cols);
            let old_values: Vec<Option<DataValue>> = old_key.iter().cloned().map(Some).collect();
            if super::same_key(&old_values, &new_key)
                || !seen.insert(visit_key(&fk.child_table, &fk.child_cols, &old_key))
            {
                continue;
            }
            out.push((fk.clone(), old_key, new_key));
        }
    }
    out
}

/// After the rows `old_rows` of `table` got `values` in their columns
/// `changed`, apply the ON UPDATE action of every foreign key referencing
/// those columns, as an UPDATE of the rows would. RESTRICT was checked
/// before anything was written, so a referencing row found now fails.
fn propagate_row_changes(
    catalog: &Catalog,
    db_name: &str,
    table: &str,
    changed: &[String],
    values: &[Option<DataValue>],
    old_rows: &[Vec<Option<DataValue>>],
    visited: &mut HashSet<Visit>,
) -> RookResult<()> {
    for (fk, old_key, new_key) in key_changes(catalog, db_name, table, changed, values, old_rows) {
        if !visited.insert(visit_key(&fk.child_table, &fk.child_cols, &old_key)) {
            log::trace!(
                "[FK] Cycle detected: skipping '{}' with value '{}'",
                fk.child_table,
                tuple_text(&old_key)
            );
            continue;
        }
        match on_update(&fk.action) {
            OnUpdate::Cascade => update_child_rows_fk(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                &new_key,
                visited,
            )?,
            OnUpdate::SetNull => set_null_child_rows(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_key,
                visited,
            )?,
            OnUpdate::Restrict => super::restrict_update(catalog, table, &fk, db_name, &old_key)?,
        }
    }
    Ok(())
}

/// Text form of a key value, for messages and cycle detection.
pub(crate) fn key_text(value: &DataValue) -> String {
    match value {
        DataValue::SmallInt(v) => v.to_string(),
        DataValue::Int(v) => v.to_string(),
        DataValue::BigInt(v) => v.to_string(),
        DataValue::Real(r) => r.0.to_string(),
        DataValue::DoublePrecision(r) => r.0.to_string(),
        DataValue::Bool(v) => v.to_string(),
        DataValue::Varchar(s) => s.clone(),
        DataValue::Char(s) => s.trim_end_matches(' ').to_string(),
        DataValue::Date(_)
        | DataValue::Time(_)
        | DataValue::Timestamp(_)
        | DataValue::Numeric(_)
        | DataValue::Bit(_) => format!("{}", value),
    }
}

/// Text form of a (possibly multi-column) key: the values joined by `", "`.
pub(crate) fn tuple_text<'a>(key: impl IntoIterator<Item = &'a DataValue>) -> String {
    key.into_iter().map(key_text).collect::<Vec<_>>().join(", ")
}

/// The foreign keys that reference `table` and run `wanted` on their action.
fn referencing_with(
    catalog: &Catalog,
    db_name: &str,
    table: &str,
    wanted: impl Fn(&str) -> bool,
) -> Vec<ReferencingFk> {
    loaders::load_referencing_foreign_keys(catalog.engine(), db_name, table)
        .map(|fks| fks.into_iter().filter(|fk| wanted(&fk.action)).collect())
        .unwrap_or_default()
}

/// Whether the foreign key `fk` (of a grandchild) references any of the
/// columns `changed` of the table it points at.
fn references_any(fk: &ReferencingFk, changed: &[String]) -> bool {
    fk.parent_cols
        .iter()
        .any(|p| changed.iter().any(|c| c.eq_ignore_ascii_case(p)))
}

fn child_columns<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    child_table: &str,
) -> Option<&'a [crate::catalog::types::Column]> {
    catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
        .map(|t| t.columns.as_slice())
}

/// Cascade delete: delete all child rows that reference a parent key value.
///
/// Called when a FOREIGN KEY with ON DELETE CASCADE is being enforced.
///
/// **Recursive**: After deleting child rows, checks if the child table itself
/// has CASCADE FKs pointing to it.  If so, extracts the referenced column
/// values from each deleted child row and recursively cascades into the
/// grandchild tables (ad infinitum).
pub(crate) fn cascade_delete_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
    visited: &mut HashSet<Visit>,
) -> RookResult<()> {
    let deleted = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_cols,
        parent_key,
        ChildAction::Delete,
        "CASCADE",
    )?;
    log::trace!(
        "[CASCADE] Deleted {} row(s) from '{}' due to ON DELETE CASCADE on parent '{}'",
        deleted.len(),
        child_table,
        tuple_text(parent_key)
    );
    if deleted.is_empty() {
        return Ok(());
    }

    // CASCADE FKs that reference THIS child table.
    let cascade_to_grandchildren = referencing_with(catalog, db_name, child_table, |action| {
        let upper = action.to_uppercase();
        upper.contains("ON DELETE CASCADE") || upper == "FOREIGN KEY CASCADE"
    });
    let Some(columns) = child_columns(catalog, db_name, child_table) else {
        return Ok(());
    };

    let mut seen: HashSet<Visit> = HashSet::new();
    let mut targets: Vec<(&ReferencingFk, Vec<DataValue>)> = Vec::new();
    for row in &deleted {
        for fk in &cascade_to_grandchildren {
            if let Some(key) = row_key(columns, row, &fk.parent_cols)
                && seen.insert(visit_key(&fk.child_table, &fk.child_cols, &key))
            {
                targets.push((fk, key));
            }
        }
    }

    for (fk, key) in targets {
        if !visited.insert(visit_key(&fk.child_table, &fk.child_cols, &key)) {
            log::trace!(
                "[CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                fk.child_table,
                tuple_text(&key)
            );
            continue;
        }
        log::trace!(
            "[CASCADE] Recursing into '{}' with value '{}' (FK cols '{}')",
            fk.child_table,
            tuple_text(&key),
            fk.child_cols.join(", ")
        );
        cascade_delete_child_rows(
            catalog,
            db_name,
            &fk.child_table,
            &fk.child_cols,
            &key,
            visited,
        )?;
    }

    Ok(())
}

/// Set the FK columns to NULL in all child rows that reference a parent key.
///
/// Called when a FOREIGN KEY with ON DELETE SET NULL is being enforced.
pub(crate) fn set_null_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
    visited: &mut HashSet<Visit>,
) -> RookResult<()> {
    let updated = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_cols,
        parent_key,
        ChildAction::SetNull,
        "SET NULL",
    )?;
    log::trace!(
        "[SET NULL] Set FK to NULL in {} row(s) from '{}' due to ON DELETE/UPDATE SET NULL on parent '{}'",
        updated.len(),
        child_table,
        tuple_text(parent_key)
    );
    if updated.is_empty() {
        return Ok(());
    }

    // The rows were updated: the rows referencing their nulled columns
    // follow those foreign keys' ON UPDATE actions.
    propagate_row_changes(
        catalog,
        db_name,
        child_table,
        child_cols,
        &vec![None; child_cols.len()],
        &updated,
        visited,
    )
}

/// Update the FK columns in all child rows that match an old parent key to
/// the new key values.
///
/// Called when a FOREIGN KEY with ON UPDATE CASCADE is being enforced.
pub(crate) fn update_child_rows_fk(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    old_key: &[DataValue],
    new_key: &[Option<DataValue>],
    visited: &mut HashSet<Visit>,
) -> RookResult<()> {
    let updated = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_cols,
        old_key,
        ChildAction::SetTo(new_key),
        "UPDATE CASCADE",
    )?;
    log::trace!(
        "[UPDATE CASCADE] Updated FK of {} row(s) from '{}' from '{}' due to ON UPDATE CASCADE",
        updated.len(),
        child_table,
        tuple_text(old_key)
    );
    if updated.is_empty() {
        return Ok(());
    }

    // The rows were updated: the rows referencing their changed columns
    // follow those foreign keys' ON UPDATE actions.
    propagate_row_changes(
        catalog,
        db_name,
        child_table,
        child_cols,
        new_key,
        &updated,
        visited,
    )
}
