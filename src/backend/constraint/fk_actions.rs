//! FOREIGN KEY action handlers.
//!
//! Implements `ON DELETE CASCADE`, `ON DELETE SET NULL`, `ON UPDATE CASCADE`,
//! and recursive propagation into grandchild tables with cycle detection.
//! All three actions share one pass over the child rows,
//! [`apply_to_child_rows`], which finds them through the child column's
//! index; they differ only in what they do to a matching row and where they
//! recurse next.

use std::collections::HashSet;

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
) -> Result<Vec<Vec<Option<DataValue>>>, String> {
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

    let new_values: Option<Vec<Option<DataValue>>> = match action {
        ChildAction::Delete => None,
        ChildAction::SetNull => Some(vec![None; col_positions.len()]),
        ChildAction::SetTo(values) => {
            let mut converted = Vec::with_capacity(values.len());
            for (value, &pos) in values.iter().zip(&col_positions) {
                converted.push(match value {
                    None => None,
                    Some(v) => Some(assign_cast(v, &columns[pos].data_type).map_err(|e| {
                        format!(
                            "{}: new key '{}' does not fit column '{}.{}': {}",
                            label,
                            key_text(v),
                            child_table,
                            columns[pos].name,
                            e
                        )
                    })?),
                });
            }
            Some(converted)
        }
    };

    let heap_path = catalog.engine().paths().heap(db_name, child_table);
    if !heap_path.exists() {
        return Ok(Vec::new());
    }
    let file = HeapFile::open(catalog.engine(), &heap_path)
        .map_err(|e| format!("Failed to open child heap for {}: {}", label, e))?;
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

    let mut matched = Vec::new();
    let mut dead = 0u32;
    let mut relocations: Vec<(Vec<u8>, Vec<u8>, u32, u32)> = Vec::new();

    // Pointers are sorted, so each page is read and written once.
    for page_pointers in pointers.chunk_by(|a, b| a.0 == b.0) {
        let page_num = page_pointers[0].0;
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(|e| e.to_string())?;
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
                .map_err(|e| e.to_string())?;
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
                return Err(format!(
                    "[{}] Failed to write back a grown row of '{}': {}",
                    label, child_table, e
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
        )
        .map_err(|e| e.to_string())?;
    }

    Ok(matched)
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
) -> Result<(), String> {
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
) -> Result<(), String> {
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

    // Recursive SET NULL into grandchild tables that reference a nulled column.
    let set_null_to_grandchildren = referencing_with(catalog, db_name, child_table, |action| {
        action.to_uppercase().contains("SET NULL")
    });
    let Some(columns) = child_columns(catalog, db_name, child_table) else {
        return Ok(());
    };

    let mut seen: HashSet<Visit> = HashSet::new();
    for fk in set_null_to_grandchildren
        .iter()
        .filter(|fk| references_any(fk, child_cols))
    {
        for row in &updated {
            let Some(key) = row_key(columns, row, &fk.parent_cols) else {
                continue;
            };
            let visit = visit_key(&fk.child_table, &fk.child_cols, &key);
            if !seen.insert(visit.clone()) {
                continue;
            }
            if !visited.insert(visit) {
                log::trace!(
                    "[SET NULL] Cycle detected: skipping '{}' with value '{}' (already set to NULL)",
                    fk.child_table,
                    tuple_text(&key)
                );
                continue;
            }
            log::trace!(
                "[SET NULL] Recursively setting NULL in '{}' cols '{}' where value = '{}'",
                fk.child_table,
                fk.child_cols.join(", "),
                tuple_text(&key)
            );
            set_null_child_rows(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &key,
                visited,
            )?;
        }
    }

    Ok(())
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
) -> Result<(), String> {
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

    // Recursive UPDATE CASCADE into grandchild tables
    let update_cascade_fks = referencing_with(catalog, db_name, child_table, |action| {
        action.to_uppercase().contains("ON UPDATE CASCADE")
    });
    let Some(columns) = child_columns(catalog, db_name, child_table) else {
        return Ok(());
    };
    let positions: Vec<usize> = child_cols
        .iter()
        .filter_map(|n| columns.iter().position(|c| c.name.eq_ignore_ascii_case(n)))
        .collect();

    let mut seen: HashSet<Visit> = HashSet::new();
    for fk in update_cascade_fks
        .iter()
        .filter(|fk| references_any(fk, child_cols))
    {
        for old_row in &updated {
            let Some(old_gc_key) = row_key(columns, old_row, &fk.parent_cols) else {
                continue;
            };
            let mut new_row = old_row.clone();
            for (&pos, value) in positions.iter().zip(new_key) {
                new_row[pos] = value.clone();
            }
            let new_gc_key: Vec<Option<DataValue>> = fk
                .parent_cols
                .iter()
                .map(|n| {
                    columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(n))
                        .and_then(|p| new_row[p].clone())
                })
                .collect();
            if old_gc_key
                .iter()
                .zip(&new_gc_key)
                .all(|(o, n)| n.as_ref() == Some(o))
            {
                continue;
            }
            let visit = visit_key(&fk.child_table, &fk.child_cols, &old_gc_key);
            if !seen.insert(visit.clone()) {
                continue;
            }
            if !visited.insert(visit) {
                log::trace!(
                    "[UPDATE CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                    fk.child_table,
                    tuple_text(&old_gc_key)
                );
                continue;
            }
            update_child_rows_fk(
                catalog,
                db_name,
                &fk.child_table,
                &fk.child_cols,
                &old_gc_key,
                &new_gc_key,
                visited,
            )?;
        }
    }

    Ok(())
}
