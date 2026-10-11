//! FOREIGN KEY action handlers.
//!
//! Implements `ON DELETE CASCADE`, `ON DELETE SET NULL`, `ON UPDATE CASCADE`,
//! and recursive propagation into grandchild tables with cycle detection.
//! All three actions share one scan of the child heap,
//! [`apply_to_child_rows`]; they differ only in what they do to a matching
//! row and where they recurse next.

use std::collections::HashSet;

use crate::catalog::types::Catalog;
use crate::types::functions::assign_cast;
use crate::types::{DataType, DataValue};

use super::loaders;

/// What a foreign-key action does to each matching child row.
enum ChildAction<'a> {
    Delete,
    SetNull,
    /// Set the FK column to this value.
    SetTo(&'a DataValue),
}

/// Apply `action` to every row of `child_table` whose `child_col` equals
/// `parent_key`, and return those rows as they were before the change.
///
/// Rows are changed in place when they still fit their slot. A row that
/// grows is marked deleted and inserted again after the scan, with its
/// index entries moved to the new location.
fn apply_to_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    parent_key: &DataValue,
    action: ChildAction,
    label: &str,
) -> Result<Vec<Vec<Option<DataValue>>>, String> {
    use crate::backend::executor::compaction_api::insert_raw_tuple;
    use crate::backend::executor::create_index::{update_index_on_delete, update_index_on_update};
    use crate::backend::heap::HeapFile;
    use crate::page::Page;
    use crate::types::Comparable;

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
    let Some(col_pos) = columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(child_col))
    else {
        return Ok(Vec::new());
    };
    let col_type = &columns[col_pos].data_type;

    // The parent key in the child column's type; a key the column cannot
    // hold matches no child row.
    let key = match assign_cast(parent_key, col_type) {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "[{}] Failed to convert value '{}' for comparison: {}",
                label,
                key_text(parent_key),
                e
            );
            return Ok(Vec::new());
        }
    };
    let new_value: Option<Option<DataValue>> = match action {
        ChildAction::Delete => None,
        ChildAction::SetNull => Some(None),
        ChildAction::SetTo(value) => match assign_cast(value, col_type) {
            Ok(v) => Some(Some(v)),
            Err(e) => {
                return Err(format!(
                    "{}: new key '{}' does not fit column '{}.{}': {}",
                    label,
                    key_text(value),
                    child_table,
                    child_col,
                    e
                ));
            }
        },
    };

    let heap_path = std::path::PathBuf::from(catalog.engine().paths().heap(db_name, child_table));
    if !heap_path.exists() {
        return Ok(Vec::new());
    }
    let file = HeapFile::open(&heap_path)
        .map_err(|e| format!("Failed to open child heap for {}: {}", label, e))?;
    let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let mut matched = Vec::new();
    let mut dead = 0u32;
    let mut relocations: Vec<(Vec<u8>, Vec<u8>, u32, u32)> = Vec::new();

    for page_num in 1..file.page_count() {
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(|e| e.to_string())?;
        let mut modified = false;

        for slot in 0..page.slot_count() {
            let Some(old_bytes) = page.tuple(slot).map(<[u8]>::to_vec) else {
                continue;
            };
            let Ok(row) = crate::types::deserialize_nullable_row(&schema, &old_bytes) else {
                continue;
            };
            let is_match = matches!(
                row.get(col_pos),
                Some(Some(v)) if matches!(v.compare(&key), Ok(std::cmp::Ordering::Equal))
            );
            if !is_match {
                continue;
            }

            match &new_value {
                None => {
                    if let Err(e) = update_index_on_delete(
                        catalog.engine(),
                        db_name,
                        child_table,
                        columns,
                        &old_bytes,
                        page_num,
                        slot,
                    ) {
                        log::warn!(
                            "[{}] Failed to update child index for deleted tuple: {}",
                            label,
                            e
                        );
                    }
                    page.mark_deleted(slot);
                    dead += 1;
                }
                Some(value) => {
                    let mut new_row = row.clone();
                    new_row[col_pos] = value.clone();
                    let new_bytes =
                        match crate::types::serialize_nullable_typed_row(&schema, &new_row) {
                            Ok(b) => b,
                            Err(e) => {
                                log::warn!("[{}] Failed to re-serialize tuple: {}", label, e);
                                continue;
                            }
                        };
                    if page.overwrite(slot, &new_bytes) {
                        let _ = update_index_on_update(
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
                        );
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
                let _ = update_index_on_update(
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
                );
            }
            Err(e) => {
                log::error!("[{}] Failed to relocate grown child tuple: {}", label, e);
            }
        }
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
    child_col: &str,
    parent_value: &DataValue,
    visited: &mut HashSet<(String, String)>,
) -> Result<(), String> {
    let deleted = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_col,
        parent_value,
        ChildAction::Delete,
        "CASCADE",
    )?;
    log::info!(
        "[CASCADE] Deleted {} row(s) from '{}' due to ON DELETE CASCADE on parent '{}'",
        deleted.len(),
        child_table,
        key_text(parent_value)
    );
    if deleted.is_empty() {
        return Ok(());
    }

    // CASCADE FKs that reference THIS child table.
    let cascade_to_grandchildren: Vec<(String, String, String)> =
        match loaders::load_referencing_foreign_keys(catalog.engine(), db_name, child_table) {
            Ok(fks) => fks
                .into_iter()
                .filter(|(_, _, _, _, action)| {
                    let upper = action.to_uppercase();
                    upper.contains("ON DELETE CASCADE") || upper == "FOREIGN KEY CASCADE"
                })
                .map(
                    |(grandchild_table, grandchild_fk_col, child_ref_col, _, _)| {
                        (grandchild_table, grandchild_fk_col, child_ref_col)
                    },
                )
                .collect(),
            Err(_) => Vec::new(),
        };
    let Some(columns) = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
        .map(|t| &t.columns)
    else {
        return Ok(());
    };

    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut targets: Vec<(String, String, DataValue)> = Vec::new();
    for row in &deleted {
        for (grandchild_table, grandchild_fk_col, child_ref_col) in &cascade_to_grandchildren {
            if let Some(pos) = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(child_ref_col))
                && let Some(Some(value)) = row.get(pos)
                && seen.insert((
                    grandchild_table.clone(),
                    grandchild_fk_col.clone(),
                    key_text(value),
                ))
            {
                targets.push((
                    grandchild_table.clone(),
                    grandchild_fk_col.clone(),
                    value.clone(),
                ));
            }
        }
    }

    for (grandchild_table, grandchild_fk_col, val) in targets {
        if !visited.insert((grandchild_table.clone(), key_text(&val))) {
            log::info!(
                "[CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                grandchild_table,
                key_text(&val)
            );
            continue;
        }
        log::info!(
            "[CASCADE] Recursing into '{}' with value '{}' (FK col '{}')",
            grandchild_table,
            key_text(&val),
            grandchild_fk_col
        );
        cascade_delete_child_rows(
            catalog,
            db_name,
            &grandchild_table,
            &grandchild_fk_col,
            &val,
            visited,
        )?;
    }

    Ok(())
}

/// Set FK column to NULL in all child rows that reference a parent key value.
///
/// Called when a FOREIGN KEY with ON DELETE SET NULL is being enforced.
pub(crate) fn set_null_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    parent_value: &DataValue,
    visited: &mut HashSet<(String, String)>,
) -> Result<(), String> {
    let updated = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_col,
        parent_value,
        ChildAction::SetNull,
        "SET NULL",
    )?;
    log::info!(
        "[SET NULL] Set FK to NULL in {} row(s) from '{}' due to ON DELETE/UPDATE SET NULL on parent '{}'",
        updated.len(),
        child_table,
        key_text(parent_value)
    );
    if updated.is_empty() {
        return Ok(());
    }

    // Recursive SET NULL into grandchild tables that reference the column.
    let set_null_to_grandchildren: HashSet<(String, String)> =
        match loaders::load_referencing_foreign_keys(catalog.engine(), db_name, child_table) {
            Ok(fks) => fks
                .into_iter()
                .filter(|(_, _, parent_col, _, action)| {
                    parent_col.eq_ignore_ascii_case(child_col)
                        && action.to_uppercase().contains("SET NULL")
                })
                .map(|(grandchild_table, grandchild_fk_col, _, _, _)| {
                    (grandchild_table, grandchild_fk_col)
                })
                .collect(),
            Err(_) => HashSet::new(),
        };

    for (grandchild_table, grandchild_fk_col) in set_null_to_grandchildren {
        if !visited.insert((grandchild_table.clone(), key_text(parent_value))) {
            log::info!(
                "[SET NULL] Cycle detected: skipping '{}' with value '{}' (already set to NULL)",
                grandchild_table,
                key_text(parent_value)
            );
            continue;
        }
        log::info!(
            "[SET NULL] Recursively setting NULL in '{}' col '{}' where value = '{}'",
            grandchild_table,
            grandchild_fk_col,
            key_text(parent_value)
        );
        set_null_child_rows(
            catalog,
            db_name,
            &grandchild_table,
            &grandchild_fk_col,
            parent_value,
            visited,
        )?;
    }

    Ok(())
}

/// Update FK column in all child rows that match an old parent value to a new value.
///
/// Called when a FOREIGN KEY with ON UPDATE CASCADE is being enforced.
pub(crate) fn update_child_rows_fk(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    old_parent_val: &DataValue,
    new_parent_val: &DataValue,
    visited: &mut HashSet<(String, String)>,
) -> Result<(), String> {
    let updated = apply_to_child_rows(
        catalog,
        db_name,
        child_table,
        child_col,
        old_parent_val,
        ChildAction::SetTo(new_parent_val),
        "UPDATE CASCADE",
    )?;
    log::info!(
        "[UPDATE CASCADE] Updated FK to '{}' in {} row(s) from '{}' due to ON UPDATE CASCADE",
        key_text(new_parent_val),
        updated.len(),
        child_table
    );

    // Recursive UPDATE CASCADE into grandchild tables
    let update_cascade_fks: Vec<(String, String)> =
        match loaders::load_referencing_foreign_keys(catalog.engine(), db_name, child_table) {
            Ok(fks) => fks
                .into_iter()
                .filter(|(_, _, parent_col, _, action)| {
                    parent_col.eq_ignore_ascii_case(child_col)
                        && action.to_uppercase().contains("ON UPDATE CASCADE")
                })
                .map(|(grandchild_table, grandchild_fk_col, _, _, _)| {
                    (grandchild_table, grandchild_fk_col)
                })
                .collect(),
            Err(_) => Vec::new(),
        };

    for (grandchild_table, grandchild_fk_col) in &update_cascade_fks {
        if !visited.insert((grandchild_table.clone(), key_text(old_parent_val))) {
            log::info!(
                "[UPDATE CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                grandchild_table,
                key_text(old_parent_val)
            );
            continue;
        }
        log::info!(
            "[UPDATE CASCADE] Recursively cascading into '{}': col '{}' from '{}' → '{}'",
            grandchild_table,
            grandchild_fk_col,
            key_text(old_parent_val),
            key_text(new_parent_val)
        );
        update_child_rows_fk(
            catalog,
            db_name,
            grandchild_table,
            grandchild_fk_col,
            old_parent_val,
            new_parent_val,
            visited,
        )?;
    }

    Ok(())
}
