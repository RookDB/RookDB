//! FOREIGN KEY action handlers.
//!
//! Implements `ON DELETE CASCADE`, `ON DELETE SET NULL`, `ON UPDATE CASCADE`,
//! and recursive propagation into grandchild tables with cycle detection.

use crate::backend::executor::create_index::parse_string_to_value;
use crate::catalog::types::Catalog;
use crate::types::{DataType, DataValue};

use super::loaders;

/// Cascade delete: delete all child rows that reference a parent key value.
///
/// Called when a FOREIGN KEY with ON DELETE CASCADE is being enforced.
/// Scans the child table's heap file, finds matching rows, marks them as
/// deleted, and updates the child's B+ Tree indexes.
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
    parent_value_str: &str,
    visited: &mut std::collections::HashSet<(String, String)>,
) -> Result<(), String> {
    use crate::backend::executor::create_index::update_index_on_delete;
    use crate::backend::heap::HeapFile;
    use crate::catalog::types::Column;
    use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, SLOT_FLAG_DELETED};
    use std::collections::HashSet;

    // Get child table schema from catalog
    let child_table_info = match catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    {
        Some(t) => t,
        None => {
            log::warn!(
                "[Constraint] Child table '{}' not found in catalog for CASCADE DELETE",
                child_table
            );
            return Ok(());
        }
    };
    let columns: &[Column] = &child_table_info.columns;

    // ── Pre-load CASCADE FKs that reference THIS child table (for recursion) ─────
    let cascade_to_grandchildren: Vec<(String, String, String)> =
        match loaders::load_referencing_foreign_keys(db_name, child_table) {
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

    // Open child heap file
    let heap_path =
        std::path::PathBuf::from(format!("database/base/{}/{}.dat", db_name, child_table));
    if !heap_path.exists() {
        return Ok(());
    }

    let file = HeapFile::open(&heap_path)
        .map_err(|e| format!("Failed to open child heap for CASCADE DELETE: {}", e))?;

    let total_pages = file.page_count();
    let mut deleted_count = 0usize;

    let mut recursive_values: Vec<(String, String, String)> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(|e| e.to_string())?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        let mut slots_to_delete: Vec<usize> = Vec::new();

        for i in 0..num_items {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length =
                u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
            let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

            if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                continue;
            }

            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            let schema_types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

            let decoded = match crate::types::deserialize_nullable_row(&schema_types, tuple_data) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let col_pos = match columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(child_col))
            {
                Some(p) => p,
                None => continue,
            };

            let col_type = &columns[col_pos].data_type;
            let key_value = match parse_string_to_value(col_type, parent_value_str) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!(
                        "[CASCADE] Failed to parse value '{}' for comparison: {}",
                        parent_value_str,
                        e
                    );
                    continue;
                }
            };

            let matches = match decoded.get(col_pos) {
                Some(Some(dv)) => {
                    use crate::types::Comparable;
                    matches!(dv.compare(&key_value), Ok(std::cmp::Ordering::Equal))
                }
                _ => false,
            };

            if !matches {
                continue;
            }

            // Collect values for recursive cascade
            for (grandchild_table, grandchild_fk_col, child_ref_col) in &cascade_to_grandchildren {
                let ref_pos = columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(child_ref_col));
                if let Some(pos) = ref_pos
                    && let Some(Some(dv)) = decoded.get(pos)
                {
                    let val_str = match dv {
                        DataValue::SmallInt(v) => v.to_string(),
                        DataValue::Int(v) => v.to_string(),
                        DataValue::BigInt(v) => v.to_string(),
                        DataValue::Real(r) => r.0.to_string(),
                        DataValue::DoublePrecision(r) => r.0.to_string(),
                        DataValue::Bool(v) => v.to_string(),
                        DataValue::Varchar(s) | DataValue::Char(s) => s.clone(),
                        DataValue::Date(_)
                        | DataValue::Time(_)
                        | DataValue::Timestamp(_)
                        | DataValue::Numeric(_)
                        | DataValue::Bit(_) => {
                            format!("{}", dv)
                        }
                    };
                    recursive_values.push((
                        grandchild_table.clone(),
                        grandchild_fk_col.clone(),
                        val_str,
                    ));
                }
            }

            // Update B+ Tree index on the child table
            if let Err(e) = update_index_on_delete(
                db_name,
                child_table,
                columns,
                tuple_data,
                page_num,
                i as u32,
            ) {
                log::warn!(
                    "[CASCADE] Failed to update child index for deleted tuple: {}",
                    e
                );
            }

            slots_to_delete.push(i);
            deleted_count += 1;
        }

        if !slots_to_delete.is_empty() {
            for idx in &slots_to_delete {
                let base = PAGE_HEADER_SIZE as usize + idx * ITEM_ID_SIZE as usize;
                let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                let new_flags = flags | SLOT_FLAG_DELETED;
                page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
            }
            file.write_page(&page, page_num)
                .map_err(|e| e.to_string())?;
            let _ = crate::backend::visibility_map::vm_clear_page(db_name, child_table, page_num);
        }
    }

    if deleted_count > 0
        && let Err(e) = file.record_dead_tuples(deleted_count as u32)
    {
        log::warn!("[CASCADE] Failed to increment dead tuple count: {}", e);
    }

    log::info!(
        "[CASCADE] Deleted {} row(s) from '{}' due to ON DELETE CASCADE on parent '{}'",
        deleted_count,
        child_table,
        parent_value_str
    );

    // Recursive cascade into grandchild tables
    if !recursive_values.is_empty() && !cascade_to_grandchildren.is_empty() {
        let unique: HashSet<(String, String, String)> = recursive_values.drain(..).collect();
        for (grandchild_table, grandchild_fk_col, val) in unique {
            if val.is_empty()
                || val.eq_ignore_ascii_case("null")
                || val.eq_ignore_ascii_case("NULL")
            {
                continue;
            }
            let cycle_key = (grandchild_table.clone(), val.clone());
            if !visited.insert(cycle_key) {
                log::info!(
                    "[CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                    grandchild_table,
                    val
                );
                continue;
            }
            log::info!(
                "[CASCADE] Recursing into '{}' with value '{}' (FK col '{}')",
                grandchild_table,
                val,
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
    }

    Ok(())
}

/// Set FK column to NULL in all child rows that reference a parent key value.
///
/// Called when a FOREIGN KEY with ON DELETE SET NULL is being enforced.
/// Scans the child table's heap file, finds matching rows, deserialises them,
/// sets the FK column to NULL, re-serialises, and writes the updated tuple
/// back to the same slot.
pub(crate) fn set_null_child_rows(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    parent_value_str: &str,
    visited: &mut std::collections::HashSet<(String, String)>,
) -> Result<(), String> {
    use crate::backend::heap::HeapFile;
    use crate::catalog::types::Column;
    use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, SLOT_FLAG_DELETED};
    use std::collections::HashSet;

    let child_table_info = match catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    {
        Some(t) => t,
        None => {
            log::warn!(
                "[Constraint] Child table '{}' not found in catalog for SET NULL",
                child_table
            );
            return Ok(());
        }
    };
    let columns: &[Column] = &child_table_info.columns;
    let schema_types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let set_null_to_grandchildren: Vec<(String, String, String)> =
        match loaders::load_referencing_foreign_keys(db_name, child_table) {
            Ok(fks) => fks
                .into_iter()
                .filter(|(_, _, parent_col, _, action)| {
                    parent_col.eq_ignore_ascii_case(child_col)
                        && action.to_uppercase().contains("SET NULL")
                })
                .map(|(grandchild_table, grandchild_fk_col, _, _, _)| {
                    (
                        grandchild_table,
                        grandchild_fk_col,
                        parent_value_str.to_string(),
                    )
                })
                .collect(),
            Err(_) => Vec::new(),
        };

    let heap_path =
        std::path::PathBuf::from(format!("database/base/{}/{}.dat", db_name, child_table));
    if !heap_path.exists() {
        return Ok(());
    }

    let file = HeapFile::open(&heap_path)
        .map_err(|e| format!("Failed to open child heap for SET NULL: {}", e))?;

    let total_pages = file.page_count();
    let mut updated_count = 0usize;

    let mut recursive_values: Vec<(String, String, String)> = Vec::new();
    let mut pending_inserts: Vec<(Vec<u8>, Vec<u8>, u32, u32)> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(|e| e.to_string())?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        let page_modified = {
            let mut modified = false;

            for i in 0..num_items {
                let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
                let length =
                    u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
                let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

                if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                    continue;
                }

                let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();

                let decoded =
                    match crate::types::deserialize_nullable_row(&schema_types, &tuple_data) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                let col_pos = match columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(child_col))
                {
                    Some(p) => p,
                    None => continue,
                };

                let col_type = &columns[col_pos].data_type;
                let key_value = match parse_string_to_value(col_type, parent_value_str) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "[SET NULL] Failed to parse value '{}' for comparison: {}",
                            parent_value_str,
                            e
                        );
                        continue;
                    }
                };

                let matches = match decoded.get(col_pos) {
                    Some(Some(dv)) => {
                        use crate::types::Comparable;
                        matches!(dv.compare(&key_value), Ok(std::cmp::Ordering::Equal))
                    }
                    _ => false,
                };

                if !matches {
                    continue;
                }

                // Collect values for recursive SET NULL
                for (grandchild_table, grandchild_fk_col, _) in &set_null_to_grandchildren {
                    let val_str = parent_value_str.to_string();
                    recursive_values.push((
                        grandchild_table.clone(),
                        grandchild_fk_col.clone(),
                        val_str,
                    ));
                }

                // Build new values with FK column set to NULL
                let mut new_values: Vec<Option<DataValue>> = Vec::with_capacity(columns.len());
                for (j, dv_opt) in decoded.iter().enumerate() {
                    if j == col_pos {
                        new_values.push(None);
                    } else {
                        new_values.push(dv_opt.clone());
                    }
                }

                // Re-serialize
                let new_bytes =
                    match crate::types::serialize_nullable_typed_row(&schema_types, &new_values) {
                        Ok(b) => b,
                        Err(e) => {
                            log::warn!("[SET NULL] Failed to re-serialize tuple: {}", e);
                            continue;
                        }
                    };

                // Write back
                let old_len = length as usize;
                let new_len = new_bytes.len();

                if new_len <= old_len {
                    page.data[offset as usize..(offset as usize + new_len)]
                        .copy_from_slice(&new_bytes);
                    if new_len < old_len {
                        for b in
                            &mut page.data[offset as usize + new_len..offset as usize + old_len]
                        {
                            *b = 0;
                        }
                    }
                    if new_len != old_len {
                        let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                        page.data[base + 4..base + 6]
                            .copy_from_slice(&(new_len as u16).to_le_bytes());
                    }
                    let _ = crate::backend::executor::create_index::update_index_on_update(
                        db_name,
                        child_table,
                        columns,
                        &tuple_data,
                        &new_bytes,
                        page_num,
                        i as u32,
                        page_num,
                        i as u32,
                    );
                } else {
                    log::warn!(
                        "[SET NULL] New tuple larger than old ({} > {}); relocating slot",
                        new_len,
                        old_len
                    );
                    let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                    let flags =
                        u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                    let new_flags = flags | SLOT_FLAG_DELETED;
                    page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
                    pending_inserts.push((tuple_data, new_bytes, page_num, i as u32));
                }

                modified = true;
                updated_count += 1;
            }

            modified
        };

        if page_modified {
            file.write_page(&page, page_num)
                .map_err(|e| e.to_string())?;
            let _ = crate::backend::visibility_map::vm_clear_page(db_name, child_table, page_num);
        }
    }

    for (old_bytes, new_bytes, old_page, old_slot) in pending_inserts {
        match crate::backend::executor::compaction_api::insert_raw_tuple(
            db_name,
            child_table,
            &new_bytes,
        ) {
            Ok((new_page_id, new_slot_id)) => {
                let _ = crate::backend::executor::create_index::update_index_on_update(
                    db_name,
                    child_table,
                    columns,
                    &old_bytes,
                    &new_bytes,
                    old_page,
                    old_slot,
                    new_page_id,
                    new_slot_id,
                );
            }
            Err(e) => {
                log::error!("[SET NULL] Failed to relocate grown child tuple: {}", e);
            }
        }
    }

    log::info!(
        "[SET NULL] Set FK to NULL in {} row(s) from '{}' due to ON DELETE/UPDATE SET NULL on parent '{}'",
        updated_count,
        child_table,
        parent_value_str
    );

    // Recursive SET NULL into grandchild tables
    if !recursive_values.is_empty() && !set_null_to_grandchildren.is_empty() {
        let unique: HashSet<(String, String, String)> = recursive_values.drain(..).collect();
        for (grandchild_table, grandchild_fk_col, val) in unique {
            if val.is_empty()
                || val.eq_ignore_ascii_case("null")
                || val.eq_ignore_ascii_case("NULL")
            {
                continue;
            }
            let cycle_key = (grandchild_table.clone(), val.clone());
            if !visited.insert(cycle_key) {
                log::info!(
                    "[SET NULL] Cycle detected: skipping '{}' with value '{}' (already set to NULL)",
                    grandchild_table,
                    val
                );
                continue;
            }
            log::info!(
                "[SET NULL] Recursively setting NULL in '{}' col '{}' where value = '{}'",
                grandchild_table,
                grandchild_fk_col,
                val
            );
            set_null_child_rows(
                catalog,
                db_name,
                &grandchild_table,
                &grandchild_fk_col,
                &val,
                visited,
            )?;
        }
    }

    Ok(())
}

/// Update FK column in all child rows that match an old parent value to a new value.
///
/// Called when a FOREIGN KEY with ON UPDATE CASCADE is being enforced.
/// Scans the child table's heap file, finds rows where `child_col == old_parent_val`,
/// deserialises them, sets `child_col` to the new value, re-serialises, and writes
/// the updated tuple back to the same slot.
pub(crate) fn update_child_rows_fk(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    old_parent_val: &str,
    new_parent_val: &str,
    visited: &mut std::collections::HashSet<(String, String)>,
) -> Result<(), String> {
    use crate::backend::heap::HeapFile;
    use crate::catalog::types::Column;
    use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, SLOT_FLAG_DELETED};

    let child_table_info = match catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    {
        Some(t) => t,
        None => {
            log::warn!(
                "[Constraint] Child table '{}' not found in catalog for UPDATE CASCADE",
                child_table
            );
            return Ok(());
        }
    };
    let columns: &[Column] = &child_table_info.columns;
    let schema_types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();

    let heap_path =
        std::path::PathBuf::from(format!("database/base/{}/{}.dat", db_name, child_table));
    if !heap_path.exists() {
        return Ok(());
    }

    let file = HeapFile::open(&heap_path)
        .map_err(|e| format!("Failed to open child heap for UPDATE CASCADE: {}", e))?;

    let total_pages = file.page_count();
    let mut total_updated = 0usize;
    let mut pending_inserts: Vec<(Vec<u8>, Vec<u8>, u32, u32)> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        file.read_page(&mut page, page_num)
            .map_err(|e| e.to_string())?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        let page_modified = {
            let mut modified = false;

            for i in 0..num_items {
                let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
                let length =
                    u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
                let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

                if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                    continue;
                }

                let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();

                let decoded =
                    match crate::types::deserialize_nullable_row(&schema_types, &tuple_data) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                let col_pos = match columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(child_col))
                {
                    Some(p) => p,
                    None => continue,
                };

                let col_type = &columns[col_pos].data_type;
                let old_key = match parse_string_to_value(col_type, old_parent_val) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "[UPDATE CASCADE] Failed to parse old value '{}': {}",
                            old_parent_val,
                            e
                        );
                        continue;
                    }
                };

                let matches = match decoded.get(col_pos) {
                    Some(Some(dv)) => {
                        use crate::types::Comparable;
                        matches!(dv.compare(&old_key), Ok(std::cmp::Ordering::Equal))
                    }
                    _ => false,
                };

                if !matches {
                    continue;
                }

                // Build new values with FK column set to new value
                let new_dv = match parse_string_to_value(col_type, new_parent_val) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "[UPDATE CASCADE] Failed to parse new value '{}': {}",
                            new_parent_val,
                            e
                        );
                        continue;
                    }
                };

                let mut new_values: Vec<Option<DataValue>> = Vec::with_capacity(columns.len());
                for (j, dv_opt) in decoded.iter().enumerate() {
                    if j == col_pos {
                        new_values.push(Some(new_dv.clone()));
                    } else {
                        new_values.push(dv_opt.clone());
                    }
                }

                let new_bytes =
                    match crate::types::serialize_nullable_typed_row(&schema_types, &new_values) {
                        Ok(b) => b,
                        Err(e) => {
                            log::warn!("[UPDATE CASCADE] Failed to re-serialize tuple: {}", e);
                            continue;
                        }
                    };

                let old_len = length as usize;
                let new_len = new_bytes.len();

                if new_len <= old_len {
                    page.data[offset as usize..(offset as usize + new_len)]
                        .copy_from_slice(&new_bytes);
                    if new_len < old_len {
                        for b in
                            &mut page.data[offset as usize + new_len..offset as usize + old_len]
                        {
                            *b = 0;
                        }
                    }
                    if new_len != old_len {
                        let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                        page.data[base + 4..base + 6]
                            .copy_from_slice(&(new_len as u16).to_le_bytes());
                    }
                    let _ = crate::backend::executor::create_index::update_index_on_update(
                        db_name,
                        child_table,
                        columns,
                        &tuple_data,
                        &new_bytes,
                        page_num,
                        i as u32,
                        page_num,
                        i as u32,
                    );
                } else {
                    log::warn!(
                        "[UPDATE CASCADE] New tuple larger than old ({} > {}); relocating slot",
                        new_len,
                        old_len
                    );
                    let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
                    let flags =
                        u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                    let new_flags = flags | SLOT_FLAG_DELETED;
                    page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
                    pending_inserts.push((tuple_data, new_bytes, page_num, i as u32));
                }

                modified = true;
                total_updated += 1;
            }

            modified
        };

        if page_modified {
            file.write_page(&page, page_num)
                .map_err(|e| e.to_string())?;
            let _ = crate::backend::visibility_map::vm_clear_page(db_name, child_table, page_num);
        }
    }

    for (old_bytes, new_bytes, old_page, old_slot) in pending_inserts {
        match crate::backend::executor::compaction_api::insert_raw_tuple(
            db_name,
            child_table,
            &new_bytes,
        ) {
            Ok((new_page_id, new_slot_id)) => {
                let _ = crate::backend::executor::create_index::update_index_on_update(
                    db_name,
                    child_table,
                    columns,
                    &old_bytes,
                    &new_bytes,
                    old_page,
                    old_slot,
                    new_page_id,
                    new_slot_id,
                );
            }
            Err(e) => {
                log::error!(
                    "[UPDATE CASCADE] Failed to relocate grown child tuple: {}",
                    e
                );
            }
        }
    }

    log::info!(
        "[UPDATE CASCADE] Updated FK to '{}' in {} row(s) from '{}' due to ON UPDATE CASCADE",
        new_parent_val,
        total_updated,
        child_table
    );

    // Recursive UPDATE CASCADE into grandchild tables
    let update_cascade_fks: Vec<(String, String)> =
        match loaders::load_referencing_foreign_keys(db_name, child_table) {
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

    if !update_cascade_fks.is_empty() {
        for (grandchild_table, grandchild_fk_col) in &update_cascade_fks {
            let cycle_key = (grandchild_table.clone(), old_parent_val.to_string());
            if !visited.insert(cycle_key) {
                log::info!(
                    "[UPDATE CASCADE] Cycle detected: skipping '{}' with value '{}' (already cascaded)",
                    grandchild_table,
                    old_parent_val
                );
                continue;
            }
            log::info!(
                "[UPDATE CASCADE] Recursively cascading into '{}': col '{}' from '{}' → '{}'",
                grandchild_table,
                grandchild_fk_col,
                old_parent_val,
                new_parent_val
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
    }

    Ok(())
}
