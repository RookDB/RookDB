//! Value lookup helpers for constraint checking.
//!
//! Every lookup goes through a B+ Tree index on the column. CREATE TABLE
//! builds the indexes a constraint needs; a table that lacks one (older data,
//! or an index file that was lost) gets it built on first use, so no check
//! ever scans a table.

use crate::backend::engine::Engine;
use crate::backend::executor::create_index::ensure_column_index;
use crate::catalog::types::{Catalog, Column};
use crate::types::{DataType, DataValue};

/// Name of the index on a foreign key's child column, as CREATE TABLE gives it.
fn child_index_name(child_table: &str, child_col: &str) -> String {
    format!("fk_{}_{}", child_table, child_col)
}

/// Name of the index on a foreign key's parent column when none exists.
fn parent_index_name(parent_table: &str, parent_col: &str) -> String {
    format!("fkp_{}_{}", parent_table, parent_col)
}

fn find_column<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
    column: &str,
) -> Option<&'a Column> {
    catalog
        .databases
        .get(db_name)?
        .tables
        .get(table_name)?
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(column))
}

/// Heap locations of every entry of the index `index_name` equal to `key`.
fn index_search(
    engine: &Engine,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    col_type: &DataType,
    key: &DataValue,
) -> Result<Vec<(u32, u32)>, String> {
    let idx_path = engine.paths().index(db_name, table_name, index_name);
    crate::backend::cache::with_btree(
        engine,
        &idx_path,
        || -> std::io::Result<crate::backend::index::btree::BTree> {
            let mut bt = crate::backend::index::btree::BTree::open(idx_path.clone())?;
            bt.set_key_type(col_type.clone());
            Ok(bt)
        },
        |bt| {
            bt.set_key_type(col_type.clone());
            bt.search_all(key)
        },
    )
    .map_err(|e| {
        format!(
            "index '{}' of '{}.{}' cannot be searched: {}",
            index_name, db_name, table_name, e
        )
    })
}

/// Check whether a specific value exists in a table column.
///
/// `key_value` must already have the column's type.
pub(crate) fn value_exists_in_table(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    column_name: &str,
    col_type: &DataType,
    key_value: &DataValue,
) -> Result<bool, String> {
    let unique = find_column(catalog, db_name, table_name, column_name)
        .is_some_and(|c| c.constraints.unique);
    let index = ensure_column_index(
        catalog,
        db_name,
        table_name,
        column_name,
        &parent_index_name(table_name, column_name),
        unique,
    )?;
    index_search(
        catalog.engine(),
        db_name,
        table_name,
        &index,
        col_type,
        key_value,
    )
    .map(|found| !found.is_empty())
}

/// Heap locations of the rows of `child_table` whose `child_col` equals
/// `parent_key`, in heap order, found through the child column's index.
///
/// `parent_key` is converted to the column's type first; a key the column
/// cannot hold matches no row.
pub(crate) fn child_row_pointers(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    parent_key: &DataValue,
) -> Result<Vec<(u32, u32)>, String> {
    let Some(col) = find_column(catalog, db_name, child_table, child_col) else {
        return Ok(Vec::new());
    };
    let key = match crate::types::functions::assign_cast(parent_key, &col.data_type) {
        Ok(v) => v,
        Err(e) => {
            log::warn!(
                "[Constraint] Failed to convert value for FK child lookup: {}",
                e
            );
            return Ok(Vec::new());
        }
    };
    let index = ensure_column_index(
        catalog,
        db_name,
        child_table,
        child_col,
        &child_index_name(child_table, child_col),
        false,
    )?;
    let mut pointers = index_search(
        catalog.engine(),
        db_name,
        child_table,
        &index,
        &col.data_type,
        &key,
    )?;
    pointers.sort_unstable();
    pointers.dedup();
    Ok(pointers)
}

/// Check whether a child table has any row whose given column matches the value.
///
/// Used by parent DELETE and UPDATE validation to detect referencing rows.
pub(crate) fn child_has_referencing_row(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    value: &DataValue,
) -> Result<bool, String> {
    child_row_pointers(catalog, db_name, child_table, child_col, value)
        .map(|pointers| !pointers.is_empty())
}

/// Decoded rows of `child_table` whose `child_col` equals `value`, read by
/// way of the column's index; each page is read once.
pub(crate) fn child_rows_referencing(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_col: &str,
    value: &DataValue,
) -> Result<Vec<Vec<Option<DataValue>>>, String> {
    let pointers = child_row_pointers(catalog, db_name, child_table, child_col, value)?;
    if pointers.is_empty() {
        return Ok(Vec::new());
    }
    let Some(table) = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(child_table))
    else {
        return Ok(Vec::new());
    };
    let schema: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
    let heap_path = catalog.engine().paths().heap(db_name, child_table);
    let file = crate::backend::heap::HeapFile::open(catalog.engine(), &heap_path)
        .map_err(|e| format!("Failed to open child heap '{}': {}", child_table, e))?;

    let mut rows = Vec::with_capacity(pointers.len());
    let mut page = crate::page::Page::new();
    let mut loaded: Option<u32> = None;
    for (page_num, slot) in pointers {
        if loaded != Some(page_num) {
            file.read_page(&mut page, page_num)
                .map_err(|e| e.to_string())?;
            loaded = Some(page_num);
        }
        if let Some(bytes) = page.tuple(slot)
            && let Ok(row) = crate::types::deserialize_nullable_row(&schema, bytes)
        {
            rows.push(row);
        }
    }
    Ok(rows)
}
