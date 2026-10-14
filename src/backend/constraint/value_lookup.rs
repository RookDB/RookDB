//! Value lookup helpers for constraint checking.
//!
//! Every lookup goes through a B+ Tree index on the key columns. CREATE TABLE
//! builds the indexes a constraint needs; a table that lacks one (older data,
//! or an index file that was lost) gets it built on first use, so no check
//! ever scans a table.

use crate::backend::engine::Engine;
use crate::backend::executor::create_index::ensure_columns_index;
use crate::catalog::types::{Catalog, Column};
use crate::types::functions::assign_cast;
use crate::types::{DataType, DataValue};

/// Name of the index on a foreign key's child columns, as CREATE TABLE gives it.
fn child_index_name(child_table: &str, child_cols: &[String]) -> String {
    format!("fk_{}_{}", child_table, child_cols.join("_"))
}

/// Name of the index on a foreign key's parent columns when none exists.
fn parent_index_name(parent_table: &str, parent_cols: &[String]) -> String {
    format!("fkp_{}_{}", parent_table, parent_cols.join("_"))
}

fn table_columns<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
) -> Option<&'a [Column]> {
    catalog
        .databases
        .get(db_name)?
        .tables
        .get(table_name)
        .map(|t| t.columns.as_slice())
}

/// The columns of `table_name` named in `names`, in that order; `None` when
/// the table or one of them does not exist.
fn find_columns<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
    names: &[String],
) -> Option<Vec<&'a Column>> {
    let columns = table_columns(catalog, db_name, table_name)?;
    names
        .iter()
        .map(|n| columns.iter().find(|c| c.name.eq_ignore_ascii_case(n)))
        .collect()
}

/// Heap locations of every entry of the index `index_name` equal to `key`
/// (one value per index column).
fn index_search(
    engine: &Engine,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    key_types: &[DataType],
    key: &[DataValue],
) -> Result<Vec<(u32, u32)>, String> {
    let idx_path = engine.paths().index(db_name, table_name, index_name);
    crate::backend::cache::with_btree(
        engine,
        &idx_path,
        || -> std::io::Result<crate::backend::index::btree::BTree> {
            let mut bt = crate::backend::index::btree::BTree::open(idx_path.clone())?;
            bt.set_key_types(key_types.to_vec());
            Ok(bt)
        },
        |bt| {
            bt.set_key_types(key_types.to_vec());
            bt.search_all_keys(key)
        },
    )
    .map_err(|e| {
        format!(
            "index '{}' of '{}.{}' cannot be searched: {}",
            index_name, db_name, table_name, e
        )
    })
}

/// Heap locations of the rows of `table_name` whose `names` columns equal
/// `key` (one typed value per name), found through an index on exactly those
/// columns, built under `new_name` when the table has none.
fn rows_with_key(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    names: &[String],
    key: &[DataValue],
    new_name: &str,
    unique: bool,
) -> Result<Vec<(u32, u32)>, String> {
    let (index, index_cols) =
        ensure_columns_index(catalog, db_name, table_name, names, new_name, unique)?;
    // The index may list the same columns in another order than the key.
    let mut types = Vec::with_capacity(index_cols.len());
    let mut ordered = Vec::with_capacity(index_cols.len());
    for index_col in &index_cols {
        let at = names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(index_col))
            .ok_or_else(|| format!("index '{}' does not match its columns", index))?;
        let col = find_columns(
            catalog,
            db_name,
            table_name,
            std::slice::from_ref(index_col),
        )
        .and_then(|c| c.first().map(|c| c.data_type.clone()))
        .ok_or_else(|| format!("column '{}' of '{}' not found", index_col, table_name))?;
        types.push(col);
        ordered.push(key[at].clone());
    }
    index_search(
        catalog.engine(),
        db_name,
        table_name,
        &index,
        &types,
        &ordered,
    )
}

/// Check whether a row of `table_name` has `key` in its `columns`.
///
/// `key` must already have the columns' types, one value per column.
pub(crate) fn key_exists_in_table(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[String],
    key: &[DataValue],
) -> Result<bool, String> {
    let unique = matches!(
        find_columns(catalog, db_name, table_name, columns).as_deref(),
        Some([c]) if c.constraints.unique
    );
    rows_with_key(
        catalog,
        db_name,
        table_name,
        columns,
        key,
        &parent_index_name(table_name, columns),
        unique,
    )
    .map(|found| !found.is_empty())
}

/// Heap locations of the rows of `child_table` whose `child_cols` equal
/// `parent_key`, in heap order, found through the index on those columns.
///
/// `parent_key` holds one value per column; each is converted to its
/// column's type first, and a key the columns cannot hold matches no row.
pub(crate) fn child_row_pointers(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    parent_key: &[DataValue],
) -> Result<Vec<(u32, u32)>, String> {
    let Some(cols) = find_columns(catalog, db_name, child_table, child_cols) else {
        return Ok(Vec::new());
    };
    let mut key = Vec::with_capacity(cols.len());
    for (col, value) in cols.iter().zip(parent_key) {
        match assign_cast(value, &col.data_type) {
            Ok(v) => key.push(v),
            Err(e) => {
                log::warn!(
                    "[Constraint] Failed to convert value for FK child lookup: {}",
                    e
                );
                return Ok(Vec::new());
            }
        }
    }
    if key.len() != child_cols.len() {
        return Ok(Vec::new());
    }
    let mut pointers = rows_with_key(
        catalog,
        db_name,
        child_table,
        child_cols,
        &key,
        &child_index_name(child_table, child_cols),
        false,
    )?;
    pointers.sort_unstable();
    pointers.dedup();
    Ok(pointers)
}

/// Check whether a child table has any row whose `child_cols` match the key.
///
/// Used by parent DELETE and UPDATE validation to detect referencing rows.
pub(crate) fn child_has_referencing_row(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    key: &[DataValue],
) -> Result<bool, String> {
    child_row_pointers(catalog, db_name, child_table, child_cols, key)
        .map(|pointers| !pointers.is_empty())
}

/// Decoded rows of `child_table` whose `child_cols` equal `key`, read by
/// way of the columns' index; each page is read once.
pub(crate) fn child_rows_referencing(
    catalog: &Catalog,
    db_name: &str,
    child_table: &str,
    child_cols: &[String],
    key: &[DataValue],
) -> Result<Vec<Vec<Option<DataValue>>>, String> {
    let pointers = child_row_pointers(catalog, db_name, child_table, child_cols, key)?;
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
