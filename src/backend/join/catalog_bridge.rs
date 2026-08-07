//! Catalog lookups.
//!
//! The only place the join subsystem reads the catalog. Everything below works
//! from a `TableRef`.

use std::path::PathBuf;

use crate::catalog::Catalog;
use crate::layout::DATABASE_DIR;

use super::error::JoinError;
use super::source::TableRef;

/// Path of a relation's heap file.
pub fn table_path(database: &str, table: &str) -> PathBuf {
    PathBuf::from(DATABASE_DIR)
        .join(database)
        .join(format!("{table}.dat"))
}

/// Resolve a catalog entry into a join input.
pub fn resolve(
    catalog: &Catalog,
    database: &str,
    table: &str,
    alias: &str,
) -> Result<TableRef, JoinError> {
    let entry = catalog
        .databases
        .get(database)
        .ok_or_else(|| JoinError::schema(format!("no database '{database}'")))?
        .tables
        .get(table)
        .ok_or_else(|| JoinError::schema(format!("no table '{table}' in '{database}'")))?;

    if entry.columns.is_empty() {
        return Err(JoinError::schema(format!("table '{table}' has no columns")));
    }

    let path = table_path(database, table);
    if !path.exists() {
        return Err(JoinError::Io(format!(
            "table '{table}' is in the catalog but its file {} is missing",
            path.display()
        )));
    }

    Ok(TableRef::new(alias, path, entry.columns.clone()))
}

/// Table names in a database, sorted so the listing is stable between runs.
///
/// The catalog stores them in a hash map, whose iteration order varies per
/// process; an unsorted menu would renumber itself between sessions.
pub fn table_names(catalog: &Catalog, database: &str) -> Vec<String> {
    let mut names: Vec<String> = catalog
        .databases
        .get(database)
        .map(|entry| entry.tables.keys().cloned().collect())
        .unwrap_or_default();
    names.sort();
    names
}
