//! Where a database's files live, relative to the working directory.
//!
//! Every table and index file path is built here, so the layout is defined
//! in one place.

/// Directory holding one database's files.
pub fn database_dir(db: &str) -> String {
    format!("database/base/{}", db)
}

/// A table's heap file.
pub fn heap(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.dat", db, table)
}

/// The free-space map fork of a table's heap file.
pub fn heap_fsm(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.dat.fsm", db, table)
}

/// A table's visibility map.
pub fn visibility_map(db: &str, table: &str) -> String {
    format!("database/base/{}/{}_vm", db, table)
}

/// A named index on a table.
pub fn index(db: &str, table: &str, index: &str) -> String {
    format!("database/base/{}/{}.{}.idx", db, table, index)
}

/// The JSON metadata sidecar of a named index.
pub fn index_meta(db: &str, table: &str, index: &str) -> String {
    format!("database/base/{}/{}.{}.idx.meta", db, table, index)
}

/// The single unnamed index file older tables may have.
pub fn legacy_index(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.idx", db, table)
}

/// The JSON metadata sidecar of the legacy index file.
pub fn legacy_index_meta(db: &str, table: &str) -> String {
    format!("database/base/{}/{}.idx.meta", db, table)
}
