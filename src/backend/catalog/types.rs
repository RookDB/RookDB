//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Sort direction for a sort key column.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Specifies a single column to sort by and its direction.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SortKey {
    /// 0-based index into the table's column list
    pub column_index: u32,
    /// Sort direction (ASC or DESC)
    pub direction: SortDirection,
}

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

/// Represents a table schema.
#[derive(Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
    /// Sort key columns (None for heap tables)
    #[serde(default)]
    pub sort_keys: Option<Vec<SortKey>>,
    /// File type: "heap" or "ordered" (None defaults to "heap")
    #[serde(default)]
    pub file_type: Option<String>,
}

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}
