//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

/// Represents a table schema with statistics for query optimization.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Table {
    pub columns: Vec<Column>,
    /// Estimated number of rows in table
    #[serde(default)]
    pub row_count: u64,
    /// Number of pages occupied by table
    #[serde(default)]
    pub page_count: u64,
    /// Average bytes per row (for selectivity estimation)
    #[serde(default = "default_avg_row_size")]
    pub avg_row_size: usize,
}

fn default_avg_row_size() -> usize { 128 }

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}
