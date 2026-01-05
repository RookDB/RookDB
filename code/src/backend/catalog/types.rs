//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a column within a table.
#[derive(Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

/// Represents a table schema.
#[derive(Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
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