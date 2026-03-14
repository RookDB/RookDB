//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::catalog::data_type::DataType;

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>, // Keep for backward compatibility
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
}

impl Column {
    /// Create a new column with typed data type
    pub fn new(name: String, data_type: DataType) -> Self {
        Column {
            name,
            data_type: Some(data_type.to_string()),
            nullable: false,
            schema_version: Some(2),
        }
    }

    /// Parse the data type string into a typed DataType
    pub fn parse_data_type(&self) -> Result<DataType, String> {
        match &self.data_type {
            Some(type_str) => DataType::parse(type_str),
            None => Err("Column has no data type".to_string()),
        }
    }
}

/// Represents a table schema.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Table {
    pub columns: Vec<Column>,
    pub schema_version: Option<u32>,
}

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}
