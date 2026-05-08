//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{DataType, DataValue};

fn default_nullable() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Constraints {
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<DataValue>,
    pub check: Option<String>,
}

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub constraints: Constraints,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
            constraints: Constraints::default(),
        }
    }
}

/// Represents a table schema.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub columns: Vec<Column>,
}

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}
