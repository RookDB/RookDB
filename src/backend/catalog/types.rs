//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// All supported column data types.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", content = "param")]
pub enum DataType {
    Int,
    Float,
    Bool,
    Text,
    Varchar(u32), // max length
    Date,         // stored as i32 days since Unix epoch
    Timestamp,    // stored as i64 microseconds since Unix epoch
}

impl DataType {
    /// Parse a type name string (from catalog JSON or user input) into a DataType.
    pub fn from_str(s: &str) -> Option<DataType> {
        let upper = s.trim().to_uppercase();
        if upper == "INT" || upper == "INTEGER" {
            return Some(DataType::Int);
        }
        if upper == "FLOAT" || upper == "REAL" || upper == "DOUBLE" {
            return Some(DataType::Float);
        }
        if upper == "BOOL" || upper == "BOOLEAN" {
            return Some(DataType::Bool);
        }
        if upper == "TEXT" {
            return Some(DataType::Text);
        }
        if upper == "DATE" {
            return Some(DataType::Date);
        }
        if upper == "TIMESTAMP" {
            return Some(DataType::Timestamp);
        }
        if upper.starts_with("VARCHAR") {
            let n: u32 = upper
                .trim_start_matches("VARCHAR")
                .trim_matches(|c: char| c == '(' || c == ')' || c == ' ')
                .parse()
                .unwrap_or(255);
            return Some(DataType::Varchar(n));
        }
        None
    }

    /// Whether this type has a fixed on-disk size.
    pub fn is_fixed(&self) -> bool {
        matches!(
            self,
            DataType::Int | DataType::Float | DataType::Bool | DataType::Date | DataType::Timestamp
        )
    }

    /// Fixed byte size for fixed-length types; None for variable-length.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Int => Some(4),
            DataType::Float => Some(8),
            DataType::Bool => Some(1),
            DataType::Date => Some(4),
            DataType::Timestamp => Some(8),
            _ => None,
        }
    }

    /// Returns a display string, used when serialising legacy JSON.
    pub fn as_legacy_str(&self) -> String {
        match self {
            DataType::Int => "INT".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Bool => "BOOL".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Varchar(n) => format!("VARCHAR({})", n),
            DataType::Date => "DATE".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
        }
    }
}

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub name: String,
    /// Stored as a plain string in JSON for backwards compat with existing catalog files.
    pub data_type: String,
}

impl Column {
    /// Parse the stored string into a DataType enum.
    pub fn parsed_type(&self) -> DataType {
        DataType::from_str(&self.data_type).unwrap_or(DataType::Text)
    }
}

/// Represents a table schema.
#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    pub columns: Vec<Column>,
}

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize, Debug)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}
