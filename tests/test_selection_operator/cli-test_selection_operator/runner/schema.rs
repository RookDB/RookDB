// Helper functions for creating test schemas.

use storage_manager::backend::catalog::types::{Column, Table};

// Standard schema we use everywhere.
// Columns: id (INT), amount (FLOAT), name (STRING), date (DATE)
pub fn default_schema() -> Table {
    Table {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "INT".to_string(),
            },
            Column {
                name: "amount".to_string(),
                data_type: "FLOAT".to_string(),
            },
            Column {
                name: "name".to_string(),
                data_type: "STRING".to_string(),
            },
            Column {
                name: "date".to_string(),
                data_type: "DATE".to_string(),
            },
        ],
    }
}

// Build a custom schema from column definitions
pub fn create_schema(columns: Vec<(&str, &str)>) -> Table {
    Table {
        columns: columns
            .into_iter()
            .map(|(name, data_type)| Column {
                name: name.to_string(),
                data_type: data_type.to_string(),
            })
            .collect(),
    }
}
