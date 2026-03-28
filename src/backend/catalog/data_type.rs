//! Data type definitions and parsing for RookDB
//! Provides typed representation of SQL data types including BLOB and ARRAY

use serde::{Deserialize, Serialize};

/// Represents the typed data types supported by RookDB
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum DataType {
    Int32,
    Boolean,
    Text,
    Blob,
    Array { element_type: Box<DataType> },
}

impl DataType {
    /// Check if this type is variable-length
    pub fn is_variable_length(&self) -> bool {
        matches!(self, DataType::Text | DataType::Blob | DataType::Array { .. })
    }

    /// Get fixed size for fixed-length types, None for variable-length
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Int32 => Some(4),
            DataType::Boolean => Some(1),
            DataType::Text | DataType::Blob | DataType::Array { .. } => None,
        }
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match self {
            DataType::Int32 => "INT".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Blob => "BLOB".to_string(),
            DataType::Array { element_type } => format!("ARRAY<{}>", element_type.to_string()),
        }
    }

    /// Parse type declaration string like "INT", "BLOB", "ARRAY<INT>"
    pub fn parse(type_str: &str) -> Result<DataType, String> {
        let normalized = type_str.trim().to_uppercase();

        match normalized.as_str() {
            "INT" | "INT32" => Ok(DataType::Int32),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            "TEXT" | "VARCHAR" => Ok(DataType::Text),
            "BLOB" | "BYTEA" => Ok(DataType::Blob),
            s if s.starts_with("ARRAY<") && s.ends_with('>') => {
                let inner = &s[6..s.len() - 1];
                let element_type = Box::new(DataType::parse(inner)?);
                Ok(DataType::Array { element_type })
            }
            _ => Err(format!("Unknown data type: {}", type_str)),
        }
    }
}

/// Represents a typed value in RookDB
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Int32(i32),
    Boolean(bool),
    Text(String),
    Blob(Vec<u8>),
    Array(Vec<Value>),
}

impl Value {
    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Text, // Null can be any type
            Value::Int32(_) => DataType::Int32,
            Value::Boolean(_) => DataType::Boolean,
            Value::Text(_) => DataType::Text,
            Value::Blob(_) => DataType::Blob,
            Value::Array(items) => {
                if items.is_empty() {
                    DataType::Array {
                        element_type: Box::new(DataType::Text),
                    }
                } else {
                    DataType::Array {
                        element_type: Box::new(items[0].data_type()),
                    }
                }
            }
        }
    }

    /// Convert value to string for display
    pub fn to_display_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Boolean(v) => v.to_string(),
            Value::Text(v) => v.clone(),
            Value::Blob(v) => format!("BLOB({}bytes)", v.len()),
            Value::Array(items) => {
                let item_strs: Vec<String> = items
                    .iter()
                    .map(|v| match v {
                        Value::Text(s) => format!("\"{}\"", s),
                        _ => v.to_display_string(),
                    })
                    .collect();
                format!("[{}]", item_strs.join(","))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_types() {
        assert_eq!(DataType::parse("INT").unwrap(), DataType::Int32);
        assert_eq!(DataType::parse("BOOLEAN").unwrap(), DataType::Boolean);
        assert_eq!(DataType::parse("TEXT").unwrap(), DataType::Text);
        assert_eq!(DataType::parse("BLOB").unwrap(), DataType::Blob);
    }

    #[test]
    fn test_parse_array_types() {
        let int_array = DataType::parse("ARRAY<INT>").unwrap();
        assert!(matches!(int_array, DataType::Array { .. }));

        let text_array = DataType::parse("ARRAY<TEXT>").unwrap();
        assert!(matches!(text_array, DataType::Array { .. }));
    }

    #[test]
    fn test_parse_nested_array_types() {
        let nested_array = DataType::parse("ARRAY<ARRAY<INT>>").unwrap();
        assert_eq!(
            nested_array,
            DataType::Array {
                element_type: Box::new(DataType::Array {
                    element_type: Box::new(DataType::Int32),
                }),
            }
        );
    }

    #[test]
    fn test_is_variable_length() {
        assert!(!DataType::Int32.is_variable_length());
        assert!(!DataType::Boolean.is_variable_length());
        assert!(DataType::Text.is_variable_length());
        assert!(DataType::Blob.is_variable_length());
        assert!(DataType::Array {
            element_type: Box::new(DataType::Int32)
        }
        .is_variable_length());
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Null.to_display_string(), "NULL");
        assert_eq!(Value::Int32(42).to_display_string(), "42");
        assert_eq!(Value::Boolean(true).to_display_string(), "true");
        assert_eq!(Value::Text("hello".to_string()).to_display_string(), "hello");
    }
}
