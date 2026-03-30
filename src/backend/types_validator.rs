
/// Supported data types in RookDB
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text { max_length: usize },
}

impl DataType {
    /// Parse a data type string (case-insensitive).
    /// Returns the normalized DataType or an error if the type is unsupported.
    pub fn from_str(type_str: &str) -> Result<Self, String> {
        let normalized_str = type_str.to_uppercase();
        let normalized = normalized_str.trim();
        
        debug_print_datatype(&format!("Parsing data type: '{}'", normalized));
        
        match normalized {
            "INT" | "INTEGER" => {
                debug_print_datatype("→ Normalized to INT");
                Ok(DataType::Integer)
            }
            "TEXT" => {
                debug_print_datatype("→ Normalized to TEXT(10)");
                Ok(DataType::Text { max_length: 10 }) // Default TEXT size is 10
            }
            typ => {
                let msg = format!(
                    "Unsupported data type: '{}'. Supported types: INT, TEXT",
                    typ
                );
                debug_print_datatype(&msg);
                Err(msg)
            }
        }
    }

    /// Get the byte size needed to store this type
    pub fn byte_size(&self) -> usize {
        match self {
            DataType::Integer => 4, // i32
            DataType::Text { max_length } => *max_length,
        }
    }

    /// Validate a value against this data type
    pub fn validate_value(&self, value: &str) -> Result<(), String> {
        match self {
            DataType::Integer => {
                if value.parse::<i32>().is_ok() {
                    debug_print_validation(&format!(" Valid INT value: '{}'", value));
                    Ok(())
                } else {
                    let msg = format!(
                        "Invalid INT value: '{}' (not a valid 32-bit integer)",
                        value
                    );
                    debug_print_validation(&msg);
                    Err(msg)
                }
            }
            DataType::Text { max_length } => {
                if value.is_empty() {
                    let msg = "TEXT value cannot be empty".to_string();
                    debug_print_validation(&msg);
                    return Err(msg);
                }
                
                if value.len() > *max_length {
                    debug_print_validation(&format!(
                        "TEXT value '{}' exceeds max length {} chars. Will be truncated to {}.",
                        value, max_length, max_length
                    ));
                    // Warning but still valid - will be truncated
                }
                
                debug_print_validation(&format!("Valid TEXT value: '{}' (length: {})", value, value.len()));
                Ok(())
            }
        }
    }

    /// Serialize a value to bytes according to this type
    pub fn serialize_value(&self, value: &str) -> Result<Vec<u8>, String> {
        match self {
            DataType::Integer => {
                let num = value.parse::<i32>().map_err(|_| {
                    format!("Failed to parse INT value: '{}'", value)
                })?;
                debug_print_validation(&format!(
                    "Serialized INT: '{}' → {} (bytes: {:?})",
                    value, num, num.to_le_bytes()
                ));
                Ok(num.to_le_bytes().to_vec())
            }
            DataType::Text { max_length } => {
                let mut text_bytes = value.as_bytes().to_vec();
                
                // Truncate if necessary
                if text_bytes.len() > *max_length {
                    debug_print_validation(&format!(
                        "Truncating TEXT '{}' from {} to {} chars",
                        value, text_bytes.len(), max_length
                    ));
                    text_bytes.truncate(*max_length);
                }
                
                // Pad with spaces to reach max_length
                if text_bytes.len() < *max_length {
                    text_bytes.extend(vec![b' '; *max_length - text_bytes.len()]);
                }
                
                debug_print_validation(&format!(
                    "Serialized TEXT: '{}' → {} bytes (padded to {})",
                    value, text_bytes.len(), max_length
                ));
                Ok(text_bytes)
            }
        }
    }

    /// Deserialize bytes back to a readable string
    pub fn deserialize_value(&self, bytes: &[u8]) -> Result<String, String> {
        match self {
            DataType::Integer => {
                if bytes.len() != 4 {
                    return Err(format!("Invalid byte length for INT: {} (expected 4)", bytes.len()));
                }
                let arr: [u8; 4] = bytes[0..4].try_into()
                    .map_err(|_| "Failed to convert bytes to INT".to_string())?;
                let num = i32::from_le_bytes(arr);
                debug_print_validation(&format!(
                    "Deserialized INT: {:?} → {}",
                    bytes, num
                ));
                Ok(num.to_string())
            }
            DataType::Text { max_length } => {
                if bytes.len() != *max_length {
                    return Err(format!(
                        "Invalid byte length for TEXT: {} (expected {})",
                        bytes.len(), max_length
                    ));
                }
                let text = String::from_utf8_lossy(bytes).trim().to_string();
                debug_print_validation(&format!(
                    "Deserialized TEXT: {:?} → '{}'",
                    bytes, text
                ));
                Ok(text)
            }
        }
    }
}

/// Check if all data types in a column list are supported
pub fn validate_schema(columns: &[(&str, &str)]) -> Result<Vec<(String, DataType)>, String> {
    debug_print_datatype(&format!("Validating schema with {} columns", columns.len()));
    
    let mut validated = Vec::new();
    
    for (i, (name, type_str)) in columns.iter().enumerate() {
        debug_print_datatype(&format!("  Column {}: '{}' with type '{}'", i + 1, name, type_str));
        
        let data_type = DataType::from_str(type_str)?;
        validated.push((name.to_string(), data_type));
    }
    
    debug_print_datatype(&format!("Schema validation successful: {} columns", validated.len()));
    Ok(validated)
}

/// Print debug information for data type operations
fn debug_print_datatype(_msg: &str) {
    // Suppressed: Clutters tuple visual displays.
}

/// Print debug information for value validation
fn debug_print_validation(_msg: &str) {
    // Suppressed: Clutters tuple visual displays.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_type() {
        let int_type = DataType::from_str("INT").unwrap();
        assert_eq!(int_type, DataType::Integer);
        assert_eq!(int_type.byte_size(), 4);
    }

    #[test]
    fn test_text_type() {
        let text_type = DataType::from_str("TEXT").unwrap();
        assert_eq!(text_type, DataType::Text { max_length: 10 });
        assert_eq!(text_type.byte_size(), 10);
    }

    #[test]
    fn test_case_insensitive() {
        assert!(DataType::from_str("int").is_ok());
        assert!(DataType::from_str("TEXT").is_ok());
        assert!(DataType::from_str("text").is_ok());
        assert!(DataType::from_str("InT").is_ok());
    }

    #[test]
    fn test_unsupported_type() {
        assert!(DataType::from_str("FLOAT").is_err());
        assert!(DataType::from_str("VARCHAR").is_err());
    }

    #[test]
    fn test_int_validation() {
        let int_type = DataType::Integer;
        assert!(int_type.validate_value("42").is_ok());
        assert!(int_type.validate_value("abc").is_err());
    }

    #[test]
    fn test_int_serialization() {
        let int_type = DataType::Integer;
        let bytes = int_type.serialize_value("42").unwrap();
        assert_eq!(bytes.len(), 4);
        let deserialized = int_type.deserialize_value(&bytes).unwrap();
        assert_eq!(deserialized, "42");
    }
}
