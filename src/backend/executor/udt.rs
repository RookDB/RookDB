use crate::catalog::types::UdtDefinition;

/// Handles serialization/deserialization of UDT composite values.
pub struct UdtSerializer;

impl UdtSerializer {
    /// Serialize a UDT value (given as field values) into bytes.
    /// Each field is serialized according to its type from the UDT definition.
    pub fn serialize(definition: &UdtDefinition, field_values: &[&str]) -> Result<Vec<u8>, String> {
        if field_values.len() != definition.fields.len() {
            return Err(format!(
                "UDT field count mismatch: expected {}, got {}",
                definition.fields.len(),
                field_values.len()
            ));
        }

        let mut bytes = Vec::new();

        for (i, field) in definition.fields.iter().enumerate() {
            let val = field_values[i].trim();
            match field.data_type.as_str() {
                "INT" => {
                    let num: i32 = val.parse().map_err(|e| {
                        format!("Failed to parse INT field '{}': {}", field.name, e)
                    })?;
                    bytes.extend_from_slice(&num.to_le_bytes());
                }
                "TEXT" => {
                    let mut t = val.as_bytes().to_vec();
                    if t.len() > 10 {
                        t.truncate(10);
                    } else if t.len() < 10 {
                        t.extend(vec![b' '; 10 - t.len()]);
                    }
                    bytes.extend_from_slice(&t);
                }
                "BOOLEAN" => {
                    let b = match val.to_lowercase().as_str() {
                        "true" | "1" | "yes" => 1u8,
                        "false" | "0" | "no" => 0u8,
                        _ => {
                            return Err(format!(
                                "Invalid BOOLEAN value '{}' for field '{}'",
                                val, field.name
                            ));
                        }
                    };
                    bytes.push(b);
                }
                other => {
                    return Err(format!(
                        "Unsupported field type '{}' in UDT field '{}'",
                        other, field.name
                    ));
                }
            }
        }

        Ok(bytes)
    }

    /// Deserialize UDT bytes back into a vector of displayable field strings.
    pub fn deserialize(definition: &UdtDefinition, data: &[u8]) -> Result<Vec<String>, String> {
        let mut cursor = 0;
        let mut values = Vec::new();

        for field in &definition.fields {
            match field.data_type.as_str() {
                "INT" => {
                    if cursor + 4 > data.len() {
                        return Err(format!("Truncated INT field '{}'", field.name));
                    }
                    let val = i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                    values.push(val.to_string());
                    cursor += 4;
                }
                "TEXT" => {
                    if cursor + 10 > data.len() {
                        return Err(format!("Truncated TEXT field '{}'", field.name));
                    }
                    let text = String::from_utf8_lossy(&data[cursor..cursor + 10])
                        .trim()
                        .to_string();
                    values.push(text);
                    cursor += 10;
                }
                "BOOLEAN" => {
                    if cursor + 1 > data.len() {
                        return Err(format!("Truncated BOOLEAN field '{}'", field.name));
                    }
                    let val = if data[cursor] != 0 { "true" } else { "false" };
                    values.push(val.to_string());
                    cursor += 1;
                }
                other => {
                    return Err(format!(
                        "Unsupported field type '{}' in UDT field '{}'",
                        other, field.name
                    ));
                }
            }
        }

        Ok(values)
    }

    /// Format UDT fields as a display string: (field1_name=value1, field2_name=value2, ...)
    pub fn to_display_string(definition: &UdtDefinition, data: &[u8]) -> Result<String, String> {
        let values = Self::deserialize(definition, data)?;
        let parts: Vec<String> = definition
            .fields
            .iter()
            .zip(values.iter())
            .map(|(f, v)| format!("{}={}", f.name, v))
            .collect();
        Ok(format!("({})", parts.join(", ")))
    }
}
