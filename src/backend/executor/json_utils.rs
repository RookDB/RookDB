/// Validates that a string is well-formed JSON.
/// Used for both JSON and JSONB types during insertion.
pub fn validate_json(json_text: &str) -> Result<(), String> {
    serde_json::from_str::<serde_json::Value>(json_text)
        .map_err(|e| format!("Invalid JSON: {}", e))?;
    Ok(())
}

/// Parses JSON text into a serde_json::Value for path queries at evaluation time.
pub fn parse_to_serde(json_text: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str::<serde_json::Value>(json_text)
        .map_err(|e| format!("Invalid JSON: {}", e))
}
