/// Validates that a string is well-formed JSON.
/// Used for both JSON and JSONB types during insertion.
pub fn validate_json(json_text: &str) -> Result<(), String> {
    serde_json::from_str::<serde_json::Value>(json_text)
        .map_err(|e| format!("Invalid JSON: {}", e))?;
    Ok(())
}
