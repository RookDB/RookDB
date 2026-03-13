use storage_manager::executor::json_utils::validate_json;

#[test]
fn test_valid_object() {
    assert!(validate_json(r#"{"key": "value"}"#).is_ok());
}

#[test]
fn test_valid_array() {
    assert!(validate_json(r#"[1, 2, 3]"#).is_ok());
}

#[test]
fn test_valid_nested() {
    assert!(validate_json(r#"{"a": [1, {"b": true}], "c": null}"#).is_ok());
}

#[test]
fn test_invalid_unclosed_brace() {
    assert!(validate_json(r#"{"key": "value""#).is_err());
}

#[test]
fn test_invalid_trailing_comma() {
    assert!(validate_json(r#"{"key": "value",}"#).is_err());
}
