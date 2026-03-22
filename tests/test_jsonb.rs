use storage_manager::executor::jsonb::{JsonbSerializer, JsonbValue};

#[test]
fn test_parse_simple_object() {
    let val = JsonbSerializer::parse(r#"{"b": 2, "a": 1}"#).unwrap();
    // Keys should be sorted
    if let JsonbValue::Object(pairs) = &val {
        assert_eq!(pairs[0].0, "a");
        assert_eq!(pairs[1].0, "b");
    } else {
        panic!("Expected Object");
    }
}

#[test]
fn test_roundtrip_null() {
    let val = JsonbSerializer::parse("null").unwrap();
    let binary = JsonbSerializer::to_binary(&val);
    let (decoded, consumed) = JsonbSerializer::from_binary(&binary).unwrap();
    assert_eq!(consumed, binary.len());
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_bool() {
    for input in &["true", "false"] {
        let val = JsonbSerializer::parse(input).unwrap();
        let binary = JsonbSerializer::to_binary(&val);
        let (decoded, _) = JsonbSerializer::from_binary(&binary).unwrap();
        assert_eq!(val, decoded);
    }
}

#[test]
fn test_roundtrip_number() {
    let val = JsonbSerializer::parse("42.5").unwrap();
    let binary = JsonbSerializer::to_binary(&val);
    let (decoded, _) = JsonbSerializer::from_binary(&binary).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_string() {
    let val = JsonbSerializer::parse(r#""hello world""#).unwrap();
    let binary = JsonbSerializer::to_binary(&val);
    let (decoded, _) = JsonbSerializer::from_binary(&binary).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_array() {
    let val = JsonbSerializer::parse(r#"[1, "two", null, true]"#).unwrap();
    let binary = JsonbSerializer::to_binary(&val);
    let (decoded, _) = JsonbSerializer::from_binary(&binary).unwrap();
    assert_eq!(val, decoded);
}

#[test]
fn test_roundtrip_nested_object() {
    let val = JsonbSerializer::parse(r#"{"z": {"b": [1,2], "a": true}, "a": null}"#).unwrap();
    let binary = JsonbSerializer::to_binary(&val);
    let (decoded, consumed) = JsonbSerializer::from_binary(&binary).unwrap();
    assert_eq!(consumed, binary.len());
    assert_eq!(val, decoded);
}

#[test]
fn test_display_string() {
    let val = JsonbSerializer::parse(r#"{"b": 2, "a": [1, "hi"]}"#).unwrap();
    let display = JsonbSerializer::to_display_string(&val);
    // Keys sorted: a before b
    assert_eq!(display, r#"{"a":[1,"hi"],"b":2}"#);
}

#[test]
fn test_invalid_json() {
    assert!(JsonbSerializer::parse("{invalid}").is_err());
}
