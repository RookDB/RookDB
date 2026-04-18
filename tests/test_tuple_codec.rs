//! Tests for tuple encode/decode roundtrip.

use storage_manager::catalog::types::Column;
use storage_manager::executor::tuple_codec::{decode_tuple, encode_tuple};
use storage_manager::executor::value::Value;

fn col(name: &str, dt: &str) -> Column {
    Column { name: name.to_string(), data_type: dt.to_string() }
}

#[test]
fn test_int_text_roundtrip() {
    let schema = vec![col("id", "INT"), col("name", "TEXT")];
    let values = vec![Value::Int(42), Value::Text("Alice".to_string())];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    assert_eq!(decoded[0], Value::Int(42));
    if let Value::Text(s) = &decoded[1] {
        assert_eq!(s, "Alice");
    } else {
        panic!("Expected text");
    }
}

#[test]
fn test_null_column_roundtrip() {
    let schema = vec![col("a", "INT"), col("b", "INT"), col("c", "TEXT")];
    let values = vec![Value::Int(1), Value::Null, Value::Text("hi".to_string())];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    assert_eq!(decoded[0], Value::Int(1));
    assert_eq!(decoded[1], Value::Null);
    if let Value::Text(s) = &decoded[2] { assert_eq!(s, "hi"); } else { panic!(); }
}

#[test]
fn test_all_nulls_roundtrip() {
    let schema = vec![col("x", "INT"), col("y", "FLOAT"), col("z", "TEXT")];
    let values = vec![Value::Null, Value::Null, Value::Null];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    for v in &decoded {
        assert_eq!(*v, Value::Null);
    }
}

#[test]
fn test_float_roundtrip() {
    let schema = vec![col("f", "FLOAT")];
    let values = vec![Value::Float(3.14159)];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    if let Value::Float(f) = decoded[0] {
        assert!((f - 3.14159).abs() < 1e-10);
    } else {
        panic!("Expected Float, got {:?}", decoded[0]);
    }
}

#[test]
fn test_bool_roundtrip() {
    let schema = vec![col("flag", "BOOL")];
    let t_bytes = encode_tuple(&[Value::Bool(true)], &schema);
    let f_bytes = encode_tuple(&[Value::Bool(false)], &schema);
    assert_eq!(decode_tuple(&t_bytes, &schema)[0], Value::Bool(true));
    assert_eq!(decode_tuple(&f_bytes, &schema)[0], Value::Bool(false));
}

#[test]
fn test_multiple_var_columns() {
    let schema = vec![
        col("id", "INT"),
        col("first", "TEXT"),
        col("last", "TEXT"),
    ];
    let values = vec![
        Value::Int(7),
        Value::Text("John".to_string()),
        Value::Text("Doe".to_string()),
    ];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    assert_eq!(decoded[0], Value::Int(7));
    assert_eq!(decoded[1], Value::Text("John".to_string()));
    assert_eq!(decoded[2], Value::Text("Doe".to_string()));
}

#[test]
fn test_empty_string_roundtrip() {
    let schema = vec![col("s", "TEXT")];
    let values = vec![Value::Text("".to_string())];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    assert_eq!(decoded[0], Value::Text("".to_string()));
}

#[test]
fn test_long_string_roundtrip() {
    let schema = vec![col("bio", "TEXT")];
    let long = "a".repeat(1000);
    let values = vec![Value::Text(long.clone())];
    let bytes = encode_tuple(&values, &schema);
    let decoded = decode_tuple(&bytes, &schema);
    assert_eq!(decoded[0], Value::Text(long));
}
