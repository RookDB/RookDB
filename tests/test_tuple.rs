//! Tests for Tuple, ColumnValue, and deserialize_tuple.

use storage_manager::catalog::types::Column;
use storage_manager::join::tuple::{ColumnValue, Tuple, deserialize_tuple};

#[test]
fn test_column_value_int_equality() {
    let a = ColumnValue::Int(42);
    let b = ColumnValue::Int(42);
    assert!(a.eq_value(&b));
}

#[test]
fn test_column_value_int_inequality() {
    let a = ColumnValue::Int(42);
    let b = ColumnValue::Int(99);
    assert!(!a.eq_value(&b));
}

#[test]
fn test_column_value_text_equality() {
    let a = ColumnValue::Text("hello     ".to_string());
    let b = ColumnValue::Text("hello     ".to_string());
    assert!(a.eq_value(&b));
}

#[test]
fn test_column_value_null_never_equals() {
    let a = ColumnValue::Null;
    let b = ColumnValue::Int(42);
    assert!(!a.eq_value(&b));
}

#[test]
fn test_column_value_ordering() {
    let a = ColumnValue::Int(10);
    let b = ColumnValue::Int(20);
    assert_eq!(a.partial_cmp_values(&b), Some(std::cmp::Ordering::Less));
    assert_eq!(b.partial_cmp_values(&a), Some(std::cmp::Ordering::Greater));
}

#[test]
fn test_deserialize_tuple_int() {
    let schema = vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
    ];
    let bytes: Vec<u8> = 42i32.to_le_bytes().to_vec();
    let tuple = deserialize_tuple(&bytes, &schema);

    assert_eq!(tuple.values.len(), 1);
    match &tuple.values[0] {
        ColumnValue::Int(v) => assert_eq!(*v, 42),
        _ => panic!("Expected Int"),
    }
}

#[test]
fn test_deserialize_tuple_text() {
    let schema = vec![
        Column { name: "name".to_string(), data_type: "TEXT".to_string() },
    ];
    let bytes = b"hello     ".to_vec(); // 10 bytes
    let tuple = deserialize_tuple(&bytes, &schema);

    assert_eq!(tuple.values.len(), 1);
    match &tuple.values[0] {
        ColumnValue::Text(s) => assert_eq!(s, "hello     "),
        _ => panic!("Expected Text"),
    }
}

#[test]
fn test_deserialize_tuple_mixed() {
    let schema = vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "name".to_string(), data_type: "TEXT".to_string() },
    ];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&7i32.to_le_bytes());
    bytes.extend_from_slice(b"Alice     "); // 10 bytes
    let tuple = deserialize_tuple(&bytes, &schema);

    assert_eq!(tuple.values.len(), 2);
    match &tuple.values[0] {
        ColumnValue::Int(v) => assert_eq!(*v, 7),
        _ => panic!("Expected Int"),
    }
    match &tuple.values[1] {
        ColumnValue::Text(s) => assert!(s.starts_with("Alice")),
        _ => panic!("Expected Text"),
    }
}

#[test]
fn test_tuple_get_field() {
    let schema = vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "name".to_string(), data_type: "TEXT".to_string() },
    ];
    let tuple = Tuple {
        values: vec![ColumnValue::Int(1), ColumnValue::Text("test      ".to_string())],
        schema,
    };
    assert!(tuple.get_field("id").is_some());
    assert!(tuple.get_field("name").is_some());
    assert!(tuple.get_field("missing").is_none());
}

#[test]
fn test_tuple_merge() {
    let left = Tuple {
        values: vec![ColumnValue::Int(1)],
        schema: vec![Column { name: "a".to_string(), data_type: "INT".to_string() }],
    };
    let right = Tuple {
        values: vec![ColumnValue::Int(2)],
        schema: vec![Column { name: "b".to_string(), data_type: "INT".to_string() }],
    };
    let merged = Tuple::merge(&left, &right);
    assert_eq!(merged.values.len(), 2);
    assert_eq!(merged.schema.len(), 2);
}

#[test]
fn test_null_tuple() {
    let schema = vec![
        Column { name: "x".to_string(), data_type: "INT".to_string() },
        Column { name: "y".to_string(), data_type: "TEXT".to_string() },
    ];
    let null_t = Tuple::null_tuple(&schema);
    assert_eq!(null_t.values.len(), 2);
    assert!(matches!(&null_t.values[0], ColumnValue::Null));
    assert!(matches!(&null_t.values[1], ColumnValue::Null));
}
