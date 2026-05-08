use storage_manager::types::{
    DataType, DataValue, NullBitmap, deserialize_nullable_row, serialize_nullable_row,
};

#[test]
fn null_bitmap_set_clear_and_query() {
    let mut bitmap = NullBitmap::new(10);
    bitmap.set_null(0);
    bitmap.set_null(7);
    bitmap.set_null(8);

    assert!(bitmap.is_null(0));
    assert!(bitmap.is_null(7));
    assert!(bitmap.is_null(8));
    assert!(!bitmap.is_null(1));

    bitmap.clear_null(7);
    assert!(!bitmap.is_null(7));
}

#[test]
fn nullable_row_roundtrip_preserves_null_positions() {
    let schema = vec![
        DataType::Int,
        DataType::Varchar(10),
        DataType::Bool,
        DataType::Date,
    ];
    let bytes = serialize_nullable_row(
        &schema,
        &[Some("42"), None, Some("true"), Some("2026-03-26")],
    )
    .unwrap();

    let decoded = deserialize_nullable_row(&schema, &bytes).unwrap();
    assert_eq!(decoded[0], Some(DataValue::Int(42)));
    assert_eq!(decoded[1], None);
    assert_eq!(decoded[2], Some(DataValue::Bool(true)));
    assert!(decoded[3].is_some());
}

#[test]
fn deserialize_rejects_short_bitmap() {
    let schema = vec![DataType::Int, DataType::Int, DataType::Int, DataType::Int, DataType::Int, DataType::Int, DataType::Int, DataType::Int, DataType::Int];
    let err = deserialize_nullable_row(&schema, &[0u8]).unwrap_err();
    assert!(err.contains("Row too short"));
}
