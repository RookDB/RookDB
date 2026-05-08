use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use storage_manager::types::{
    DataType, DataValue, NumericValue, Row, deserialize_nullable_row, serialize_nullable_row,
};

#[test]
fn mixed_typed_row_roundtrip_covers_all_datatypes() {
    let schema = vec![
        DataType::SmallInt,
        DataType::Int,
        DataType::BigInt,
        DataType::Real,
        DataType::DoublePrecision,
        DataType::Numeric {
            precision: 10,
            scale: 2,
        },
        DataType::Decimal {
            precision: 8,
            scale: 3,
        },
        DataType::Bool,
        DataType::Char(5),
        DataType::Character(5),
        DataType::Varchar(20),
        DataType::Date,
        DataType::Time,
        DataType::Bit(6),
        DataType::Timestamp,
    ];

    let row_bytes = serialize_nullable_row(
        &schema,
        &[
            Some("-12"),
            Some("42"),
            Some("-9000000000"),
            Some("3.25"),
            Some("2.5"),
            Some("1234.56"),
            Some("12.340"),
            Some("true"),
            Some("ab"),
            Some("xy"),
            Some("rookdb"),
            Some("2026-03-26"),
            Some("13:14:15.100000"),
            Some("B'101011'"),
            Some("2026-03-26 13:14:15.123456"),
        ],
    )
    .unwrap();

    let values = deserialize_nullable_row(&schema, &row_bytes).unwrap();

    assert_eq!(values[0], Some(DataValue::SmallInt(-12)));
    assert_eq!(values[1], Some(DataValue::Int(42)));
    assert_eq!(values[2], Some(DataValue::BigInt(-9000000000)));
    assert_eq!(values[3], Some(DataValue::Real(storage_manager::types::OrderedF32(3.25))));
    assert_eq!(
        values[4],
        Some(DataValue::DoublePrecision(storage_manager::types::OrderedF64(2.5)))
    );
    assert_eq!(
        values[5],
        Some(DataValue::Numeric(NumericValue {
            unscaled: 123456,
            scale: 2,
        }))
    );
    assert_eq!(
        values[6],
        Some(DataValue::Numeric(NumericValue {
            unscaled: 12340,
            scale: 3,
        }))
    );
    assert_eq!(values[7], Some(DataValue::Bool(true)));
    assert_eq!(values[8], Some(DataValue::Char("ab   ".to_string())));
    assert_eq!(values[9], Some(DataValue::Char("xy   ".to_string())));
    assert_eq!(values[10], Some(DataValue::Varchar("rookdb".to_string())));
    assert_eq!(
        values[11],
        Some(DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 26).unwrap()))
    );
    assert_eq!(
        values[12],
        Some(DataValue::Time(
            NaiveTime::from_hms_micro_opt(13, 14, 15, 100000).unwrap()
        ))
    );
    assert_eq!(values[13], Some(DataValue::Bit("101011".to_string())));
    assert_eq!(
        values[14],
        Some(DataValue::Timestamp(
            NaiveDateTime::parse_from_str("2026-03-26 13:14:15.123456", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
        ))
    );
}

#[test]
fn row_set_get_serialize_deserialize() {
    let schema = vec![DataType::Int, DataType::Varchar(20), DataType::Bool];
    let mut row = Row::new(schema.clone());

    row.set_value(0, &DataValue::Int(101)).unwrap();
    row.set_value(1, &DataValue::Varchar("alice".to_string())).unwrap();
    row.set_value(2, &DataValue::Bool(true)).unwrap();
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Int(101)));

    row.set_null(1).unwrap();
    assert_eq!(row.get_value(1).unwrap(), None);

    let bytes = row.serialize();
    let decoded = Row::deserialize(&schema, &bytes).unwrap();
    assert_eq!(decoded.get_value(0).unwrap(), Some(DataValue::Int(101)));
    assert_eq!(decoded.get_value(1).unwrap(), None);
    assert_eq!(decoded.get_value(2).unwrap(), Some(DataValue::Bool(true)));
}

#[test]
fn test_varlen_resizing_shifts_offsets() {
    let schema = vec![
        DataType::Varchar(20),
        DataType::Int,
        DataType::Varchar(20),
        DataType::Varchar(20),
    ];
    let mut row = Row::new(schema.clone());

    // Populate: A, 42, B, C
    row.set_value(0, &DataValue::Varchar("A".to_string())).unwrap();
    row.set_value(1, &DataValue::Int(42)).unwrap();
    row.set_value(2, &DataValue::Varchar("B".to_string())).unwrap();
    row.set_value(3, &DataValue::Varchar("C".to_string())).unwrap();

    // Verify initial
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Varchar("A".to_string())));
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("B".to_string())));
    assert_eq!(row.get_value(3).unwrap(), Some(DataValue::Varchar("C".to_string())));

    // Expand B
    row.set_value(2, &DataValue::Varchar("B_longer".to_string())).unwrap();
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Varchar("A".to_string())));
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("B_longer".to_string())));
    assert_eq!(row.get_value(3).unwrap(), Some(DataValue::Varchar("C".to_string())));

    // Shrink B
    row.set_value(2, &DataValue::Varchar("b".to_string())).unwrap();
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Varchar("A".to_string())));
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("b".to_string())));
    assert_eq!(row.get_value(3).unwrap(), Some(DataValue::Varchar("C".to_string())));

    // Nullify A
    row.set_null(0).unwrap();
    assert_eq!(row.get_value(0).unwrap(), None);
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("b".to_string())));
    assert_eq!(row.get_value(3).unwrap(), Some(DataValue::Varchar("C".to_string())));

    // Set A back to completely new layout size
    row.set_value(0, &DataValue::Varchar("reborn".to_string())).unwrap();
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Varchar("reborn".to_string())));
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("b".to_string())));
    assert_eq!(row.get_value(3).unwrap(), Some(DataValue::Varchar("C".to_string())));
    
    // Nullify C
    row.set_null(3).unwrap();
    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Varchar("reborn".to_string())));
    assert_eq!(row.get_value(2).unwrap(), Some(DataValue::Varchar("b".to_string())));
    assert_eq!(row.get_value(3).unwrap(), None);
}

