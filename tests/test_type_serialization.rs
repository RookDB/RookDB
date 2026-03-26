use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use storage_manager::types::{DataType, DataValue, NumericValue, OrderedF32, OrderedF64};

#[test]
fn roundtrip_serialization_for_all_datatypes() {
    let numeric_ty = DataType::Numeric {
        precision: 10,
        scale: 2,
    };
    let decimal_ty = DataType::Decimal {
        precision: 8,
        scale: 3,
    };

    let cases: Vec<(DataType, &str, DataValue)> = vec![
        (DataType::SmallInt, "-32768", DataValue::SmallInt(-32768)),
        (DataType::Int, "2147483647", DataValue::Int(2147483647)),
        (
            DataType::BigInt,
            "-900000000000",
            DataValue::BigInt(-900000000000),
        ),
        (
            DataType::Real,
            "3.5",
            DataValue::Real(OrderedF32(3.5_f32)),
        ),
        (
            DataType::DoublePrecision,
            "2.718281828",
            DataValue::DoublePrecision(OrderedF64(2.718281828_f64)),
        ),
        (
            numeric_ty.clone(),
            "12345.67",
            DataValue::Numeric(NumericValue {
                unscaled: 1234567,
                scale: 2,
            }),
        ),
        (
            decimal_ty.clone(),
            "-12.340",
            DataValue::Numeric(NumericValue {
                unscaled: -12340,
                scale: 3,
            }),
        ),
        (DataType::Bool, "true", DataValue::Bool(true)),
        (
            DataType::Char(6),
            "abc",
            DataValue::Char("abc   ".to_string()),
        ),
        (
            DataType::Character(6),
            "xyz",
            DataValue::Char("xyz   ".to_string()),
        ),
        (
            DataType::Varchar(20),
            "hello world",
            DataValue::Varchar("hello world".to_string()),
        ),
        (
            DataType::Date,
            "2026-03-26",
            DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 26).unwrap()),
        ),
        (
            DataType::Time,
            "13:14:15.123456",
            DataValue::Time(NaiveTime::from_hms_micro_opt(13, 14, 15, 123456).unwrap()),
        ),
        (
            DataType::Bit(10),
            "B'1011001110'",
            DataValue::Bit("1011001110".to_string()),
        ),
        (
            DataType::Timestamp,
            "2026-03-26 13:14:15.654321",
            DataValue::Timestamp(
                NaiveDateTime::parse_from_str("2026-03-26 13:14:15.654321", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
        ),
    ];

    for (ty, input, expected) in cases {
        let encoded = DataValue::parse_and_encode(&ty, input).unwrap();
        let decoded = DataValue::from_bytes(&ty, &encoded).unwrap();
        assert_eq!(decoded, expected);
    }
}

#[test]
fn varchar_decoding_rejects_truncated_payload() {
    let err = DataValue::from_bytes(&DataType::Varchar(10), &[5, 0, b'a', b'b']).unwrap_err();
    assert!(err.contains("truncated"));
}
