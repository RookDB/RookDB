use chrono::NaiveDate;
use std::cmp::Ordering;

use super::*;

#[test]
fn parse_phase_one_types() {
    assert_eq!("SMALLINT".parse::<DataType>().unwrap(), DataType::SmallInt);
    assert_eq!("INT".parse::<DataType>().unwrap(), DataType::Int);
    assert_eq!("BIGINT".parse::<DataType>().unwrap(), DataType::BigInt);
    assert_eq!(
        "VARCHAR(64)".parse::<DataType>().unwrap(),
        DataType::Varchar(64)
    );
    assert_eq!("BOOLEAN".parse::<DataType>().unwrap(), DataType::Bool);
    assert_eq!("DATE".parse::<DataType>().unwrap(), DataType::Date);
    assert_eq!("BIT(12)".parse::<DataType>().unwrap(), DataType::Bit(12));
}

#[test]
fn parse_unknown_type_is_error() {
    assert!("BLOB".parse::<DataType>().is_err());
}

#[test]
fn serde_roundtrip() {
    let types = vec![
        DataType::SmallInt,
        DataType::Int,
        DataType::BigInt,
        DataType::Bool,
        DataType::Varchar(32),
        DataType::Date,
        DataType::Bit(9),
    ];
    for dt in &types {
        let json = serde_json::to_string(dt).unwrap();
        let back: DataType = serde_json::from_str(&json).unwrap();
        assert_eq!(dt, &back);
    }
}

#[test]
fn display_matches_parse() {
    let types = vec![
        DataType::SmallInt,
        DataType::Int,
        DataType::BigInt,
        DataType::Bool,
        DataType::Varchar(8),
        DataType::Date,
        DataType::Bit(5),
    ];
    for dt in &types {
        let s = dt.to_string();
        let back: DataType = s.parse().unwrap();
        assert_eq!(dt, &back);
    }
}

#[test]
fn phase_two_layout_rules() {
    assert_eq!(DataType::SmallInt.alignment(), 2);
    assert_eq!(DataType::Int.alignment(), 4);
    assert_eq!(DataType::BigInt.alignment(), 8);
    assert_eq!(DataType::Date.alignment(), 4);
    assert_eq!(DataType::Bool.alignment(), 1);
    assert_eq!(DataType::Varchar(64).alignment(), 1);
    assert_eq!(DataType::Bit(13).alignment(), 1);

    assert_eq!(DataType::SmallInt.fixed_size(), Some(2));
    assert_eq!(DataType::Int.fixed_size(), Some(4));
    assert_eq!(DataType::BigInt.fixed_size(), Some(8));
    assert_eq!(DataType::Date.fixed_size(), Some(4));
    assert_eq!(DataType::Bool.fixed_size(), Some(1));
    assert_eq!(DataType::Bit(13).fixed_size(), Some(2));
    assert_eq!(DataType::Varchar(64).fixed_size(), None);

    assert_eq!(DataType::Varchar(64).min_storage_size(), 2);
    assert!(DataType::Varchar(64).is_variable_length());
    assert!(!DataType::Date.is_variable_length());
    assert!(!DataType::Bit(13).is_variable_length());
}

#[test]
fn roundtrip_smallint() {
    let encoded = DataValue::parse_and_encode(&DataType::SmallInt, "-12").unwrap();
    assert_eq!(
        DataValue::from_bytes(&DataType::SmallInt, &encoded).unwrap(),
        DataValue::SmallInt(-12)
    );
}

#[test]
fn roundtrip_int() {
    let encoded = DataValue::parse_and_encode(&DataType::Int, "42").unwrap();
    assert_eq!(
        DataValue::from_bytes(&DataType::Int, &encoded).unwrap(),
        DataValue::Int(42)
    );
}

#[test]
fn roundtrip_bool() {
    let t = DataValue::parse_and_encode(&DataType::Bool, "true").unwrap();
    let f = DataValue::parse_and_encode(&DataType::Bool, "0").unwrap();
    assert_eq!(DataValue::from_bytes(&DataType::Bool, &t).unwrap(), DataValue::Bool(true));
    assert_eq!(DataValue::from_bytes(&DataType::Bool, &f).unwrap(), DataValue::Bool(false));
}

#[test]
fn roundtrip_varchar() {
    let encoded = DataValue::parse_and_encode(&DataType::Varchar(32), "Alice").unwrap();
    assert_eq!(u16::from_le_bytes([encoded[0], encoded[1]]), 5);
    assert_eq!(
        DataValue::from_bytes(&DataType::Varchar(32), &encoded).unwrap(),
        DataValue::Varchar("Alice".to_string())
    );
}

#[test]
fn roundtrip_date() {
    let encoded = DataValue::parse_and_encode(&DataType::Date, "2026-03-13").unwrap();
    assert_eq!(
        DataValue::from_bytes(&DataType::Date, &encoded).unwrap(),
        DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap())
    );
}

#[test]
fn roundtrip_bit() {
    let encoded = DataValue::parse_and_encode(&DataType::Bit(10), "B'1011001110'").unwrap();
    assert_eq!(encoded.len(), 2);
    assert_eq!(
        DataValue::from_bytes(&DataType::Bit(10), &encoded).unwrap(),
        DataValue::Bit("1011001110".to_string())
    );
}

#[test]
fn varchar_length_violation_is_error() {
    let err = DataValue::parse_and_encode(&DataType::Varchar(3), "Alice").unwrap_err();
    assert!(err.contains("VARCHAR(3)"));
}

#[test]
fn validate_smallint_bounds() {
    assert!(validate_smallint("-32768").is_ok());
    assert!(validate_smallint("32767").is_ok());
    assert!(validate_smallint("32768").is_err());
}

#[test]
fn validate_int_bounds() {
    assert!(validate_int("2147483647").is_ok());
    assert!(validate_int("2147483648").is_err());
}

#[test]
fn roundtrip_bigint() {
    let encoded = DataValue::parse_and_encode(&DataType::BigInt, "-9000000000").unwrap();
    assert_eq!(encoded.len(), 8);
    assert_eq!(
        DataValue::from_bytes(&DataType::BigInt, &encoded).unwrap(),
        DataValue::BigInt(-9_000_000_000)
    );
}

#[test]
fn validate_bigint_bounds() {
    assert!(validate_bigint("9223372036854775807").is_ok());
    assert!(validate_bigint("9223372036854775808").is_err());
    assert!(validate_bigint("-9223372036854775808").is_ok());
}

#[test]
fn compare_bigint_promotion() {
    let a = DataValue::SmallInt(100);
    let b = DataValue::BigInt(200);
    assert_eq!(a.compare(&b).unwrap(), std::cmp::Ordering::Less);
    let c = DataValue::Int(300);
    assert_eq!(b.compare(&c).unwrap(), std::cmp::Ordering::Less);
    assert!(a.is_equal(&DataValue::BigInt(100)).unwrap());
}

#[test]
fn validate_bool_values() {
    assert!(validate_bool("true").is_ok());
    assert!(validate_bool("FALSE").is_ok());
    assert!(validate_bool("1").is_ok());
    assert!(validate_bool("0").is_ok());
    assert!(validate_bool("maybe").is_err());
}

#[test]
fn validate_varchar_length() {
    assert!(validate_varchar("abc", 3).is_ok());
    assert!(validate_varchar("abcd", 3).is_err());
}

#[test]
fn validate_date_format() {
    assert!(validate_date("2026-03-13").is_ok());
    assert!(validate_date("2026-13-40").is_err());
    assert!(validate_date("13-03-2026").is_err());
}

#[test]
fn validate_bit_rules() {
    assert!(validate_bit("B'0101'", 4).is_ok());
    assert!(validate_bit("0101", 4).is_ok());
    assert!(validate_bit("011", 4).is_err());
    assert!(validate_bit("B'01A1'", 4).is_err());
}

#[test]
fn null_bitmap_set_clear_and_probe() {
    let mut bitmap = NullBitmap::new(10);
    bitmap.set_null(0);
    bitmap.set_null(3);
    bitmap.set_null(9);

    assert!(bitmap.is_null(0));
    assert!(!bitmap.is_null(1));
    assert!(bitmap.is_null(3));
    assert!(bitmap.is_null(9));

    bitmap.clear_null(3);
    assert!(!bitmap.is_null(3));
}

#[test]
fn nullable_row_roundtrip() {
    let schema = vec![
        DataType::Int,
        DataType::Varchar(16),
        DataType::Date,
        DataType::SmallInt,
    ];

    let encoded =
        serialize_nullable_row(&schema, &[Some("42"), None, Some("2026-03-13"), Some("-7")])
            .unwrap();

    assert_eq!(encoded[0] & (1 << 1), 1 << 1);

    let decoded = deserialize_nullable_row(&schema, &encoded).unwrap();
    assert_eq!(decoded.len(), 4);
    assert_eq!(decoded[0], Some(DataValue::Int(42)));
    assert_eq!(decoded[1], None);
    assert_eq!(
        decoded[2],
        Some(DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()))
    );
    assert_eq!(decoded[3], Some(DataValue::SmallInt(-7)));
}

#[test]
fn compare_numeric_with_promotion() {
    let a = DataValue::SmallInt(7);
    let b = DataValue::Int(10);
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
    assert_eq!(b.compare(&a).unwrap(), Ordering::Greater);
    assert!(a.is_equal(&DataValue::Int(7)).unwrap());
}

#[test]
fn compare_varchar_lexicographic() {
    let a = DataValue::Varchar("Alice".to_string());
    let b = DataValue::Varchar("Bob".to_string());
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
}

#[test]
fn compare_boolean_ordering() {
    let f = DataValue::Bool(false);
    let t = DataValue::Bool(true);
    assert_eq!(f.compare(&t).unwrap(), Ordering::Less);
}

#[test]
fn compare_date_chronological() {
    let a = DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 12).unwrap());
    let b = DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap());
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
}

#[test]
fn compare_type_mismatch_errors() {
    let a = DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap());
    let b = DataValue::Varchar("2026-03-13".to_string());
    assert!(a.compare(&b).is_err());
}

#[test]
fn compare_bit_lexicographic() {
    let a = DataValue::Bit("0011".to_string());
    let b = DataValue::Bit("0100".to_string());
    assert_eq!(a.compare(&b).unwrap(), Ordering::Less);
}

#[test]
fn compare_nullable_unknown_when_null_present() {
    let a = DataValue::Int(1);
    assert_eq!(compare_nullable(Some(&a), None).unwrap(), None);
    assert_eq!(nullable_equals(Some(&a), None).unwrap(), None);
}

#[test]
fn compare_nullable_non_null_values() {
    let a = DataValue::Int(5);
    let b = DataValue::SmallInt(5);
    assert_eq!(
        compare_nullable(Some(&a), Some(&b)).unwrap(),
        Some(Ordering::Equal)
    );
    assert_eq!(nullable_equals(Some(&a), Some(&b)).unwrap(), Some(true));
}

#[test]
fn row_set_get_and_null() {
    let schema = vec![DataType::Int, DataType::Bool, DataType::Varchar(16), DataType::Date];
    let mut row = Row::new(schema);

    row.set_value(0, &DataValue::Int(99)).unwrap();
    row.set_value(1, &DataValue::Bool(true)).unwrap();
    row.set_value(2, &DataValue::Varchar("alice".to_string()))
        .unwrap();
    row.set_value(
        3,
        &DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()),
    )
    .unwrap();

    assert_eq!(row.get_value(0).unwrap(), Some(DataValue::Int(99)));
    assert_eq!(row.get_value(1).unwrap(), Some(DataValue::Bool(true)));
    assert_eq!(
        row.get_value(2).unwrap(),
        Some(DataValue::Varchar("alice".to_string()))
    );

    row.set_null(2).unwrap();
    assert_eq!(row.get_value(2).unwrap(), None);
}

#[test]
fn row_serialize_deserialize_roundtrip() {
    let schema = vec![
        DataType::SmallInt,
        DataType::Varchar(8),
        DataType::Date,
        DataType::Bit(4),
    ];
    let mut row = Row::new(schema.clone());
    row.set_value(0, &DataValue::SmallInt(-5)).unwrap();
    row.set_value(1, &DataValue::Varchar("rook".to_string()))
        .unwrap();
    row.set_null(2).unwrap();
    row.set_value(3, &DataValue::Bit("1010".to_string())).unwrap();

    let bytes = row.serialize();
    let restored = Row::deserialize(&schema, &bytes).unwrap();

    assert_eq!(restored.get_value(0).unwrap(), Some(DataValue::SmallInt(-5)));
    assert_eq!(
        restored.get_value(1).unwrap(),
        Some(DataValue::Varchar("rook".to_string()))
    );
    assert_eq!(restored.get_value(2).unwrap(), None);
    assert_eq!(
        restored.get_value(3).unwrap(),
        Some(DataValue::Bit("1010".to_string()))
    );
}

#[test]
fn row_set_value_type_mismatch_is_error() {
    let schema = vec![DataType::Int];
    let mut row = Row::new(schema);
    assert!(
        row.set_value(0, &DataValue::Varchar("oops".to_string()))
            .is_err()
    );
}

#[test]
fn fn_length_and_trim() {
    let v = DataValue::Varchar("  rookdb  ".to_string());
    assert_eq!(length(&v).unwrap(), 10);
    assert_eq!(trim(&v).unwrap(), DataValue::Varchar("rookdb".to_string()));
}

#[test]
fn fn_substring() {
    let v = DataValue::Varchar("database".to_string());
    assert_eq!(
        substring(&v, 1, 4).unwrap(),
        DataValue::Varchar("data".to_string())
    );
    assert_eq!(
        substring(&v, 5, 99).unwrap(),
        DataValue::Varchar("base".to_string())
    );
    assert_eq!(
        substring(&v, 50, 2).unwrap(),
        DataValue::Varchar("".to_string())
    );
    assert!(substring(&v, 0, 2).is_err());
}

#[test]
fn fn_upper_and_lower() {
    let v = DataValue::Varchar("RookDb".to_string());
    assert_eq!(upper(&v).unwrap(), DataValue::Varchar("ROOKDB".to_string()));
    assert_eq!(lower(&v).unwrap(), DataValue::Varchar("rookdb".to_string()));
}

#[test]
fn fn_extract_date_parts() {
    let d = DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap());
    assert_eq!(extract(DatePart::Year, &d).unwrap(), 2026);
    assert_eq!(extract(DatePart::Month, &d).unwrap(), 3);
    assert_eq!(extract(DatePart::Day, &d).unwrap(), 13);
}

#[test]
fn fn_type_mismatch_errors() {
    let i = DataValue::Int(7);
    assert!(length(&i).is_err());
    assert!(extract(DatePart::Year, &i).is_err());
}
