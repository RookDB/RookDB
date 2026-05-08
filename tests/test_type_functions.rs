use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use storage_manager::types::{
    DataType, DataValue, DatePart, NumericValue, OrderedF32, OrderedF64, abs, cast, ceiling,
    coalesce, extract, floor, length, lower, nullif, round, substring, trim, upper,
};

#[test]
fn string_functions_work() {
    let v = DataValue::Varchar("  RookDB  ".to_string());
    assert_eq!(length(&v).unwrap(), 10);
    assert_eq!(trim(&v).unwrap(), DataValue::Varchar("RookDB".to_string()));
    assert_eq!(upper(&v).unwrap(), DataValue::Varchar("  ROOKDB  ".to_string()));
    assert_eq!(lower(&v).unwrap(), DataValue::Varchar("  rookdb  ".to_string()));
    assert_eq!(
        substring(&DataValue::Varchar("abcdef".to_string()), 2, 3).unwrap(),
        DataValue::Varchar("bcd".to_string())
    );
}

#[test]
fn numeric_functions_work_for_real_double_and_numeric() {
    assert_eq!(
        abs(&DataValue::Real(OrderedF32(-3.5))).unwrap(),
        DataValue::Real(OrderedF32(3.5))
    );
    assert_eq!(
        round(&DataValue::DoublePrecision(OrderedF64(3.14159)), 3).unwrap(),
        DataValue::DoublePrecision(OrderedF64(3.142))
    );

    let numeric = DataValue::Numeric(NumericValue {
        unscaled: -12345,
        scale: 2,
    });
    assert_eq!(
        floor(&numeric).unwrap(),
        DataValue::Numeric(NumericValue {
            unscaled: -124,
            scale: 0,
        })
    );
    assert_eq!(
        ceiling(&numeric).unwrap(),
        DataValue::Numeric(NumericValue {
            unscaled: -123,
            scale: 0,
        })
    );
}

#[test]
fn temporal_functions_work() {
    let date = DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 26).unwrap());
    let time = DataValue::Time(NaiveTime::from_hms_opt(13, 14, 15).unwrap());
    let ts = DataValue::Timestamp(
        NaiveDateTime::parse_from_str("2026-03-26 13:14:15", "%Y-%m-%d %H:%M:%S").unwrap(),
    );

    assert_eq!(extract(DatePart::Year, &date).unwrap(), 2026);
    assert_eq!(extract(DatePart::Minute, &time).unwrap(), 14);
    assert_eq!(extract(DatePart::Second, &ts).unwrap(), 15);
}

#[test]
fn cast_and_null_semantics_work() {
    let casted = cast(&DataValue::Varchar("42".to_string()), &DataType::Int).unwrap();
    assert_eq!(casted, DataValue::Int(42));

    let first = coalesce(&[None, Some(DataValue::Int(10)), Some(DataValue::Int(20))]);
    assert_eq!(first, Some(DataValue::Int(10)));

    assert_eq!(nullif(DataValue::Int(7), DataValue::Int(7)).unwrap(), None);
    assert_eq!(
        nullif(DataValue::Int(7), DataValue::Int(8)).unwrap(),
        Some(DataValue::Int(7))
    );
}
