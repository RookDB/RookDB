use std::cmp::Ordering;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use storage_manager::types::{
    Comparable, DataValue, NumericValue, OrderedF32, OrderedF64, compare_nullable,
};

#[test]
fn numeric_and_integer_comparisons_work() {
    assert_eq!(DataValue::SmallInt(5).compare(&DataValue::Int(6)).unwrap(), Ordering::Less);
    assert_eq!(DataValue::Int(6).compare(&DataValue::BigInt(6)).unwrap(), Ordering::Equal);
    assert_eq!(
        DataValue::Numeric(NumericValue {
            unscaled: 1234,
            scale: 2,
        })
        .compare(&DataValue::Numeric(NumericValue {
            unscaled: 12340,
            scale: 3,
        }))
        .unwrap(),
        Ordering::Equal
    );
}

#[test]
fn floating_string_and_temporal_comparisons_work() {
    assert_eq!(
        DataValue::Real(OrderedF32(1.0)).compare(&DataValue::Real(OrderedF32(2.0))).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        DataValue::DoublePrecision(OrderedF64(3.0))
            .compare(&DataValue::DoublePrecision(OrderedF64(1.0)))
            .unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        DataValue::Char("abc".to_string())
            .compare(&DataValue::Char("abd".to_string()))
            .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        DataValue::Varchar("abc".to_string())
            .compare(&DataValue::Varchar("abc".to_string()))
            .unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 25).unwrap())
            .compare(&DataValue::Date(NaiveDate::from_ymd_opt(2026, 3, 26).unwrap()))
            .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        DataValue::Time(NaiveTime::from_hms_opt(10, 0, 0).unwrap())
            .compare(&DataValue::Time(NaiveTime::from_hms_opt(9, 59, 59).unwrap()))
            .unwrap(),
        Ordering::Greater
    );

    assert_eq!(
        DataValue::Timestamp(
            NaiveDateTime::parse_from_str("2026-03-26 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
        )
        .compare(&DataValue::Timestamp(
            NaiveDateTime::parse_from_str("2026-03-26 10:00:01", "%Y-%m-%d %H:%M:%S").unwrap()
        ))
        .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        DataValue::Bit("0011".to_string())
            .compare(&DataValue::Bit("0100".to_string()))
            .unwrap(),
        Ordering::Less
    );
}

#[test]
fn nullable_comparison_returns_none_if_any_side_is_null() {
    let value = DataValue::Int(10);
    assert_eq!(compare_nullable(Some(&value), None).unwrap(), None);
}

#[test]
fn team_robustness_edge_cases_comparison() {

    assert_eq!(
        DataValue::Char("abc".to_string())
            .compare(&DataValue::Char("abc  ".to_string()))
            .unwrap(),
        Ordering::Equal
    );

    assert_eq!(
        DataValue::DoublePrecision(OrderedF64(std::f64::INFINITY))
            .compare(&DataValue::DoublePrecision(OrderedF64(999999.99)))
            .unwrap(),
        Ordering::Greater
    );
    
    assert_eq!(
        DataValue::Real(OrderedF32(std::f32::NEG_INFINITY))
            .compare(&DataValue::Real(OrderedF32(-999999.99)))
            .unwrap(),
        Ordering::Less
    );

    assert_eq!(
        DataValue::Varchar("Rook".to_string())
            .compare(&DataValue::Varchar("rook".to_string()))
            .unwrap(),
        Ordering::Less // 'R' comes before 'r' in ASCII/UTF-8
    );
}
