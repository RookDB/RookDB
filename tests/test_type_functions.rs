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
    assert_eq!(
        upper(&v).unwrap(),
        DataValue::Varchar("  ROOKDB  ".to_string())
    );
    assert_eq!(
        lower(&v).unwrap(),
        DataValue::Varchar("  rookdb  ".to_string())
    );
    assert_eq!(
        substring(&DataValue::Varchar("abcdef".to_string()), 2, 3).unwrap(),
        DataValue::Varchar("bcd".to_string())
    );
}

#[test]
#[allow(clippy::approx_constant)]
fn numeric_functions_work_for_real_double_and_numeric() {
    assert_eq!(
        abs(&DataValue::Real(OrderedF32(-3.5))).unwrap(),
        DataValue::Real(OrderedF32(3.5))
    );
    assert_eq!(
        round(&DataValue::DoublePrecision(OrderedF64(3.14159)), 3).unwrap(),
        DataValue::DoublePrecision(OrderedF64(3.142))
    );

    let numeric = DataValue::numeric(NumericValue {
        unscaled: -12345,
        scale: 2,
    });
    assert_eq!(
        floor(&numeric).unwrap(),
        DataValue::numeric(NumericValue {
            unscaled: -124,
            scale: 0,
        })
    );
    assert_eq!(
        ceiling(&numeric).unwrap(),
        DataValue::numeric(NumericValue {
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

#[test]
fn assignment_rounds_fractions_into_integer_columns() {
    use storage_manager::types::functions::{assign_cast, cast};
    use storage_manager::types::value::{OrderedF32, OrderedF64};

    let double = |v: f64| DataValue::DoublePrecision(OrderedF64(v));
    // Halves round away from zero, as in PostgreSQL.
    assert_eq!(
        assign_cast(&double(1.7), &DataType::Int).unwrap(),
        DataValue::Int(2)
    );
    assert_eq!(
        assign_cast(&double(2.5), &DataType::Int).unwrap(),
        DataValue::Int(3)
    );
    assert_eq!(
        assign_cast(&double(-2.5), &DataType::BigInt).unwrap(),
        DataValue::BigInt(-3)
    );
    assert_eq!(
        assign_cast(&double(1.4), &DataType::SmallInt).unwrap(),
        DataValue::SmallInt(1)
    );
    assert_eq!(
        assign_cast(&DataValue::Real(OrderedF32(9.5)), &DataType::Int).unwrap(),
        DataValue::Int(10)
    );
    let numeric = cast(
        &DataValue::Varchar("4.50".to_string()),
        &DataType::Numeric {
            precision: 5,
            scale: 2,
        },
    )
    .unwrap();
    assert_eq!(
        assign_cast(&numeric, &DataType::Int).unwrap(),
        DataValue::Int(5)
    );
    // Out of range after rounding is still an error.
    assert!(assign_cast(&double(3e9), &DataType::Int).is_err());
    // An explicit CAST keeps truncating.
    assert_eq!(
        cast(&double(1.7), &DataType::Int).unwrap(),
        DataValue::Int(1)
    );
}

#[test]
fn integer_casts_truncate_and_check_each_width() {
    let double = |v: f64| DataValue::DoublePrecision(OrderedF64(v));
    let real = |v: f32| DataValue::Real(OrderedF32(v));
    let numeric = |unscaled: i128, scale: u8| DataValue::numeric(NumericValue { unscaled, scale });
    // (target, smallest, largest) for each integer width.
    let widths: [(DataType, i128, i128); 3] = [
        (DataType::SmallInt, i16::MIN as i128, i16::MAX as i128),
        (DataType::Int, i32::MIN as i128, i32::MAX as i128),
        (DataType::BigInt, i64::MIN as i128, i64::MAX as i128),
    ];
    let as_i128 = |v: DataValue| match v {
        DataValue::SmallInt(n) => n as i128,
        DataValue::Int(n) => n as i128,
        DataValue::BigInt(n) => n as i128,
        other => panic!("not an integer: {:?}", other),
    };
    for (ty, min, max) in widths {
        // Fractions truncate toward zero from every source type.
        for v in [double(-7.9), real(-7.9), numeric(-79, 1)] {
            assert_eq!(as_i128(cast(&v, &ty).unwrap()), -7, "{:?} -> {:?}", v, ty);
        }
        // The exact limits fit; one past them does not.
        assert_eq!(as_i128(cast(&numeric(max, 0), &ty).unwrap()), max);
        assert_eq!(as_i128(cast(&numeric(min, 0), &ty).unwrap()), min);
        assert!(cast(&numeric(max + 1, 0), &ty).is_err());
        assert!(cast(&numeric(min - 1, 0), &ty).is_err());
        // -2^(n-1) is exact as a float and fits; 2^(n-1) does not.
        assert_eq!(as_i128(cast(&double(min as f64), &ty).unwrap()), min);
        assert!(cast(&double(-(min as f64)), &ty).is_err());
        assert_eq!(as_i128(cast(&real(min as f32), &ty).unwrap()), min);
        assert!(cast(&real(-(min as f32)), &ty).is_err());
        assert!(cast(&double(f64::NAN), &ty).is_err());
        // Narrowing between integer types checks the range.
        assert_eq!(as_i128(cast(&DataValue::BigInt(-5), &ty).unwrap()), -5);
    }
    assert!(cast(&DataValue::Int(32768), &DataType::SmallInt).is_err());
    assert!(cast(&DataValue::BigInt(2147483648), &DataType::Int).is_err());
    assert_eq!(
        cast(&DataValue::BigInt(-2147483648), &DataType::Int).unwrap(),
        DataValue::Int(i32::MIN)
    );
}
