use storage_manager::types::{
    DataType, DataValue, validate_bigint, validate_bit, validate_bool, validate_char,
    validate_date, validate_double, validate_int, validate_numeric, validate_real,
    validate_smallint, validate_time, validate_timestamp, validate_varchar,
};

#[test]
fn integer_boundaries_and_failures() {
    assert!(validate_smallint("-32768").is_ok());
    assert!(validate_smallint("32768").is_err());

    assert!(validate_int("2147483647").is_ok());
    assert!(validate_int("2147483648").is_err());

    assert!(validate_bigint("9223372036854775807").is_ok());
    assert!(validate_bigint("9223372036854775808").is_err());
}

#[test]
fn floating_numeric_and_boolean_constraints() {
    assert!(validate_real("3.14").is_ok());
    assert!(validate_real("not-real").is_err());

    assert!(validate_double("-1.0e100").is_ok());
    assert!(validate_double("not-double").is_err());

    assert!(validate_numeric("123.45", 8, 2).is_ok());
    assert!(validate_numeric("123.456", 8, 2).is_err());

    assert!(validate_bool("true").is_ok());
    assert!(validate_bool("maybe").is_err());
}

#[test]
fn string_temporal_and_bit_constraints() {
    assert!(validate_char("abc", 5).is_ok());
    assert!(validate_char("abcdef", 5).is_err());

    assert!(validate_varchar("abc", 5).is_ok());
    assert!(validate_varchar("abcdef", 5).is_err());

    assert!(validate_date("2026-03-26").is_ok());
    assert!(validate_date("2026-13-01").is_err());

    assert!(validate_time("13:14:15.123456").is_ok());
    assert!(validate_time("25:00:00").is_err());

    assert!(validate_timestamp("2026-03-26 13:14:15").is_ok());
    assert!(validate_timestamp("2026-03-26").is_err());

    assert!(validate_bit("B'1011'", 4).is_ok());
    assert!(validate_bit("B'10110'", 4).is_err());
}

#[test]
fn varchar_length_prefix_violation_is_rejected() {
    let too_long = vec![6_u8, 0, b'a', b'b', b'c', b'd', b'e', b'f'];
    let err = DataValue::from_bytes(&DataType::Varchar(5), &too_long).unwrap_err();
    assert!(err.contains("exceeds declared limit"));
}

#[test]
fn team_robustness_edge_cases_validation() {
    
    assert!(validate_date("2024-02-29").is_ok()); 
    assert!(validate_date("2026-02-29").is_err()); 
    assert!(validate_bit("B'101'", 4).is_err()); 

    
    assert!(validate_real("NaN").is_ok());
    assert!(validate_real("-Infinity").is_ok());
    assert!(validate_time("23:59:59.999999").is_ok());
    assert!(validate_time("23:59:59.1000000").is_err()); 

    
    assert!(validate_double("NaN").is_ok());
    assert!(validate_double("Infinity").is_ok());
    assert!(validate_numeric("1234567.89", 8, 2).is_err());
    assert!(validate_timestamp("2024-02-29 23:59:59").is_ok());
    assert!(validate_timestamp("2026-02-29 00:00:00").is_err());
}
