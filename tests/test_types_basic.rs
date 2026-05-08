use storage_manager::types::DataType;

#[test]
fn data_type_parse_display_and_metadata_for_all_supported_types() {
    let cases = vec![
        ("SMALLINT", DataType::SmallInt, Some(2), 2, false),
        ("INT", DataType::Int, Some(4), 4, false),
        ("BIGINT", DataType::BigInt, Some(8), 8, false),
        ("REAL", DataType::Real, Some(4), 4, false),
        (
            "DOUBLE PRECISION",
            DataType::DoublePrecision,
            Some(8),
            8,
            false,
        ),
        (
            "NUMERIC(10,2)",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            Some(6),
            1,
            false,
        ),
        (
            "DECIMAL(10,2)",
            DataType::Decimal {
                precision: 10,
                scale: 2,
            },
            Some(6),
            1,
            false,
        ),
        ("BOOLEAN", DataType::Bool, Some(1), 1, false),
        ("CHAR(8)", DataType::Char(8), Some(8), 1, false),
        (
            "CHARACTER(8)",
            DataType::Character(8),
            Some(8),
            1,
            false,
        ),
        ("VARCHAR(32)", DataType::Varchar(32), None, 1, true),
        ("DATE", DataType::Date, Some(4), 4, false),
        ("TIME", DataType::Time, Some(8), 8, false),
        ("BIT(9)", DataType::Bit(9), Some(2), 1, false),
        ("TIMESTAMP", DataType::Timestamp, Some(8), 8, false),
    ];

    for (raw, expected, fixed_size, alignment, variable_len) in cases {
        let parsed: DataType = raw.parse().unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(parsed.fixed_size(), fixed_size);
        assert_eq!(parsed.alignment(), alignment);
        assert_eq!(parsed.is_variable_length(), variable_len);

        let rendered = parsed.to_string();
        let reparsed: DataType = rendered.parse().unwrap();
        assert_eq!(reparsed, parsed);
    }
}

#[test]
fn data_type_invalid_definitions_fail() {
    assert!("BLOB".parse::<DataType>().is_err());
    assert!("NUMERIC(0,0)".parse::<DataType>().is_err());
    assert!("NUMERIC(4,5)".parse::<DataType>().is_err());
    assert!("VARCHAR(x)".parse::<DataType>().is_err());
}
