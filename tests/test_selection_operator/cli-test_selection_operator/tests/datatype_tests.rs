// Tests for different data types (INT, FLOAT, DATE, STRING)

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_int_greater_than() -> io::Result<()> {
    execute_test(
        "datatype_int_gt",
        "INT Data Type: Greater Than",
        "id > 500",
        int_gt("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_float_greater_than() -> io::Result<()> {
    execute_test(
        "datatype_float_gt",
        "FLOAT Data Type: Greater Than",
        "amount > 500.0",
        float_gt("amount", 500.0),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_date_greater_or_equal() -> io::Result<()> {
    execute_test(
        "datatype_date_ge",
        "DATE Comparisons: Greater Or Equal",
        "date >= '2024-06-01'",
        date_ge("date", "2024-06-01"),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_string_equals() -> io::Result<()> {
    execute_test(
        "datatype_string_eq",
        "STRING Comparisons: Equals",
        "name = 'Emma'",
        text_eq("name", "Emma"),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_multi_column_predicates() -> io::Result<()> {
    execute_test(
        "datatype_multi_column",
        "Multi Column Predicates",
        "id > 300 AND amount < 700.0",
        and(int_gt("id", 300), float_lt("amount", 700.0)),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Verify predicates referencing multiple columns"),
    )
}

pub fn test_variable_length_text_field() -> io::Result<()> {
    execute_test(
        "datatype_variable_text",
        "Variable Length Field: TEXT",
        "name = 'Emma'",
        text_eq("name", "Emma"),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Ensure tuple decoding works for variable-length TEXT fields"),
    )
}

pub fn test_variable_length_date_field() -> io::Result<()> {
    execute_test(
        "datatype_variable_date",
        "Variable Length Field: DATE",
        "date = '2024-06-15'",
        date_eq("date", "2024-06-15"),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Ensure tuple decoding works for variable-length DATE fields"),
    )
}
