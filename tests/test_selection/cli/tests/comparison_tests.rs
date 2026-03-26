// Basic comparison operator tests (=, !=, <, >, <=, >=)

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_equals_operator() -> io::Result<()> {
    execute_test(
        "comparison_equals",
        "Comparison Operator: Equals (=)",
        "id = 500",
        int_eq("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_not_equals_operator() -> io::Result<()> {
    execute_test(
        "comparison_not_equals",
        "Comparison Operator: Not Equals (≠)",
        "id ≠ 500",
        int_ne("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_less_than_operator() -> io::Result<()> {
    execute_test(
        "comparison_less_than",
        "Comparison Operator: Less Than (<)",
        "id < 500",
        int_lt("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_greater_than_operator() -> io::Result<()> {
    execute_test(
        "comparison_greater_than",
        "Comparison Operator: Greater Than (>)",
        "id > 500",
        int_gt("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_less_or_equal_operator() -> io::Result<()> {
    execute_test(
        "comparison_less_or_equal",
        "Comparison Operator: Less Or Equal (≤)",
        "id ≤ 500",
        int_le("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_greater_or_equal_operator() -> io::Result<()> {
    execute_test(
        "comparison_greater_or_equal",
        "Comparison Operator: Greater Or Equal (≥)",
        "id ≥ 500",
        int_ge("id", 500),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_all_comparison_operators() -> io::Result<()> {
    println!("\nRunning all comparison operator tests...");
    test_equals_operator()?;
    test_not_equals_operator()?;
    test_less_than_operator()?;
    test_greater_than_operator()?;
    test_less_or_equal_operator()?;
    test_greater_or_equal_operator()?;
    println!("\nAll comparison operators tested");
    Ok(())
}
