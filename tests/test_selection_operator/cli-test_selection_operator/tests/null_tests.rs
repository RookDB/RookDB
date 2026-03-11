// NULL handling and three-valued logic tests

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_null_semantics_basic() -> io::Result<()> {
    execute_test(
        "null_semantics_basic",
        "NULL Semantics: Three-Valued Logic",
        "id > 500",
        int_gt("id", 500),
        default_schema(),
        "tuple_storage.bin",
        Some("Note: NULL comparisons return UNKNOWN and are filtered out"),
    )
}

pub fn test_null_propagation_in_and() -> io::Result<()> {
    execute_test(
        "null_propagation_and",
        "NULL Propagation: AND Operator",
        "(id > 400) AND (amount < 800.0)",
        and(int_gt("id", 400), float_lt("amount", 800.0)),
        default_schema(),
        "tuple_storage.bin",
        Some("Expected: NULL rows produce UNKNOWN and are filtered"),
    )
}

pub fn test_is_null_predicate() -> io::Result<()> {
    execute_test(
        "is_null_predicate",
        "IS NULL Predicate",
        "id IS NULL",
        is_null("id"),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Test IS NULL returns True for NULL values, False otherwise"),
    )
}

pub fn test_is_not_null_predicate() -> io::Result<()> {
    execute_test(
        "is_not_null_predicate",
        "IS NOT NULL Predicate",
        "id IS NOT NULL",
        is_not_null("id"),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Test IS NOT NULL returns True for non-NULL values, False otherwise"),
    )
}

pub fn test_is_null_with_and() -> io::Result<()> {
    execute_test(
        "is_null_with_and",
        "IS NULL with AND Logic",
        "(id IS NULL) AND (amount IS NOT NULL)",
        and(is_null("id"), is_not_null("amount")),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Test IS NULL combined with IS NOT NULL using AND operator"),
    )
}

pub fn test_is_null_with_or() -> io::Result<()> {
    execute_test(
        "is_null_with_or",
        "IS NULL with OR Logic",
        "(id IS NULL) OR (id > 500)",
        or(is_null("id"), int_gt("id", 500)),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Test IS NULL combined with comparison using OR operator"),
    )
}

pub fn test_is_not_null_multiple_columns() -> io::Result<()> {
    execute_test(
        "is_not_null_multi",
        "IS NOT NULL on Multiple Columns",
        "(id IS NOT NULL) AND (amount IS NOT NULL)",
        and(is_not_null("id"), is_not_null("amount")),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Test IS NOT NULL on multiple columns to filter complete rows"),
    )
}
