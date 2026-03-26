// Logical operator tests (AND, OR)

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_logical_and_range() -> io::Result<()> {
    execute_test(
        "logical_and_range",
        "Logical AND: Range Predicate",
        "(id >= 100) AND (id <= 500)",
        and(int_ge("id", 100), int_le("id", 500)),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_logical_or_extremes() -> io::Result<()> {
    execute_test(
        "logical_or_extremes",
        "Logical OR: Extreme Values",
        "(id < 100) OR (id > 900)",
        or(int_lt("id", 100), int_gt("id", 900)),
        default_schema(),
        "tuple_storage.bin",
        None,
    )
}

pub fn test_nested_predicates() -> io::Result<()> {
    execute_test(
        "logical_nested_predicates",
        "Nested Predicate Tree",
        "(id > 500 AND amount < 600.0) OR name = 'Emma'",
        or(
            and(int_gt("id", 500), float_lt("amount", 600.0)),
            text_eq("name", "Emma"),
        ),
        default_schema(),
        "tuple_storage.bin",
        Some("Purpose: Verify recursive predicate evaluation"),
    )
}
