// Edge cases: empty tables, no matches, everything matches

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_empty_relation() -> io::Result<()> {
    execute_test(
        "edge_empty_relation",
        "Empty Relation",
        "id > 500",
        int_gt("id", 500),
        default_schema(),
        "empty",
        Some("What happens when there's no data?"),
    )
}

pub fn test_no_matching_tuples() -> io::Result<()> {
    execute_test(
        "edge_no_matches",
        "No Matching Tuples",
        "id > 9999",
        int_gt("id", 9999),
        default_schema(),
        "tuple_storage.bin",
        Some("What if the predicate matches nothing?"),
    )
}

pub fn test_all_tuples_match() -> io::Result<()> {
    execute_test(
        "edge_all_match",
        "All Tuples Match",
        "id >= 0",
        int_ge("id", 0),
        default_schema(),
        "tuple_storage.bin",
        Some("What if everything matches?"),
    )
}
