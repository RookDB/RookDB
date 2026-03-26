// Tests using random tuples from our generated data

use std::io;

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_test;

pub fn test_complex_predicate_on_random_data() -> io::Result<()> {
    let mut result = execute_test(
        "random_complex_predicate",
        "Complex Predicate on Random Data",
        "amount > 500.0 AND name != 'Emma'",
        and(float_gt("amount", 500.0), text_ne("name", "Emma")),
        default_schema(),
        "tuple_storage.bin",
        Some("Testing with randomly generated tuples. Check tuple_rows.txt to see what data we're working with."),
    );

    if result.is_ok() {
        println!("Hint: Check tuple_rows.txt for readable data");
    }

    result
}
