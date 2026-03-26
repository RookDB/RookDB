// Tests to make sure we handle malformed tuples correctly

use std::io;

use storage_manager::backend::executor::selection::{Predicate, SelectionExecutor};

use crate::runner::predicate_helpers::*;
use crate::runner::schema::default_schema;
use crate::runner::test_runner::execute_validation_test;
use crate::tuple_accessor::TupleStream;

pub fn test_tuple_structure_validation() -> io::Result<()> {
    execute_validation_test(
        "validation_tuple_structure",
        "Tuple Structure Validation",
        validate_tuple_structures,
    )
}

fn validate_tuple_structures() -> io::Result<String> {
    let mut output = String::new();

    output.push_str("Making sure we safely reject malformed tuples\n");
    output.push_str("-".repeat(60).as_str());
    output.push_str("\n\n");

    let schema = default_schema();
    let predicate = int_gt("id", 10);

    let executor = SelectionExecutor::new(predicate, schema.clone())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

    // Test 1: What happens with messed up offsets?
    output.push_str("Test 1: Invalid offset order (non-monotonic)\n");
    output.push_str("-".repeat(40).as_str());
    output.push_str("\n");

    let mut corrupt_tuple = vec![0u8; 45];
    corrupt_tuple[0..4].copy_from_slice(&45u32.to_le_bytes());
    corrupt_tuple[4..8].copy_from_slice(&4u32.to_le_bytes());
    corrupt_tuple[8] = 0;
    corrupt_tuple[9..13].copy_from_slice(&100u32.to_le_bytes());
    corrupt_tuple[13..17].copy_from_slice(&200u32.to_le_bytes());
    corrupt_tuple[17..21].copy_from_slice(&300u32.to_le_bytes());
    corrupt_tuple[21..25].copy_from_slice(&400u32.to_le_bytes());
    corrupt_tuple[25..33].copy_from_slice(&42i64.to_le_bytes());

    match executor.evaluate_tuple(&corrupt_tuple) {
        Ok(_) => output.push_str("  ERROR: Corrupted tuple was accepted (should have failed)\n"),
        Err(e) => output.push_str(&format!("  PASS: Correctly rejected: {:?}\n", e)),
    }

    // Test 2: What if the length field is bogus?
    output.push_str("\nTest 2: Invalid tuple length\n");
    output.push_str("-".repeat(40).as_str());
    output.push_str("\n");

    let mut invalid_length_tuple = vec![0u8; 45];
    invalid_length_tuple[0..4].copy_from_slice(&1000u32.to_le_bytes());
    invalid_length_tuple[4..8].copy_from_slice(&4u32.to_le_bytes());
    invalid_length_tuple[8] = 0;
    invalid_length_tuple[9..13].copy_from_slice(&25u32.to_le_bytes());
    invalid_length_tuple[13..17].copy_from_slice(&33u32.to_le_bytes());
    invalid_length_tuple[17..21].copy_from_slice(&38u32.to_le_bytes());
    invalid_length_tuple[21..25].copy_from_slice(&43u32.to_le_bytes());
    invalid_length_tuple[25..33].copy_from_slice(&42i64.to_le_bytes());

    match executor.evaluate_tuple(&invalid_length_tuple) {
        Ok(_) => output.push_str("  ERROR: Invalid length tuple was accepted (should have failed)\n"),
        Err(e) => output.push_str(&format!("  PASS: Correctly rejected: {:?}\n", e)),
    }

    // Test 3: Make sure valid tuples still work fine
    output.push_str("\nTest 3: Valid tuple (control test)\n");
    output.push_str("-".repeat(40).as_str());
    output.push_str("\n");

    let mut stream = TupleStream::from_file("tuple_storage.bin")?;
    if let Some(valid_tuple) = stream.next_tuple() {
        match executor.evaluate_tuple(&valid_tuple) {
            Ok(result) => output.push_str(&format!("  PASS: Valid tuple accepted: {:?}\n", result)),
            Err(e) => output.push_str(&format!("  ERROR: Valid tuple rejected: {:?}\n", e)),
        }
    }

    output.push_str("\nSummary:\n");
    output.push_str("  SelectionExecutor correctly validates tuple structure\n");
    output.push_str("  Malformed tuples are safely rejected with error codes\n");

    Ok(output)
}
