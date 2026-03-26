// Generic test runner for Selection Operator tests.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use storage_manager::backend::catalog::types::Table;
use storage_manager::backend::executor::selection::{Predicate, SelectionExecutor, TriValue};

use crate::tuple_accessor::TupleStream;

// What we get back after running a test
pub struct TestResult {
    pub total_processed: usize,
    pub total_matched: usize,
}

// Run a predicate test on a stream of tuples.
// Returns how many we processed and how many matched.
pub fn execute_predicate_test(
    schema: &Table,
    predicate: Predicate,
    stream: &mut TupleStream,
) -> io::Result<TestResult> {
    let executor = SelectionExecutor::new(predicate, schema.clone())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Executor error: {:?}", e)))?;

    let mut total_processed = 0;
    let mut total_matched = 0;

    while let Some(tuple) = stream.next_tuple() {
        total_processed += 1;
        match executor.evaluate_tuple(&tuple) {
            Ok(TriValue::True) => {
                total_matched += 1;
            }
            Ok(TriValue::False) | Ok(TriValue::Unknown) => {}
            Err(_e) => {
                // Continue processing on error
            }
        }
    }

    Ok(TestResult {
        total_processed,
        total_matched,
    })
}

// Run a complete test: load tuples, apply predicate, write results.
// test_name: used for output filename
// description: what shows up in the output
// predicate_desc: human-readable version of the predicate
// predicate: what we're testing
// schema: table schema
// tuple_source: file path or "empty" for no tuples
// additional_notes: any extra info to include
pub fn execute_test(
    test_name: &str,
    description: &str,
    predicate_desc: &str,
    predicate: Predicate,
    schema: Table,
    tuple_source: &str,
    additional_notes: Option<&str>,
) -> io::Result<()> {
    let mut output = String::new();

    // Header
    output.push_str(&format!("TEST: {}\n", description));
    output.push_str(&"=".repeat(60));
    output.push_str("\n\n");

    if let Some(notes) = additional_notes {
        output.push_str(&format!("{}\n", notes));
        output.push_str("-".repeat(60).as_str());
        output.push_str("\n");
    }

    output.push_str(&format!("Predicate: {}\n", predicate_desc));
    output.push_str("-".repeat(60).as_str());
    output.push_str("\n");

    // Execute test
    let mut stream = if tuple_source == "empty" {
        TupleStream::new(vec![])
    } else {
        TupleStream::from_file(tuple_source)?
    };

    let result = execute_predicate_test(&schema, predicate, &mut stream)?;

    // Statistics
    output.push_str("\nStatistics:\n");
    output.push_str(&format!("  Total tuples processed: {}\n", result.total_processed));
    output.push_str(&format!("  Matched tuples: {}\n", result.total_matched));
    output.push_str("\n");

    // Write output
    write_test_output(test_name, &output)?;

    println!("\nCOMPLETE: {} complete. Results written to output/{}.txt", description, test_name);
    Ok(())
}

/// Writes test output to a file in the output directory.
fn write_test_output(test_name: &str, content: &str) -> io::Result<()> {
    // Create output directory if it doesn't exist
    if !Path::new("output").exists() {
        fs::create_dir("output")?;
    }

    let filename = format!("output/{}.txt", test_name);
    let mut file = File::create(filename)?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

/// Executes a test that validates tuple structure (special case).
pub fn execute_validation_test(
    test_name: &str,
    description: &str,
    validation_fn: fn() -> io::Result<String>,
) -> io::Result<()> {
    let mut output = String::new();

    output.push_str(&format!("TEST: {}\n", description));
    output.push_str(&"=".repeat(60));
    output.push_str("\n\n");

    let result = validation_fn()?;
    output.push_str(&result);

    write_test_output(test_name, &output)?;

    println!("\nCOMPLETE: {} complete. Results written to output/{}.txt", description, test_name);
    Ok(())
}
