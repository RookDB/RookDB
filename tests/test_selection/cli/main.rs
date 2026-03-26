// ============================================================
// Selection Operator Test CLI
// ============================================================
// Mode 0 → Interactive query mode  (NEW)
// Mode 1 → Automated test suite    (EXISTING — unchanged)
// ============================================================

use std::io;

// ---------- Local modules ---------------------------------------------------
mod cli;
mod parser;
mod parser_test;
mod runner;
mod tests;
mod tuple_accessor;
mod tuple_generator;

// ---------- Imports needed by the automated‑test mode ----------------------
use cli::input::read_input;
use cli::menu::{display_menu, find_test_by_number, TestCase, TestCategory};
use parser_test::test_parser_examples;
use tuple_generator::generate_and_store_random_tuples;

// Import every test function (existing behaviour – untouched)
use tests::comparison_tests::*;
use tests::datatype_tests::*;
use tests::edge_case_tests::*;
use tests::logical_tests::*;
use tests::null_tests::*;
use tests::random_tests::*;
use tests::validation_tests::*;

// ---------- New: interactive mode ------------------------------------------
use cli::interactive::run_interactive_mode;

// ===========================================================================
// main
// ===========================================================================
fn main() -> io::Result<()> {
    println!("========================================");
    println!(" Selection Operator CLI");
    println!("========================================\n");

    // Always generate fresh random tuples so interactive mode has data.
    println!("Generating 100 random tuples...");
    generate_and_store_random_tuples(10)?;
    println!("PASS: Tuples generated and stored:");
    println!("  - Binary storage: tuple_storage.bin");
    println!("  - Human readable: tuple_rows.txt\n");

    // -----------------------------------------------------------------------
    // Mode selector
    // -----------------------------------------------------------------------
    println!("Select mode:");
    println!("  0 → Interactive query mode");
    println!("  1 → Run automated tests");

    let raw = read_input("\nEnter mode (0 or 1): ")?;

    let mode: u8 = match raw.trim().parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("ERROR: '{}' is not a valid mode. Expected 0 or 1.", raw);
            return Ok(());
        }
    };

    match mode {
        0 => run_interactive_mode(),
        1 => run_tests_mode(),
        _ => {
            eprintln!("ERROR: Unknown mode '{}'. Expected 0 or 1.", mode);
            Ok(())
        }
    }
}

// ===========================================================================
// Mode 0 — interactive query
// (implementation lives in cli/interactive.rs — nothing to do here)
// ===========================================================================

// ===========================================================================
// Mode 1 — automated test suite  (EXISTING — do not modify)
// ===========================================================================
fn run_tests_mode() -> io::Result<()> {
    println!("\n========================================");
    println!(" Automated Test Suite");
    println!("========================================\n");

    let test_registry = build_test_registry();

    loop {
        display_menu(&test_registry);

        let choice = read_input("Enter your choice: ")?;

        let choice_num: usize = match choice.parse() {
            Ok(num) => num,
            Err(_) => {
                println!("Invalid input. Please enter a number.\n");
                continue;
            }
        };

        // Exit option
        if choice_num == test_registry.len() + 1 {
            println!("Exiting. Goodbye!");
            break;
        }

        if let Some(test_case) = find_test_by_number(&test_registry, choice_num) {
            (test_case.handler)()?;
        } else {
            println!("Invalid option. Please try again.\n");
        }
    }

    Ok(())
}

// ===========================================================================
// Test registry (EXISTING — do not modify)
// ===========================================================================
fn build_test_registry() -> Vec<TestCase> {
    vec![
        // Basic Comparison Tests
        TestCase {
            id: "comparison_equals",
            name: "Equals Operator (=)",
            category: TestCategory::Comparison,
            handler: test_equals_operator,
        },
        TestCase {
            id: "comparison_not_equals",
            name: "Not Equals Operator (≠)",
            category: TestCategory::Comparison,
            handler: test_not_equals_operator,
        },
        TestCase {
            id: "comparison_less_than",
            name: "Less Than Operator (<)",
            category: TestCategory::Comparison,
            handler: test_less_than_operator,
        },
        TestCase {
            id: "comparison_greater_than",
            name: "Greater Than Operator (>)",
            category: TestCategory::Comparison,
            handler: test_greater_than_operator,
        },
        TestCase {
            id: "comparison_less_or_equal",
            name: "Less Or Equal Operator (≤)",
            category: TestCategory::Comparison,
            handler: test_less_or_equal_operator,
        },
        TestCase {
            id: "comparison_greater_or_equal",
            name: "Greater Or Equal Operator (≥)",
            category: TestCategory::Comparison,
            handler: test_greater_or_equal_operator,
        },
        TestCase {
            id: "comparison_all",
            name: "All Comparison Operators",
            category: TestCategory::Comparison,
            handler: test_all_comparison_operators,
        },
        // Logical Predicate Tests
        TestCase {
            id: "logical_and",
            name: "Logical AND Range Predicate",
            category: TestCategory::Logical,
            handler: test_logical_and_range,
        },
        TestCase {
            id: "logical_or",
            name: "Logical OR Extremes",
            category: TestCategory::Logical,
            handler: test_logical_or_extremes,
        },
        TestCase {
            id: "logical_nested",
            name: "Nested Predicate Tree",
            category: TestCategory::Logical,
            handler: test_nested_predicates,
        },
        // Data Type Tests
        TestCase {
            id: "datatype_int",
            name: "INT Data Type Predicates",
            category: TestCategory::DataType,
            handler: test_int_greater_than,
        },
        TestCase {
            id: "datatype_float",
            name: "FLOAT Data Type Predicates",
            category: TestCategory::DataType,
            handler: test_float_greater_than,
        },
        TestCase {
            id: "datatype_date",
            name: "DATE Comparisons",
            category: TestCategory::DataType,
            handler: test_date_greater_or_equal,
        },
        TestCase {
            id: "datatype_string",
            name: "STRING Comparisons",
            category: TestCategory::DataType,
            handler: test_string_equals,
        },
        TestCase {
            id: "datatype_multi_column",
            name: "Multi Column Predicates",
            category: TestCategory::DataType,
            handler: test_multi_column_predicates,
        },
        TestCase {
            id: "datatype_var_text",
            name: "Variable Length TEXT Fields",
            category: TestCategory::DataType,
            handler: test_variable_length_text_field,
        },
        TestCase {
            id: "datatype_var_date",
            name: "Variable Length DATE Fields",
            category: TestCategory::DataType,
            handler: test_variable_length_date_field,
        },
        // NULL Handling Tests
        TestCase {
            id: "null_basic",
            name: "NULL Semantics (Three-Valued Logic)",
            category: TestCategory::NullHandling,
            handler: test_null_semantics_basic,
        },
        TestCase {
            id: "null_propagation",
            name: "NULL Propagation in AND",
            category: TestCategory::NullHandling,
            handler: test_null_propagation_in_and,
        },
        TestCase {
            id: "is_null_predicate",
            name: "IS NULL Predicate",
            category: TestCategory::NullHandling,
            handler: test_is_null_predicate,
        },
        TestCase {
            id: "is_not_null_predicate",
            name: "IS NOT NULL Predicate",
            category: TestCategory::NullHandling,
            handler: test_is_not_null_predicate,
        },
        TestCase {
            id: "is_null_with_and",
            name: "IS NULL with AND Logic",
            category: TestCategory::NullHandling,
            handler: test_is_null_with_and,
        },
        TestCase {
            id: "is_null_with_or",
            name: "IS NULL with OR Logic",
            category: TestCategory::NullHandling,
            handler: test_is_null_with_or,
        },
        TestCase {
            id: "is_not_null_multi",
            name: "IS NOT NULL on Multiple Columns",
            category: TestCategory::NullHandling,
            handler: test_is_not_null_multiple_columns,
        },
        // Random Data Tests
        TestCase {
            id: "random_complex",
            name: "Complex Predicate on Random Data",
            category: TestCategory::RandomData,
            handler: test_complex_predicate_on_random_data,
        },
        // Edge Case Tests
        TestCase {
            id: "edge_empty",
            name: "Empty Relation",
            category: TestCategory::EdgeCase,
            handler: test_empty_relation,
        },
        TestCase {
            id: "edge_no_matches",
            name: "No Matching Tuples",
            category: TestCategory::EdgeCase,
            handler: test_no_matching_tuples,
        },
        TestCase {
            id: "edge_all_match",
            name: "All Tuples Match",
            category: TestCategory::EdgeCase,
            handler: test_all_tuples_match,
        },
        // Validation Tests
        TestCase {
            id: "validation_structure",
            name: "Tuple Structure Validation",
            category: TestCategory::Validation,
            handler: test_tuple_structure_validation,
        },
        // Parser Tests
        TestCase {
            id: "parser_examples",
            name: "SQL Query Parser Examples",
            category: TestCategory::Parser,
            handler: test_parser_examples,
        },
        // Full Test Suite
        TestCase {
            id: "full_suite",
            name: "Run Full Test Suite",
            category: TestCategory::FullSuite,
            handler: run_full_test_suite,
        },
    ]
}

// Run through all tests one after another (EXISTING — do not modify)
fn run_full_test_suite() -> io::Result<()> {
    println!("\n========================================");
    println!("Running Full Test Suite");
    println!("========================================\n");

    let registry = build_test_registry();

    // Run everything except the "run all" option itself (avoid infinite loop!)
    for test_case in registry.iter().take(registry.len() - 1) {
        println!("\n▶ Running: {}", test_case.name);
        (test_case.handler)()?;
    }

    println!("\n========================================");
    println!("COMPLETE: Full Test Suite Complete!");
    println!("========================================");
    println!("All test results are in the 'output/' directory");

    Ok(())
}
