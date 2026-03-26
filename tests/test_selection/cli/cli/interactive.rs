// Interactive query mode for the Selection Operator CLI.
// Prompts the user to enter a SQL-like predicate string, parses it,
// runs it through the SelectionExecutor on sample tuples, and
// prints per-tuple results plus a summary.
//
// NOTE: Only this file + main.rs are new/modified — no engine code touched.

use std::io;

use storage_manager::backend::executor::selection::{SelectionExecutor, TriValue};

use crate::cli::input::read_input;
use crate::parser::parser::parse_predicate;
use crate::runner::schema::default_schema;
use crate::tuple_accessor::TupleStream;

/// Entry point for Mode 0 — interactive predicate query.
pub fn run_interactive_mode() -> io::Result<()> {
    println!("\n========================================");
    println!("Interactive Query Mode");
    println!("========================================");
    println!("Schema: (id INT, amount FLOAT, name STRING, date DATE)");
    println!("Tuple data loaded from: tuple_storage.bin");
    println!("Type 'back' to return to the main menu.\n");

    loop {
        // --- 1. Read query from user -----------------------------------------
        let raw = read_input("Enter query: ")?;

        if raw.is_empty() {
            println!("(empty input — please enter a predicate or 'back')\n");
            continue;
        }

        if raw.eq_ignore_ascii_case("back") {
            println!("Returning to main menu.\n");
            break;
        }

        println!("\nQuery: {}\n", raw);

        // --- 2. Parse -----------------------------------------------------------
        let predicate = match parse_predicate(&raw) {
            Ok(p) => p,
            Err(e) => {
                println!("ERROR: Parse failed — {}", e);
                println!("  Hint: Try something like:  id > 10  or  id > 5 AND amount < 500.0\n");
                continue;
            }
        };

        // --- 3. Build schema & executor -----------------------------------------
        let schema = default_schema();
        let executor = match SelectionExecutor::new(predicate, schema) {
            Ok(ex) => ex,
            Err(e) => {
                println!("ERROR: Executor creation failed — {:?}\n", e);
                continue;
            }
        };

        // --- 4. Load tuples -----------------------------------------------------
        let mut stream = match TupleStream::from_file("tuple_storage.bin") {
            Ok(s) => s,
            Err(e) => {
                println!("ERROR: Could not load tuple_storage.bin — {}", e);
                println!("  Make sure you have generated tuples first (they are created at startup).\n");
                continue;
            }
        };

        // --- 5. Evaluate & print ------------------------------------------------
        let mut tuple_num = 0usize;
        let mut matched = 0usize;
        let mut unknown_count = 0usize;

        println!("{}", "-".repeat(44));

        while let Some(tuple) = stream.next_tuple() {
            tuple_num += 1;
            let label = match executor.evaluate_tuple(&tuple) {
                Ok(TriValue::True) => {
                    matched += 1;
                    "TRUE"
                }
                Ok(TriValue::False) => "FALSE",
                Ok(TriValue::Unknown) => {
                    unknown_count += 1;
                    "UNKNOWN"
                }
                Err(_) => "ERROR",
            };
            println!("  Tuple {:>3} → {}", tuple_num, label);
        }

        println!("{}", "-".repeat(44));

        // --- 6. Summary ---------------------------------------------------------
        if tuple_num == 0 {
            println!("  (no tuples in file)\n");
        } else {
            println!("  Total tuples : {}", tuple_num);
            println!("  Matched (TRUE): {}", matched);
            if unknown_count > 0 {
                println!("  Unknown      : {}", unknown_count);
            }
            println!();
        }
    }

    Ok(())
}
