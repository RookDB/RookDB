// Parser test examples

use crate::parser::parse_predicate;
use crate::runner::schema::default_schema;
use storage_manager::backend::executor::selection::SelectionExecutor;
use std::io;

/// Test basic parser functionality
pub fn test_parser_examples() -> io::Result<()> {
    println!("\n========================================");
    println!("Testing SQL Parser");
    println!("========================================\n");

    let examples = vec![
        "age > 25",
        "salary >= 50000",
        "name = \"John\"",
        "age > 25 AND salary > 50000",
        "age < 18 OR age > 65",
        "status IS NULL",
        "email IS NOT NULL",
        "NOT active = 1",
        "(age > 18 AND age < 65) OR retired = 1",
        "salary + bonus > 100000",
        "price * quantity > 1000",
    ];

    for query in examples {
        print!("  Query: {:25} => ", query);
        match parse_predicate(query) {
            Ok(predicate) => {
                println!("✓ Parsed successfully");
                // Try to create executor to verify it's a valid predicate
                match SelectionExecutor::new(predicate, default_schema()) {
                    Ok(_) => println!("    └─ Executor created successfully"),
                    Err(e) => println!("    └─ Executor error: {}", e),
                }
            }
            Err(e) => println!("✗ Parse error: {}", e),
        }
    }

    println!("\n========================================");
    println!("Parser test completed!");
    println!("========================================\n");

    Ok(())
}
