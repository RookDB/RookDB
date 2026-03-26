// Menu display and test registry for the CLI.

use std::io;

// A single test in our registry
pub struct TestCase {
    pub id: &'static str,
    pub name: &'static str,
    pub category: TestCategory,
    pub handler: fn() -> io::Result<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestCategory {
    Comparison,
    Logical,
    DataType,
    NullHandling,
    RandomData,
    EdgeCase,
    Validation,
    Parser,
    FullSuite,
}

impl TestCategory {
    pub fn display_name(&self) -> &str {
        match self {
            TestCategory::Comparison => "Basic Comparison Tests",
            TestCategory::Logical => "Logical Predicate Tests",
            TestCategory::DataType => "Data Type Tests",
            TestCategory::NullHandling => "NULL Handling Tests",
            TestCategory::RandomData => "Random Tuple Tests",
            TestCategory::EdgeCase => "Edge Case Tests",
            TestCategory::Validation => "Internal Validation Tests",
            TestCategory::Parser => "SQL Parser Tests",
            TestCategory::FullSuite => "Full Test Suite",
        }
    }
}

// Show the main menu with all tests organized by category
pub fn display_menu(test_registry: &[TestCase]) {
    println!("\n========================================");
    println!("Select a test:");
    println!("========================================");

    let mut current_category = None;
    let mut counter = 1;

    for test in test_registry {
        if current_category != Some(test.category) {
            println!("\n{}", test.category.display_name());
            println!("{}", "-".repeat(40));
            current_category = Some(test.category);
        }
        println!("{}. {}", counter, test.name);
        counter += 1;
    }

    println!("\n{}. Exit", counter);
    println!("========================================");
}

// Look up which test the user picked
pub fn find_test_by_number(test_registry: &[TestCase], choice: usize) -> Option<&TestCase> {
    if choice == 0 || choice > test_registry.len() {
        None
    } else {
        Some(&test_registry[choice - 1])
    }
}
