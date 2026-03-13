use std::fs;
use std::path::Path;

use storage_manager::catalog::{init_catalog};

use storage_manager::layout::CATALOG_FILE;

#[test]
fn test_init_catalog() {
    // Step 1: Ensure catalog.json doesn’t exist before test
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to remove existing catalog file");
    }

    // Step 2: Run init_catalog()
    init_catalog();

    // Step 3: Verify the file now exists
    assert!(Path::new(CATALOG_FILE).exists(), "catalog.json was not created");

    // Step 4: Read file content and check it’s valid JSON
    let content = fs::read_to_string(CATALOG_FILE).expect("Failed to read catalog.json");
    let parsed: serde_json::Value =
        serde_json::from_str(&content).expect("catalog.json contains invalid JSON");

    // Step 5: Verify structure is { "databases": {} }
    assert!(
        parsed.get("databases").is_some(),
        "catalog.json does not contain 'databases' field"
    );

    // Step 6: Clean up (optional)
    fs::remove_file(CATALOG_FILE).expect("Failed to clean up test catalog.json");
}
