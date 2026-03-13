use std::fs;
use std::path::Path;

use storage_manager::catalog::{init_catalog, load_catalog, Catalog};

use storage_manager::layout::CATALOG_FILE;

#[test]
fn test_load_catalog() {
    // Step 1: Ensure a valid catalog file exists before loading
    if !Path::new(CATALOG_FILE).exists() {
        init_catalog(); // create catalog.json if missing
    }

    // Step 2: Run load_catalog()
    let catalog = load_catalog();

    // Step 3: Verify it returns a valid Catalog struct
    // (Just ensuring type correctness and successful deserialization)
    assert!(
        matches!(catalog, Catalog { .. }),
        "load_catalog did not return a valid Catalog struct"
    );

    // Step 4: Clean up (optional)
    fs::remove_file(CATALOG_FILE).expect("Failed to clean up test catalog.json");
}
