use std::fs;
use std::path::Path;

use storage_manager::catalog::{Catalog, init_catalog, load_catalog};
use storage_manager::layout::CATALOG_FILE;

#[test]
fn test_load_catalog() {
    // Step 1: Ensure a valid catalog file exists before loading
    if !Path::new(CATALOG_FILE).exists() {
        let mut bm = storage_manager::buffer_manager::BufferManager::new();
init_catalog(&mut bm); // create catalog.json if missing
    }

    // Step 2: Run load_catalog()
    let mut bm2 = storage_manager::buffer_manager::BufferManager::new();
    let catalog = load_catalog(&mut bm2);

    // Step 3: Verify it returns a valid Catalog struct
    assert!(
        matches!(catalog, Catalog { .. }),
        "load_catalog did not return a valid Catalog struct"
    );

    // Step 4: Clean up (optional)
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to clean up test catalog.json");
    }
}
