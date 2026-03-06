use std::fs;
use std::path::Path;

use storage_manager::catalog::init_catalog;
use storage_manager::layout::{CATALOG_FILE, CATALOG_PAGES_DIR};

#[test]
fn test_init_catalog() {
    // Step 1: Ensure catalog file/dir does not exist before test
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to remove existing catalog file");
    }

    // Step 2: Run init_catalog()
    init_catalog();

    // Step 3: Verify that either the JSON file or the page-based directory was created.
    // In legacy/bootstrap mode init_catalog writes catalog.json.
    // In full page-backend mode it creates catalog_pages/.
    let json_created  = Path::new(CATALOG_FILE).exists();
    let pages_created = Path::new(CATALOG_PAGES_DIR).exists();

    assert!(
        json_created || pages_created,
        "init_catalog() did not create catalog.json or catalog_pages/"
    );

    // Step 4: If JSON was written, check it contains valid JSON with a "databases" key
    if json_created {
        let content = fs::read_to_string(CATALOG_FILE).expect("Failed to read catalog.json");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("catalog.json contains invalid JSON");
        assert!(
            parsed.get("databases").is_some(),
            "catalog.json does not contain 'databases' field"
        );
    }

    // Step 5: Clean up
    if Path::new(CATALOG_FILE).exists() {
        let _ = fs::remove_file(CATALOG_FILE);
    }
}
