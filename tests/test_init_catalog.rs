use std::fs;
use std::path::Path;

use storage_manager::catalog::init_catalog;
use storage_manager::layout::{CATALOG_FILE, CATALOG_PAGES_DIR};

#[test]
fn test_init_catalog() {
    if Path::new(CATALOG_FILE).exists() {
        fs::remove_file(CATALOG_FILE).expect("Failed to remove existing catalog file");
    }

    let mut bm = storage_manager::buffer_manager::BufferManager::new();
    init_catalog(&mut bm);

    let json_created  = Path::new(CATALOG_FILE).exists();
    let pages_created = Path::new(CATALOG_PAGES_DIR).exists();

    assert!(
        json_created || pages_created,
        "init_catalog did not create catalog.json or catalog_pages/"
    );

    if json_created {
        let content = fs::read_to_string(CATALOG_FILE).expect("Failed to read catalog.json");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("catalog.json contains invalid JSON");
        assert!(
            parsed.get("databases").is_some(),
            "catalog.json does not contain 'databases' field"
        );
    }

    if Path::new(CATALOG_FILE).exists() {
        let _ = fs::remove_file(CATALOG_FILE);
    }
}
