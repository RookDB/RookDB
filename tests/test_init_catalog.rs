use std::path::Path;

use storage_manager::catalog::init_catalog;
use storage_manager::layout::CATALOG_PAGES_DIR;

#[test]
fn test_init_catalog() {
    // Legacy file check removed

    let mut bm = storage_manager::buffer_manager::BufferManager::new();
    init_catalog(&mut bm);

    let pages_created = Path::new(CATALOG_PAGES_DIR).exists();

    assert!(pages_created, "init_catalog did not create catalog_pages/");
}
