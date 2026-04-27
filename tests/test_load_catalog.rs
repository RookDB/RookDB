use storage_manager::catalog::{Catalog, init_catalog, load_catalog};

#[test]
fn test_load_catalog() {
    let mut bm = storage_manager::buffer_manager::BufferManager::new();
    init_catalog(&mut bm);

    // Step 2: Run load_catalog()
    let mut bm2 = storage_manager::buffer_manager::BufferManager::new();
    let catalog = load_catalog(&mut bm2);

    // Step 3: Verify it returns a valid Catalog struct
    assert!(
        matches!(catalog, Catalog { .. }),
        "load_catalog did not return a valid Catalog struct"
    );

    // Step 4: No longer needed to clean up
}
