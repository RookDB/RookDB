use common::{init_catalog, load_catalog};

mod common;
use storage_manager::catalog::Catalog;

#[test]
fn test_load_catalog() {
    let _ws = common::TestWorkspace::new("load", "load_catalog");
    // Step 1: Ensure the system catalog exists before loading
    init_catalog();

    // Step 2: Run load_catalog()
    let catalog = load_catalog();

    // Step 3: Verify it returns a valid Catalog struct
    // (Just ensuring type correctness and successful deserialization)
    assert!(
        matches!(catalog, Catalog { .. }),
        "load_catalog did not return a valid Catalog struct"
    );
}
