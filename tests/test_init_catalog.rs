use common::init_catalog;

mod common;

/// `init_catalog()` bootstraps the heap-file system catalog: since the
/// system-catalog stage it creates `database/system/*.dat` instead of the
/// legacy `catalog.json` (which is migrated once, then retired).
#[test]
fn test_init_catalog() {
    // Isolated workspace: init_catalog() bootstraps the system tables here.
    let _ws = common::TestWorkspace::new("initcat", "bootstrap");

    // Run init_catalog()
    init_catalog();

    // Step 3: the core system tables must now exist as heap files
    assert!(
        common::path("database/system/databases.dat").exists(),
        "sys_databases was not created"
    );
    assert!(
        common::path("database/system/tables.dat").exists(),
        "sys_tables was not created"
    );
}
