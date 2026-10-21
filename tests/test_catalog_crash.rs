//! A catalog change a crash interrupted is rolled back when the next
//! process loads the catalog. Alone in its binary: the test drops its engine
//! to stand for the process exiting, and another test's checkpoint could
//! briefly keep that engine alive.

use std::path::PathBuf;

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::backend::system_table::begin_catalog_change;
use storage_manager::catalog::{Database, init_catalog_in, save_catalog, try_load_catalog_in};

struct TempRoot(PathBuf);

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_change_a_crash_interrupted_is_rolled_back_on_load() {
    let root = TempRoot(std::env::temp_dir().join(format!("rookdb_crash_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    std::fs::create_dir_all(&root.0).unwrap();

    {
        let engine = Engine::open(&root.0);
        init_catalog_in(&engine).unwrap();
        let mut s = Session::new(try_load_catalog_in(&engine).unwrap());
        s.execute(rook_parser::parse_sql("CREATE DATABASE kept").unwrap())
            .unwrap();

        // A statement edits the catalog, then the process dies: the change
        // neither commits nor rolls back.
        let change = begin_catalog_change(&engine).unwrap();
        let mut catalog = try_load_catalog_in(&engine).unwrap();
        catalog.databases.insert(
            "half_done".to_string(),
            Database {
                tables: Default::default(),
                views: Default::default(),
            },
        );
        save_catalog(&catalog).unwrap();
        std::mem::forget(change);
        storage_manager::backend::cache::checkpoint_durable().unwrap();
        assert!(engine.paths().system_staging_dir().exists());
    }

    // The next process.
    let engine = Engine::open(&root.0);
    let mut names: Vec<String> = try_load_catalog_in(&engine)
        .unwrap()
        .databases
        .into_keys()
        .collect();
    names.sort();
    assert_eq!(names, ["kept"]);
    assert!(!engine.paths().system_staging_dir().exists());
}
