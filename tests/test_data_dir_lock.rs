//! One process per data directory: the engine holds `database/rookdb.lock`
//! while it is open. Alone in its binary because another test's checkpoint
//! briefly holds every open engine, which would delay the release.

use std::path::PathBuf;

use storage_manager::backend::engine::Engine;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};

/// A fresh directory under the system temp dir, removed on drop.
struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("rookdb_lock_{}_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempRoot(dir)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_data_directory_is_locked_while_its_engine_is_open() {
    let root = TempRoot::new("lock");
    let engine = Engine::open(&root.0);
    init_catalog_in(&engine).expect("init catalog");
    // Locking again from the same engine is a no-op.
    init_catalog_in(&engine).expect("init catalog twice");

    // Another holder, as a second process would be, is refused.
    let lock_path = engine.paths().lock_file();
    let other = std::fs::File::open(&lock_path).unwrap();
    assert!(matches!(
        other.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));

    // Dropping the engine releases the directory.
    drop(engine);
    other.try_lock().expect("lock released with the engine");
    other.unlock().unwrap();

    // Loading a catalog takes the lock too, held while the catalog lives.
    let catalog = load_catalog_in(&Engine::open(&root.0));
    assert!(matches!(
        other.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(catalog);
    other.try_lock().expect("lock released with the catalog");
}
