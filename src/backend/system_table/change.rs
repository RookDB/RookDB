//! All-or-nothing catalog changes.
//!
//! A statement that changes the catalog may edit several system tables in
//! place and save the whole catalog. Before the first edit,
//! [`begin_catalog_change`] copies `system/` to `system.undo/` and fsyncs
//! the copy. [`CatalogChange::commit`] makes the edits durable and drops the
//! copy; [`CatalogChange::rollback`] puts the copy back. After a crash,
//! recovery puts it back too (see [`restore_undo_dir`]), so the catalog
//! is always as one statement left it, or as it was before that statement.
//!
//! Data files (heaps, indexes) are not part of the copy.

use std::sync::atomic::Ordering;

use super::*;
use crate::backend::engine::Engine;

/// An open catalog change; see the module documentation.
///
/// Dropped without [`commit`](Self::commit) or
/// [`rollback`](Self::rollback), it rolls back.
pub struct CatalogChange<'a> {
    engine: &'a Engine,
    /// False for a change opened inside another: the outer one decides.
    owner: bool,
}

/// Open a catalog change on `engine`, or join the one already open.
pub fn begin_catalog_change(engine: &Engine) -> RookResult<CatalogChange<'_>> {
    if engine.catalog_change_open.swap(true, Ordering::AcqRel) {
        return Ok(CatalogChange {
            engine,
            owner: false,
        });
    }
    let change = CatalogChange {
        engine,
        owner: true,
    };
    write_undo_copy(engine).map_err(|e| e.with_context("Failed to save the catalog undo copy"))?;
    Ok(change)
}

/// Run `f` as one catalog change: committed if it succeeds, rolled back if
/// it fails. Inside an open change it simply runs.
pub fn in_catalog_change<T>(engine: &Engine, f: impl FnOnce() -> RookResult<T>) -> RookResult<T> {
    let change = begin_catalog_change(engine)?;
    match f() {
        Ok(value) => change.commit().map(|()| value),
        Err(e) => {
            change.rollback();
            Err(e)
        }
    }
}

impl CatalogChange<'_> {
    /// Keep the change: write the edited system tables durably, then drop
    /// the undo copy.
    pub fn commit(mut self) -> RookResult<()> {
        if !self.owner {
            return Ok(());
        }
        self.owner = false;
        let result = commit_change(self.engine);
        if result.is_err() {
            restore_undo_copy(self.engine);
        }
        self.engine
            .catalog_change_open
            .store(false, Ordering::Release);
        result
    }

    /// Undo the change: put the undo copy back. The in-memory catalog must
    /// be reloaded afterwards.
    pub fn rollback(mut self) {
        if self.owner {
            self.owner = false;
            restore_undo_copy(self.engine);
            self.engine
                .catalog_change_open
                .store(false, Ordering::Release);
        }
    }
}

impl Drop for CatalogChange<'_> {
    fn drop(&mut self) {
        if self.owner {
            restore_undo_copy(self.engine);
            self.engine
                .catalog_change_open
                .store(false, Ordering::Release);
        }
    }
}

/// Write out and drop every cached handle on the system tables, so the
/// files hold everything and nothing keeps reading them after they move.
fn release_system_files(engine: &Engine) -> RookResult<()> {
    let mut result = Ok(());
    for name in SYSTEM_TABLES {
        let path = sys_path(engine, name);
        result = result.and(crate::backend::cache::release_file(&path));
        // `release_file` stops at a failed flush; the handle must go anyway.
        crate::backend::buffer_manager::shared_pool::invalidate(&path);
    }
    result
}

fn write_undo_copy(engine: &Engine) -> RookResult<()> {
    let paths = engine.paths();
    release_system_files(engine)?;
    let tmp = crate::backend::paths::with_suffix(&paths.system_undo_dir(), ".tmp");
    if tmp.exists() {
        std::fs::remove_dir_all(&tmp)?;
    }
    std::fs::create_dir_all(&tmp)?;
    for entry in std::fs::read_dir(paths.system_dir())? {
        let entry = entry?;
        let to = tmp.join(entry.file_name());
        std::fs::copy(entry.path(), &to)?;
        std::fs::File::open(&to)?.sync_all()?;
    }
    sync_dir(&tmp)?;
    std::fs::rename(&tmp, paths.system_undo_dir())?;
    sync_dir(&paths.data_dir())
}

fn commit_change(engine: &Engine) -> RookResult<()> {
    let paths = engine.paths();
    release_system_files(engine)?;
    sync_dir(&paths.system_dir())?;
    // The rename is the commit point: a half-deleted undo copy must never
    // be mistaken for a whole one.
    let done = crate::backend::paths::with_suffix(&paths.system_undo_dir(), ".done");
    std::fs::rename(paths.system_undo_dir(), &done)?;
    sync_dir(&paths.data_dir())?;
    if let Err(e) = std::fs::remove_dir_all(&done) {
        log::warn!("[SystemCatalog] could not remove {}: {}", done.display(), e);
    }
    Ok(())
}

/// Put the undo copy back in place of `system/`. Failures are logged: the
/// copy stays, and recovery retries at the next start.
fn restore_undo_copy(engine: &Engine) {
    if let Err(e) = release_system_files(engine) {
        log::warn!("[SystemCatalog] discarding unwritten catalog edits: {}", e);
    }
    engine.invalidate_metadata();
    if let Err(e) = restore_undo_dir(engine.paths()) {
        log::error!(
            "[SystemCatalog] could not restore the catalog undo copy; it is \
             restored at the next start: {}",
            e
        );
    }
}
