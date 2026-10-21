//! All-or-nothing catalog changes.
//!
//! A statement that changes the catalog may edit several system tables in
//! place and save the whole catalog. [`begin_catalog_change`] copies
//! `system/` to `system.new/`, and until the change ends every system table
//! is read and written in that copy (see [`sys_path`]). Nothing is fsynced
//! along the way. [`CatalogChange::commit`] fsyncs each file of the copy
//! once and swaps it in with two renames, which recovery completes after a
//! crash (see [`recover_system_dir`]); [`CatalogChange::rollback`] deletes
//! the copy. `system/` itself is never edited, so the catalog is always as
//! one statement left it, or as it was before that statement.
//!
//! Data files (heaps, indexes) are not part of the copy.

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
    if engine.catalog_shadow().is_some() {
        return Ok(CatalogChange {
            engine,
            owner: false,
        });
    }
    copy_system_dir(engine).map_err(|e| e.with_context("Failed to start a catalog change"))?;
    engine.set_catalog_shadow(Some(engine.paths().system_staging_dir()));
    Ok(CatalogChange {
        engine,
        owner: true,
    })
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
    /// Keep the change: make the copy durable and swap it in.
    pub fn commit(mut self) -> RookResult<()> {
        if !self.owner {
            return Ok(());
        }
        self.owner = false;
        let shadow = self.engine.set_catalog_shadow(None);
        let result = match &shadow {
            Some(dir) => sync_copy(dir).and_then(|()| swap_in(self.engine, dir)),
            None => Ok(()),
        };
        if result.is_err()
            && let Some(dir) = &shadow
        {
            discard_copy(self.engine, dir);
        }
        result
    }

    /// Undo the change: delete the copy. The in-memory catalog must be
    /// reloaded afterwards.
    pub fn rollback(mut self) {
        self.end_without_commit();
    }

    fn end_without_commit(&mut self) {
        if self.owner {
            self.owner = false;
            if let Some(dir) = self.engine.set_catalog_shadow(None) {
                discard_copy(self.engine, &dir);
            }
        }
    }
}

impl Drop for CatalogChange<'_> {
    fn drop(&mut self) {
        self.end_without_commit();
    }
}

/// Copy the committed system tables to `system.new`, without fsync.
fn copy_system_dir(engine: &Engine) -> RookResult<()> {
    let paths = engine.paths();
    // Cached pages must be in the files before they are copied, and the
    // handles must not outlive the swap that replaces the files.
    for name in SYSTEM_TABLES {
        crate::backend::cache::write_out_file(&paths.system_table(name))?;
    }
    let shadow = paths.system_staging_dir();
    if shadow.exists() {
        std::fs::remove_dir_all(&shadow)?;
    }
    std::fs::create_dir_all(&shadow)?;
    for entry in std::fs::read_dir(paths.system_dir())? {
        let entry = entry?;
        std::fs::copy(entry.path(), shadow.join(entry.file_name()))?;
    }
    Ok(())
}

/// Write out the copy's cached pages and fsync each of its files once.
fn sync_copy(dir: &std::path::Path) -> RookResult<()> {
    for name in SYSTEM_TABLES {
        crate::backend::cache::write_out_file(&dir.join(format!("{}.dat", name)))?;
    }
    for entry in std::fs::read_dir(dir)? {
        std::fs::File::open(entry?.path())?.sync_all()?;
    }
    sync_dir(dir)
}

/// Replace `system/` with the durable copy `dir`. A crash between the
/// renames is completed by [`recover_system_dir`].
fn swap_in(engine: &Engine, dir: &std::path::Path) -> RookResult<()> {
    let paths = engine.paths();
    let old = paths.system_old_dir();
    if old.exists() {
        std::fs::remove_dir_all(&old)?;
    }
    std::fs::rename(paths.system_dir(), &old)?;
    std::fs::rename(dir, paths.system_dir())?;
    sync_dir(&paths.data_dir())?;
    // Committed: a leftover `system.old` is removed by the next recovery.
    if let Err(e) = std::fs::remove_dir_all(&old) {
        log::warn!("[SystemCatalog] could not remove {}: {}", old.display(), e);
    }
    Ok(())
}

/// Delete the copy `dir` and forget what was read from it.
fn discard_copy(engine: &Engine, dir: &std::path::Path) {
    for name in SYSTEM_TABLES {
        crate::backend::cache::discard_file(&dir.join(format!("{}.dat", name)));
    }
    engine.invalidate_metadata();
    if dir.exists()
        && let Err(e) = std::fs::remove_dir_all(dir)
    {
        // `system/` is untouched; the next recovery removes the copy.
        log::warn!("[SystemCatalog] could not remove {}: {}", dir.display(), e);
    }
}
