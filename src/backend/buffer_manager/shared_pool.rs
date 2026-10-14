//! Per-file buffer pool views, one per heap file.
//!
//! All pages are cached in one global frame store with a single memory budget
//! (see [`shared_pool_capacity`] and `buffer_pool`); a [`BufferPool`] is only
//! the per-file view onto it. Two `HeapManager`s opened on the same `.dat`
//! file must still see each other's dirty pages, so this module keeps a
//! process-wide registry mapping canonical file paths to `Weak` references
//! to a single view. The first manager registers it; later managers on the
//! same file receive clones of the same `Arc`. When the last manager drops
//! its handle, the view drops, writing its dirty pages back and releasing its
//! frames to the store, and the expired registry entry is swept on the next
//! lookup.
//!
//! The registry is keyed by file identity, not by engine, so it stays
//! process-wide: engines on different data directories never share a path.

use crate::backend::error::RookResult;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use super::buffer_pool::BufferPool;

/// Frames in the global buffer pool: the one memory budget for every table
/// and index in the process.
///
/// The default is 8192 frames (64 MiB) in total; override it with
/// `ROOK_POOL_FRAMES` (e.g. 32768 = 256 MiB). Read once, at first use.
pub fn shared_pool_capacity() -> usize {
    const DEFAULT: usize = 8192;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("ROOK_POOL_FRAMES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| (8..=(1 << 20)).contains(&n))
            .unwrap_or(DEFAULT)
    })
}

/// A pool handle shared between every HeapManager on one file.
pub type SharedPool = Arc<Mutex<BufferPool>>;

type Registry = HashMap<PathBuf, Weak<Mutex<BufferPool>>>;

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Canonical identity for a heap file.
///
/// Relative paths are resolved against the current directory so that
/// `"database/base/t.dat"` and `"./database/base/t.dat"` map to one pool.
fn canonical_key(path: &Path) -> PathBuf {
    crate::backend::cache::canonical(path)
}

fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    let mut reg = registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut reg)
}

/// Return the shared pool for `file_path`, opening the file if no live pool
/// exists yet.
pub fn get_or_create(file_path: &Path) -> RookResult<SharedPool> {
    let key = canonical_key(file_path);

    // Sweep expired entries, then reuse a live pool when present.
    if let Some(shared) = with_registry(|reg| {
        reg.retain(|_, weak| weak.upgrade().is_some());
        reg.get(&key).and_then(Weak::upgrade)
    }) {
        return Ok(shared);
    }

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)?;
    let pool = BufferPool::with_file(file, file_path.to_path_buf())?;
    Ok(register_pool(file_path, pool))
}

/// Register an already-constructed pool for `file_path`, replacing any stale
/// entry (used by `HeapManager::create`, which builds a brand-new file).
pub fn register_pool(file_path: &Path, pool: BufferPool) -> SharedPool {
    let key = canonical_key(file_path);
    let shared = Arc::new(Mutex::new(pool));
    with_registry(|reg| {
        reg.insert(key, Arc::downgrade(&shared));
    });
    shared
}

/// Whether `pool` is still the registered pool of `file_path`: false once the
/// file was released or recreated, so a handle that predates that is stale.
pub fn is_current(file_path: &Path, pool: &SharedPool) -> bool {
    let key = canonical_key(file_path);
    with_registry(|reg| {
        reg.get(&key)
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, pool))
    })
}

/// Forget the cached pool for `file_path`.
///
/// Used after external (direct-I/O) rewrites such as VACUUM: subsequent
/// opens construct a fresh pool that reads the new on-disk state instead of
/// serving frames cached before the rewrite. A pool still held live by
/// another manager cannot be dropped; callers should ensure quiescence
/// (single-user engine) before performing external rewrites.
pub fn invalidate(file_path: &Path) {
    let key = canonical_key(file_path);
    with_registry(|reg| {
        reg.remove(&key);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_path_shares_one_pool() {
        let dir = std::env::temp_dir().join(format!(
            "rookdb_shared_p{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        let file_a = dir.join("a.dat");
        let file_b = dir.join("./sub/../a.dat");

        // Create a valid empty heap-ish file (header page).
        std::fs::write(&file_a, vec![0u8; 8192]).unwrap();

        let p1 = get_or_create(&file_a).unwrap();
        let p2 = get_or_create(&file_b).unwrap();
        assert!(
            Arc::ptr_eq(&p1, &p2),
            "two paths resolving to the same file must share one pool"
        );

        // Drop both handles → entry expires → next call creates fresh.
        let weak = Arc::downgrade(&p1);
        drop(p1);
        drop(p2);
        let _p3 = get_or_create(&file_a).unwrap();
        assert!(
            weak.upgrade().is_none(),
            "expired entry must be swept, not resurrected"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn canonical_key_handles_missing_files() {
        let k1 = canonical_key(Path::new("definitely/not/here.dat"));
        assert!(k1.is_absolute());
    }
}
