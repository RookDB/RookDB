//! The engine context: the state of one data directory.
//!
//! An [`Engine`] owns everything the engine remembers about one data
//! directory between statements: where its files are ([`Engine::paths`]),
//! the open tables and indexes, the per-table metadata, statistics and
//! foreign-key caches, index discovery, the plan cache and the time of the
//! last fsync. Code reaches it through the
//! [`Catalog`](crate::catalog::Catalog) it is given (`catalog.engine()`) or
//! an explicit `&Engine` parameter, never through a process-wide static, so
//! two directories never share cached state and invalidation is a method
//! call on the engine that changed.
//!
//! [`Engine::open`] returns the one engine of a directory: opening the same
//! directory twice yields the same instance, as one process owns a data
//! directory, so every catalog loaded from it sees the same caches.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use crate::backend::cache::{RefFks, TableMeta};

type TableKey = (String, String);

/// The state of one data directory. See the module documentation.
pub struct Engine {
    paths: crate::backend::paths::Paths,
    pub(crate) table_meta: Mutex<HashMap<TableKey, Arc<TableMeta>>>,
    pub(crate) statistics: Mutex<HashMap<TableKey, (u64, Arc<crate::statistics::TableStatistics>)>>,
    pub(crate) referencing_fks: Mutex<HashMap<TableKey, Arc<RefFks>>>,
    pub(crate) index_discovery:
        Mutex<HashMap<TableKey, crate::backend::executor::create_index::IndexList>>,
    pub(crate) plans: Mutex<crate::backend::planner::plan_cache::PlanCache>,
    /// Open tables, kept between statements.
    pub(crate) heaps: Mutex<crate::backend::cache::HeapCacheMap>,
    /// Open indexes, kept between statements.
    pub(crate) btrees: Mutex<crate::backend::cache::BTreeCacheMap>,
    /// When this engine's files were last fsynced.
    pub(crate) last_sync: Mutex<Option<std::time::Instant>>,
}

impl Engine {
    /// The engine of the data directory `root`, opening it on first use.
    pub fn open(root: impl AsRef<Path>) -> Arc<Engine> {
        let root = root
            .as_ref()
            .canonicalize()
            .unwrap_or_else(|_| root.as_ref().to_path_buf());
        let mut open = lock(open_registry());
        if let Some(engine) = open.get(&root).and_then(Weak::upgrade) {
            return engine;
        }
        let engine = Arc::new(Engine {
            paths: crate::backend::paths::Paths::new(root.clone()),
            table_meta: Mutex::default(),
            statistics: Mutex::default(),
            referencing_fks: Mutex::default(),
            index_discovery: Mutex::default(),
            heaps: Mutex::default(),
            btrees: Mutex::default(),
            last_sync: Mutex::default(),
            plans: Mutex::new(crate::backend::planner::plan_cache::PlanCache::new(
                crate::backend::planner::plan_cache::DEFAULT_CACHE_CAPACITY,
            )),
        });
        open.retain(|_, e| e.strong_count() > 0);
        open.insert(root, Arc::downgrade(&engine));
        engine
    }

    /// The engine of the working directory.
    pub fn current_dir() -> Arc<Engine> {
        Engine::open(".")
    }

    /// The data directory this engine manages.
    pub fn root(&self) -> &Path {
        self.paths.root()
    }

    /// Where this data directory keeps each of its files.
    pub fn paths(&self) -> &crate::backend::paths::Paths {
        &self.paths
    }

    /// Forget everything cached about tables: metadata, statistics,
    /// referencing foreign keys, index discovery and cached plans. Call after
    /// DDL changes the system tables.
    pub fn invalidate_metadata(&self) {
        lock(&self.table_meta).clear();
        lock(&self.referencing_fks).clear();
        lock(&self.statistics).clear();
        lock(&self.index_discovery).clear();
        lock(&self.plans).clear();
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("root", &self.paths.root())
            .finish()
    }
}

/// The engines currently open, one per directory. Held weakly: an engine
/// closes when the last catalog using it is dropped.
fn open_registry() -> &'static Mutex<HashMap<PathBuf, Weak<Engine>>> {
    static OPEN: OnceLock<Mutex<HashMap<PathBuf, Weak<Engine>>>> = OnceLock::new();
    OPEN.get_or_init(Mutex::default)
}

/// Every engine that is open right now.
pub(crate) fn open_engines() -> Vec<Arc<Engine>> {
    lock(open_registry())
        .values()
        .filter_map(Weak::upgrade)
        .collect()
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
