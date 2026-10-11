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
//!
//! A few things stay process-wide on purpose:
//!
//! * The registries that give each open file one buffer pool
//!   (`shared_pool`), one free-space map (`fsm`) and one set of page locks
//!   (`page_lock`), and the memo of canonical paths. They are keyed by the
//!   file itself (its absolute path or inode), not by table name, so engines
//!   on different directories never meet in them, and two routes to the
//!   same file still get one copy of its pages. They play the part of a
//!   process's open-file table.
//! * The parser hooks (`register_*_parser`): code supplied once by the
//!   front end, not data about a directory.
//! * The table of open engines behind [`Engine::open`].

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
    /// Cached visibility maps of this directory's tables.
    pub(crate) visibility_maps: Mutex<crate::backend::visibility_map::vm::VmRegistry>,
    /// When this engine's files were last fsynced.
    pub(crate) last_sync: Mutex<Option<std::time::Instant>>,
    /// Scans decode every column, not only the ones a query reads. For
    /// checking that the read-column analysis changes no result.
    decode_all_columns: std::sync::atomic::AtomicBool,
    /// The optimizer keeps joins in written order. For checking that join
    /// ordering changes no result.
    keep_join_order: std::sync::atomic::AtomicBool,
    /// Bytes of rows an `ORDER BY` holds in memory before it sorts on disk.
    sort_memory_budget: std::sync::atomic::AtomicUsize,
    /// Most sorted runs an `ORDER BY` on disk merges at a time.
    sort_merge_fan_in: std::sync::atomic::AtomicUsize,
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
            decode_all_columns: std::sync::atomic::AtomicBool::new(false),
            keep_join_order: std::sync::atomic::AtomicBool::new(false),
            sort_memory_budget: std::sync::atomic::AtomicUsize::new(
                crate::backend::executor::physical::external_sort::DEFAULT_SORT_MEMORY_BYTES,
            ),
            sort_merge_fan_in: std::sync::atomic::AtomicUsize::new(
                crate::backend::executor::physical::external_sort::DEFAULT_MERGE_FAN_IN,
            ),
            visibility_maps: Mutex::default(),
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

    /// Make this engine's scans decode every column of every row instead of
    /// only the columns each query reads. Results are the same; this exists
    /// to check that they are.
    pub fn set_decode_all_columns(&self, all: bool) {
        self.decode_all_columns
            .store(all, std::sync::atomic::Ordering::Relaxed);
    }

    /// See [`Engine::set_decode_all_columns`].
    pub(crate) fn decodes_all_columns(&self) -> bool {
        self.decode_all_columns
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Make the optimizer join tables in the order each query writes them
    /// instead of reordering inner joins by estimated size. Results are the
    /// same up to row order; this exists to check that they are. Forgets
    /// cached plans, which were made under the old setting.
    pub fn set_keep_join_order(&self, keep: bool) {
        self.keep_join_order
            .store(keep, std::sync::atomic::Ordering::Relaxed);
        lock(&self.plans).clear();
    }

    /// See [`Engine::set_keep_join_order`].
    pub(crate) fn keeps_join_order(&self) -> bool {
        self.keep_join_order
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Let an `ORDER BY` hold `bytes` of rows in memory; a larger input is
    /// sorted on disk, in run files under this directory's temp area.
    pub fn set_sort_memory_budget(&self, bytes: usize) {
        self.sort_memory_budget
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Let a sort on disk merge up to `runs` sorted runs at a time (at least
    /// 2). More runs than that are merged in several passes.
    pub fn set_sort_merge_fan_in(&self, runs: usize) {
        self.sort_merge_fan_in
            .store(runs.max(2), std::sync::atomic::Ordering::Relaxed);
    }

    /// When and where this engine's sorts spill to disk.
    pub(crate) fn sort_spill(
        &self,
    ) -> crate::backend::executor::physical::external_sort::SortSpill {
        crate::backend::executor::physical::external_sort::SortSpill {
            memory_budget: self
                .sort_memory_budget
                .load(std::sync::atomic::Ordering::Relaxed),
            merge_fan_in: self
                .sort_merge_fan_in
                .load(std::sync::atomic::Ordering::Relaxed),
            temp_dir: self.paths.temp_dir(),
        }
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
