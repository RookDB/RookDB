//! Process-wide hot-path caches for the executor tier.
//!
//! The per-row DML paths used to re-pay heavy fixed costs on every tuple:
//!
//! * `HeapManager::open` twice per row (file open + header read + FSM open)
//!   and the flush-on-last-drop of the shared buffer pool after every row.
//! * 5+ full scans of the system tables per row (`resolve_table_id`, FK and
//!   CHECK loaders, index discovery), deserialising every catalog row each time.
//! * `BTree::open` plus `sync_all()` (fsync) once per row per index.
//!
//! This module keeps small process-wide registries so those costs are paid
//! once per table/index instead of once per row:
//!
//! * [`with_heap`] — one cached `HeapManager` per canonical `.dat` path.
//! * [`metadata`] — constraint/index metadata per `(db, table)`.
//! * `indexes` — discovered index files per `(db, table)` (create_index.rs).
//! * [`with_btree`] — one cached `BTree` handle per `.idx` path, with fsync
//!   batching (sync every `BTREE_SYNC_INTERVAL` mutations instead of per op).
//!
//! Correctness contract (single-user engine):
//!
//! * Reads that bypass the buffer pool (`HeapManager::scan` uses direct I/O)
//!   call [`checkpoint`] first, so any scan observes prior writes.
//! * `checkpoint()` is cheap when nothing is dirty and is safe to call often;
//!   callers include statement boundaries, the CLI exit path and benchmarks.
//! * External rewrites of a heap file (VACUUM) and `HeapManager::create`
//!   evict the affected entries via [`evict_heap`] / [`evict_btree`].
//! * Any DDL that rewrites system tables invalidates [`metadata`] /
//!   `indexes` caches via [`invalidate_metadata`].

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::backend::engine::Engine;
use crate::backend::heap::heap_manager::HeapManager;

// ── tuning knobs ─────────────────────────────────────────────────────────────

/// Flush a cached heap after this many inserts routed through the cache.
/// Bounds the dirty-page window without paying a flush per row.
const HEAP_FLUSH_INTERVAL: u32 = 10_000;

/// fsync a cached B+ Tree after this many unflushed key mutations.
const BTREE_SYNC_INTERVAL: u32 = 4_096;

/// Longest a statement's writes stay un-fsynced while statements keep coming.
/// A power loss can cost at most this window; a process crash costs nothing,
/// because every statement boundary writes its pages to the OS.
const SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Resolved paths remembered per `(cwd, path)`.
///
/// `canonicalize()` issues one `readlink` per path component; calling it for
/// every row made it ~30% of a plain insert. Only successful resolutions are
/// remembered, so a path that does not exist yet is re-resolved once it does.
/// The working directory is part of the key because relative paths are
/// resolved against it (tests switch workspaces with `set_current_dir`).
type CanonicalMemo = HashMap<(PathBuf, PathBuf), PathBuf>;
const CANONICAL_MEMO_LIMIT: usize = 4_096;

fn canonical_memo() -> &'static Mutex<CanonicalMemo> {
    static MEMO: OnceLock<Mutex<CanonicalMemo>> = OnceLock::new();
    MEMO.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn canonical(path: &Path) -> PathBuf {
    let cwd = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().unwrap_or_default()
    };
    let memo_key = (cwd, path.to_path_buf());
    if let Some(hit) = lock(canonical_memo()).get(&memo_key) {
        return hit.clone();
    }

    match path.canonicalize() {
        Ok(resolved) => {
            let mut memo = lock(canonical_memo());
            if memo.len() >= CANONICAL_MEMO_LIMIT {
                memo.clear();
            }
            memo.insert(memo_key, resolved.clone());
            resolved
        }
        Err(_) => {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()
                    .map(|cwd| cwd.join(path))
                    .unwrap_or_else(|_| path.to_path_buf())
            }
        }
    }
}

// ── heap manager cache ───────────────────────────────────────────────────────

struct CachedHeap {
    manager: HeapManager,
    ops_since_flush: u32,
}

type HeapCacheMap = HashMap<PathBuf, CachedHeap>;

fn heap_cache() -> &'static Mutex<HeapCacheMap> {
    static CACHE: OnceLock<Mutex<HeapCacheMap>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Run `f` against a cached `HeapManager` for `path`.
///
/// On a hit the header is refreshed from the (shared) buffer pool first so
/// page counts reflect allocations made through other managers. The manager
/// stays resident — its pool is NOT flushed on return — which removes the
/// per-row open/close churn and the flush-on-last-drop write amplification.
pub fn with_heap<T>(
    path: &Path,
    f: impl FnOnce(&mut HeapManager) -> io::Result<T>,
) -> io::Result<T> {
    let key = canonical(path);

    // Fast path: existing entry.
    {
        let mut cache = lock(heap_cache());
        if let Some(entry) = cache.get(&key)
            && entry.manager.is_file_stale(path)
        {
            cache.remove(&key);
            crate::backend::buffer_manager::shared_pool::invalidate(path);
        }
        if let Some(entry) = cache.get_mut(&key) {
            let result = f(&mut entry.manager)?;
            entry.ops_since_flush += 1;
            if entry.ops_since_flush >= HEAP_FLUSH_INTERVAL {
                entry.manager.flush()?;
                entry.ops_since_flush = 0;
            }
            return Ok(result);
        }
    }

    // Miss: open outside the map lock, then insert (double-check).
    let mut manager = HeapManager::open(path.to_path_buf())?;
    let mut cache = lock(heap_cache());
    if let Some(entry) = cache.get(&key)
        && entry.manager.is_file_stale(path)
    {
        cache.remove(&key);
        crate::backend::buffer_manager::shared_pool::invalidate(path);
    }
    if let Some(entry) = cache.get_mut(&key) {
        // Raced with another opener in between; use the resident one.
        let result = f(&mut entry.manager)?;
        entry.ops_since_flush += 1;
        return Ok(result);
    }
    let result = f(&mut manager)?;
    cache.insert(
        key,
        CachedHeap {
            manager,
            ops_since_flush: 1,
        },
    );
    Ok(result)
}

/// Drop the cached manager for `path` (after flushing it).
///
/// Called when the underlying file is replaced or externally rewritten:
/// `HeapManager::create` truncates the file and registers a fresh shared
/// pool, and VACUUM's direct-I/O compaction leaves stale frames behind.
pub fn evict_heap(path: &Path) -> io::Result<()> {
    let key = canonical(path);
    let removed = lock(heap_cache()).remove(&key);
    if let Some(mut entry) = removed {
        entry.manager.flush()?;
    }
    Ok(())
}

/// Flush and drop every cached heap manager.
pub fn flush_and_clear_heaps() -> io::Result<()> {
    let mut cache = lock(heap_cache());
    for (_, mut entry) in cache.drain() {
        entry.manager.flush()?;
    }
    Ok(())
}

// ── btree handle cache ───────────────────────────────────────────────────────

type BTreeCacheMap = HashMap<PathBuf, (crate::backend::index::btree::BTree, u32)>;

fn btree_cache() -> &'static Mutex<BTreeCacheMap> {
    static CACHE: OnceLock<Mutex<BTreeCacheMap>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Run `f` against a cached `BTree` for `path`.
///
/// `open` is only invoked on a miss (first touch of that index file). Every
/// call counts as one mutation; a real `sync_all()` runs every
/// `BTREE_SYNC_INTERVAL` calls instead of on every row.
///
/// `set_key_types` must be applied by `open` — the cached tree already has it.
pub fn with_btree<T, E>(
    path: &Path,
    open: impl FnOnce() -> Result<crate::backend::index::btree::BTree, E>,
    f: impl FnOnce(&mut crate::backend::index::btree::BTree) -> Result<T, E>,
) -> Result<T, E> {
    let key = canonical(path);
    let mut cache = lock(btree_cache());

    if !cache.contains_key(&key) {
        let tree = open()?;
        cache.insert(key.clone(), (tree, 0));
    }
    let (tree, ops) = cache.get_mut(&key).expect("just inserted");
    let out = f(tree)?;
    *ops += 1;
    if *ops >= BTREE_SYNC_INTERVAL {
        if let Err(e) = tree.sync() {
            log::warn!("[cache] btree sync {:?} failed: {}", key, e);
        }
        *ops = 0;
    }
    Ok(out)
}

/// fsync every cached B+ Tree with pending mutations and drop the handles.
pub fn sync_and_clear_btrees<E>() {
    let mut cache = lock(btree_cache());
    for (_, (mut tree, ops)) in cache.drain() {
        if ops > 0
            && let Err(e) = tree.sync()
        {
            log::warn!("[cache] btree sync on close failed: {}", e);
        }
    }
}

/// Drop a cached B+ Tree handle without syncing (file replaced/removed).
pub fn evict_btree(path: &Path) {
    let key = canonical(path);
    lock(btree_cache()).remove(&key);
}

// ── checkpoints ──────────────────────────────────────────────────────────────

/// Make all prior writes visible to direct-I/O readers, and durable at most
/// [`SYNC_INTERVAL`] later.
///
/// * writes every cached heap manager (buffer pool + FSM + header) and every
///   cached B+ Tree through to its file
/// * fsyncs them only if the last fsync is older than [`SYNC_INTERVAL`], so
///   a burst of statements shares one fsync while a statement typed at a
///   prompt is still synced before the next prompt
///
/// Handles are KEPT — this is a flush, not an eviction — so repeated
/// checkpoints during read-heavy workloads stay free when nothing is dirty
/// and never trigger file-reopen churn. Callers include statement
/// boundaries and process exit. Use [`flush_and_clear_heaps`] /
/// [`sync_and_clear_btrees`] to actually release resources.
pub fn checkpoint() {
    let due = {
        let mut last = lock(last_sync());
        let due = last.is_none_or(|t| t.elapsed() >= SYNC_INTERVAL);
        if due {
            *last = Some(std::time::Instant::now());
        }
        due
    };
    flush_caches(due);
}

/// [`checkpoint`] with an unconditional fsync. Call before the process exits.
pub fn checkpoint_durable() {
    *lock(last_sync()) = Some(std::time::Instant::now());
    flush_caches(true);
}

fn last_sync() -> &'static Mutex<Option<std::time::Instant>> {
    static LAST: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    LAST.get_or_init(|| Mutex::new(None))
}

fn flush_caches(durable: bool) {
    {
        let mut cache = lock(heap_cache());
        for entry in cache.values_mut() {
            let flushed = if durable {
                entry.manager.flush()
            } else {
                entry.manager.write_through()
            };
            if let Err(e) = flushed {
                log::warn!("[cache] heap checkpoint flush failed: {}", e);
            }
            entry.ops_since_flush = 0;
        }
    }
    {
        let mut cache = lock(btree_cache());
        for (tree, ops) in cache.values_mut() {
            let flushed = if durable {
                tree.sync()
            } else {
                tree.write_through()
            };
            if let Err(e) = flushed {
                log::warn!("[cache] btree checkpoint sync failed: {}", e);
            }
            if durable {
                *ops = 0;
            }
        }
    }
    // Metadata/discovery caches stay valid across checkpoints — they only go
    // stale on DDL, which calls invalidate_metadata explicitly.
}

// ── table metadata cache ─────────────────────────────────────────────────────

pub type CheckParserFn = fn(&str) -> Result<rook_ast::PredicateNode, String>;
static CHECK_PARSER: OnceLock<CheckParserFn> = OnceLock::new();

/// Register a parser hook for raw CHECK constraint expressions.
pub fn register_check_parser(parser: CheckParserFn) {
    let _ = CHECK_PARSER.set(parser);
}

/// Retrieve the registered CHECK constraint parser, if any.
pub fn get_check_parser() -> Option<CheckParserFn> {
    CHECK_PARSER.get().copied()
}

/// Constraint/index metadata for one table, loaded from the system tables.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Resolved `sys_tables.id`.
    pub table_id: i32,
    /// Unique indexes as `(index_name, Vec<column_name>)`.
    pub unique_indexes: Vec<(String, Vec<String>)>,
    /// Foreign keys as `(child_col, parent_table, parent_col)`.
    pub foreign_keys: Vec<(String, String, String)>,
    /// CHECK constraint expressions.
    pub check_exprs: Vec<String>,
    /// Precompiled CHECK constraints as `(expression_sql, AST_node)`.
    pub check_ast: Vec<(String, rook_ast::PredicateNode)>,
    /// All named indexes as `(index_name, Vec<column_name>, is_unique)`
    /// (superset view of `unique_indexes`, kept for callers needing layout).
    pub named_indexes: Vec<(String, Vec<String>, bool)>,
}

fn load_meta(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> Option<Arc<TableMeta>> {
    use crate::backend::system_table::{SYS_CONSTRAINTS_SCHEMA, SYS_INDEXES_SCHEMA};

    // One resolve pass (scans sys_databases + sys_tables).
    let (table_id, db_id) =
        match crate::backend::system_table::resolve_table_id(engine, db_name, table_name) {
            Ok(ids) => ids,
            Err(_) => return None,
        };

    // Scan sys_indexes once.
    let mut unique_indexes = Vec::new();
    let mut named_indexes = Vec::new();
    if let Some(rows) = scan_sys(engine, "indexes", SYS_INDEXES_SCHEMA) {
        for row in rows {
            if row.len() < 6 {
                continue;
            }
            let row_table_id = match row[1] {
                Some(crate::types::DataValue::Int(id)) => id,
                _ => continue,
            };
            if row_table_id != table_id {
                continue;
            }
            let name = text_at(&row, 2);
            let columns_field = text_at(&row, 5);
            if name.is_empty() || columns_field.is_empty() {
                continue;
            }
            let cols: Vec<String> = columns_field
                .split(',')
                .map(|c| c.trim().to_string())
                .collect();
            let is_unique = matches!(&row[3], Some(crate::types::DataValue::Bool(v)) if *v);
            // Only genuinely UNIQUE indexes feed the UNIQUE checker —
            // regular indexes must NOT enforce uniqueness.
            if is_unique {
                unique_indexes.push((name.clone(), cols.clone()));
            }
            named_indexes.push((name, cols, is_unique));
        }
    }
    let _ = db_id;

    // Scan sys_constraints once for CHECK + FK rows of this table.
    let mut foreign_keys = Vec::new();
    let mut check_exprs = Vec::new();
    if let Some(rows) = scan_sys(engine, "constraints", SYS_CONSTRAINTS_SCHEMA) {
        for row in rows {
            if row.len() < 6 {
                continue;
            }
            let row_table_id = match row[1] {
                Some(crate::types::DataValue::Int(id)) => id,
                _ => continue,
            };
            if row_table_id != table_id {
                continue;
            }
            let ctype = text_at(&row, 2).to_uppercase();
            let child_col = text_at(&row, 3);
            let parent_table = text_at(&row, 4);
            let parent_col = text_at(&row, 5);
            if ctype.contains("FOREIGN KEY") {
                foreign_keys.push((child_col, parent_table, parent_col));
            } else if ctype == "CHECK" {
                let expr = strip_check_wrapper(&child_col);
                if !expr.is_empty() {
                    check_exprs.push(expr);
                }
            }
        }
    }

    let mut check_ast = Vec::new();
    if !check_exprs.is_empty()
        && let Some(parser) = CHECK_PARSER.get()
    {
        for expr in &check_exprs {
            if let Ok(ast_node) = parser(expr) {
                check_ast.push((expr.clone(), ast_node));
            }
        }
    }

    Some(Arc::new(TableMeta {
        table_id,
        unique_indexes,
        foreign_keys,
        check_exprs,
        check_ast,
        named_indexes,
    }))
}

fn strip_check_wrapper(field: &str) -> String {
    if let Some(inner) = field.strip_prefix("CHECK(") {
        if let Some(end) = inner.rfind(')') {
            return inner[..end].to_string();
        }
        return inner.to_string();
    }
    field.to_string()
}

fn text_at(row: &[Option<crate::types::DataValue>], idx: usize) -> String {
    match row.get(idx) {
        Some(Some(crate::types::DataValue::Varchar(s))) => s.clone(),
        Some(Some(crate::types::DataValue::Char(s))) => s.clone(),
        _ => String::new(),
    }
}

fn scan_sys(
    engine: &crate::backend::engine::Engine,
    name: &str,
    schema: &[crate::types::DataType],
) -> Option<Vec<Vec<Option<crate::types::DataValue>>>> {
    let path = engine.paths().system_table(name.trim_end_matches(".dat"));
    if !path.exists() {
        return None;
    }
    let heap = HeapManager::open(path).ok()?;
    let mut rows = Vec::new();
    for result in heap.scan() {
        let (_, _, raw) = result.ok()?;
        rows.push(crate::types::deserialize_nullable_row(schema, &raw).ok()?);
    }
    Some(rows)
}

/// Fetch (and memoise) metadata for `(db, table)`. Returns `None` when the
/// table is not present in the system tables yet.
pub fn metadata(engine: &Engine, db_name: &str, table_name: &str) -> Option<Arc<TableMeta>> {
    let key = (db_name.to_string(), table_name.to_string());
    if let Some(hit) = lock(&engine.table_meta).get(&key) {
        return Some(Arc::clone(hit));
    }
    let loaded = load_meta(engine, db_name, table_name)?;
    lock(&engine.table_meta).insert(key, Arc::clone(&loaded));
    Some(loaded)
}

/// Forget all cached table metadata (call after any DDL persists).
pub fn invalidate_metadata(engine: &Engine) {
    engine.invalidate_metadata();
}

// ── table statistics cache ───────────────────────────────────────────────────

/// Memoised `collect_table_statistics`, validated by heap file size.
///
/// The optimizer calls this for every table on every query; collecting scans
/// EVERY page of the heap (122 MB at one million rows). Statistics feed
/// cardinality estimates only, so a size-keyed snapshot is sufficient: any
/// append/truncate/compaction changes the file length and forces a recompute.
pub fn table_statistics(
    engine: &Engine,
    db_name: &str,
    table_name: &str,
) -> io::Result<std::sync::Arc<crate::statistics::TableStatistics>> {
    let key = (db_name.to_string(), table_name.to_string());
    let path = PathBuf::from(engine.paths().heap(db_name, table_name));
    let file_len = std::fs::metadata(&path)?.len();

    if let Some((len, hit)) = lock(&engine.statistics).get(&key)
        && *len == file_len
    {
        return Ok(std::sync::Arc::clone(hit));
    }

    let stats = crate::statistics::collect_table_statistics(engine, db_name, table_name)?;
    let arc = std::sync::Arc::new(stats);
    lock(&engine.statistics).insert(key, (file_len, std::sync::Arc::clone(&arc)));
    Ok(arc)
}

// ── referencing-FK cache ─────────────────────────────────────────────────────

/// Foreign keys where OUR table is the parent, as returned by
/// `load_referencing_foreign_keys`:
/// `(child_table, child_column, parent_column, parent_table_name, action_type)`.
pub type RefFks = Vec<(String, String, String, String, String)>;

/// Memoised [`crate::backend::constraint::loaders::load_referencing_foreign_keys`].
///
/// Called once per updated/deleted row by FK propagation — a full system-table
/// sweep per row made bulk UPDATE/DELETE quadratic.
pub fn referencing_fks(engine: &Engine, db_name: &str, table_name: &str) -> std::sync::Arc<RefFks> {
    let key = (db_name.to_string(), table_name.to_string());
    if let Some(hit) = lock(&engine.referencing_fks).get(&key) {
        return std::sync::Arc::clone(hit);
    }
    let loaded = crate::backend::constraint::loaders::load_referencing_foreign_keys(
        engine, db_name, table_name,
    )
    .unwrap_or_default();
    let arc = std::sync::Arc::new(loaded);
    lock(&engine.referencing_fks).insert(key, std::sync::Arc::clone(&arc));
    arc
}

// ── util ─────────────────────────────────────────────────────────────────────

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
