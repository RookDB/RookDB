//! FSM (Free Space Map) - Implements PostgreSQL-style 3-level binary max-tree
//! for efficient page-level free-space tracking.
//!
//! Key Design:
//! - Each heap page maps to one u8 free-space category (0-255) where:
//!   category = floor(free_bytes × 255 / PAGE_SIZE)
//! - 3-level tree: Level 2 (root) covers billions of pages, Level 0 (leaves)
//! - FSM fork is treated as a hint; can be rebuilt from heap after crash
//!
//! Constants (for 8KB pages):
//! - FSM_NODES_PER_PAGE: 7999 bytes (binary max-tree array)
//! - FSM_SLOTS_PER_PAGE: 4000 usable leaf slots
//! - FSM_LEVELS: 3 (Level 0=leaves, Level 2=root, constant height)

use crate::backend::error::{RookError, RookResult};
use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

// ─────────────────────────────────────────────────────────────────────────
// FSM Constants
// ─────────────────────────────────────────────────────────────────────────

/// Size of binary max-tree node array in one FSM page (bytes)
pub const FSM_NODES_PER_PAGE: usize = 7999;

/// Number of leaf slots (heap pages) one Level-0 FSM page covers
pub const FSM_SLOTS_PER_PAGE: u32 = 4000;

/// Number of internal nodes in the FSM page tree
pub const FSM_NON_LEAF_NODES: usize = (FSM_SLOTS_PER_PAGE as usize) - 1;

/// Number of tree levels (constant height)
pub const FSM_LEVELS: u32 = 3;

/// Size of one FSM page (same as heap page)
pub const FSM_PAGE_SIZE: usize = 8192;

// ─────────────────────────────────────────────────────────────────────────
// FSMPage Struct
// ─────────────────────────────────────────────────────────────────────────

/// One disk page in the FSM fork file.
/// Stores a binary max-tree where leaf nodes hold free-space categories (0–255).
///
/// Layout:
/// - `tree[0]`: root of this FSM page's subtree
/// - `tree[1..]`: internal and leaf nodes (index 0 is root, leaves occupy the right half)

#[derive(Clone, Debug)]
pub struct FSMPage {
    /// Binary max-tree stored as array of u8 (0-255 categories)
    pub tree: [u8; FSM_NODES_PER_PAGE],
}

impl FSMPage {
    /// Create a new empty FSM page (all zeros).
    pub fn new() -> Self {
        log::trace!("[FSMPage::new] Creating new FSM page");
        Self {
            tree: [0u8; FSM_NODES_PER_PAGE],
        }
    }

    /// Get the root value of this FSM page's tree.
    pub fn root_value(&self) -> u8 {
        self.tree[0]
    }

    /// Set the root value of this FSM page's tree.
    pub fn set_root_value(&mut self, value: u8) {
        self.tree[0] = value;
    }

    /// Serialize FSM page to exactly FSM_PAGE_SIZE bytes.
    pub fn serialize(&self) -> Vec<u8> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_serialize_page_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut buf = vec![0u8; FSM_PAGE_SIZE];

        // Write tree array
        buf[0..FSM_NODES_PER_PAGE].copy_from_slice(&self.tree);

        buf
    }

    /// Deserialize FSM page from exactly FSM_PAGE_SIZE bytes.
    pub fn deserialize(bytes: &[u8]) -> RookResult<Self> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_deserialize_page_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if bytes.len() < FSM_PAGE_SIZE {
            return Err(RookError::io(
                io::ErrorKind::InvalidData,
                format!(
                    "FSMPage buffer too small: {} < {}",
                    bytes.len(),
                    FSM_PAGE_SIZE
                ),
            ));
        }

        let mut tree = [0u8; FSM_NODES_PER_PAGE];
        tree.copy_from_slice(&bytes[0..FSM_NODES_PER_PAGE]);

        Ok(Self { tree })
    }
}

impl Default for FSMPage {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────
// FSM page cache
// ─────────────────────────────────────────────────────────────────────────

/// Write-behind image of one FSM fork file.
///
/// Every heap insert used to `statx`, read and rewrite an 8 KB FSM page,
/// which was most of the cost of the insert. Pages are now held here, keyed
/// by block number, and written out by [`FSM::sync`] or when a handle drops.
/// `file_len` is the fork's logical length: the on-disk length plus any
/// blocks that have been written here but not flushed yet.
#[derive(Default)]
struct FsmCache {
    pages: HashMap<u64, FSMPage>,
    dirty: BTreeSet<u64>,
    file_len: u64,
}

type SharedFsmCache = Arc<Mutex<FsmCache>>;

fn lock_cache(cache: &SharedFsmCache) -> MutexGuard<'_, FsmCache> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The cache for `file`, shared by every handle open on the same inode so
/// two `HeapManager`s on one table keep seeing each other's FSM updates.
#[cfg(unix)]
fn shared_cache(file: &File) -> RookResult<SharedFsmCache> {
    use std::os::unix::fs::MetadataExt;
    use std::sync::{OnceLock, Weak};

    type Registry = HashMap<(u64, u64), Weak<Mutex<FsmCache>>>;
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

    let meta = file.metadata()?;
    let key = (meta.dev(), meta.ino());
    let mut registry = REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // A live entry implies an open handle, so its inode cannot have been reused.
    registry.retain(|_, weak| weak.strong_count() > 0);
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    let cache = Arc::new(Mutex::new(FsmCache {
        file_len: meta.len(),
        ..FsmCache::default()
    }));
    registry.insert(key, Arc::downgrade(&cache));
    Ok(cache)
}

/// Without a stable file identity each handle keeps a private cache.
#[cfg(not(unix))]
fn shared_cache(file: &File) -> RookResult<SharedFsmCache> {
    Ok(Arc::new(Mutex::new(FsmCache {
        file_len: file.metadata()?.len(),
        ..FsmCache::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────
// FSM Struct
// ─────────────────────────────────────────────────────────────────────────

/// In-memory handle for the entire FSM fork file.
/// Manages 3-level binary max-tree spanning potentially billions of heap pages.
pub struct FSM {
    fsm_file: File,
    heap_page_count: u32, // Tracks total heap pages for growth detection
    /// Set when in-memory changes were written to the OS but not yet
    /// `sync()`ed. Lets callers skip fsync cycles when the FSM is clean.
    dirty: bool,
    /// Cached pages of this fork, shared with other handles on the same file.
    cache: SharedFsmCache,
}

impl FSM {
    /// Open existing FSM fork file or create if missing.
    pub fn open(fsm_path: PathBuf, heap_page_count: u32) -> RookResult<Self> {
        log::trace!(
            "[FSM::open] Opening FSM fork at {:?} for heap_page_count={}",
            fsm_path,
            heap_page_count
        );

        let fsm_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&fsm_path)?;

        let cache = shared_cache(&fsm_file)?;

        Ok(Self {
            fsm_file,
            heap_page_count,
            dirty: false,
            cache,
        })
    }

    /// Build or rebuild FSM fork by scanning heap pages.
    ///
    /// This function is called after opening a heap file to ensure FSM is consistent
    /// with actual heap contents. After a crash, FSM can be rebuilt from heap (no WAL needed).
    ///
    /// # Arguments
    /// * `heap_file` - Open heap file handle
    /// * `fsm_path` - Path where FSM fork will be created/updated
    ///
    /// # Steps
    /// 1. Read Page 0 header to get page_count
    /// 2. Scan each heap page (1..page_count) to compute free-space categories
    /// 3. Build FSM tree structure with all categories
    /// 4. Write FSM pages to fork file
    /// 5. Update fsm_page_count in header
    pub fn build_from_heap(heap_file: &mut File, fsm_path: PathBuf) -> RookResult<Self> {
        log::trace!(
            "[FSM::build_from_heap] Building FSM from heap, writing to {:?}",
            fsm_path
        );

        // Read header page (Page 0) to get heap page count
        let mut header_bytes = vec![0u8; 24];
        heap_file.seek(SeekFrom::Start(0))?;
        heap_file.read_exact(&mut header_bytes)?;

        let page_count = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);

        log::trace!("[FSM::build_from_heap] Found {} heap pages", page_count);

        // Calculate FSM structure
        let fsm_page_count = FSM::calculate_fsm_page_count(page_count);
        log::trace!(
            "[FSM::build_from_heap] Requires {} FSM pages",
            fsm_page_count
        );

        // Map of (level, log_page) -> FSMPage to build in memory
        let mut in_memory_pages: std::collections::HashMap<(u32, u32), FSMPage> =
            std::collections::HashMap::new();

        // Scan heap pages and fully rebuild FSM categories in memory
        log::trace!("[FSM::build_from_heap] Scanning heap pages...");

        // Optimized read: just read first 8 bytes of each page
        for heap_page_id in 1..page_count {
            let mut page_bytes = [0u8; 8];
            let offset = heap_page_id as u64 * 8192; // PAGE_SIZE
            heap_file.seek(SeekFrom::Start(offset))?;

            if heap_file.read_exact(&mut page_bytes).is_ok() {
                // Calculate free space in this page
                let lower = u32::from_le_bytes(page_bytes[0..4].try_into().unwrap());
                let upper = u32::from_le_bytes(page_bytes[4..8].try_into().unwrap());

                let free_bytes = upper.saturating_sub(lower);

                let category = Self::bytes_to_category(free_bytes);

                let log_l0 = heap_page_id / FSM_SLOTS_PER_PAGE;
                let slot_l0 = (heap_page_id % FSM_SLOTS_PER_PAGE) as usize;

                let leaf_page = in_memory_pages.entry((0, log_l0)).or_default();
                leaf_page.tree[FSM_NON_LEAF_NODES + slot_l0] = category;

                if heap_page_id % 1000 == 0 {
                    log::trace!(
                        "[FSM::build_from_heap] Processed {} heap pages...",
                        heap_page_id
                    );
                }
            }
        }

        // Bubble up Level 0 -> Level 1 (Only if needed)
        let max_l0 = page_count / FSM_SLOTS_PER_PAGE;
        for log_l0 in 0..=max_l0 {
            if let Some(page_l0) = in_memory_pages.get_mut(&(0, log_l0)) {
                // Bubble up internally
                for i in (0..FSM_NON_LEAF_NODES).rev() {
                    page_l0.tree[i] =
                        std::cmp::max(page_l0.tree[2 * i + 1], page_l0.tree[2 * i + 2]);
                }

                // Only create Level 1 if we have exceeded Level 0
                if page_count > FSM_SLOTS_PER_PAGE {
                    let root_val = page_l0.tree[0];
                    let log_l1 = log_l0 / FSM_SLOTS_PER_PAGE;
                    let slot_l1 = (log_l0 % FSM_SLOTS_PER_PAGE) as usize;

                    let page_l1 = in_memory_pages.entry((1, log_l1)).or_default();
                    page_l1.tree[FSM_NON_LEAF_NODES + slot_l1] = root_val;
                }
            }
        }

        // Bubble up Level 1 -> Level 2 (Only if needed)
        if page_count > FSM_SLOTS_PER_PAGE {
            let max_l1 = max_l0 / FSM_SLOTS_PER_PAGE;
            for log_l1 in 0..=max_l1 {
                if let Some(page_l1) = in_memory_pages.get_mut(&(1, log_l1)) {
                    // Bubble up internally
                    for i in (0..FSM_NON_LEAF_NODES).rev() {
                        page_l1.tree[i] =
                            std::cmp::max(page_l1.tree[2 * i + 1], page_l1.tree[2 * i + 2]);
                    }

                    // Only create Level 2 if we have exceeded Level 1
                    let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
                    if page_count > threshold_l2 {
                        let root_val = page_l1.tree[0];
                        let log_l2 = log_l1 / FSM_SLOTS_PER_PAGE;
                        let slot_l2 = (log_l1 % FSM_SLOTS_PER_PAGE) as usize;

                        let page_l2 = in_memory_pages.entry((2, log_l2)).or_default();
                        page_l2.tree[FSM_NON_LEAF_NODES + slot_l2] = root_val;
                    }
                }
            }

            // Bubble up Level 2 internally
            let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
            if page_count > threshold_l2 {
                let max_l2 = max_l1 / FSM_SLOTS_PER_PAGE;
                for log_l2 in 0..=max_l2 {
                    if let Some(page_l2) = in_memory_pages.get_mut(&(2, log_l2)) {
                        for i in (0..FSM_NON_LEAF_NODES).rev() {
                            page_l2.tree[i] =
                                std::cmp::max(page_l2.tree[2 * i + 1], page_l2.tree[2 * i + 2]);
                        }
                    }
                }
            }
        }

        // Create FSM handle and reset file contents
        let mut fsm = FSM::open(fsm_path.clone(), page_count)?;
        fsm.fsm_file.set_len(0)?; // Truncate to ensure clean build
        {
            let mut cache = lock_cache(&fsm.cache);
            cache.pages.clear();
            cache.dirty.clear();
            cache.file_len = 0;
        }

        log::trace!("[FSM::build_from_heap] Writing FSM pages to disk...");

        // Write out our populated pages
        // The highest block needed establishes file size.
        for (&(level, log_page), page) in &in_memory_pages {
            fsm.write_fsm_page(level, log_page, page)?;
        }

        fsm.sync()?;

        log::trace!(
            "[FSM::build_from_heap] FSM successfully built with {} pages",
            fsm_page_count
        );

        Ok(fsm)
    }

    /// Get heap page count currently tracked by this FSM.
    pub fn heap_page_count(&self) -> u32 {
        self.heap_page_count
    }

    /// Update heap page count (used during allocation).
    pub fn set_heap_page_count(&mut self, count: u32) {
        self.heap_page_count = count;
    }

    /// Compute required FSM fork page count for given heap page count.
    /// For small counts, we calculate based on 3-level tree structure.
    pub fn calculate_fsm_page_count(heap_pages: u32) -> u32 {
        if heap_pages == 0 {
            return 0;
        }
        if heap_pages <= FSM_SLOTS_PER_PAGE {
            return 1;
        }

        let l0_count = heap_pages.div_ceil(FSM_SLOTS_PER_PAGE);

        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
        if heap_pages <= threshold_l2 {
            let l1_count = l0_count.div_ceil(FSM_SLOTS_PER_PAGE);
            return l0_count + l1_count;
        }

        let l1_count = l0_count.div_ceil(FSM_SLOTS_PER_PAGE);
        let l2_count = l1_count.div_ceil(FSM_SLOTS_PER_PAGE);

        l0_count + l1_count + l2_count
    }

    /// Read FSM page at logical position (level, page_no, slot) into FSMPage struct.
    ///
    /// If the page doesn't exist in the file yet, returns an empty FSMPage.
    fn logical_to_physical(&self, level: u32, page_no: u32) -> u64 {
        // Store pages contiguously by active levels to avoid sparse 3-level padding
        // when the heap only needs Level 0.
        let heap_pages = self.heap_page_count as u64;
        let f = FSM_SLOTS_PER_PAGE as u64;

        if heap_pages <= f {
            match level {
                0 => page_no as u64,
                _ => panic!("Invalid FSM level for single-level tree"),
            }
        } else {
            let l0_count = heap_pages.div_ceil(f);
            let l1_count = l0_count.div_ceil(f);
            let threshold_l2 = f.saturating_mul(f);

            if heap_pages <= threshold_l2 {
                match level {
                    1 => page_no as u64,
                    0 => l1_count + page_no as u64,
                    _ => panic!("Invalid FSM level for two-level tree"),
                }
            } else {
                let l2_count = l1_count.div_ceil(f);
                match level {
                    2 => page_no as u64,
                    1 => l2_count + page_no as u64,
                    0 => l2_count + l1_count + page_no as u64,
                    _ => panic!("Invalid FSM level for three-level tree"),
                }
            }
        }
    }

    pub fn read_fsm_page(&mut self, level: u32, page_no: u32) -> RookResult<FSMPage> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_read_page_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let block = self.logical_to_physical(level, page_no);
        let block_offset = block * FSM_PAGE_SIZE as u64;

        let mut cache = lock_cache(&self.cache);
        if let Some(page) = cache.pages.get(&block) {
            return Ok(page.clone());
        }

        // Check if file is large enough
        let file_size = cache.file_len;

        if block_offset + FSM_PAGE_SIZE as u64 > file_size {
            return Ok(FSMPage::new());
        }

        self.fsm_file.seek(SeekFrom::Start(block_offset))?;

        let mut page_bytes = vec![0u8; FSM_PAGE_SIZE];
        match self.fsm_file.read_exact(&mut page_bytes) {
            Ok(_) => {
                let page = FSMPage::deserialize(&page_bytes)?;
                cache.pages.insert(block, page.clone());
                Ok(page)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(FSMPage::new()),
            Err(e) => Err(RookError::from(e)),
        }
    }

    /// Write FSM page at logical position (buffered until `sync` or drop).
    pub fn write_fsm_page(&mut self, level: u32, page_no: u32, page: &FSMPage) -> RookResult<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_write_page_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Held in the cache until `sync` or drop; the fork grows logically
        // now and physically when the block is flushed.
        let block = self.logical_to_physical(level, page_no);
        let mut cache = lock_cache(&self.cache);
        cache.pages.insert(block, page.clone());
        cache.dirty.insert(block);
        cache.file_len = cache.file_len.max((block + 1) * FSM_PAGE_SIZE as u64);
        drop(cache);
        self.dirty = true;

        Ok(())
    }

    /// Whether unsynced FSM writes exist.
    pub fn has_pending(&self) -> bool {
        self.dirty || !lock_cache(&self.cache).dirty.is_empty()
    }

    /// Write every cached dirty page to the fork file (no fsync).
    ///
    /// Blocks go out in ascending order; a block past the current end of
    /// file leaves a zero-filled gap, which reads back as an empty FSM page.
    fn flush_pages(&mut self) -> RookResult<()> {
        let mut cache = lock_cache(&self.cache);
        while let Some(&block) = cache.dirty.iter().next() {
            let bytes = match cache.pages.get(&block) {
                Some(page) => page.serialize(),
                None => {
                    cache.dirty.remove(&block);
                    continue;
                }
            };
            self.fsm_file
                .seek(SeekFrom::Start(block * FSM_PAGE_SIZE as u64))?;
            self.fsm_file.write_all(&bytes)?;
            cache.dirty.remove(&block);
        }
        Ok(())
    }

    /// Write cached dirty pages to the fork file without fsync.
    pub fn write_through(&mut self) -> RookResult<()> {
        if !lock_cache(&self.cache).dirty.is_empty() {
            self.flush_pages()?;
            self.dirty = true;
        }
        Ok(())
    }

    /// Sync all changes to disk.
    pub fn sync(&mut self) -> RookResult<()> {
        log::trace!("[FSM::sync] Syncing FSM fork file");
        self.flush_pages()?;
        self.fsm_file.sync_all()?;
        self.dirty = false;
        Ok(())
    }

    /// Run `f` on the cached page at `(level, page_no)`, loading it first.
    ///
    /// `f` returns `(changed, result)`; a changed page is marked dirty. The
    /// page is edited where it lives in the cache: FSM pages are ~8 KB, and
    /// passing copies around was most of the cost of a heap insert.
    fn with_cached_page<R>(
        &mut self,
        level: u32,
        page_no: u32,
        f: impl FnOnce(&mut FSMPage) -> (bool, R),
    ) -> RookResult<R> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_read_page_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let block = self.logical_to_physical(level, page_no);
        let block_offset = block * FSM_PAGE_SIZE as u64;
        let mut cache = lock_cache(&self.cache);

        if !cache.pages.contains_key(&block) {
            let mut page = FSMPage::new();
            if block_offset + FSM_PAGE_SIZE as u64 <= cache.file_len {
                self.fsm_file.seek(SeekFrom::Start(block_offset))?;
                match self.fsm_file.read_exact(&mut page.tree) {
                    Ok(()) => {}
                    // Past the physical end of file: an empty page.
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => page = FSMPage::new(),
                    Err(e) => return Err(RookError::from(e)),
                }
            }
            cache.pages.insert(block, page);
        }

        let page = cache.pages.get_mut(&block).expect("just loaded");
        let (changed, result) = f(page);
        if changed {
            FSM_METRICS
                .fsm_write_page_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            cache.dirty.insert(block);
            cache.file_len = cache.file_len.max(block_offset + FSM_PAGE_SIZE as u64);
            drop(cache);
            self.dirty = true;
        }
        Ok(result)
    }

    /// Walk one FSM page's max-tree towards a leaf holding at least
    /// `min_category`. Returns the leaf's index among the page's slots.
    fn descend(page: &FSMPage, min_category: u8) -> Option<usize> {
        // Check if even the root has space
        if page.tree[0] < min_category {
            return None;
        }
        let mut idx = 0; // root of this FSM page
        while idx < FSM_NON_LEAF_NODES {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < FSM_NODES_PER_PAGE && page.tree[left] >= min_category {
                idx = left;
            } else if right < FSM_NODES_PER_PAGE && page.tree[right] >= min_category {
                idx = right;
            } else {
                break;
            }
        }
        (idx >= FSM_NON_LEAF_NODES).then(|| idx - FSM_NON_LEAF_NODES)
    }

    /// Set one slot of an FSM page and restore the max-tree above it.
    /// Returns `false` when the slot already held `value`.
    fn set_slot(page: &mut FSMPage, slot: usize, value: u8) -> bool {
        let leaf_index = FSM_NON_LEAF_NODES + slot;
        if page.tree[leaf_index] == value {
            return false;
        }
        page.tree[leaf_index] = value;

        // Binary max-tree in array form: node i has children 2i+1 and 2i+2,
        // and each internal node stores the max of its children.
        let mut idx = leaf_index;
        while idx > 0 {
            let parent_idx = (idx - 1) / 2;
            let left_child = 2 * parent_idx + 1;
            let right_child = 2 * parent_idx + 2;
            let new_value = if right_child < FSM_NODES_PER_PAGE {
                page.tree[left_child].max(page.tree[right_child])
            } else if left_child < FSM_NODES_PER_PAGE {
                page.tree[left_child]
            } else {
                0
            };
            if page.tree[parent_idx] != new_value {
                page.tree[parent_idx] = new_value;
                idx = parent_idx;
            } else {
                break; // No change, no need to bubble further
            }
        }
        true
    }

    /// Highest FSM level in use for the current heap size (0 = leaves only).
    fn root_level(&self) -> u32 {
        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
        if self.heap_page_count <= FSM_SLOTS_PER_PAGE {
            0
        } else if self.heap_page_count <= threshold_l2 {
            1
        } else {
            2
        }
    }

    /// Find a heap page whose free-space category is at least `min_category`.
    ///
    /// Descends from the root FSM page to a leaf; returns `None` when the
    /// root is below `min_category` (no page has that much space) or when
    /// the descent lands on heap page 0, which is the header page.
    pub fn search_page(&mut self, min_category: u8) -> RookResult<Option<u32>> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_search_avail_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut level = self.root_level();
        let mut page_no = 0u32;
        loop {
            FSM_METRICS
                .fsm_search_tree_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let hit = self.with_cached_page(level, page_no, |page| {
                (false, Self::descend(page, min_category))
            })?;
            let Some(leaf_offset) = hit else {
                return Ok(None);
            };
            // A leaf of a level-L page names a level-(L-1) page; at level 0
            // it names a heap page.
            let next = page_no * FSM_SLOTS_PER_PAGE + leaf_offset as u32;
            if level == 0 {
                // Heap page 0 is the header page, never a data page.
                return Ok((next != 0).then_some(next));
            }
            level -= 1;
            page_no = next;
        }
    }

    /// Like [`FSM::search_page`], also returning a copy of the level-0 FSM
    /// page that covers the result.
    pub fn fsm_search_avail(&mut self, min_category: u8) -> RookResult<Option<(u32, FSMPage)>> {
        match self.search_page(min_category)? {
            Some(heap_page_id) => {
                let page = self.read_fsm_page(0, heap_page_id / FSM_SLOTS_PER_PAGE)?;
                Ok(Some((heap_page_id, page)))
            }
            None => Ok(None),
        }
    }

    /// Update FSM to reflect new available space for a heap page
    ///
    /// 1. Locate the Level 0 FSM page containing this heap page
    /// 2. Update the leaf with the new category and bubble up within the page
    /// 3. Propagate the page's root to Level 1 and Level 2 while it changes
    ///
    /// `cached_page`, when given, is refreshed with the updated Level 0 page.
    pub fn fsm_set_avail(
        &mut self,
        heap_page_id: u32,
        new_free_bytes: u32,
        cached_page: Option<&mut FSMPage>,
    ) -> RookResult<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_set_avail_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let fsm_page_no = heap_page_id / FSM_SLOTS_PER_PAGE;
        let max_level = self.root_level();

        let mut value = Self::bytes_to_category(new_free_bytes);
        let mut page_no = fsm_page_no;
        let mut slot = (heap_page_id % FSM_SLOTS_PER_PAGE) as usize;
        for level in 0..=max_level {
            let (changed, root) = self.with_cached_page(level, page_no, |page| {
                let changed = Self::set_slot(page, slot, value);
                (changed, (changed, page.root_value()))
            })?;
            if !changed {
                break;
            }
            value = root;
            slot = (page_no % FSM_SLOTS_PER_PAGE) as usize;
            page_no /= FSM_SLOTS_PER_PAGE;
        }

        if let Some(page) = cached_page {
            *page = self.read_fsm_page(0, fsm_page_no)?;
        }
        Ok(())
    }

    /// Update free space after a tuple is deleted or page is vacuumed.
    /// Wrapper around fsm_set_avail for Project 10 integration.
    pub fn fsm_vacuum_update(
        &mut self,
        heap_page_id: u32,
        absolute_free_bytes: u32,
    ) -> RookResult<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS
            .fsm_vacuum_update_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::trace!(
            "[FSM::fsm_vacuum_update] Recording vacuumed bytes: page_id={}, bytes={}",
            heap_page_id,
            absolute_free_bytes
        );
        // Delegate to fsm_set_avail
        self.fsm_set_avail(heap_page_id, absolute_free_bytes, None)?;
        Ok(())
    }

    /// Convert free bytes to free-space category (0-255).
    /// Formula: category = floor(free_bytes / 32) (max 255)
    fn bytes_to_category(free_bytes: u32) -> u8 {
        (free_bytes / 32).min(255) as u8
    }
}

impl Drop for FSM {
    fn drop(&mut self) {
        if let Err(e) = self.flush_pages() {
            log::error!("[FSM::drop] Error flushing cached pages: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_fsm_page_new() {
        let page = FSMPage::new();
        assert_eq!(page.root_value(), 0);
    }

    #[test]
    fn test_fsm_page_serialize_deserialize() {
        let mut page = FSMPage::new();
        page.tree[0] = 100;

        let bytes = page.serialize();
        assert_eq!(bytes.len(), FSM_PAGE_SIZE);

        let page2 = FSMPage::deserialize(&bytes).unwrap();
        assert_eq!(page2.tree[0], 100);
    }

    #[test]
    fn test_calculate_fsm_page_count() {
        // Small counts
        assert_eq!(FSM::calculate_fsm_page_count(1), 1); // 1 L0
        assert_eq!(FSM::calculate_fsm_page_count(100), 1);

        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);

        // Large count (4000 heap pages → exactly 1 L0)
        let count = FSM::calculate_fsm_page_count(FSM_SLOTS_PER_PAGE);
        assert_eq!(count, 1);

        // Exceed Level 0 (4001 heap pages) -> 2 L0 + 1 L1
        let count_l1 = FSM::calculate_fsm_page_count(FSM_SLOTS_PER_PAGE + 1);
        assert_eq!(count_l1, 3);

        // Exceed Level 1 -> requires Level 2
        let count_l2 = FSM::calculate_fsm_page_count(threshold_l2 + 1);
        assert!(count_l2 > threshold_l2 / FSM_SLOTS_PER_PAGE);
    }

    #[test]
    fn test_small_heap_uses_single_fsm_block_on_disk() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rookdb_fsm_small_{nanos}.fsm"));

        let mut fsm = FSM::open(path.clone(), 15).unwrap();
        let page = FSMPage::new();
        fsm.write_fsm_page(0, 0, &page).unwrap();
        fsm.sync().unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        assert_eq!(file_size, FSM_PAGE_SIZE as u64);

        fs::remove_file(path).unwrap();
    }

    fn temp_fsm_path(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rookdb_fsm_{tag}_{}_{nanos}.fsm",
            std::process::id()
        ))
    }

    #[test]
    fn test_writes_are_buffered_until_sync_and_survive_reopen() {
        let path = temp_fsm_path("buffered");
        {
            let mut fsm = FSM::open(path.clone(), 15).unwrap();
            fsm.fsm_set_avail(3, 4096, None).unwrap();
            assert!(fsm.has_pending());
            assert_eq!(fs::metadata(&path).unwrap().len(), 0, "not written yet");

            fsm.sync().unwrap();
            assert!(!fsm.has_pending());
            assert_eq!(fs::metadata(&path).unwrap().len(), FSM_PAGE_SIZE as u64);
        }

        let mut reopened = FSM::open(path.clone(), 15).unwrap();
        let (page_id, _) = reopened.fsm_search_avail(100).unwrap().expect("page 3");
        assert_eq!(page_id, 3);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_drop_flushes_unsynced_pages() {
        let path = temp_fsm_path("drop");
        {
            let mut fsm = FSM::open(path.clone(), 15).unwrap();
            fsm.fsm_set_avail(5, 8000, None).unwrap();
        }
        let mut reopened = FSM::open(path.clone(), 15).unwrap();
        let (page_id, _) = reopened.fsm_search_avail(200).unwrap().expect("page 5");
        assert_eq!(page_id, 5);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn test_two_handles_on_one_file_share_unsynced_updates() {
        let path = temp_fsm_path("shared");
        let mut a = FSM::open(path.clone(), 15).unwrap();
        let mut b = FSM::open(path.clone(), 15).unwrap();

        a.fsm_set_avail(4, 6400, None).unwrap();
        let (page_id, _) = b.fsm_search_avail(150).unwrap().expect("seen through b");
        assert_eq!(page_id, 4);

        // b marks the page full; a must stop offering it.
        b.fsm_set_avail(4, 0, None).unwrap();
        assert!(a.fsm_search_avail(150).unwrap().is_none());

        drop(a);
        drop(b);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_pages_past_the_first_are_written_at_their_block() {
        let path = temp_fsm_path("blocks");
        let heap_pages = FSM_SLOTS_PER_PAGE * 2 + 10; // two-level tree
        {
            let mut fsm = FSM::open(path.clone(), heap_pages).unwrap();
            fsm.fsm_set_avail(FSM_SLOTS_PER_PAGE * 2 + 5, 3200, None)
                .unwrap();
            fsm.sync().unwrap();
        }
        let mut reopened = FSM::open(path.clone(), heap_pages).unwrap();
        let (page_id, _) = reopened.fsm_search_avail(100).unwrap().expect("found");
        assert_eq!(page_id, FSM_SLOTS_PER_PAGE * 2 + 5);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }
}
