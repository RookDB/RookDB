/// FSM (Free Space Map) - Implements PostgreSQL-style 3-level binary max-tree
/// for efficient page-level free-space tracking.
///
/// Key Design:
/// - Each heap page maps to one u8 free-space category (0-255) where:
///   category = floor(free_bytes × 255 / PAGE_SIZE)
/// - 3-level tree: Level 2 (root) covers billions of pages, Level 0 (leaves)
/// - FSM fork is treated as a hint; can be rebuilt from heap after crash
///
/// Constants (for 8KB pages):
/// - FSM_NODES_PER_PAGE: 7999 bytes (binary max-tree array)
/// - FSM_SLOTS_PER_PAGE: 4000 usable leaf slots
/// - FSM_LEVELS: 3 (Level 0=leaves, Level 2=root, constant height)

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::PathBuf;

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
/// - tree[0]: root of this FSM page's subtree
/// - tree[1..]: internal and leaf nodes (index 0 is root, leaves occupy the right half)

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
        FSM_METRICS.fsm_serialize_page_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut buf = vec![0u8; FSM_PAGE_SIZE];

        // Write tree array
        buf[0..FSM_NODES_PER_PAGE].copy_from_slice(&self.tree);

        log::trace!(
            "[FSMPage::serialize] Serialized FSMPage: root_value={}",
            self.tree[0]
        );

        buf
    }

    /// Deserialize FSM page from exactly FSM_PAGE_SIZE bytes.
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_deserialize_page_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if bytes.len() < FSM_PAGE_SIZE {
            return Err(io::Error::new(
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

        log::trace!(
            "[FSMPage::deserialize] Deserialized FSMPage: root_value={}",
            tree[0]
        );

        Ok(Self {
            tree,
        })
    }
}

impl Default for FSMPage {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────
// FSM Struct
// ─────────────────────────────────────────────────────────────────────────

/// In-memory handle for the entire FSM fork file.
/// Manages 3-level binary max-tree spanning potentially billions of heap pages.
pub struct FSM {
    #[allow(dead_code)]
    fsm_path: PathBuf,
    fsm_file: File,
    heap_page_count: u32,  // Tracks total heap pages for growth detection
}

impl FSM {
    /// Open existing FSM fork file or create if missing.
    pub fn open(fsm_path: PathBuf, heap_page_count: u32) -> io::Result<Self> {
        log::trace!(
            "[FSM::open] Opening FSM fork at {:?} for heap_page_count={}",
            fsm_path, heap_page_count
        );

        let fsm_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&fsm_path)?;

        Ok(Self {
            fsm_path,
            fsm_file,
            heap_page_count,
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
    pub fn build_from_heap(
        heap_file: &mut File,
        fsm_path: PathBuf,
    ) -> io::Result<Self> {
        log::trace!(
            "[FSM::build_from_heap] Building FSM from heap, writing to {:?}",
            fsm_path
        );

        // Read header page (Page 0) to get heap page count
        let mut header_bytes = vec![0u8; 20];
        heap_file.seek(SeekFrom::Start(0))?;
        heap_file.read_exact(&mut header_bytes)?;

        let page_count = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]);

        log::trace!(
            "[FSM::build_from_heap] Found {} heap pages",
            page_count
        );

        // Calculate FSM structure
        let fsm_page_count = FSM::calculate_fsm_page_count(page_count);
        log::trace!(
            "[FSM::build_from_heap] Requires {} FSM pages",
            fsm_page_count
        );

        // Map of (level, log_page) -> FSMPage to build in memory
        let mut in_memory_pages: std::collections::HashMap<(u32, u32), FSMPage> = std::collections::HashMap::new();

        // Scan heap pages and fully rebuild FSM categories in memory
        log::trace!("[FSM::build_from_heap] Scanning heap pages...");

        // Optimized read: just read first 8 bytes of each page
        for heap_page_id in 1..page_count {
            let mut page_bytes = [0u8; 8];
            let offset = heap_page_id as u64 * 8192; // PAGE_SIZE
            heap_file.seek(SeekFrom::Start(offset))?;

            if let Ok(_) = heap_file.read_exact(&mut page_bytes) {
                // Calculate free space in this page
                let lower = u32::from_le_bytes(page_bytes[0..4].try_into().unwrap());
                let upper = u32::from_le_bytes(page_bytes[4..8].try_into().unwrap());

                let free_bytes = if upper >= lower {
                    upper - lower
                } else {
                    0
                };

                let category = Self::bytes_to_category(free_bytes);

                let log_l0 = heap_page_id / FSM_SLOTS_PER_PAGE as u32;
                let slot_l0 = (heap_page_id % FSM_SLOTS_PER_PAGE as u32) as usize;

                let leaf_page = in_memory_pages.entry((0, log_l0)).or_insert_with(FSMPage::new);
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
        let max_l0 = page_count / FSM_SLOTS_PER_PAGE as u32;
        for log_l0 in 0..=max_l0 {
            if let Some(page_l0) = in_memory_pages.get_mut(&(0, log_l0)) {
                // Bubble up internally
                for i in (0..FSM_NON_LEAF_NODES).rev() {
                    page_l0.tree[i] = std::cmp::max(page_l0.tree[2 * i + 1], page_l0.tree[2 * i + 2]);
                }

                // Only create Level 1 if we have exceeded Level 0
                if page_count > FSM_SLOTS_PER_PAGE {
                    let root_val = page_l0.tree[0];
                    let log_l1 = log_l0 / FSM_SLOTS_PER_PAGE as u32;
                    let slot_l1 = (log_l0 % FSM_SLOTS_PER_PAGE as u32) as usize;

                    let page_l1 = in_memory_pages.entry((1, log_l1)).or_insert_with(FSMPage::new);
                    page_l1.tree[FSM_NON_LEAF_NODES + slot_l1] = root_val;
                }
            }
        }

        // Bubble up Level 1 -> Level 2 (Only if needed)
        if page_count > FSM_SLOTS_PER_PAGE {
            let max_l1 = max_l0 / FSM_SLOTS_PER_PAGE as u32;
            for log_l1 in 0..=max_l1 {
                if let Some(page_l1) = in_memory_pages.get_mut(&(1, log_l1)) {
                    // Bubble up internally
                    for i in (0..FSM_NON_LEAF_NODES).rev() {
                        page_l1.tree[i] = std::cmp::max(page_l1.tree[2 * i + 1], page_l1.tree[2 * i + 2]);
                    }

                    // Only create Level 2 if we have exceeded Level 1
                    let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
                    if page_count > threshold_l2 {
                        let root_val = page_l1.tree[0];
                        let log_l2 = log_l1 / FSM_SLOTS_PER_PAGE as u32;
                        let slot_l2 = (log_l1 % FSM_SLOTS_PER_PAGE as u32) as usize;

                        let page_l2 = in_memory_pages.entry((2, log_l2)).or_insert_with(FSMPage::new);
                        page_l2.tree[FSM_NON_LEAF_NODES + slot_l2] = root_val;
                    }
                }
            }

            // Bubble up Level 2 internally
            let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
            if page_count > threshold_l2 {
                let max_l2 = max_l1 / FSM_SLOTS_PER_PAGE as u32;
                for log_l2 in 0..=max_l2 {
                    if let Some(page_l2) = in_memory_pages.get_mut(&(2, log_l2)) {
                        for i in (0..FSM_NON_LEAF_NODES).rev() {
                            page_l2.tree[i] = std::cmp::max(page_l2.tree[2 * i + 1], page_l2.tree[2 * i + 2]);
                        }
                    }
                }
            }
        }

        // Create FSM handle and reset file contents
        let mut fsm = FSM::open(fsm_path.clone(), page_count)?;
        fsm.fsm_file.set_len(0)?; // Truncate to ensure clean build

        log::trace!("[FSM::build_from_heap] Writing FSM pages to disk...");

        // Write out our populated pages
        // The highest block needed establishes file size.
        for (&(level, log_page), page) in &in_memory_pages {
             fsm.write_fsm_page(level, log_page, 0, page)?;
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
        log::trace!("[FSM::set_heap_page_count] Updating to {}", count);
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

        let l0_count = (heap_pages + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;

        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
        if heap_pages <= threshold_l2 {
            let l1_count = (l0_count + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;
            return l0_count + l1_count;
        }

        let l1_count = (l0_count + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;
        let l2_count = (l1_count + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;

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

    pub fn read_fsm_page(
        &mut self,
        level: u32,
        page_no: u32,
        slot: u32,
    ) -> io::Result<FSMPage> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_read_page_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[FSM::read_fsm_page] Reading level={}, page_no={}, slot={}",
            level, page_no, slot
        );

        let block_offset = self.logical_to_physical(level, page_no) * FSM_PAGE_SIZE as u64;

        // Check if file is large enough
        let file_size = self.fsm_file.metadata()?.len();

        if block_offset + FSM_PAGE_SIZE as u64 > file_size {
            log::trace!(
                "[FSM::read_fsm_page] File too small ({} < {}), returning empty page",
                file_size, block_offset + FSM_PAGE_SIZE as u64
            );
            return Ok(FSMPage::new());
        }

        self.fsm_file.seek(SeekFrom::Start(block_offset))?;

        let mut page_bytes = vec![0u8; FSM_PAGE_SIZE];
        match self.fsm_file.read_exact(&mut page_bytes) {
            Ok(_) => FSMPage::deserialize(&page_bytes),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                log::trace!(
                    "[FSM::read_fsm_page] Unexpected EOF, returning empty page"
                );
                Ok(FSMPage::new())
            }
            Err(e) => Err(e),
        }
    }

    /// Write FSM page at logical position to disk.
    pub fn write_fsm_page(
        &mut self,
        level: u32,
        page_no: u32,
        slot: u32,
        page: &FSMPage,
    ) -> io::Result<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_write_page_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[FSM::write_fsm_page] Writing level={}, page_no={}, slot={}",
            level, page_no, slot
        );

        let block_offset = self.logical_to_physical(level, page_no) * FSM_PAGE_SIZE as u64;

        // Get current file size
        let current_size = self.fsm_file.metadata()?.len();

        // If file is too small, pad it with empty pages
        if block_offset + FSM_PAGE_SIZE as u64 > current_size {
            self.fsm_file.seek(SeekFrom::End(0))?;

            // Avoid calling serialize() on an empty FSMPage to prevent double-serialization logs
            let empty_bytes = vec![0u8; FSM_PAGE_SIZE];

            let mut pages_to_write = ((block_offset + FSM_PAGE_SIZE as u64 - current_size) / FSM_PAGE_SIZE as u64) as u64;
            while pages_to_write > 0 {
                self.fsm_file.write_all(&empty_bytes)?;
                pages_to_write -= 1;
            }
        }

        self.fsm_file.seek(SeekFrom::Start(block_offset))?;

        let page_bytes = page.serialize();
        self.fsm_file.write_all(&page_bytes)?;

        Ok(())
    }

    /// Sync all changes to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        log::trace!("[FSM::sync] Syncing FSM fork file");
        self.fsm_file.sync_all()?;
        Ok(())
    }

    /// Search the 3-level FSM tree to find a heap page with sufficient free space.
    ///
    /// # Arguments
    /// * `min_category` - Minimum required free-space category (0-255)
    ///
    /// # Returns
    /// Some(heap_page_id) if found, None if root < min_category (no page has space)
    ///
    /// # Algorithm
    /// 1. Read root FSM page (Level 2)
    /// 2. If root value < min_category: return None
    /// 3. Traverse Level 2 → Level 1 → Level 0
    /// 4. Compute heap page ID from (fsm_page_no, slot)
    /// 5. Return Some(heap_page_id)
    pub fn fsm_search_avail(&mut self, min_category: u8) -> io::Result<Option<(u32, FSMPage)>> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_search_avail_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[FSM::fsm_search_avail] Searching for page with category >= {}",
            min_category
        );

        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
        let root_level = if self.heap_page_count <= FSM_SLOTS_PER_PAGE {
            0
        } else if self.heap_page_count <= threshold_l2 {
            1
        } else {
            2
        };

        // Traverse tree from root to find a leaf with sufficient free space
        // The search function will read the root page natively avoiding redundant IO
        let result = self.search_tree_for_available_page(root_level, 0, min_category)?;

        if let Some((page_id, fsm_page)) = result {
            log::trace!(
                "[FSM::fsm_search_avail] Found page with sufficient space: page_id={}",
                page_id
            );
            Ok(Some((page_id, fsm_page)))
        } else {
            log::trace!("[FSM::fsm_search_avail] No page found with sufficient space");
            Ok(None)
        }
    }

    /// search FSM tree for a page with free space >= min_category
    /// Returns Ok(Option<(u32, FSMPage)>) where u32 is the page_id, or None if not found
    fn search_tree_for_available_page(
        &mut self,
        level: u32,
        page_no: u32,
        min_category: u8,
    ) -> io::Result<Option<(u32, FSMPage)>> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_search_tree_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if level == 0 {
            // Leaf level: this FSM page's tree array contains heap page categories
            let fsm_page = self.read_fsm_page(0, page_no, 0)?;

            // Get the starting heap page ID for this FSM page
            // Logic derived from: start_heap_page = (L2_slot * 4000 * 4000) + (L1_slot * 4000) + L0_slot
            let start_heap_page = page_no * FSM_SLOTS_PER_PAGE;

            log::trace!("[FSM::search_tree] Level 0 (page_no={}): Searching tree of {} leaves starting from heap_page={}",
                page_no, FSM_SLOTS_PER_PAGE, start_heap_page);

            // Check if even the root has space
            if fsm_page.tree[0] < min_category {
                log::trace!("[FSM::search_tree] Root has value {} < min_category {}, returning None", fsm_page.tree[0], min_category);
                return Ok(None);
            }

            let mut idx = 0; // root of this FSM page

            while idx < FSM_NON_LEAF_NODES {
                let left = 2 * idx + 1;
                let right = 2 * idx + 2;

                if left < FSM_NODES_PER_PAGE && fsm_page.tree[left] >= min_category {
                    idx = left;
                } else if right < FSM_NODES_PER_PAGE && fsm_page.tree[right] >= min_category {
                    idx = right;
                } else {
                    break;
                }
            }

            if idx >= FSM_NON_LEAF_NODES {
                let leaf_offset = idx - FSM_NON_LEAF_NODES;
                let heap_page_id = start_heap_page + leaf_offset as u32;

                // IMPORTANT: Skip heap page 0 - it's the header page, not a data page!
                if heap_page_id == 0 {
                    // Try to find another slot because page 0 is invalid
                    log::trace!("[FSM::search_tree] Level 0 hit heap_page 0 (header), ignoring and returning None this branch");
                    // In a real optimized system, we would backtrack and keep searching but for now we just return None to let caller retry or allocate
                    return Ok(None);
                }

                log::trace!(
                    "[FSM::search_tree] Found heap page {} with category {} >= {}",
                    heap_page_id, fsm_page.tree[idx], min_category
                );
                return Ok(Some((heap_page_id, fsm_page)));
            }

            log::trace!("[FSM::search_tree] No suitable leaf found in Level 0 page_no={}", page_no);
            Ok(None)
        } else {
            // Internal level: traverse child FSM pages
            let fsm_page = self.read_fsm_page(level, page_no, 0)?;

            log::trace!("[FSM::search_tree] Level {} (page_no={}): Searching internal nodes", level, page_no);

            let mut idx = 0; // root of this FSM page

            // Check if even the root has space
            if fsm_page.tree[0] < min_category {
                log::trace!("[FSM::search_tree] Root has value {} < min_category {}, returning None", fsm_page.tree[0], min_category);
                return Ok(None);
            }

            while idx < FSM_NON_LEAF_NODES {
                let left = 2 * idx + 1;
                let right = 2 * idx + 2;

                if left < FSM_NODES_PER_PAGE && fsm_page.tree[left] >= min_category {
                    idx = left;
                } else if right < FSM_NODES_PER_PAGE && fsm_page.tree[right] >= min_category {
                    idx = right;
                } else {
                    break;
                }
            }

            if idx >= FSM_NON_LEAF_NODES {
                let leaf_offset = idx - FSM_NON_LEAF_NODES;
                // leaf_offset is the index among the leaves (0 to 3999)
                // for level L, its leaves refer to Level L-1 pages.
                // Each Level L page spans FSM_SLOTS_PER_PAGE Level L-1 pages
                let next_page_no = page_no * (FSM_SLOTS_PER_PAGE as u32) + leaf_offset as u32;

                if let Some(result) = self.search_tree_for_available_page(
                    level - 1,
                    next_page_no,
                    min_category,
                )? {
                    return Ok(Some(result));
                }
            }

            log::trace!("[FSM::search_tree] No suitable child found in Level {} page_no={}", level, page_no);
            Ok(None)
        }
    }

    /// Update FSM to reflect new available space for a heap page
    ///
    /// Properly implements the full tree update:
    /// 1. Locate Level 0 FSM page containing this heap page
    /// 2. Update the leaf node with new category
    /// 3. Bubble-up changes within Level 0 FSM page
    /// 4. Propagate up to Level 1 and Level 2 if roots changed
    pub fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32, cached_page: Option<&mut FSMPage>) -> io::Result<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_set_avail_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[FSM::fsm_set_avail] Updating heap_page_id={} with {} free bytes",
            heap_page_id, new_free_bytes
        );

        // Compute category from free bytes
        let category = Self::bytes_to_category(new_free_bytes);
        log::trace!(
            "[FSM::fsm_set_avail] Computed category: {}",
            category
        );

        // Find which Level 0 FSM page contains this heap_page_id
        // Each Level 0 FSM page tracks FSM_SLOTS_PER_PAGE heap pages
        let fsm_page_no = (heap_page_id / FSM_SLOTS_PER_PAGE) as u32;
        let slot_within_page = (heap_page_id % FSM_SLOTS_PER_PAGE) as usize;

        log::trace!(
            "[FSM::fsm_set_avail] Heap page {} → FSM Level 0 page {}, slot {}",
            heap_page_id, fsm_page_no, slot_within_page
        );

        // Step 1: Update the leaf in Level 0 FSM page
        let mut owned_leaf;
        let leaf_page = if let Some(page) = cached_page {
            page
        } else {
            owned_leaf = self.read_fsm_page(0, fsm_page_no, 0)?;
            &mut owned_leaf
        };

        let leaf_index = FSM_NON_LEAF_NODES + slot_within_page;

        if leaf_page.tree[leaf_index] == category {
            log::trace!(
                "[FSM::fsm_set_avail] Category unchanged ({}), skipping update",
                category
            );
            return Ok(());
        }

        log::trace!(
            "[FSM::fsm_set_avail] Updating leaf at index {} from {} to {}",
            leaf_index, leaf_page.tree[leaf_index], category
        );

        leaf_page.tree[leaf_index] = category;

        // Step 2: Update internal nodes within the Level 0 page (bubble-up)
        // Binary tree max-tree: internal nodes store max of their children
        // In a standard binary heap with array storage:
        // - Node at index i has children at 2*i+1 and 2*i+2
        // - Parent of node i is at (i-1)/2

        // Start from the parent of the just-updated leaf
        let mut idx = leaf_index;
        while idx > 0 {
            let parent_idx = (idx - 1) / 2;
            let left_child = 2 * parent_idx + 1;
            let right_child = 2 * parent_idx + 2;

            let new_value = if right_child < FSM_NODES_PER_PAGE {
                leaf_page.tree[left_child].max(leaf_page.tree[right_child])
            } else if left_child < FSM_NODES_PER_PAGE {
                leaf_page.tree[left_child]
            } else {
                0
            };

            if leaf_page.tree[parent_idx] != new_value {
                leaf_page.tree[parent_idx] = new_value;
                idx = parent_idx;
            } else {
                break; // No change, no need to bubble further
            }
        }

        // Write updated Level 0 page
        self.write_fsm_page(0, fsm_page_no, 0, &leaf_page)?;

        let new_level0_root = leaf_page.root_value();
        log::trace!(
            "[FSM::fsm_set_avail] Level 0 page root is now: {}",
            new_level0_root
        );

        // Step 3: Propagate changes up to necessary upper levels
        let mut curr_val = new_level0_root;
        let mut curr_page_no = fsm_page_no;

        let threshold_l2 = FSM_SLOTS_PER_PAGE.saturating_mul(FSM_SLOTS_PER_PAGE);
        let max_level = if self.heap_page_count <= FSM_SLOTS_PER_PAGE {
            0
        } else if self.heap_page_count <= threshold_l2 {
            1
        } else {
            2
        };

        for level in 1..=max_level {
            let parent_page_no = curr_page_no / FSM_SLOTS_PER_PAGE;
            let slot = (curr_page_no % FSM_SLOTS_PER_PAGE) as usize;

            let mut page = self.read_fsm_page(level, parent_page_no, 0)?;
            let leaf_idx = FSM_NON_LEAF_NODES + slot;

            if page.tree[leaf_idx] != curr_val {
                page.tree[leaf_idx] = curr_val;

                let mut idx = leaf_idx;
                while idx > 0 {
                    let p_idx = (idx - 1) / 2;
                    let l_idx = 2 * p_idx + 1;
                    let r_idx = 2 * p_idx + 2;

                    let new_val = if r_idx < FSM_NODES_PER_PAGE {
                        page.tree[l_idx].max(page.tree[r_idx])
                    } else if l_idx < FSM_NODES_PER_PAGE {
                        page.tree[l_idx]
                    } else {
                        0
                    };

                    if page.tree[p_idx] != new_val {
                        page.tree[p_idx] = new_val;
                        idx = p_idx;
                    } else {
                        break;
                    }
                }

                self.write_fsm_page(level, parent_page_no, 0, &page)?;
                curr_val = page.root_value();
                curr_page_no = parent_page_no;
            } else {
                break;
            }
        }

        log::trace!("[FSM::fsm_set_avail] Update complete");

        Ok(())
    }

    /// Update free space after a tuple is deleted or page is vacuumed.
    /// Wrapper around fsm_set_avail for Project 10 integration.
    pub fn fsm_vacuum_update(&mut self, heap_page_id: u32, reclaimed_bytes: u32) -> io::Result<()> {
        use crate::backend::instrumentation::FSM_METRICS;
        FSM_METRICS.fsm_vacuum_update_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::trace!(
            "[FSM::fsm_vacuum_update] Recording vacuumed bytes: page_id={}, bytes={}",
            heap_page_id, reclaimed_bytes
        );
        // Delegate to fsm_set_avail
        self.fsm_set_avail(heap_page_id, reclaimed_bytes, None)?;
        Ok(())
    }

    /// Convert free bytes to free-space category (0-255).
    /// Formula: category = floor(free_bytes / 32) (max 255)
    fn bytes_to_category(free_bytes: u32) -> u8 {
        (free_bytes / 32).min(255) as u8
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
        fsm.write_fsm_page(0, 0, 0, &page).unwrap();
        fsm.sync().unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        assert_eq!(file_size, FSM_PAGE_SIZE as u64);

        fs::remove_file(path).unwrap();
    }
}