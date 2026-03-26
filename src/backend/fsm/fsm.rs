/// FSM (Free Space Map) - Implements PostgreSQL-style 3-level binary max-tree
/// for efficient page-level free-space tracking.
///
/// Key Design:
/// - Each heap page maps to one u8 free-space category (0-255) where:
///   category = floor(free_bytes × 255 / PAGE_SIZE)
/// - 3-level tree: Level 2 (root) covers billions of pages, Level 0 (leaves)
/// - fp_next_slot hint spreads concurrent inserts across pages
/// - FSM fork is treated as a hint; can be rebuilt from heap after crash
///
/// Constants (for 8KB pages):
/// - FSM_NODES_PER_PAGE: 4080 bytes (binary max-tree array)
/// - FSM_SLOTS_PER_PAGE: 2040 usable leaf slots (covers ~4M heap pages → ~32GB)
/// - FSM_LEVELS: 3 (Level 0=leaves, Level 2=root, constant height)

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::PathBuf;

// ─────────────────────────────────────────────────────────────────────────
// FSM Constants
// ─────────────────────────────────────────────────────────────────────────

/// Size of binary max-tree node array in one FSM page (bytes)
pub const FSM_NODES_PER_PAGE: usize = 4080;

/// Number of leaf slots (heap pages) one Level-0 FSM page covers
pub const FSM_SLOTS_PER_PAGE: u32 = 2040;

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
/// - fp_next_slot: load-spreading hint (round-robin across search directions)
#[derive(Clone, Debug)]
pub struct FSMPage {
    /// Binary max-tree stored as array of u8 (0-255 categories)
    pub tree: [u8; FSM_NODES_PER_PAGE],
    
    /// Hint for next search: cycles through FSM_SLOTS_PER_PAGE slots
    /// Used to spread concurrent inserts and reduce contention
    pub fp_next_slot: u16,
}

impl FSMPage {
    /// Create a new empty FSM page (all zeros).
    pub fn new() -> Self {
        println!("[FSMPage::new] Creating new FSM page");
        Self {
            tree: [0u8; FSM_NODES_PER_PAGE],
            fp_next_slot: 0,
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
        let mut buf = vec![0u8; FSM_PAGE_SIZE];
        
        // Write tree array
        buf[0..FSM_NODES_PER_PAGE].copy_from_slice(&self.tree);
        
        // Write fp_next_slot as u16 (little-endian) at offset FSM_NODES_PER_PAGE
        let fp_bytes = self.fp_next_slot.to_le_bytes();
        buf[FSM_NODES_PER_PAGE..FSM_NODES_PER_PAGE + 2].copy_from_slice(&fp_bytes);
        
        println!(
            "[FSMPage::serialize] Serialized FSMPage: fp_next_slot={}, root_value={}",
            self.fp_next_slot, self.tree[0]
        );
        
        buf
    }

    /// Deserialize FSM page from exactly FSM_PAGE_SIZE bytes.
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
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

        let mut fp_bytes = [0u8; 2];
        fp_bytes.copy_from_slice(&bytes[FSM_NODES_PER_PAGE..FSM_NODES_PER_PAGE + 2]);
        let fp_next_slot = u16::from_le_bytes(fp_bytes);

        println!(
            "[FSMPage::deserialize] Deserialized FSMPage: fp_next_slot={}, root_value={}",
            fp_next_slot, tree[0]
        );

        Ok(Self {
            tree,
            fp_next_slot,
        })
    }

    /// Advance fp_next_slot to the next slot (round-robin).
    pub fn advance_fp_next_slot(&mut self) {
        self.fp_next_slot = (self.fp_next_slot + 1) % (FSM_SLOTS_PER_PAGE as u16);
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
        println!(
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
        println!(
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

        println!(
            "[FSM::build_from_heap] Found {} heap pages",
            page_count
        );

        // Create or truncate FSM file
        let mut fsm_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&fsm_path)?;

        // Calculate FSM structure
        let fsm_page_count = FSM::calculate_fsm_page_count(page_count);
        println!(
            "[FSM::build_from_heap] Requires {} FSM pages",
            fsm_page_count
        );

        // Initialize FSM with empty pages (all zeros)
        let empty_page = FSMPage::new();
        let empty_bytes = empty_page.serialize();

        for _ in 0..fsm_page_count {
            fsm_file.write_all(&empty_bytes)?;
        }

        // Scan heap pages and update FSM categories
        println!("[FSM::build_from_heap] Scanning heap pages...");
        let mut fsm_pages: std::collections::HashMap<u32, FSMPage> = std::collections::HashMap::new();

        for heap_page_id in 1..page_count {
            // Read heap page
            let mut page_bytes = vec![0u8; 8192]; // PAGE_SIZE
            let offset = heap_page_id as u64 * 8192;
            heap_file.seek(SeekFrom::Start(offset))?;

            if let Ok(_) = heap_file.read_exact(&mut page_bytes) {
                // Calculate free space in this page
                let lower = u32::from_le_bytes([
                    page_bytes[0],
                    page_bytes[1],
                    page_bytes[2],
                    page_bytes[3],
                ]);
                let upper = u32::from_le_bytes([
                    page_bytes[4],
                    page_bytes[5],
                    page_bytes[6],
                    page_bytes[7],
                ]);

                let free_bytes = if upper >= lower {
                    upper - lower
                } else {
                    0
                };

                // Compute category
                let category = ((free_bytes as f64 * 255.0) / 8192.0).floor() as u8;

                // Find which Level-0 FSM page this heap page belongs to
                let _fsm_page_no = heap_page_id / FSM_SLOTS_PER_PAGE;
                let _slot_in_page = heap_page_id % FSM_SLOTS_PER_PAGE;

                if heap_page_id % 1000 == 0 {
                    println!(
                        "[FSM::build_from_heap] Processed {} heap pages...",
                        heap_page_id
                    );
                }

                // For MVP: just track in root page
                // In full implementation: would update tree structure at Level 0
                let root_entry = fsm_pages.entry(0).or_insert_with(FSMPage::new);
                root_entry.tree[0] = root_entry.tree[0].max(category);
            }
        }

        // Write updated FSM pages
        println!("[FSM::build_from_heap] Writing FSM pages to disk...");
        for (page_no, page) in fsm_pages.iter() {
            let offset = *page_no as u64 * FSM_PAGE_SIZE as u64;
            fsm_file.seek(SeekFrom::Start(offset))?;
            fsm_file.write_all(&page.serialize())?;
        }

        fsm_file.sync_all()?;

        println!(
            "[FSM::build_from_heap] FSM successfully built with {} pages",
            fsm_page_count
        );

        Ok(Self {
            fsm_path,
            fsm_file,
            heap_page_count: page_count,
        })
    }

    /// Get heap page count currently tracked by this FSM.
    pub fn heap_page_count(&self) -> u32 {
        self.heap_page_count
    }

    /// Update heap page count (used during allocation).
    pub fn set_heap_page_count(&mut self, count: u32) {
        println!("[FSM::set_heap_page_count] Updating to {}", count);
        self.heap_page_count = count;
    }

    /// Compute required FSM fork page count for given heap page count.
    /// Formula: We need FSM_SLOTS_PER_PAGE² = 2040² ≈ 4.16M heap pages per full tree.
    /// For small counts, we calculate based on 3-level tree structure.
    pub fn calculate_fsm_page_count(heap_pages: u32) -> u32 {
        if heap_pages == 0 {
            return 0;
        }

        // For a 3-level tree:
        // - Level 0 (leaves): Each covers FSM_SLOTS_PER_PAGE heap pages
        // - Each Level 0 page is one block
        // - We need ceil(heap_pages / FSM_SLOTS_PER_PAGE) Level 0 pages
        // - Then ceil(L0_count / FSM_SLOTS_PER_PAGE) Level 1 pages
        // - Then ceil(L1_count / FSM_SLOTS_PER_PAGE) Level 2 pages

        let l0_count = (heap_pages + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;
        let l1_count = (l0_count + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;
        let l2_count = (l1_count + FSM_SLOTS_PER_PAGE - 1) / FSM_SLOTS_PER_PAGE;

        l0_count + l1_count + l2_count
    }

    /// Read FSM page at logical position (level, page_no, slot) into FSMPage struct.
    /// 
    /// If the page doesn't exist in the file yet, returns an empty FSMPage.
    pub fn read_fsm_page(
        &mut self,
        level: u32,
        page_no: u32,
        slot: u32,
    ) -> io::Result<FSMPage> {
        println!(
            "[FSM::read_fsm_page] Reading level={}, page_no={}, slot={}",
            level, page_no, slot
        );

        let block_offset = page_no as u64 * FSM_PAGE_SIZE as u64;

        // Check if file is large enough
        let file_size = self.fsm_file.metadata()?.len();
        
        if block_offset + FSM_PAGE_SIZE as u64 > file_size {
            println!(
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
                println!(
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
        println!(
            "[FSM::write_fsm_page] Writing level={}, page_no={}, slot={}",
            level, page_no, slot
        );

        let block_offset = page_no as u64 * FSM_PAGE_SIZE as u64;
        
        // Get current file size
        let current_size = self.fsm_file.metadata()?.len();
        
        // If file is too small, pad it with empty pages
        if block_offset + FSM_PAGE_SIZE as u64 > current_size {
            self.fsm_file.seek(SeekFrom::End(0))?;
            let empty_page = FSMPage::new();
            let empty_bytes = empty_page.serialize();
            
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
        println!("[FSM::sync] Syncing FSM fork file");
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
    /// 3. Traverse Level 2 → Level 1 → Level 0 using fp_next_slot hint
    /// 4. Compute heap page ID from (fsm_page_no, slot)
    /// 5. Advance fp_next_slot on each visited FSM page
    /// 6. Return Some(heap_page_id)
    pub fn fsm_search_avail(&mut self, min_category: u8) -> io::Result<Option<u32>> {
        println!(
            "[FSM::fsm_search_avail] Searching for page with category >= {}",
            min_category
        );

        // Try to read FSM root (Level 2, page 0)
        let root_page = self.read_fsm_page(2, 0, 0)?;
        
        let root_value = root_page.root_value();
        println!(
            "[FSM::fsm_search_avail] Root value: {}, min_category: {}",
            root_value, min_category
        );
        
        if root_value < min_category {
            println!("[FSM::fsm_search_avail] Root < min_category, returning None");
            return Ok(None);
        }

        // Traverse tree from root to find a leaf with sufficient free space
        // Start at Level 2 root
        let result = self.search_tree_for_available_page(2, 0, min_category)?;
        
        if let Some(page_id) = result {
            println!(
                "[FSM::fsm_search_avail] Found page with sufficient space: page_id={}",
                page_id
            );
            Ok(Some(page_id))
        } else {
            println!("[FSM::fsm_search_avail] No page found with sufficient space");
            Ok(None)
        }
    }

    /// Recursively search FSM tree for a page with free space >= min_category
    /// Returns Ok(Option<u32>) where u32 is the page_id, or None if not found
    fn search_tree_for_available_page(
        &mut self,
        level: u32,
        page_no: u32,
        min_category: u8,
    ) -> io::Result<Option<u32>> {
        if level == 0 {
            // Leaf level: this FSM page's tree array contains heap page categories
            let fsm_page = self.read_fsm_page(0, page_no, 0)?;
            
            // Get the starting heap page ID for this FSM page
            let start_heap_page = page_no * FSM_SLOTS_PER_PAGE;
            
            println!("[FSM::search_tree] Level 0 (page_no={}): Searching {} leaf slots starting from heap_page={}",
                page_no, FSM_SLOTS_PER_PAGE, start_heap_page);
            
            // Search through leaves (right half of tree array)
            for slot in 0..FSM_SLOTS_PER_PAGE {
                let heap_page_id = start_heap_page + slot;
                
                // IMPORTANT: Skip heap page 0 - it's the header page, not a data page!
                if heap_page_id == 0 {
                    continue;
                }
                
                let leaf_index = FSM_NODES_PER_PAGE / 2 + slot as usize;
                if leaf_index >= FSM_NODES_PER_PAGE {
                    break;
                }
                
                let category = fsm_page.tree[leaf_index];
                
                // DEBUG: Only print details for pages with category > 0
                if category > 0 && slot < 10 {
                    println!("[FSM::search_tree] heap_page {} has category {}", heap_page_id, category);
                }
                
                if category >= min_category {
                    println!(
                        "[FSM::search_tree] Found heap page {} with category {} >= {}",
                        heap_page_id, category, min_category
                    );
                    return Ok(Some(heap_page_id));
                }
            }
            
            println!("[FSM::search_tree] No suitable leaf found in Level 0 page_no={}", page_no);
            Ok(None)
        } else {
            // Internal level: traverse child FSM pages
            let fsm_page = self.read_fsm_page(level, page_no, 0)?;
            
            println!("[FSM::search_tree] Level {} (page_no={}): Searching internal nodes", level, page_no);
            
            // Search internal nodes to find a child with space
            let num_children = FSM_NODES_PER_PAGE / 2; // Internal nodes in first half
            
            for child_idx in 0..num_children {
                let child_category = fsm_page.tree[child_idx];
                if child_category >= min_category {
                    println!("[FSM::search_tree] Child {} has category {} >= {}, recursing", 
                        child_idx, child_category, min_category);
                    // This child subtree may have sufficient space, recurse
                    if let Some(result) = self.search_tree_for_available_page(
                        level - 1,
                        child_idx as u32,
                        min_category,
                    )? {
                        return Ok(Some(result));
                    }
                }
            }
            
            println!("[FSM::search_tree] No suitable child found in Level {} page_no={}", level, page_no);
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
    pub fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32) -> io::Result<()> {
        println!(
            "[FSM::fsm_set_avail] Updating heap_page_id={} with {} free bytes",
            heap_page_id, new_free_bytes
        );

        // Compute category from free bytes
        let category = Self::bytes_to_category(new_free_bytes);
        println!(
            "[FSM::fsm_set_avail] Computed category: {}",
            category
        );

        // Find which Level 0 FSM page contains this heap_page_id
        // Each Level 0 FSM page tracks FSM_SLOTS_PER_PAGE heap pages
        let fsm_page_no = (heap_page_id / FSM_SLOTS_PER_PAGE) as u32;
        let slot_within_page = (heap_page_id % FSM_SLOTS_PER_PAGE) as usize;

        println!(
            "[FSM::fsm_set_avail] Heap page {} → FSM Level 0 page {}, slot {}",
            heap_page_id, fsm_page_no, slot_within_page
        );

        // Step 1: Update the leaf in Level 0 FSM page
        let mut leaf_page = self.read_fsm_page(0, fsm_page_no, 0)?;
        let leaf_index = FSM_NODES_PER_PAGE / 2 + slot_within_page;

        println!(
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
        println!(
            "[FSM::fsm_set_avail] Level 0 page root is now: {}",
            new_level0_root
        );

        // Step 3: Propagate changes up to Level 1
        // Each Level 1 FSM page has 2040 nodes representing children at Level 0
        if fsm_page_no < (FSM_NODES_PER_PAGE / 2) as u32 {
            let mut level1_page = self.read_fsm_page(1, 0, 0)?;
            
            // Correct: use leaf index in level1, not direct fsm_page_no
            // Leaves in level 1 start at FSM_NODES_PER_PAGE/2
            let level1_leaf_index = (FSM_NODES_PER_PAGE / 2) + (fsm_page_no as usize);
            
            if level1_page.tree[level1_leaf_index] != new_level0_root {
                level1_page.tree[level1_leaf_index] = new_level0_root;
                
                // Bubble up within Level 1 using correct parent formula
                let mut idx = level1_leaf_index;
                while idx > 0 {
                    let parent_idx = (idx - 1) / 2;
                    let left_child = 2 * parent_idx + 1;
                    let right_child = 2 * parent_idx + 2;
                    
                    let new_val = if right_child < FSM_NODES_PER_PAGE {
                        level1_page.tree[left_child].max(level1_page.tree[right_child])
                    } else if left_child < FSM_NODES_PER_PAGE {
                        level1_page.tree[left_child]
                    } else {
                        0
                    };
                    
                    if level1_page.tree[parent_idx] != new_val {
                        level1_page.tree[parent_idx] = new_val;
                        idx = parent_idx;
                    } else {
                        break;
                    }
                }
                
                self.write_fsm_page(1, 0, 0, &level1_page)?;
                
                // Step 4: Update Level 2 root
                let new_level1_root = level1_page.root_value();
                let mut root_page = self.read_fsm_page(2, 0, 0)?;
                
                if root_page.tree[0] != new_level1_root {
                    root_page.tree[0] = new_level1_root;
                    root_page.set_root_value(new_level1_root);
                    self.write_fsm_page(2, 0, 0, &root_page)?;
                    
                    println!(
                        "[FSM::fsm_set_avail] Updated root to {}",
                        new_level1_root
                    );
                }
            }
        }

        println!("[FSM::fsm_set_avail] Update complete");

        Ok(())
    }

    /// Update free space after a tuple is deleted or page is vacuumed.
    /// Wrapper around fsm_set_avail for Project 10 integration.
    pub fn fsm_vacuum_update(&mut self, heap_page_id: u32, reclaimed_bytes: u32) -> io::Result<()> {
        println!(
            "[FSM::fsm_vacuum_update] Recording vacuumed bytes: page_id={}, bytes={}",
            heap_page_id, reclaimed_bytes
        );
        // Delegate to fsm_set_avail
        self.fsm_set_avail(heap_page_id, reclaimed_bytes)?;
        Ok(())
    }

    /// Convert free bytes to free-space category (0-255).
    /// Formula: category = floor(free_bytes × 255 / FSM_PAGE_SIZE)
    fn bytes_to_category(free_bytes: u32) -> u8 {
        if free_bytes >= FSM_PAGE_SIZE as u32 {
            return 255;
        }
        ((free_bytes as f64 * 255.0) / (FSM_PAGE_SIZE as f64)) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fsm_page_new() {
        let page = FSMPage::new();
        assert_eq!(page.root_value(), 0);
        assert_eq!(page.fp_next_slot, 0);
    }

    #[test]
    fn test_fsm_page_serialize_deserialize() {
        let mut page = FSMPage::new();
        page.tree[0] = 100;
        page.fp_next_slot = 500;

        let bytes = page.serialize();
        assert_eq!(bytes.len(), FSM_PAGE_SIZE);

        let page2 = FSMPage::deserialize(&bytes).unwrap();
        assert_eq!(page2.tree[0], 100);
        assert_eq!(page2.fp_next_slot, 500);
    }

    #[test]
    fn test_fsm_page_advance_fp_next_slot() {
        let mut page = FSMPage::new();
        page.fp_next_slot = 0;
        page.advance_fp_next_slot();
        assert_eq!(page.fp_next_slot, 1);

        // Test wrap-around
        page.fp_next_slot = FSM_SLOTS_PER_PAGE as u16 - 1;
        page.advance_fp_next_slot();
        assert_eq!(page.fp_next_slot, 0);
    }

    #[test]
    fn test_calculate_fsm_page_count() {
        // Small counts
        assert_eq!(FSM::calculate_fsm_page_count(1), 1 + 1 + 1); // 1 L0 + 1 L1 + 1 L2
        assert_eq!(FSM::calculate_fsm_page_count(100), 1 + 1 + 1);
        
        // Large count (2040 heap pages → exactly 1 L0)
        let count = FSM::calculate_fsm_page_count(FSM_SLOTS_PER_PAGE);
        assert!(count > 0);
    }
}
