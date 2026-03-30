//! In-memory FSM (Free Space Map) manager.
//!
//! Wraps the per-file free-space state so that `heap/mod.rs` and `executor/delete.rs`
//! can call plain free functions (`fsm_set_avail`, `fsm_search_avail`) without holding
//! a reference to a disk-backed FSM struct.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

type FileIdentity = u64;
type PageId = u32;

const FSM_NODES_PER_PAGE: usize = 4080;
const FSM_SLOTS_PER_PAGE: u32 = 2040;
const FSM_PAGE_SIZE: u32 = 8192;

// ─────────────────────────────────────────────────────────────────────────
// In-memory page (not the disk-backed FSMPage from the fsm crate)
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct MemFSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],
    fp_next_slot: u16,
}

impl Default for MemFSMPage {
    fn default() -> Self {
        Self {
            tree: [0u8; FSM_NODES_PER_PAGE],
            fp_next_slot: 0,
        }
    }
}

impl MemFSMPage {
    fn root_value(&self) -> u8 {
        self.tree[0]
    }

    fn set_root_value(&mut self, value: u8) {
        self.tree[0] = value;
    }

    fn advance_fp_next_slot(&mut self) {
        self.fp_next_slot = (self.fp_next_slot + 1) % (FSM_SLOTS_PER_PAGE as u16);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Per-file in-memory FSM state
// ─────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct InMemoryFSMFile {
    pages: HashMap<u32, MemFSMPage>,
}

impl InMemoryFSMFile {
    fn read_fsm_page(&mut self, _level: u32, page_no: u32, _slot: u32) -> MemFSMPage {
        self.pages.get(&page_no).cloned().unwrap_or_default()
    }

    fn write_fsm_page(&mut self, _level: u32, page_no: u32, _slot: u32, page: &MemFSMPage) {
        self.pages.insert(page_no, page.clone());
    }

    fn search_tree_for_available_page(
        &mut self,
        level: u32,
        page_no: u32,
        min_category: u8,
    ) -> Option<PageId> {
        if level == 0 {
            let fsm_page = self.read_fsm_page(0, page_no, 0);
            let start_heap_page = page_no * FSM_SLOTS_PER_PAGE;

            for slot in 0..FSM_SLOTS_PER_PAGE {
                let heap_page_id = start_heap_page + slot;
                if heap_page_id == 0 {
                    continue;
                }

                let leaf_index = FSM_NODES_PER_PAGE / 2 + slot as usize;
                if leaf_index >= FSM_NODES_PER_PAGE {
                    break;
                }

                let category = fsm_page.tree[leaf_index];
                if category >= min_category {
                    return Some(heap_page_id);
                }
            }

            None
        } else {
            let mut fsm_page = self.read_fsm_page(level, page_no, 0);
            let num_children = FSM_NODES_PER_PAGE / 2;

            let start_child = fsm_page.fp_next_slot as usize;
            for step in 0..num_children {
                let child_idx = (start_child + step) % num_children;
                let child_category = fsm_page.tree[child_idx];
                if child_category >= min_category {
                    fsm_page.fp_next_slot = child_idx as u16;
                    fsm_page.advance_fp_next_slot();
                    self.write_fsm_page(level, page_no, 0, &fsm_page);

                    if let Some(result) = self.search_tree_for_available_page(
                        level - 1,
                        child_idx as u32,
                        min_category,
                    ) {
                        return Some(result);
                    }
                }
            }

            None
        }
    }

    fn fsm_search_avail(&mut self, min_category: u8) -> Option<PageId> {
        let root_page = self.read_fsm_page(2, 0, 0);
        let root_value = root_page.root_value();
        if root_value < min_category {
            return None;
        }

        self.search_tree_for_available_page(2, 0, min_category)
    }

    fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32) {
        if heap_page_id == 0 {
            return;
        }

        let category = bytes_to_category(new_free_bytes);

        let fsm_page_no = heap_page_id / FSM_SLOTS_PER_PAGE;
        let slot_within_page = (heap_page_id % FSM_SLOTS_PER_PAGE) as usize;

        let mut leaf_page = self.read_fsm_page(0, fsm_page_no, 0);
        let leaf_index = FSM_NODES_PER_PAGE / 2 + slot_within_page;
        if leaf_index >= FSM_NODES_PER_PAGE {
            return;
        }

        leaf_page.tree[leaf_index] = category;
        bubble_up_max_tree(&mut leaf_page, leaf_index);
        self.write_fsm_page(0, fsm_page_no, 0, &leaf_page);

        let new_level0_root = leaf_page.root_value();
        if fsm_page_no < (FSM_NODES_PER_PAGE / 2) as u32 {
            let mut level1_page = self.read_fsm_page(1, 0, 0);
            let level1_leaf_index = (FSM_NODES_PER_PAGE / 2) + (fsm_page_no as usize);

            if level1_leaf_index < FSM_NODES_PER_PAGE
                && level1_page.tree[level1_leaf_index] != new_level0_root
            {
                level1_page.tree[level1_leaf_index] = new_level0_root;
                bubble_up_max_tree(&mut level1_page, level1_leaf_index);
                self.write_fsm_page(1, 0, 0, &level1_page);

                let new_level1_root = level1_page.root_value();
                let mut root_page = self.read_fsm_page(2, 0, 0);
                if root_page.tree[0] != new_level1_root {
                    root_page.tree[0] = new_level1_root;
                    root_page.set_root_value(new_level1_root);
                    self.write_fsm_page(2, 0, 0, &root_page);
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Global singleton
// ─────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct InMemoryFSM {
    by_file: HashMap<FileIdentity, InMemoryFSMFile>,
}

static IN_MEMORY_FSM: OnceLock<Mutex<InMemoryFSM>> = OnceLock::new();

fn global_fsm() -> &'static Mutex<InMemoryFSM> {
    IN_MEMORY_FSM.get_or_init(|| Mutex::new(InMemoryFSM::default()))
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

fn bytes_to_category(free_bytes: u32) -> u8 {
    if free_bytes >= FSM_PAGE_SIZE {
        return 255;
    }
    ((free_bytes as f64 * 255.0) / (FSM_PAGE_SIZE as f64)) as u8
}

fn bubble_up_max_tree(page: &mut MemFSMPage, start_index: usize) {
    let mut idx = start_index;
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
            break;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────

/// Search for a heap page with at least `required` free bytes.
/// Returns the page id or `None` if no page has enough space.
pub fn fsm_search_avail(file_identity: FileIdentity, required: u32) -> Option<PageId> {
    let min_category = bytes_to_category(required);
    let mut guard = global_fsm().lock().ok()?;
    let file_fsm = guard.by_file.get_mut(&file_identity)?;
    file_fsm.fsm_search_avail(min_category)
}

/// Record that heap page `page_id` in file `file_identity` now has `free_space` free bytes.
pub fn fsm_set_avail(file_identity: FileIdentity, page_id: PageId, free_space: u32) {
    if let Ok(mut guard) = global_fsm().lock() {
        let file_fsm = guard.by_file.entry(file_identity).or_default();
        file_fsm.fsm_set_avail(page_id, free_space);
    }
}

/// Remove all FSM state for a given file (e.g. after the file is deleted or in tests).
pub fn fsm_clear_file(file_identity: FileIdentity) {
    if let Ok(mut guard) = global_fsm().lock() {
        guard.by_file.remove(&file_identity);
    }
}
