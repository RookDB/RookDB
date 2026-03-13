# RookDB: Solution Design Phase - Free Space Manager and Heap File Manager

**Project:** 6. Free Space Manager and Heap File Manager

**Date:** 13th March, 2026

---

## PROJECT SCOPE

Project 6 and Project 10 have clearly separated responsibilities based on operational granularity. Project 6 handles page-level operations: tuple insertion (using the FSM binary max-tree search algorithm), tuple retrieval by coordinates (page_id, slot_id), and sequential scans across all pages. The Free Space Map tracks available space at page granularity using a PostgreSQL-style 3-level tree of FSM pages stored in a separate fork file.

Project 10 will handle tuple-level operations including deletion, updates, in-page compaction, slot reorganization, and tuple relocation. These are explicitly out of scope for our implementation, which operates exclusively at the page level tracking which pages have space, not managing individual tuple lifecycles within pages.

---

## 1. Database and Architecture Design Changes

### 1.1 Current Architecture

**Existing Structure:**

- Page 0: Header with 4-byte page count only
- Pages 1+: Slotted pages (8-byte header: lower/upper offsets)
- Insertion: Append-only, checks last page only
- **Problem:** No free space tracking in earlier pages

### 1.2 Proposed Changes

#### Insertion Strategy Update

- Replace append-only insertion with FSM tree-based page selection
- Every insert calls `FSM::fsm_search_avail(min_category)` to find a page with sufficient free-space category
- Fallback: if root value < required category, extend the relation with a new heap page and update the FSM
- Result: existing free space is reused before file growth; load is spread across pages via `fp_next_slot`

#### Enhanced Header Page (Page 0)

```
Offset | Size | Field              | Purpose
-------|------|--------------------|---------------------------------
0-4    | 4    | Page Count         | Total heap pages in file
4-7    | 4    | FSM Page Count     | Total pages in FSM fork file
8-11   | 4    | Total Tuples (Low) | Tuple count lower 32 bits
12-15  | 4    | Total Tuples (High)| Tuple count upper 32 bits
16-19  | 4    | Last Vacuum        | Timestamp
20+    | ...  | Reserved           | Future use
```

**Justification:**

- **FSM Page Count:** Allows FSM fork to be sized and located without scanning
- **Tuple Count:** O(1) COUNT(\*) queries
- **Persistence:** Survives crashes; FSM fork is treated as a hint and can be fully rebuilt from heap data

#### Target Page Algorithm — FSM Binary Max-Tree Search

**Objective:** Find a page with sufficient free space by traversing a 3-level binary max-tree (one byte per heap page, 0–255 scale), using a per-FSM-page `fp_next_slot` hint to spread insertions.

**Free Space Quantization:**

Exact byte counts are not stored. Each heap page maps to one `u8` category:

```
category = floor(free_bytes × 255 / PAGE_SIZE)   // 0 = full, 255 = completely empty
```

For an 8 KB page this gives ~32 bytes of resolution per category step.

**Logic:**

```
FindPage(min_category):
    1. Read root FSM page (Level 2).
       If root value < min_category: RETURN None (no page has enough space).
    2. Traverse Level 2 → Level 1 → Level 0:
       - At each internal node pick a child whose value >= min_category,
         preferring the child indicated by fp_next_slot (load-spreading).
    3. Reach Level-0 leaf: compute heap PageID from (fsm_page_no, slot_no).
    4. Advance fp_next_slot on each visited FSM page.
    5. RETURN heap PageID.
    6. If no candidate found: extend relation, call fsm_set_avail for new page, return it.
```

**Example (single-level tree with 4 leaf slots):**

```
    4             ← root (max of all children)
  4     2
 3 4   0 2       ← leaf nodes (one per heap page)
```

- Need: category ≥ 2
- Root = 4 ≥ 2 → traverse; fp_next_slot points right subtree (value = 2)
- Descend right → pick leaf with value 2
- **Result:** heap page mapped to that leaf slot is returned

**Why Appropriate:**

- **Scalability:** O(log N) search; O(1) rejection via root when table is full
- **Load spreading:** `fp_next_slot` steers successive inserts to different pages, reducing contention
- **Memory efficiency:** 1 byte/page overhead (e.g., 1 MB FSM covers an 8 GB table)
- **Self-correcting:** corrupted parent nodes are rebuilt in-place during traversal; no WAL logging required

#### FSM Fork File

**Mechanism:** A dedicated sidecar file `<table>.fsm` holds all FSM pages as a flat sequence. The 3-level tree maps logical `(level, page_no, slot)` addresses to physical block numbers:

```
Physical block = page_no + (page_no / F) + (page_no / F²) + 1
  where F = FSM_SLOTS_PER_PAGE

Layout (simplified with F=4):
  Block 0:   Level-2 root FSM page
  Block 1:   Level-1 FSM page 0     (covers heap pages 0-15)
    Block 2:   Level-0 FSM page 0   (covers heap pages 0-3)
    Block 3:   Level-0 FSM page 1   (covers heap pages 4-7)
    Block 4:   Level-0 FSM page 2   (covers heap pages 8-11)
    Block 5:   Level-0 FSM page 3   (covers heap pages 12-15)
  Block 6:   Level-1 FSM page 1     (covers heap pages 16-31)
    ...
```

**FSM page constants (8 KB page):**

- `FSM_NODES_PER_PAGE: usize = 4080` — bytes in the binary max-tree array
- `FSM_SLOTS_PER_PAGE: u32 = 2040` — usable leaf slots per Level-0 FSM page
- `FSM_LEVELS: u32 = 3` — Level 0 = leaves, Level 2 = root

**Benefits:**

- No intrusive overhead written to heap pages
- Treated as a hint: can be rebuilt from the heap after a crash without WAL
- Constant-height 3-level tree covers up to 2040² ≈ 4 M heap pages (~32 GB at 8 KB/page)

#### Integration

**Buffer/Disk Manager:** Use existing read_page/write_page, add update_header_page helper

**Page Structure:** No changes; use existing page_free_space() and slotted page layout

**Catalog:** No changes for MVP

---

## 2. Backend Data Structures

### 2.1 Data Structures to be Created

#### `FSM` - Free Space Map

**Location:** `src/backend/fsm/fsm.rs`

```rust
// ── FSM layout constants ──────────────────────────────────────────────────
const FSM_NODES_PER_PAGE: usize = 4080; // binary max-tree array (bytes)
const FSM_SLOTS_PER_PAGE: u32  = 2040; // usable leaf nodes per FSM page
const FSM_LEVELS: u32          = 3;    // Level 0 = leaves, Level 2 = root

/// One disk page in the FSM fork.
/// Stores a binary max-tree; leaf nodes hold free-space categories (0–255).
pub struct FSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],  // index 0 = root; leaves in right half
    fp_next_slot: u16,               // hint: start slot for next search
}

/// In-memory handle for the entire 3-level FSM fork file.
pub struct FSM {
    fsm_path: PathBuf,       // path to <table>.fsm sidecar file
    fsm_file: File,          // open file handle on the FSM fork
    heap_page_count: u32,    // number of heap pages currently tracked
}
```

**Purpose:** Model the PostgreSQL-style FSM fork — a 3-level tree of `FSMPage` values, each containing a binary max-tree of 1-byte free-space categories covering all heap pages.
**Justification:** O(log N) tree search; 1 byte/page memory overhead; `fp_next_slot` spreads concurrent inserts; self-correcting without WAL logging.

#### `HeaderMetadata`

**Location:** `src/backend/heap/types.rs`

```rust
#[derive(Debug, Clone, Copy)]
pub struct HeaderMetadata {
    pub page_count: u32,      // total heap pages
    pub fsm_page_count: u32,  // total pages in FSM fork file
    pub total_tuples: u64,
    pub last_vacuum: u32,
}
```

**Purpose:** Type-safe Page 0 representation
**Justification:** Structured access to file metadata; 64-bit tuple count for scalability

#### `HeapManager`

**Location:** `src/backend/heap/heap_manager.rs`

```rust
pub struct HeapManager {
    file_path: PathBuf,
    file_handle: File,
    fsm: FSM,
}
```

**Purpose:** High-level API for table operations
**Justification:** Encapsulates FSM logic; keeps file open for performance

#### `HeapScanIterator`

**Location:** `src/backend/heap/heap_manager.rs`

```rust
pub struct HeapScanIterator {
    file_path: PathBuf,
    current_page: u32,
    current_slot: u32,
    total_pages: u32,
    cached_page: Option<Page>,
}

impl Iterator for HeapScanIterator {
    type Item = io::Result<(u32, u32, Vec<u8>)>;
}
```

**Purpose:** Memory-efficient sequential scan
**Justification:** Rust iterator trait; pages lazy-loaded (8KB vs 100GB table)

### 2.2 Data Structures to be Modified

#### `Page` - Add Helper Functions

**Location:** `src/backend/page/mod.rs`

**No struct changes**, add methods:

```rust
pub fn get_tuple_count(page: &Page) -> io::Result<u32>
pub fn get_slot_entry(page: &Page, slot_id: u32) -> io::Result<(u32, u32)>
```

**Purpose:** Centralized slot parsing for get_tuple/scan
**Justification:** Eliminates code duplication; maintains module encapsulation

#### `DiskManager` - Add Header Helper

**Location:** `src/backend/disk/disk_manager.rs`

```rust
pub fn update_header_page(file: &File, header: &HeaderMetadata) -> io::Result<()>
```

**Purpose:** Dedicated header persistence
**Justification:** Separates concern; atomic header updates

---

## 3. Backend Functions

### 3.1 Functions to be Created

#### `FSM::build_from_heap`

```rust
pub fn build_from_heap(heap_path: PathBuf) -> io::Result<Self>
```

**Description:** Construct or rebuild the FSM fork by scanning all heap pages and computing free-space categories.  
**Inputs:** Heap file path (FSM path derived as `<heap_path>.fsm`)  
**Outputs:** Initialized `FSM` handle or error  
**Steps:**

1. Open heap file; read Page 0 for `HeaderMetadata` (page_count, fsm_page_count)
2. Open (or create) `<table>.fsm` fork file
3. For each heap page 1..page_count:
   a. Read page, compute `category = floor(free_bytes × 255 / PAGE_SIZE)`
   b. Locate the corresponding Level-0 FSM page and leaf slot
   c. Write category into the leaf node of that FSM page
4. Bubble up within every modified Level-0 FSM page (recompute parents as max of children)
5. Propagate max root values up through Level-1 and Level-2 FSM pages
6. Persist all modified FSM pages to disk
7. Update `fsm_page_count` in header; return `FSM` struct

#### `FSM::fsm_search_avail` (Tree Search)

```rust
pub fn fsm_search_avail(&mut self, min_category: u8) -> io::Result<Option<u32>>
```

**Description:** Traverse the 3-level FSM tree to find a heap page whose free-space category is at least `min_category`, using `fp_next_slot` on each FSM page to spread load.  
**Inputs:** `min_category` — minimum required free-space category (0–255)  
**Outputs:** heap `PageID` or `None` (root value < min_category means no eligible page)  
**Steps:**

1. Read root FSM page (Level 2). If `root.tree[0] < min_category`: return `None`.
2. Starting at Level 2, descend to Level 1 then Level 0:
   - At each internal node, choose a child whose value >= `min_category`.
   - Prefer the child at `fp_next_slot`; fall back to the other child.
3. At the Level-0 leaf, read the slot index to compute: `heap_page_id = fsm_page_no × FSM_SLOTS_PER_PAGE + slot`.
4. Advance `fp_next_slot` on each visited FSM page (wrapping at last slot); mark pages dirty.
5. Return `Some(heap_page_id)`.

**Implementation sketch:**

```rust
let root_page = self.read_fsm_page(fsm_block_for(2, 0))?;
if root_page.tree[0] < min_category {
    return Ok(None);
}
// descend level 2 → 1 → 0 following fp_next_slot hint ...
let heap_page_id = leaf_fsm_page * FSM_SLOTS_PER_PAGE + leaf_slot;
Ok(Some(heap_page_id))
```

#### `FSM::fsm_set_avail` (Leaf Update + Bubble-Up)

```rust
pub fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32) -> io::Result<()>
```

**Description:** Update the free-space category for one heap page, then bubble the change up through all ancestor nodes within the FSM page, and propagate updated root values up through Level-1 and Level-2 FSM pages.  
**Inputs:** `heap_page_id`, `new_free_bytes`  
**Steps:**

1. Compute `category = floor(new_free_bytes × 255 / PAGE_SIZE)`.
2. Locate Level-0 FSM page and leaf slot: `(fsm_page_no, slot) = divmod(heap_page_id, FSM_SLOTS_PER_PAGE)`.
3. Read the Level-0 FSM page from disk.
4. Set `tree[leaf_index(slot)] = category`.
5. Walk up the binary tree: for each parent, recompute as `max(left_child, right_child)`. Stop when the parent value does not change (or root is reached).
6. Write the updated Level-0 FSM page.
7. If the root value of this Level-0 page changed, recursively call `fsm_set_avail` on the corresponding Level-1 page (passing `fsm_page_no` as the "heap page" and the new root as category), and so on up to Level 2.
8. Mark all modified FSM pages dirty (treated as hints — no WAL entry written).

#### `FSM::fsm_vacuum_update`

```rust
pub fn fsm_vacuum_update(&mut self, heap_page_id: u32, reclaimed_bytes: u32) -> io::Result<()>
```

**Description:** Called by Project 10 (VACUUM / compaction) when a heap page gains free space. Converts the reclaimed bytes to a category and calls `fsm_set_avail` so the page becomes searchable again.  
**Inputs:** `heap_page_id`, `reclaimed_bytes` (may be the full page free space if the page became empty)  
**Steps:**

1. Delegate to `self.fsm_set_avail(heap_page_id, reclaimed_bytes)`
2. If the Level-0 root value increased, propagate up (handled inside `fsm_set_avail`).

**Note:** There is no separate "free list" or page header modification. An empty page (category = 255) is simply searchable at the top of the binary max-tree. Recovery after a crash is handled by rebuilding from the heap via `build_from_heap`; no WAL entry is written.

#### `HeapManager::open`

```rust
pub fn open(file_path: PathBuf) -> io::Result<Self>
```

**Steps:**

1. Verify heap file exists
2. Open heap file handle
3. Call `FSM::build_from_heap(file_path.clone())` to open (or rebuild) the FSM fork
4. Return `HeapManager { file_path, file_handle, fsm }`

#### `HeapManager::insert_tuple`

```rust
pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> io::Result<(u32, u32)>
```

**Description:** Insert tuple using the FSM binary max-tree to locate a suitable page.  
**Outputs:** (page_id, slot_id)  
**Steps:**

1. Calculate `min_category = ceil((tuple_size + 8) × 255 / PAGE_SIZE)`
2. Call `fsm.fsm_search_avail(min_category)` to obtain a candidate `page_id`
3. If `None` returned (root < min_category): call `allocate_new_page()` to extend the heap
4. Read the chosen heap page, verify actual free space
5. Insert tuple into slotted page (update lower/upper offsets)
6. Compute updated `new_category` and call `fsm.fsm_set_avail(page_id, actual_free_after)`
7. Increment `total_tuples` in header
8. Return (page_id, slot_id)

#### `HeapManager::get_tuple`

```rust
pub fn get_tuple(&self, page_id: u32, slot_id: u32) -> io::Result<Vec<u8>>
```

**Steps:**

1. Validate page_id < page_count
2. Read page
3. Validate slot_id < tuple_count
4. Read slot entry (offset, length)
5. Extract and return tuple data

#### `HeapManager::scan`

```rust
pub fn scan(&self) -> HeapScanIterator
```

**Steps:**

1. Create iterator with current_page=1, current_slot=0
2. Return iterator

**Iterator next() logic:**

1. If current_page >= total_pages: return None
2. Load page if not cached
3. If current_slot >= tuple_count: move to next page
4. Extract tuple, increment slot
5. Return (page_id, slot_id, data)

#### `HeapManager::allocate_new_page`

```rust
fn allocate_new_page(&mut self) -> io::Result<u32>
```

**Steps:**

1. `new_page_id = header.page_count`
2. Create and initialize an empty slotted page (full free space = PAGE_SIZE − 8)
3. Append page to heap file
4. Increment `header.page_count`; extend FSM fork if needed (`header.fsm_page_count` may grow)
5. Call `fsm.fsm_set_avail(new_page_id, PAGE_SIZE - 8)` to register the new page with category 255
6. Return `new_page_id`

#### `HeapManager::flush`

```rust
pub fn flush(&mut self) -> io::Result<()>
```

**Steps:**

1. Persist header using update_header_page
2. file.sync_all()

### 3.2 Functions to be Modified

#### `heap/mod.rs::init_table`

**Change:** Use HeaderMetadata instead of raw bytes  
**Updated Steps:**

1. Create file
2. Create Page 0, write HeaderMetadata::new()
3. Create Page 1, init as slotted page
4. Write both pages

#### `heap/mod.rs::insert_tuple` (Legacy)

**Change:** Mark as deprecated

```rust
#[deprecated(note = "Use HeapManager::insert_tuple")]
pub fn insert_tuple(...) { ... }
```

---

## 4. Frontend Changes (CLI Inputs)

### Existing Commands

All work transparently with FSM (no user-visible changes):

- `CREATE TABLE` - Uses modified `init_table()`
- `LOAD CSV` - Uses `HeapManager::insert_tuple()`
- `SELECT *` - Uses `HeapManager::scan()`

### New Command: `CHECK_HEAP`

**Syntax:** `CHECK_HEAP <table_name>`

**Purpose:** Display FSM statistics and health metrics

**Example Output:**

```
=== Heap Info: users ===
Total Heap Pages:    10,240
FSM Fork Pages:      8  (3-level tree: 1 root + 3 L1 + 4 L0)
FSM Coverage:        10,240 heap pages tracked
Total Tuples:        1,234,567
FSM Root Value:      182/255  (pages with ~3.1 KB free space exist)
Avg Free Category:   94/255   (~2 KB average free per tracked page)
```

**Implementation:**

```rust
fn check_heap_command(table_name: &str) -> io::Result<()> {
    let heap = HeapManager::open(table_path)?;
    // Print statistics from heap.fsm
}
```

---

## 5. Overall Component Workflow (End-to-End View)

### Workflow 1: Tuple Insertion (FSM Binary Max-Tree)

```
User: INSERT INTO users VALUES (...)
           ↓
Frontend: Parse SQL → serialize tuple (50 bytes)
           ↓
Executor: Call HeapManager::insert_tuple(&[u8])
           ↓
HeapManager: min_category = ceil(58 × 255 / 8192) = 2
           ↓
    ┌──────────────────────────────────────┐
    │ fsm_search_avail(2)                  │  FSM Binary Max-Tree
    │  1. Read root FSM page (Level 2)     │
    │  2. root value ≥ 2? → YES, descend  │
    │  3. L2 → L1 → L0, follow fp_next_slot│
    │  4. Reach leaf → return heap PageID  │
    └──────────────┬───────────────────────┘
                   │
    Found? ├─ YES → Page 47
           │         (fp_next_slot advanced on all visited FSM pages)
           │
           └─ NO (root < 2) → allocate_new_page()
                              → fsm_set_avail(new_page, 8184)
           ↓
Read Heap Page from Disk
           ↓
Insert into Slotted Page (update lower/upper offsets)
           ↓
fsm_set_avail(page_id, actual_free_after)
  → update leaf category, bubble up to root
           ↓
Write Heap Page to Disk
           ↓
Return (page_id, slot_id)
```

**Key Point:** Insertion is **not append-only**; `fsm_search_avail` traverses the binary max-tree using `fp_next_slot` to spread load, and the root provides O(1) early-exit when no page has sufficient space

### Workflow 2: Tuple Retrieval

```
Executor: get_tuple(page=5, slot=3)
           ↓
HeapManager: Validate page < page_count
           ↓
Read Page 5 from Disk
           ↓
Get tuple_count from page header
           ↓
Validate slot < tuple_count
           ↓
Read slot entry at offset 8+(3*8)=32
  → (offset=7800, length=50)
           ↓
Extract page.data[7800..7850]
           ↓
Return tuple bytes
```

**No FSM involvement** - direct O(1) access

### Workflow 3: Sequential Scan

```
User: SELECT * FROM users
           ↓
Executor: heap.scan() → HeapScanIterator
           ↓
Iterator Loop:
  For page in 1..page_count:
    Load page (cache it)
    For slot in 0..tuple_count:
      Extract tuple
      YIELD (page, slot, data)
           ↓
Process all tuples
```

**Optimization:** Each page read once, yielding all its tuples

### Workflow 4: Project 10 Integration (FSM Update After Compaction)

```
Project 10: Compacts page 45 → 8,176 bytes now free
           ↓
Project 10: Calls fsm.fsm_vacuum_update(45, 8176)
           ↓
fsm_vacuum_update delegates to:
  fsm_set_avail(45, 8176)
           ↓
category = floor(8176 × 255 / 8192) = 254
           ↓
Locate Level-0 FSM page for heap page 45
  leaf_slot = 45 mod FSM_SLOTS_PER_PAGE
           ↓
Write category 254 into tree[leaf_index(leaf_slot)]
           ↓
Bubble up: recompute parent nodes as max(left, right)
  → root of Level-0 page may increase
           ↓
If Level-0 root changed:
  propagate new root value up to Level-1, then Level-2
           ↓
FSM tree now reflects page 45 as nearly empty (category 254)
Next fsm_search_avail call will find page 45
```

**Critical:** We never decide to compact — only receive `fsm_vacuum_update` notifications. No heap page bytes are modified; all changes are confined to the FSM fork file.

### Component Interaction

```
┌──────────┐
│   CLI    │
└────┬─────┘
     ↓
┌──────────┐
│ Executor │
└────┬─────┘
     ↓
┌─────────────┐
│ HeapManager │───owns──→ FSM
└────┬────────┘            ↓
     ↓                HeaderMetadata
┌─────────────┐
│ DiskManager │
└────┬────────┘
     ↓
 .dat File
```

---

## 6. Codebase Structure Changes

### New Files

```
src/backend/
├── fsm/
│   ├── mod.rs                    # Module declaration (5 lines)
│   └── fsm.rs                    # FSMPage + FSM implementation (750 lines)
└── heap/
    ├── heap_manager.rs           # HeapManager + HeapScanIterator (800 lines)
    └── types.rs                  # HeaderMetadata struct (150 lines)
```

**fsm/fsm.rs** (750 lines)

- Constants: `FSM_NODES_PER_PAGE`, `FSM_SLOTS_PER_PAGE`, `FSM_LEVELS`
- `FSMPage` struct: binary max-tree array, `fp_next_slot`
- `FSM` struct: `build_from_heap`, `fsm_search_avail`, `fsm_set_avail`, `fsm_vacuum_update`
- Private helpers: `read_fsm_page`, `write_fsm_page`, `fsm_block_for`, `leaf_index`, `bubble_up`

**heap/heap_manager.rs** (800 lines)

- HeapManager struct, open, insert_tuple, get_tuple, scan, allocate_new_page, flush
- HeapScanIterator implementation

**heap/types.rs** (150 lines)

- HeaderMetadata struct, from_page, write_to_page

### Modified Files

**backend/mod.rs** (+1 line)

```rust
pub mod fsm;
```

**backend/heap/mod.rs** (+5 lines)

```rust
pub mod heap_manager;
pub mod types;
pub use heap_manager::HeapManager;
pub use types::HeaderMetadata;
```

**backend/page/mod.rs** (+60 lines)

- Add `get_tuple_count()`, `get_slot_entry()` helper functions

**backend/disk/disk_manager.rs** (+8 lines)

- Add `update_header_page()` function

### Summary

- **New files:** 4 files (~1,705 lines)
- **Modified files:** 4 files (+74 lines)
- **Total:** ~1,779 lines of code
- **Sidecar files generated at runtime:** `<table>.fsm` (FSM fork, 1 byte/heap page overhead)

---

## 7. Test Cases

### 7.1 Testing Strategy

**Approach:** Integration tests using Rust's `tests/` directory. Each test creates isolated table files to prevent interference.

**Test Framework:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_table(name: &str) -> PathBuf { ... }
    fn teardown_test_table(path: &PathBuf) { ... }
}
```

---

## 7. Test Cases

### Testing Strategy

- Integration tests in `tests/` directory
- Isolated table files per test
- 24 tests covering all components

### Test Categories

#### FSM Initialization (3 tests)

1. **test_fsm_build_empty_table**: `build_from_heap` on empty table creates `.fsm` fork with all categories = 0
2. **test_fsm_build_with_data**: `build_from_heap` with 100 tuples yields correct categories and tree root > 0
3. **test_fsm_tree_parent_equals_max_children**: After build, every non-leaf node equals `max(left, right)` child

#### FSM Tree Search (3 tests)

4. **test_fsm_search_finds_candidate**: `fsm_search_avail` returns a page whose category ≥ requested minimum
5. **test_fsm_search_root_early_exit**: When root < min_category, returns `None` without reading Level-0 pages
6. **test_fsm_search_fp_next_slot_spreads**: Successive calls with same min_category return different pages via `fp_next_slot`

#### Insertion (3 tests)

7. **test_insert_uses_fsm_tree_search**: Insert uses `fsm_search_avail`; result page has category ≥ required min
8. **test_insert_allocates_new_page**: When root < min_category, `allocate_new_page` is called and FSM extended
9. **test_insert_returns_valid_location**: Returned (page_id, slot_id) can retrieve the original tuple

#### FSM Tree Integrity (3 tests)

10. **test_fsm_set_avail_bubbles_up**: Setting a leaf to category X causes all ancestor nodes to reflect X if X is new max
11. **test_fsm_set_avail_root_equals_global_max**: Root always equals the maximum category across all leaves
12. **test_fsm_vacuum_update_makes_page_searchable**: After `fsm_vacuum_update`, freed page is returned by next `fsm_search_avail`

#### Get Tuple (3 tests)

13. **test_get_tuple**: Retrieves inserted tuple by coordinates
14. **test_get_tuple_across_pages**: Retrieves from multiple pages
15. **test_get_tuple_invalid**: Errors on invalid page/slot

#### Scan (3 tests)

16. **test_scan_empty**: Empty table returns zero tuples
17. **test_scan_single_page**: Yields all tuples from single page
18. **test_scan_multiple_pages**: Scans 100 tuples with monotonic page IDs

#### FSM Updates (2 tests)

19. **test_fsm_set_avail_decreases_category**: After insert, leaf category decreases and parents update correctly
20. **test_fsm_full_page_category_zero**: Page with no free space gets category 0 and is not returned by search

#### Persistence (2 tests)

21. **test_fsm_fork_persists_across_restart**: FSM fork file retains correct categories after `HeapManager` is closed and reopened
22. **test_header_fsm_page_count_updated**: `fsm_page_count` in HeaderMetadata reflects actual FSM fork page count after growth

#### Performance (2 tests)

23. **bench_insert_throughput**: 10K inserts, verify >1K inserts/sec
24. **bench_scan_throughput**: 100K tuples, verify >50K tuples/sec

### Test Coverage Summary

| Component        | Tests  | Coverage                                        |
| ---------------- | ------ | ----------------------------------------------- |
| FSM Init         | 3      | Build, categories, tree parent invariant         |
| FSM Tree Search  | 3      | Candidate found, root early-exit, fp_next_slot   |
| Insertion        | 3      | Tree-guided reuse, allocation, location          |
| FSM Tree Integrity | 3    | Bubble-up, root max, vacuum update               |
| Get Tuple        | 3      | Basic, multi-page, invalid                       |
| Scan             | 3      | Empty, single, multi-page                        |
| FSM Updates      | 2      | Category decrease, full-page zeroed              |
| Persistence      | 2      | FSM fork file, fsm_page_count header             |
| Performance      | 2      | Insert/scan throughput                           |
| **Total**        | **24** | **Comprehensive**                                |

---

## Conclusion

This design implements RookDB's Free Space Manager and Heap File Manager (Project 6) with:

1. **Scalability**: O(log N) FSM tree search; O(1) early-exit via root node when table is full
2. **Performance**: PostgreSQL-style binary max-tree with `fp_next_slot` spreads concurrent inserts and avoids contention
3. **Modularity**: Clear Project 6/10 separation; FSM fork is a pure sidecar with no heap-page overhead
4. **Persistence**: FSM fork treated as a hint — rebuilt from heap on crash without WAL; header survives crashes
5. **Testing**: 24 comprehensive tests covering tree invariants, search, insertion and persistence

**Implementation**: ~1,700 lines of Rust code, minimal changes to existing components

**Status**: Ready for Implementation  
**Estimated Time**: 3-4 weeks (2 developers)  
**Dependencies**: None (uses existing Disk Manager, Page, Buffer Manager)
