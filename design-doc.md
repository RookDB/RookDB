# RookDB: Solution Design Phase - Free Space Manager and Heap File Manager

**Project:** 6. Free Space Manager and Heap File Manager

**Date:** 23rd April, 2026

---

## PROJECT SCOPE

Project 6 and Project 10 have clearly separated responsibilities based on operational granularity. Project 6 handles page-level operations: tuple insertion (using the FSM binary max-tree search algorithm), tuple retrieval by coordinates (page_id, slot_id). The Free Space Map tracks available space at page granularity using a PostgreSQL-style 3-level tree of FSM pages stored in a separate fork file.

Project 10 will handle tuple-level operations including deletion, updates, in-page compaction, slot reorganization, and tuple relocation. These are explicitly out of scope for our implementation, which operates exclusively at the page level tracking which pages have space, not managing individual tuple life cycles within pages.

---

## 1. Database and Architecture Design Changes

### 1.0 Database File Changes

#### The FSM Sidecar File (`<table>.dat.fsm`)

**What It Is:**

The FSM (Free Space Manager) sidecar file is a dedicated companion file to the heap data file (`<table>.dat`). Instead of embedding metadata within heap pages (which would intrude on tuple storage), RookDB maintains a separate 3-level binary max-tree in `<table>.dat.fsm` that tracks the free space availability of every page in the heap.

**Why It Exists:**

1. **No Heap Page Intrusion:** Adding metadata to heap pages would consume precious tuple storage space. The sidecar keeps heap pages pristine.
2. **Efficient Searches:** The binary max-tree enables O(\log N) searches by free-space category, replacing a naive O(N) linear scan of all pages.
3. **Rebuild on Crash:** The FSM file is treated as a hint-like structure. If corrupted or deleted, it can be rebuilt from the heap file without data loss or WAL complexity.

**File Structure:**

The FSM fork is organized as a flat sequence of 8 KB pages, conceptually arranged as a 3-level tree:

```
Level 2 (Root):       Block 0       ← 1 page covering up to ~64B heap pages
Level 1 (Internal):   Blocks 1..N   ← Multiple pages, each covering 4000 heap pages
Level 0 (Leaves):     Blocks N+1.. ← Multiple pages, each tracking 4000 heap pages directly
```

Each FSM page contains a binary max-tree array where the root (index 0) holds the maximum free-space category among all descendants. Leaves directly store free-space categories (0–255) for heap pages.

---

### 1.1 Current Architecture (Before FSM/Heap Mgr)

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

#### Enhanced Header Page (Page 0) - 20-Byte HeaderMetadata

**The New 20-Byte Structure:**

```
Offset | Size | Field              | Purpose
-------|------|--------------------|---------------------------------
0-3    | 4    | Page Count         | Total heap pages in file
4-7    | 4    | FSM Page Count     | Total pages in FSM fork file
8-11   | 4    | Total Tuples (Low) | Tuple count lower 32 bits
12-15  | 4    | Total Tuples (High)| Tuple count upper 32 bits
16-19  | 4    | Last Vacuum        | Timestamp (or reserved)
20+    | ...  | Reserved           | Future use
```

**How This Changes RookDB's Table Boundary Tracking:**

Previously, RookDB had no reliable way to track table boundaries:
- **Old way:** The system would scan the file sequentially to find the last page, leading to O(N) cost on initialization.
- **New way:** `page_count` is stored persistently in the header, enabling O(1) table boundary lookup.

The `fsm_page_count` field similarly allows the FSM fork to grow dynamically. When a new heap page is allocated:
1. `page_count` increments.
2. If the new page triggers FSM growth, `fsm_page_count` increments.
3. The header is atomically flushed to disk, ensuring consistency.

**Justification for Each Field:**

- **Page Count (4 bytes):** Tracks heap boundaries without scanning.
- **FSM Page Count (4 bytes):** Sizes the FSM fork; the fork can grow on demand as the heap grows
- **Total Tuples (8 bytes):** Enables O(1) COUNT(\*) queries without scanning; supports up to 2^64 tuples
- **Last Vacuum (4 bytes):** Reserved for future integration with Project 10 (compaction/vacuum tracking)
- **Persistence:** The header survives system crashes. The FSM fork is treated as a hint and can be rebuilt from the heap using `FSM::build_from_heap()` without data loss.

#### FSM Binary Max-Tree Search

**Objective:** Find a page with sufficient free space by traversing a 3-level binary max-tree (one byte per heap page, 0–255 scale).

**Free Space Quantization:**

Exact byte counts are not stored. Each heap page maps to one `u8` category:

```
category = floor(free_bytes / 32)   // 0 = full, 255 = completely empty
```

For an 8 KB page this gives ~32 bytes of resolution per category step.

**Logic:**

```
FindPage(min_category):
    1. Read root FSM page (Level 2).
       If root value < min_category: RETURN None (no page has enough space).
    2. Traverse Level 2 → Level 1 → Level 0:
       - At each internal node pick a child whose value >= min_category.
    3. Reach Level-0 leaf: compute heap PageID from (fsm_page_no, slot_no).
    4. RETURN heap PageID.
    5. If no candidate found: extend relation, call fsm_set_avail for new page, return it.
```



**FSM page constants (8 KB page):**

- `FSM_NODES_PER_PAGE: usize = 7999` - bytes in the binary max-tree array
- `FSM_SLOTS_PER_PAGE: u32 = 4000` - usable leaf slots per Level-0 FSM page
- `FSM_LEVELS: u32 = 3` - Level 0 = leaves, Level 2 = root

**Benefits:**

- No intrusive overhead written to heap pages
- Treated as a hint: can be rebuilt from the heap after a crash without WAL

#### Slotted Page Layout (Heap Pages 1+)

**No Major Changes to Tuple Layout:**

The fundamental tuple organization remains unchanged from the base system. Each heap page (except Page 0) follows the slotted page format:


**Key Points:**

- **Lower Pointer (`lower`):** Points to the next available slot in the directory (grows downward)
- **Upper Pointer (`upper`):** Points to the start of free space for new tuple data (grows downward from end of page)
- **Slot Entry Format:** 8 bytes per slot = (offset: u32, length: u32) → tuple is located at `page.data[offset..offset+length]`. Note: A deleted slot has an `offset == 0` and `length == 0`.
- **Slot Reclaiming / Rollback:** During deletion, if a tuple is functionally flush against the upper bounds, the `upper` boundary explicitly rolls backwards. The tuple arrays can uniquely track and reclaim empty structures sequentially to drastically diminish dead vacuum spaces within blocks.
- **Contiguous Free Space Only:** `free_bytes = upper - lower` (the contiguous gap between directory and data)
  - RookDB **only inserts into this last contiguous space**, never reusing fragmented holes from deletions
  - **Why:** Compacting a page during INSERT is too expensive; FSM must only advertise space that's 100% ready to use
  - **Example:** If a page has 2 KB contiguous free but 3 KB total (including holes), FSM marks it as having only 2 KB available
  - Fragmented "dead space" from deletions remains until Project 10 (VACUUM/Compaction) reorganizes the page

**Phase-Dependent Free Space Tracking:**

**Current Phase (Project 6 - Contiguous Only):**
- **What FSM Tracks:** Contiguous free space only (`upper - lower`)
- **Why:** Heap Manager cannot reorganize pages during INSERT (too expensive)
- **Result:** If FSM routes an insert to a fragmented page, insertion fails and FSM updates the page's true contiguous space
- **Tradeoff:** Wasted space from fragmentation until compaction, but fast inserts with guaranteed success

**Future Phase (Total Space with On-Fly Compaction):**
- **What FSM Will Track:** Total free space (contiguous + fragmented holes)
- **Why:** Heap Manager can quickly compact pages during INSERT (Postgres-style)
- **Result:** If insert lands on fragmented page, heap manager shifts memory to merge holes, then inserts
- **Tradeoff:** Slower inserts (occasional compaction overhead) but zero wasted space

**Why No Major Changes to Layout:**

The slotted page format is already tuple-efficient. The FSM layer operates *above* pages: it never modifies page internals, only reads `page_free_space()` to compute categories after inserts. This separation of concerns keeps pages simple and maximizes tuple density.

---

## 1.3 Complexity Analysis: FSM Tree Operations 

### Time Complexity (CPU Execution)

**1. Insertion Search (`fsm_search_avail`)**
* **Complexity: $O(1)$** relative to the total number of heap pages ($N$).
* **Reasoning:** The FSM does not perform linear $O(N)$ scans. It performs a greedy $O(\log S)$ binary search down a fixed-size array, where $S$ is the number of slots per page (4000). 
* **Execution:** Searching a single FSM page takes exactly ~12 array index lookups (`(2 * i) + 1`). Even for a 64-billion-page database requiring all 3 tree levels, the search guarantees a maximum of ~36 array lookups. In Rust, this resolves in nanoseconds.

**2. Update Free Space (`fsm_set_avail`)**
* **Complexity: $O(1)$** relative to total heap pages. 
* **Reasoning:** Updating the FSM requires calculating the new space category and iteratively "bubbling up" the maximum value from the leaf to the root of the page's array. 
* **Execution:** This is strictly bound to the height of a single page's internal tree (~12 iterative loops). It does not scale with the size of the database.

---

### I/O Complexity (Disk Access)

FSM employs **Lossy Compression** (`floor(free_bytes / 32)`), which safely aborts disk writes if the space category does not change.

**Current Implementation (Direct Disk / No Buffer Manager)**
* **Read I/O:** * 1 to 3 Reads for the FSM path (Level-2 down to Level-0).
  * 1 Read to fetch the target Heap Data Page.
  * *Total: 2 to 4 Disk Reads per insert.*
* **Write I/O:**
  * 1 Write for the updated Heap Data Page.
  * 0 Writes for the FSM if the category is unchanged (Optimization triggers ~31% of the time).
  * 1 to 3 Writes for the FSM if the category boundary is crossed and must propagate up the tree.
  * *Total: 1 to 4 Disk Writes per insert.*

**Target Architecture (With Buffer Pool Manager)**
Once the BPM is integrated, the I/O cost per transaction drops to effectively zero synchronous operations:
* **Read I/O:** The Level-2 root and Level-1 FSM pages are accessed so frequently they will remain permanently "pinned" in RAM. Level-0 and Heap pages will likely require 1 initial disk read, but subsequent inserts to the same page will hit the cache.
* **Write I/O:** All FSM and Heap page mutations will occur entirely in memory. The BPM will flush dirty pages to disk asynchronously in the background. *Transaction Write I/O becomes 0.*

---

### Space Complexity (Disk & Memory Footprint)

**FSM Tree: $O(N / 4000)$ pages**

* **Storage Density:** A single 8KB FSM leaf page tracks exactly 4,000 heap pages. The 3-level hierarchical tree scales to track up to 64 billion heap pages using a single entry point.
* **FSM Fork Size vs Heap Size (Base-10 Math):** * 4,000 Heap pages (32 MB table) $\rightarrow$ 1 FSM page (8 KB).
  * 16,000,000 Heap pages (131 GB table) $\rightarrow$ 4,001 FSM pages (~32 MB).
  * 64,000,000,000 Heap pages (524 TB table) $\rightarrow$ 16,004,001 FSM pages (~131 GB).
* **Total Overhead:** The entire FSM file represents strictly **~0.025%** of the total database heap file size, regardless of scale.
* **In-Memory Overhead:** The FSM uses lazy dynamic allocation, starting with only 1 Level-0 page for the first 4,000 heap pages, and growing levels only when capacity overflows. It does not materialize the entire tree in memory; only the active 8KB `FSMPage` struct currently being searched or updated is loaded into RAM.
---

## 2. Backend Data Structures

### 2.1 Data Structures to be Created

#### `FSM` - Free Space Map

**Location:** `src/backend/fsm/fsm.rs`

```rust
// ── FSM layout constants ──────────────────────────────────────────────────
const FSM_NODES_PER_PAGE: usize = 7999; // binary max-tree array (bytes)
const FSM_SLOTS_PER_PAGE: u32  = 4000; // usable leaf nodes per FSM page
const FSM_LEVELS: u32          = 3;    // Level 0 = leaves, Level 2 = root

/// One disk page in the FSM fork.
/// Stores a binary max-tree; leaf nodes hold free-space categories (0–255).
pub struct FSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],  // index 0 = root; leaves in right half

}

/// In-memory handle for the entire 3-level FSM fork file.
pub struct FSM {
    fsm_path: PathBuf,       // path to <table>.fsm sidecar file
    fsm_file: File,          // open file handle on the FSM fork
    heap_page_count: u32,    // number of heap pages currently tracked
}
```

**Purpose:** Model the PostgreSQL-style FSM fork - a 3-level tree of `FSMPage` values, each containing a binary max-tree of 1-byte free-space categories covering all heap pages.


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

---

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

## 3. Execution, Testing, and Instrumentation

### Running the Application

#### 1. The Normal UI Run (Clean)

If you just want to run the database and see the normal UI, query outputs, and ASCII tables (without backend noise):

```bash
cargo run
```

**Note:** If you see too many logs, you can hush them by running:
```bash
RUST_LOG=off cargo run
# or
RUST_LOG=warn cargo run
```

#### 2. High-Level App Milestones (Info)

If you want to see standard high-level operations (like when a Database/Table is initialized, or a catalog is created):

```bash
RUST_LOG=info cargo run
```

#### 3. Developer / Debugging Mode (Debug)

If you want to track file-path validations, page creations, and understand what the system is doing behind the scenes without getting flooded by byte-level math:

```bash
RUST_LOG=debug cargo run
```

#### 4. Extreme Detail / Step-by-Step (Trace)

If you are actively debugging a corrupted page or FSM issue and need to see *everything* (byte offsets, upper/lower bounds calculations, tuple insertions):

```bash
RUST_LOG=trace cargo run
```

### Logging Hierarchy & Waterfall Model

In Rust's `log` ecosystem, logging levels operate on a **waterfall hierarchy** based on severity. When you set a specific log level using `RUST_LOG=...`, you are telling the system: *"Show me this level of detail, **and everything more critical than it**"*

**The Strict Hierarchy (quietest to loudest):**

1. **`off`** (Total silence – hides everything)
2. **`error`** (Only show critical failures)
3. **`warn`** (Show warnings + errors)
4. **`info`** (Show high-level milestones + warnings + errors)
5. **`debug`** (Show developer diagnostics + info + warnings + errors)
6. **`trace`** (Show absolutely everything, including byte-level math)

**Important Note:** Standard `println!` and `print!` statements bypass this system entirely, which is why your UI menus always show up regardless of the `RUST_LOG` setting.


### Running Tests with Logs

By default, `cargo test` hides all output unless a test fails. If you want to see your new logs during testing to debug why a specific test might be failing:

```bash
RUST_LOG=debug cargo test -- --nocapture
```

You can swap `debug` for `trace` here as well if you need deep FSM debugging during tests:

```bash
RUST_LOG=trace cargo test -- --nocapture
```

### Running Benchmarks

To run the FSM heap benchmark and monitor performance:

```bash
cargo run --release --bin benchmark_fsm_heap
```

With logging enabled:

```bash
RUST_LOG=debug cargo run --release --bin benchmark_fsm_heap
```

Results are stored in `benchmark_runs/` directory as JSON files for historical tracking.

### Running All Tests

To run the complete test suite:

```bash
cargo test
```

With output capture enabled:

```bash
cargo test -- --nocapture
```

To run a specific test:

```bash
cargo test test_fsm_search_finds_candidate -- --nocapture
```

---

## 4. Important: Catalog Manager Recovery

### Graceful Recovery Without Manual Intervention

A critical flaw of RookDB's architecture is **catalog persistence**. If the catalog file (`catalog.json`) gets corrupted or deleted:

1. **Detection:** The system should detect that the catalog is missing or invalid during initialization.
2. **Automatic Recovery:** Rather than failing, the system should:
   - Scan the data directory for existing `<table>.dat` files
   - Rebuild the catalog from the discovered tables by reading headers
   - Re-register all existing tables in a fresh `catalog.json`
3. **Data Preservation:** No table data is lost; existing tuples are fully recoverable from the heap files.


---

### 5. Future Compaction (Project 10)

The Compaction Team (Project 10) will handle fragmentation elimination via:

1. **`update_page_free_space(page_id, reclaimed_bytes)`**
   - After in-place compaction, notify FSM of consolidated free space
   - FSM updates category; page becomes available again

2. **`rebuild_table_fsm(table_name)`**
   - Full-table FSM rebuild after major reorganization
   - Ensures FSM accuracy after extensive compaction

3. **Future: On-Fly Compaction**
   - If insert encounters fragmentation, compact before inserting
   - Postgres-style approach: merge holes in-memory, then insert
   - Tradeoff: Slower inserts but zero wasted space

---

## 6. Compaction Team Integration APIs

RookDB provides **3 high-level facade APIs** for the Compaction Team (Project 10) to merge their code safely:

### 6.1 `insert_raw_tuple(db_name, table_name, tuple_data) -> (page_id, slot_id)`

**Purpose:** Insert a tuple without FSM search (for tuple relocation during compaction)

**Use:**
```rust
// Compaction team relocates tuples:
let (page_id, slot_id) = insert_raw_tuple("mydb", "users", tuple_bytes)?;
// Internally searches FSM for available page and inserts
```

### 6.2 `update_page_free_space(db_name, table_name, page_id, reclaimed_bytes) -> ()`

**Purpose:** Notify FSM that a page's contiguous free space has changed

**Use:**
```rust
// After in-place compaction on page 5:
update_page_free_space("mydb", "users", 5, 5000)?;
// FSM updates: category = floor(5000 * 255 / 8192) = 156
// Bubble-up propagates changes through tree
```

### 6.3 `rebuild_table_fsm(db_name, table_name) -> ()`

**Purpose:** Full table FSM rebuild (after large-scale reorganization)

**Use:**
```rust
// After full-table reorganization:
rebuild_table_fsm("mydb", "users")?;
// Scans all heap pages, rebuilds FSM from scratch
// Ensures FSM is accurate
```

---

## 7. Backend Functions

### 7.1 Functions Created

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


**Inputs:** `min_category` - minimum required free-space category (0–255)  
**Outputs:** heap `PageID` or `None` (root value < min_category means no eligible page)  
**Steps:**

1. Read root FSM page (Level 2). If `root.tree[0] < min_category`: return `None`.
2. Starting at Level 2, descend to Level 1 then Level 0:
   - At each internal node, choose a child whose value >= `min_category`.
   - Uses binary search to navigate the tree efficiently.
3. At the Level-0 leaf, read the slot index to compute: `heap_page_id = fsm_page_no × FSM_SLOTS_PER_PAGE + slot`.

5. Return `Some(heap_page_id)`.

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
7. If the root value of this Level-0 page changed, iteratively propagate the new maximum category up to the parent nodes using a loop.
8. Mark all modified FSM pages dirty (treated as hints - no WAL entry written).

#### `FSM::fsm_vacuum_update`

```rust
pub fn fsm_vacuum_update(&mut self, heap_page_id: u32, reclaimed_bytes: u32) -> io::Result<()>
```

**Description:** Called by Project 10 (VACUUM / compaction) when a heap page gains free space. Converts the reclaimed bytes to a category and calls `fsm_set_avail` so the page becomes searchable again.  
**Inputs:** `heap_page_id`, `reclaimed_bytes` (may be the full page free space if the page became empty)  
**Steps:**

1. Delegate to `self.fsm_set_avail(heap_page_id, reclaimed_bytes)`
2. If the Level-0 root value increased, propagate up (handled inside `fsm_set_avail`).

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

**Description:** Insert tuple using the FSM binary max-tree to locate a suitable page. Implements a **3-attempt retry strategy** to handle fragmentation gracefully.  
**Outputs:** (page_id, slot_id)  

**The 3-Attempt Insertion Algorithm:**

RookDB uses a retry strategy to handle the mismatch between FSM categories (quantized to 0–255) and actual contiguous free space:

```
Attempt 1: Trust FSM's suggestion
  ├─ Calculate min_category for tuple size
  ├─ Call fsm_search_avail(min_category)
  ├─ If None: skip to Attempt 3 (allocate new page)
  ├─ Try to insert into suggested page
  ├─ If insertion succeeds: DONE ✓ (99% of inserts succeed here)
  └─ If insertion fails: page has internal fragmentation
      └─ Update FSM with true contiguous free space

Attempt 2: Search again with updated FSM
  ├─ Call fsm_search_avail(min_category) again
  │   (FSM now has corrected categories)
  ├─ Try to insert into newly suggested page
  ├─ If insertion succeeds: DONE ✓ (handles most fragmentation cases)
  └─ If insertion fails: entire FSM tree is fragmented

Attempt 3: Allocate a brand new page
  ├─ Call allocate_new_page()
  ├─ Brand new page has 100% free space: insertion guaranteed to succeed
  └─ DONE ✓ (fallback, rarely needed)
```

**Why 3 Attempts?**

1. **Attempt 1 handles 99% of cases:** FSM is usually accurate; one-shot success is common
2. **Attempt 2 handles fragmentation:** If a page became fragmented, retry with updated FSM category
3. **Attempt 3 guarantees success:** Always have a brand-new page as fallback

**Steps:**

1. Calculate `min_category = ceil((tuple_size + 8) × 255 / PAGE_SIZE)`
2. **Attempt 1:** Call `fsm.fsm_search_avail(min_category)`:
   - If `Some(page_id)`: Try insertion
     - If success: Update FSM and return (page_id, slot_id)
     - If fails: Update FSM with true contiguous space, continue to Attempt 2
   - If `None`: Skip to Attempt 3
3. **Attempt 2:** Call `fsm.fsm_search_avail(min_category)` again:
   - If `Some(page_id)`: Try insertion
     - If success: Return (page_id, slot_id)
     - If fails: Continue to Attempt 3
   - If `None`: Attempt 3
4. **Attempt 3:** Call `allocate_new_page()`:
   - Insert into brand new page (guaranteed to succeed)
   - Return (page_id, slot_id)
5. Increment `total_tuples` in header
6. Persist changes to disk

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

---

## 8. Frontend Changes (CLI Inputs)

### New Command: `CHECK_HEAP`

**Syntax:** `CHECK_HEAP <table_name>`

**Purpose:** Display FSM statistics and health metrics

**Example Output:**
```

Total Heap Pages:  2
FSM Fork Pages:    1
Total Tuples:      1

╔══════════════════════════════════════════════════════════════╗
║                    OPERATION METRICS                         ║
╠══════════════════════════════════════════════════════════════╣
║ FSM Operations:                                              ║
║  - fsm_search_avail:            1 calls                      ║
║  - fsm_search_tree:             1 calls                      ║
║  - fsm_read_page:               1 calls                      ║
║  - fsm_write_page:              1 calls                      ║
║  - fsm_serialize_page:          1 calls                      ║
║  - fsm_deserialize_page:        1 calls                      ║
║  - fsm_set_avail:               1 calls                      ║
║  - fsm_vacuum_update:           0 calls                      ║
╠══════════════════════════════════════════════════════════════╣
║ Heap Operations:                                             ║
║  - insert_tuple:                1 calls                      ║
║  - get_tuple:                   0 calls                      ║
║  - allocate_page:               0 calls                      ║
║  - write_page:                  1 calls                      ║
║  - read_page:                   1 calls                      ║
║  - page_free_space:             0 calls                      ║
╚══════════════════════════════════════════════════════════════╝
```


---

## 9. Overall Component Workflow (End-to-End View)

### Workflow 1: Tuple Insertion (FSM Binary Max-Tree)

```
Executor: Call HeapManager::insert_tuple(&[u8])
           ↓
HeapManager: min_category = ceil(58 × 255 / 8192) = 2
           ↓
           FSM Binary Max-Tree Search:
    ┌──────────────────────────────────────┐
    │ fsm_search_avail(2)                  │  
    │  1. Read root FSM page (Level 2)     │
    │  2. root value ≥ 2? → YES, descend   │
    │  3. L2 → L1 → L0, traversal          │
    │  4. Reach leaf → return heap PageID  │
    └──────────────┬───────────────────────┘
                   │
    Found? ├─ YES → Page 47

           │
           └─ NO (root < 2) → allocate_new_page()  → fsm_set_avail(new_page, 8184)
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


### Workflow 4: Project 10 Integration (FSM Update After Compaction)

```
Project 10: Compacts page 45 → 8,176 bytes now free
           ↓
Project 10: Calls fsm.fsm_vacuum_update(45, 8176)
           ↓
fsm_vacuum_update delegates to:
  fsm_set_avail(45, 8176)
           ↓
category = floor(8176 /32 ) = 255
           ↓
Locate Level-0 FSM page for heap page 45
  leaf_slot = 45 mod FSM_SLOTS_PER_PAGE
           ↓
Write category 255 into tree[leaf_index(leaf_slot)]
           ↓
Bubble up: recompute parent nodes as max(left, right)
  → root of Level-0 page may increase
           ↓
If Level-0 root changed:
  propagate new root value up to Level-1, then Level-2
           ↓
FSM tree now reflects page 45 as nearly empty (category 255)
Next fsm_search_avail call will find page 45
```

**Critical:** We never decide to compact - only receive `fsm_vacuum_update` notifications. No heap page bytes are modified; all changes are confined to the FSM fork file.

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

## 10. Codebase Structure Changes

### New Files

```
src/backend/
├── fsm/
│   ├── mod.rs                    # Module declaration
│   └── fsm.rs                    # FSMPage + FSM implementation
└── heap/
    ├── heap_manager.rs           # HeapManager + HeapScanIterator 
    └── types.rs                  # HeaderMetadata struct 
```

**fsm/fsm.rs** 

- Constants: `FSM_NODES_PER_PAGE`, `FSM_SLOTS_PER_PAGE`, `FSM_LEVELS`

- `FSM` struct: `build_from_heap`, `fsm_search_avail`, `fsm_set_avail`, `fsm_vacuum_update`
- Private helpers: `read_fsm_page`, `write_fsm_page`, `fsm_block_for`, `leaf_index`, `bubble_up`

**heap/heap_manager.rs** 

- HeapManager struct, open, insert_tuple, get_tuple, scan, allocate_new_page, flush
- HeapScanIterator implementation

**heap/types.rs** 

- HeaderMetadata struct, from_page, write_to_page

### Modified Files

**backend/mod.rs**

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

**backend/disk/disk_manager.rs** 

- Add `update_header_page()` function


---

