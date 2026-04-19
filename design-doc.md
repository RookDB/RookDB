# RookDB: Solution Design Phase - Free Space Manager and Heap File Manager

**Project:** 6. Free Space Manager and Heap File Manager

**Date:** 19th April, 2026

---

## PROJECT SCOPE

Project 6 and Project 10 have clearly separated responsibilities based on operational granularity. Project 6 handles page-level operations: tuple insertion (using the FSM binary max-tree search algorithm), tuple retrieval by coordinates (page_id, slot_id), and sequential scans across all pages. The Free Space Map tracks available space at page granularity using a PostgreSQL-style 3-level tree of FSM pages stored in a separate fork file.

Project 10 will handle tuple-level operations including deletion, updates, in-page compaction, slot reorganization, and tuple relocation. These are explicitly out of scope for our implementation, which operates exclusively at the page level tracking which pages have space, not managing individual tuple lifecycles within pages.

---

## 1. Database and Architecture Design Changes

### 1.0 Database File Changes

#### The FSM Sidecar File (`<table>.dat.fsm`)

**What It Is:**

The FSM (Free Space Manager) sidecar file is a dedicated companion file to the heap data file (`<table>.dat`). Instead of embedding metadata within heap pages (which would intrude on tuple storage), RookDB maintains a separate 3-level binary max-tree in `<table>.dat.fsm` that tracks the free space availability of every page in the heap.

**Why It Exists:**

1. **No Heap Page Intrusion:** Adding metadata to heap pages would consume precious tuple storage space. The sidecar keeps heap pages pristine.
2. **Efficient Searches:** The binary max-tree enables O(log N) page searches by free-space category, replacing a naive O(N) linear scan of all pages.
3. **Rebuild on Crash:** The FSM file is treated as a hint-like structure. If corrupted or deleted, it can be rebuilt from the heap file without data loss or WAL complexity.
4. **Scalability:** A single byte per heap page (free-space category 0–255) means 1 MB of FSM overhead covers an 8 GB table.

**File Structure:**

The FSM fork is organized as a flat sequence of 8 KB pages, conceptually arranged as a 3-level tree:

```
Level 2 (Root):       Block 0       ← 1 page covering up to ~4M heap pages
Level 1 (Internal):   Blocks 1..N   ← Multiple pages, each covering 2040 heap pages
Level 0 (Leaves):     Blocks N+1.. ← Multiple pages, each tracking 2040 heap pages directly
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

- **Page Count (4 bytes):** Tracks heap boundaries without scanning; supports tables up to 2^32 pages (~32 TB at 8 KB/page)
- **FSM Page Count (4 bytes):** Sizes the FSM fork; the fork can grow on demand as the heap grows
- **Total Tuples (8 bytes):** Enables O(1) COUNT(\*) queries without scanning; supports up to 2^64 tuples
- **Last Vacuum (4 bytes):** Reserved for future integration with Project 10 (compaction/vacuum tracking)
- **Persistence:** The header survives system crashes. The FSM fork is treated as a hint and can be rebuilt from the heap using `FSM::build_from_heap()` without data loss.

#### FSM Binary Max-Tree Search

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
       - At each internal node pick a child whose value >= min_category.
    3. Reach Level-0 leaf: compute heap PageID from (fsm_page_no, slot_no).
    4. RETURN heap PageID.
    5. If no candidate found: extend relation, call fsm_set_avail for new page, return it.
```

**Example (single-level tree with 4 leaf slots):**

```
    4             ← root (max of all children)
  4     2
 3 4   0 2       ← leaf nodes (one per heap page)
```

- Need: category ≥ 2
- Root = 4 ≥ 2 → traverse sequentially (`fp_next_slot` unused)
- Descend right → pick leaf with value 2
- **Result:** heap page mapped to that leaf slot is returned

**Why Appropriate:**

- **Scalability:** O(log N) search; O(1) rejection via root when table is full
- **Future Multi-Threading:** `fp_next_slot` can be extended in future phases to spread concurrent inserts across different pages
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

- `FSM_NODES_PER_PAGE: usize = 4080` - bytes in the binary max-tree array
- `FSM_SLOTS_PER_PAGE: u32 = 2040` - usable leaf slots per Level-0 FSM page
- `FSM_LEVELS: u32 = 3` - Level 0 = leaves, Level 2 = root

**Benefits:**

- No intrusive overhead written to heap pages
- Treated as a hint: can be rebuilt from the heap after a crash without WAL
- Constant-height 3-level tree covers up to 2040² ≈ 4 M heap pages (~32 GB at 8 KB/page)

#### Slotted Page Layout (Heap Pages 1+)

**No Major Changes to Tuple Layout:**

The fundamental tuple organization remains unchanged from the base system. Each heap page (except Page 0) follows the slotted page format:

```
Page Layout (8 KB page):

┌─────────────────────────┐
│ Header (8 bytes)        │  Offset 0-7
│  - lower (u32): points  │  Next free slot in directory
│  - upper (u32): points  │  Start of tuple data region
├─────────────────────────┤
│                         │
│ Slot Directory          │  Growing downward from offset 8
│ (4 bytes per slot)      │  Each slot: (offset, length)
│                         │
├────────────────────────ー┤
│                         │
│ Free Space              │  Contiguous gap between
│ (fragmented or not)     │  directory and tuple data
│                         │
├─────────────────────────┤
│ Tuple Data              │  Growing upward from PAGE_SIZE
│ (variable-length)       │
│                         │
└─────────────────────────┘
```

**Key Points:**

- **Lower Pointer (`lower`):** Points to the next available slot in the directory (grows downward)
- **Upper Pointer (`upper`):** Points to the start of free space for new tuple data (grows downward from end of page)
- **Slot Entry Format:** 8 bytes per slot = (offset: u32, length: u32) → tuple is located at `page.data[offset..offset+length]`
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

**Future Phase (Project 10 - Total Space with On-Fly Compaction):**
- **What FSM Will Track:** Total free space (contiguous + fragmented holes)
- **Why:** Heap Manager can quickly compact pages during INSERT (Postgres-style)
- **Result:** If insert lands on fragmented page, heap manager shifts memory to merge holes, then inserts
- **Tradeoff:** Slower inserts (occasional compaction overhead) but zero wasted space

**Why No Major Changes to Layout:**

The slotted page format is already tuple-efficient. The FSM layer operates *above* pages: it never modifies page internals, only reads `page_free_space()` to compute categories after inserts. This separation of concerns keeps pages simple and maximizes tuple density.

---

#### Integration

**Buffer/Disk Manager:** Use existing `read_page`/`write_page`, add `update_header_page` helper for header persistence

**Page Structure:** No changes; use existing `page_free_space()` calculation and slotted page layout

**Catalog:** No changes for MVP (Project 6 scope)

---

## 1.3 Complexity Analysis: FSM Tree Operations 

### Time Complexity

**FSM Tree Search (`fsm_search_avail`): O(log N)**

- **Tree Traversal:** The 3-level tree (Level 2 root → Level 1 → Level 0 leaves) requires 3 disk reads in the worst case, regardless of the number of heap pages.
- **Per Page Count:** If the heap has N pages:
  - Leaves required: ≈ N / FSM_SLOTS_PER_PAGE ≈ N / 2040
  - Level-1 pages: ≈ (N / 2040) / 2040 ≈ N / 4,161,600
  - Level-2 pages: 1 (root)
  - Depth: ≈ log₂₀₄₀(N) ≈ 3 for N up to ~4 million pages
- **Result:** O(log N) = O(1) for practical table sizes (even a 32 TB table only needs 3 I/Os)

### I/O Complexity

**FSM Tree Search: O(log N) disk reads**

- **Read 1:** Root FSM page (Level 2)
- **Read 2:** One Level-1 FSM page (selected via tree navigation)
- **Read 3:** One Level-0 FSM page (leaf)
- **Read 4:** Actual heap page (to insert tuple)
- **Write 1:** Updated heap page
- **Write 2-4:** Updated FSM pages (levels 0, 1, 2) if categories changed
- **Total:** ~3 reads + ~4 writes per insert


### Space Complexity

**FSM Tree: O(N / 2040) pages, 1 byte per heap page**

- **FSM Fork Size:** For N heap pages, approximately N / 2040 FSM pages required
  - Example: 1M heap pages → ~500 FSM pages → 4 MB FSM fork
  - 1B heap pages (8 TB table) → ~500K FSM pages → 4 GB FSM fork
- **In-Memory Overhead:** Minimal. Only the current heap page and up to 3 FSM pages are cached (per-insertion). No full-tree materialization in memory.
- **Header Overhead:** 20 bytes on Page 0
- **Total:** < 0.05% of heap size


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
    fp_next_slot: u16,               // Currently unused, reserved for future load spreading
}

/// In-memory handle for the entire 3-level FSM fork file.
pub struct FSM {
    fsm_path: PathBuf,       // path to <table>.fsm sidecar file
    fsm_file: File,          // open file handle on the FSM fork
    heap_page_count: u32,    // number of heap pages currently tracked
}
```

**Purpose:** Model the PostgreSQL-style FSM fork - a 3-level tree of `FSMPage` values, each containing a binary max-tree of 1-byte free-space categories covering all heap pages.
**Justification:** O(log N) tree search; 1 byte/page memory overhead; `fp_next_slot` reserved for future concurrent inserts; self-correcting without WAL logging.

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

### 2.2 Component Redundancy: `create_page` vs. `allocate_new_page`

**The Distinction:**

These two functions operate at different levels of abstraction and serve different purposes:

#### `create_page` (Low-Level I/O)

**Location:** `src/backend/disk/disk_manager.rs`

**Purpose:** Raw disk I/O operation

**Signature:**
```rust
pub fn create_page(file: &File, page_id: u32) -> io::Result<Page>
```

**Behavior:**
- Takes an open file and a page ID
- Seeks to the correct file offset: `offset = page_id * PAGE_SIZE`
- Reads (or initializes) 8 KB of data from disk
- Returns a `Page` struct
- **No state tracking** – purely mechanical I/O
- **No side effects** – doesn't update file size, headers, or any metadata

**Use Case:** Direct page reads for specific pages when you already know the page_id

---

#### `allocate_new_page` (Stateful Heap Manager)

**Location:** `src/backend/heap/heap_manager.rs`

**Purpose:** High-level heap state management

**Signature:**
```rust
fn allocate_new_page(&mut self) -> io::Result<u32>
```

**Behavior:**
- Computes `new_page_id = header.page_count` (must be mutable to track state)
- Creates an empty slotted page structure (lower=8, upper=PAGE_SIZE)
- **Appends** to the heap file (extending file size)
- **Updates** `header.page_count` in memory
- **Extends** FSM fork if necessary (`header.fsm_page_count` may grow)
- **Registers** the new page with FSM: calls `fsm.fsm_set_avail(new_page_id, PAGE_SIZE - 8)` with category 255 (fully empty)
- **Mutates state:** The `HeapManager` must track the growing heap
- **Returns:** The new page_id

**Use Case:** When the FSM has no pages with sufficient free space, grow the heap and immediately register the new page for reuse

---

**Why Both Exist (Not Redundant):**

| Aspect               | `create_page`          | `allocate_new_page`      |
|----------------------|------------------------|---------------------------|
| **Abstraction**      | Low-level I/O          | High-level state mgmt    |
| **Mutability**       | Immutable (reads page) | Mutable (modifies header) |
| **Side effects**     | None                   | Updates header, FSM       |
| **File growth**      | No                     | Yes (appends to file)    |
| **Usage**            | Read existing pages    | Allocate new pages       |
| **Called by**        | `fsm_search_avail`     | `insert_tuple` fallback  |

**Example Workflow:**

```
insert_tuple(50 bytes):
  │
  ├─ min_category = ceil(58 * 255 / 8192) = 2
  │
  ├─ fsm_search_avail(2)  ← may internally call create_page to read FSM pages
  │   └─ Returns Some(47)
  │
  ├─ Read heap page 47 (uses create_page internally)
  │
  ├─ Actual free_space = 1200 bytes ✓ sufficient
  │
  ├─ Insert tuple into page 47
  │
  ├─ fsm_set_avail(47, 1150)  ← update FSM
  │
  └─ Return (47, 3)  ← (page_id, slot_id)

BUT if fsm_search_avail returned None:
  │
  ├─ allocate_new_page()  ← HIGH-level, mutable
  │   │
  │   ├─ Compute new_page_id = 10240 (assuming header.page_count was 10240)
  │   ├─ Create empty slotted page
  │   ├─ Append to heap file  ← FILE GROWTH
  │   ├─ header.page_count = 10241  ← STATE UPDATE
  │   ├─ Extend FSM fork if needed  ← FSM GROWTH
  │   ├─ fsm_set_avail(10240, 8184)  ← REGISTER with FSM
  │   │
  │   └─ Return 10240
  │
  ├─ Insert tuple into page 10240
  │
  └─ Return (10240, 0)  ← brand new page
```

---

### 2.3 Data Structures to be Modified

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

## 8. Execution, Testing, and Instrumentation

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

### How the Hierarchy Applies

**1. `RUST_LOG=off cargo run` or just `cargo run`**

- **Where you are:** Top of hierarchy
- **What you see:** Only explicit UI `println!` statements (the ASCII menus, the tables). Zero backend logs.

**2. `RUST_LOG=info cargo run`**

- **Where you are:** Middle of hierarchy
- **What you see:** All `log::info!` messages (e.g., "Created database X", "Inserted 50 rows"). Due to the waterfall, you also see any `log::warn!` and `log::error!` messages if something goes wrong.

**3. `RUST_LOG=debug cargo run`**

- **Where you are:** Lower down (high detail)
- **What you see:** `log::debug!` messages (e.g., checking file paths, calculating category limits). You also inherit all `info!`, `warn!`, and `error!` logs.

**4. `RUST_LOG=trace cargo run`**

- **Where you are:** Absolute bottom (maximum detail)
- **What you see:** Every single log in the application. Deep byte-level calculations (`log::trace!`) PLUS all the `debug!`, `info!`, `warn!`, and `error!` logs.

### Custom Filtering (Advanced)

You can also filter logs to show up *only* for specific modules. For example, if you just want to see trace logs for the `fsm` module and nothing else:

```bash
RUST_LOG=storage_manager::backend::fsm=trace cargo run
```

Or for multiple modules:

```bash
RUST_LOG=storage_manager::backend::fsm=trace,storage_manager::backend::heap=debug cargo run
```

**TL;DR (Volume Knob Analogy):**

- Setting it to `error` means you only want to hear loud alarms
- Setting it to `trace` means you want to hear everything down to a pin dropping

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

## 9. Important: Catalog Manager Recovery

### Graceful Recovery Without Manual Intervention

A critical flaw of RookDB's architecture is **catalog persistence**. If the catalog file (`catalog.json`) gets corrupted or deleted:

1. **Detection:** The system should detect that the catalog is missing or invalid during initialization.
2. **Automatic Recovery:** Rather than failing, the system should:
   - Scan the data directory for existing `<table>.dat` files
   - Rebuild the catalog from the discovered tables by reading headers
   - Re-register all existing tables in a fresh `catalog.json`
3. **Data Preservation:** No table data is lost; existing tuples are fully recoverable from the heap files.


---

## 2. Sequential Insertion Guarantees & Fragmentation Handling

### 2.1 Sequential Fill Within Pages

RookDB's insertion strategy is **strictly sequential within each page** until that page is full:

**Page Filling Sequence:**

```
Page 1:  Insert T1 → [Header][Slot 0] ........ [T1]
         Insert T2 → [Header][Slot 0][Slot 1] ... [T2][T1]
         Insert T3 → [Header][Slot 0..2] ..... [T3][T2][T1]
                     (tuples pack toward end, slots grow from start)

Page 1 Eventually:
         50 inserts → [Header][50 Slots] [50 tuples packed at end]
                      lower = 408         upper ≈ 100 (nearly full!)
         
Page 1 is FULL:
         FSM marks category = 0, page becomes unavailable

Next Insert searches FSM → finds Page 2 (category > 0) → repeats
```

**Why Sequential, Not Scattered?**

1. **Minimize Disk Seeks:** Pages fill contiguously, reducing fragmented page access patterns
2. **FSM Simplicity:** Only advertise space in pages with actual contiguous gaps
3. **Optimal Caching:** Sequential access is cache-friendly on modern storage systems
4. **Predictable Growth:** Table size growth is linear and predictable

### 2.2 Fragmentation & Dead Space

**What Happens on Deletion:**

```
Before Delete:     [Header][Slot A, B, C] [T1][T2][T3]
                                          upper=100
After Delete T2:   [Header][Slot A, _, C] [T1][XX][T3]
                                          upper=100
                   (Dead space left at T2's location)

FSM sees:
  Contiguous free = upper - lower = 100 - 24 = 76 bytes (unchanged!)
  Total free = 76 + 50 (dead space) = 126 bytes (not tracked)
```

**Key Point:** RookDB does NOT reuse dead space during INSERT (no inline compaction). A page with 76 contiguous free bytes but 126 total will only accept tuples ≤ 76 bytes.

**If insertion fails due to fragmentation:**

```
Attempt 1: Insert 100-byte tuple into page with 76 contiguous free
           ├─ Fails (76 < 100)
           └─ Update FSM: "Page actually has 76 free, not 80"

Attempt 2: Search FSM again with corrected categories
           └─ FSM routes to a less fragmented page

Attempt 3: If all pages fragmented, allocate new page
           └─ Guaranteed success
```

### 2.3 Future Compaction (Project 10)

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

## 3. Compaction Team Integration APIs

RookDB provides **3 high-level facade APIs** for the Compaction Team (Project 10) to merge their code safely:

### 3.1 `insert_raw_tuple(db_name, table_name, tuple_data) -> (page_id, slot_id)`

**Purpose:** Insert a tuple without FSM search (for tuple relocation during compaction)

**Use:**
```rust
// Compaction team relocates tuples:
let (page_id, slot_id) = insert_raw_tuple("mydb", "users", tuple_bytes)?;
// Internally searches FSM for available page and inserts
```

### 3.2 `update_page_free_space(db_name, table_name, page_id, reclaimed_bytes) -> ()`

**Purpose:** Notify FSM that a page's contiguous free space has changed

**Use:**
```rust
// After in-place compaction on page 5:
update_page_free_space("mydb", "users", 5, 5000)?;
// FSM updates: category = floor(5000 * 255 / 8192) = 156
// Bubble-up propagates changes through tree
```

### 3.3 `rebuild_table_fsm(db_name, table_name) -> ()`

**Purpose:** Full table FSM rebuild (after large-scale reorganization)

**Use:**
```rust
// After full-table reorganization:
rebuild_table_fsm("mydb", "users")?;
// Scans all heap pages, rebuilds FSM from scratch
// Ensures FSM is accurate
```

---

## 4. Backend Functions

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

**Description:** Traverse the 3-level FSM tree to find a heap page whose free-space category is at least `min_category`, sequentially (as `fp_next_slot` is currently unused, reserved for future load spreading).  
**Inputs:** `min_category` - minimum required free-space category (0–255)  
**Outputs:** heap `PageID` or `None` (root value < min_category means no eligible page)  
**Steps:**

1. Read root FSM page (Level 2). If `root.tree[0] < min_category`: return `None`.
2. Starting at Level 2, descend to Level 1 then Level 0:
   - At each internal node, choose a child whose value >= `min_category`.
   - Search sequentially from first child; fall back to the other child.
3. At the Level-0 leaf, read the slot index to compute: `heap_page_id = fsm_page_no × FSM_SLOTS_PER_PAGE + slot`.
4. Mark pages dirty (Note: advancing `fp_next_slot` is deferred to a future phase).
5. Return `Some(heap_page_id)`.

**Implementation sketch:**

```rust
let root_page = self.read_fsm_page(fsm_block_for(2, 0))?;
if root_page.tree[0] < min_category {
    return Ok(None);
}
// descend level 2 → 1 → 0 following sequential traversal (`fp_next_slot` unused) ...
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
    │  3. L2 → L1 → L0, sequential traversal│
    │  4. Reach leaf → return heap PageID  │
    └──────────────┬───────────────────────┘
                   │
    Found? ├─ YES → Page 47
           │         (advancing `fp_next_slot` planned for future)
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

**Key Point:** Insertion is **not append-only**; `fsm_search_avail` traverses the binary max-tree sequentially (`fp_next_slot` reserved for future), and the root provides O(1) early-exit when no page has sufficient space

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

## 6. Codebase Structure Changes

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
- `FSMPage` struct: binary max-tree array, `fp_next_slot`
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
6. **test_fsm_search_fp_next_slot_spreads**: Successive calls with same min_category use sequential traversal (`fp_next_slot` is planned for future)

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
| FSM Tree Search  | 3      | Candidate found, root early-exit, sequential traversal   |
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
2. **Performance**: PostgreSQL-style binary max-tree with `fp_next_slot` reserved for future concurrent inserts and avoids contention
3. **Modularity**: Clear Project 6/10 separation; FSM fork is a pure sidecar with no heap-page overhead
4. **Persistence**: FSM fork treated as a hint - rebuilt from heap on crash without WAL; header survives crashes
5. **Testing**: 24 comprehensive tests covering tree invariants, search, insertion and persistence

