# Free Space Manager (FSM) - Deep Dive & Integrations

**Project:** 6. Free Space Manager and Heap File Manager  
**Date:** 22th April, 2026  
**Scope:** FSM tree mechanics, integration APIs, and operational guarantees

---

## 1. FSM Tree Structure Overview

### 1.1 The Dynamic 3-Level Binary Max-Tree

RookDB uses a **PostgreSQL-style binary max-tree** to efficiently track free space availability across all heap pages. The tree grows dynamically in height (levels) as the heap expands to avoid unnecessary overhead and sparse files:

#### Initial Phase: Level 0 Only (0-4000 pages)
- **Structure:** The FSM starts with only **1 level (Level 0)**.
- **Coverage:** A single Level-0 page tracks the free space of up to 4000 heap pages.
- **Why?** For small tables, introducing Level 1 and 2 is pure overhead. This single page acts as the root and leaf simultaneously.

#### Two-Level Phase: Levels 0 and 1 (4,001 - 16,000,000 pages)
- **Structure:** When the 4001st heap page is added, the FSM introduces a second Level-0 page AND a new **Level-1 root page**.
- **What Each Page Stores:** 
  - The Level-1 page stores a binary max-tree pointing to the Level-0 pages.
  - The Level-0 pages continue tracking individual heap pages.

#### Three-Level Phase: Levels 0, 1, and 2 (> 16,000,000 pages)
- **Structure:** Reaching 16 million pages triggers the creation of a **Level-2 root page**.
- **Coverage:** Up to ~64 billion heap pages ( practical limit ).
- **Roles:** Level-2 points to Level-1, Level-1 points to Level-0, Level-0 points to heap pages.

### 1.2 Visual Tree Diagrams

**FSM Level Tree Structure:**
![FSM Level Tree Structure](./FSM_Level_Tree_Structure.png)

**FSM Page Layout:**
![FSM Page Layout](./FSM_Page_Layout.png)

**FSM Insertion:**
![FSM Insertion](./FSM_Insertion.png)

**Key Points:**
- Each node in a Level-1 or Level-0 page is 1 byte (8-bit unsigned integer 0–255)
- The root (`tree[0]`) of any page = max of all descendants
- Leaves directly store free-space categories for heap pages
- All pages are 8 KB, containing a binary tree array of up to 4,080 bytes

### 1.3 Binary Max-Tree Properties

**For any parent node in the tree:**
```
parent = max(left_child, right_child, right_child.right, ...)
```

**Example:**
```
         200              (parent = max(180, 220))
        /   \
      180   220           (left and right children)
      / \   / \
    150 180 200 220       (grandchildren, etc.)
```

When you update a leaf node, the change **bubbles up** through all ancestors until the value stabilizes. This ensures the root always reflects the true maximum in the tree.

---

## 2. How Quantization Works (0–255 Scale)

### 2.1 Converting Bytes to Categories

The FSM does NOT store the exact number of free bytes per page. Instead, it quantizes free space into one of 256 categories (0–255):

**Formula:**
```
category = floor(free_bytes / 32) (max 255)
```

**For an 8 KB page:**
```
category = floor(free_bytes /32 ) (max 255)
```

**Resolution:** Each category step represents ~32 bytes (8192 / 256 ≈ 32)

### 2.2 Quantization Table (8 KB Page)

| Free Bytes | Calculation | Category | Interpretation |
|------------|-------------|----------|------------------|
| 0          | 0 / 32 = 0 | 0 | Page is full |
| 500        | 500 / 32 ≈ 15.6 → 15 | 15 | ~2% free |
| 1,024      | 1024 / 32 = 32 | 32 | ~12.5% free |
| 2,048      | 2048 / 32 = 64 | 64 | ~25% free |
| 4,096      | 4096 / 32 = 128 | 128 | ~50% free |
| 7,000      | 7000 / 32 ≈ 218.8 → 218 | 218 | ~85.5% free |
| 8,159      | 8159 / 32 ≈ 254.9 → 254 | 254 | ~99.2% free |
| 8,176      | 8176 / 32 ≈ 255.5 → 255 | 255 | ~99.8% free (nearly empty) |
| 8,184      | 8184 / 32 = 255.75 → 255 | 255 | Page is completely empty |

### 2.3 Why Quantization?

1. **Memory Efficiency:** 1 byte per page vs. 4–8 bytes if storing exact free bytes
   
2. **Search Efficiency:** Binary search is faster on quantized categories
   - "Find any page with ≥100 bytes free" → "Find any page with category ≥12"
   - Exact byte counts don't matter, only the relative ordering

3. **Simplicity:** No need to track exact free space after every insert
   - Approximation is good enough for page selection
   - More exact tracking happens during insertion validation

---

## 3. FSM Tree Construction & Initialization

### 3.1 First-Time Tree Construction

**When is the FSM built?**
1. When a new table is created → `FSM::build_from_heap(heap_path)`
2. When the `.fsm` file is missing but heap exists → Rebuild automatically
3. When recovering from a crash → Rebuild from heap file

**Step-by-Step Construction:**

```
build_from_heap(heap_path):
  │
  ├─ Open heap file
  ├─ Read Page 0 (HeaderMetadata) → get page_count
  │
  ├─ Create or open <table>.fsm fork file
  │
  ├─ For each heap page 1..page_count:
  │   ├─ Read heap page
  │   ├─ Calculate free_bytes = upper - lower
  │   ├─ Quantize: category = floor(free_bytes × 255 / PAGE_SIZE)
  │   └─ Store in Level-0 FSM leaf slot
  │
  ├─ For each Level-0 FSM page:
  │   ├─ Bubble up: compute parent nodes as max(children)
  │   ├─ Write updated Level-0 page to disk
  │   └─ Mark parents dirty for propagation
  │
  ├─ For each Level-1 FSM page:
  │   ├─ Bubble up: recompute parents
  │   ├─ Write updated Level-1 page to disk
  │   └─ Pass updated root to Level-2
  │
  ├─ Update Level-2 (root) FSM page
  │   ├─ Recompute root as max(all Level-1 children)
  │   └─ Write to disk
  │
  └─ Return FSM handle with initialized state
```


### 3.2 How Pages Are Linked Together

The FSM uses **logical addressing**, not physical pointers:

**Level Mapping:**
```
Logical Address (level, page_no) → Physical Disk Block Offset
```

**Logical Coordinate Calculation:**
```
For a heap page H:
  level_0_page_no = H / FSM_SLOTS_PER_PAGE             (≈ H / 4000)
  slot_in_l0      = H % FSM_SLOTS_PER_PAGE             (≈ H % 4000)
  
For a Level-1 page tracking L0:
  level_1_page_no = level_0_page_no / FSM_SLOTS_PER_PAGE
  slot_in_l1      = level_0_page_no % FSM_SLOTS_PER_PAGE

For a Level-2 page tracking L1:
  level_2_page_no = level_1_page_no / FSM_SLOTS_PER_PAGE
  slot_in_l2      = level_1_page_no % FSM_SLOTS_PER_PAGE
```

**Physical File Layout Calculation:**
To avoid sparse 3-level padding for small heaps, RookDB stores pages contiguously by active levels based on `heap_page_count`.

For example, when `heap_page_count <= 4000` (1 level tree):
```
Physical block = page_no (Level 0 only)
```
When `heap_page_count <= 16,000,000` (2 level tree):
```
L0_count = ceil(heap_page_count / 4000)
L1_count = ceil(L0_count / 4000)

If Level == 1: Physical block = page_no
If Level == 0: Physical block = L1_count + page_no
```
This formula dynamically packs the file tightly, completely eliminating huge padding bytes normally required for a deep static tree.

**No Physical Pointers:** Pages don't store file offsets or addresses. Instead, the address is computed deterministically from the page number and overall heap size.

---

## 4. Future Work: The `fp_next_slot` Variable for Sequential Slot Tracking

### 4.1 What `fp_next_slot` Will Do 

**Clarification:** `fp_next_slot` will be used to **avoid rescanning from slot 0 every time** when searching for available pages. It provides a **sequential tracking hint**, NOT load balancing. 

Currently, our max-tree correctly directs us to the available space without rescanning, starting its search from the root node. However, keeping track of the last successfully filled spot (`fp_next_slot`) can further optimize search speed when sequentially filling many tuples into a single newly allocated page. 

**Future Data Structure Adjustment:**
```rust
pub struct FSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],  // 4,080 bytes: binary max-tree
    // fp_next_slot: u16,            // track index for faster intra-page sequential inserts
}
```

### 4.2 Handling Intra-Page Searches

**For the future:** We could attempt finding space beginning at `fp_next_slot` via a localized sub-tree traversal or scan, reducing array index navigation steps on hot inserts.

---

## 5. Sequential Insertion Logic: Greedy Max Binary Tree Routing

### 5.1 The Left-Preferred Greedy Search

When `fsm_search_avail(min_category)` needs to find a heap page with enough free space, it traverses the binary max-tree top-down (root to leaf). At each internal node, the search algorithm dynamically looks at both left and right children:

```rust
while idx < FSM_NON_LEAF_NODES {
    let left = 2 * idx + 1;
    let right = 2 * idx + 2;

    if left < FSM_NODES_PER_PAGE && fsm_page.tree[left] >= min_category {
        idx = left;     // Greedily prefer the left path!
    } else if right < FSM_NODES_PER_PAGE && fsm_page.tree[right] >= min_category {
        idx = right;    // Fallback to the right path
    } else {
        break; // Should not occur since root indicated space is available
    }
}
```

### 5.2 Why "Greedy Left" Causes Sequential Fills

Because the search checks the **left child first** and takes that path if space is available, it inherently directs new database inserts to the lowest possible leaf index (the lowest Page ID). 

1. **Initial Pages First:** The FSM will continuously direct tuples to Page 1 until Page 1’s free space drops below the requested size category.
2. **Spill Over:** Once the left child's maximum space falls below `min_category`, the search branches to the right child (Page 2), and begins filling it.
3. **No Wasted Logic:** This means we achieve dense, sequential data packing without manually maintaining pointers or arrays of active pages.

### 5.3 Contiguous Free Space Only

**Key Principle:** RookDB **only inserts into the last contiguous free space** between `upper` and `lower` pointers.

**Why This Approach?**

1. **No Inline Compaction:** Compacting a page during `INSERT` is expensive (memory shifts, cache misses)
2. **FSM Simplicity:** Only track space that is 100% ready to use (contiguous)
3. **Insertion Guarantees:** Once FSM selects a page, insertion is guaranteed to succeed

### 5.2 Insertion Flow (Sequential Within Page)

```
insert_tuple(tuple_data):
  │
  ├─ Calculate min_category needed for tuple size
  │   min_category = ceil((tuple_size + 8) × 255 / PAGE_SIZE)
  │
  ├─ Call fsm_search_avail(min_category)
  │   └─ Returns Some(page_id) or None
  │
  ├─ If None: Allocate new page, use that
  │
  ├─ Read heap page
  │
  ├─ Check contiguous free space: (upper - lower)
  │   └─ If (upper - lower) >= (tuple_size + 8):
  │       ├─ Write tuple data at position [upper - tuple_size]
  │       ├─ Create slot entry at [lower]
  │       ├─ Update upper -= tuple_size, lower += 8
  │       └─ Success ✓
  │   └─ Else:
  │       ├─ FSM gave wrong category (due to fragmentation)
  │       ├─ Update FSM with true contiguous free space
  │       └─ Retry with different page
  │
  └─ Write page back to disk, update FSM
```

### 5.3 Why Sequential, Not Scattered?

**Sequential fills pages until full:**
```
Page 1:  [Header] [Slot Dir] .......... [Tuple 1][Tuple 2]
                    ↑                         ↑
                  lower                    upper
                  
Page 1 After 100 inserts:
[Header] [Slot Dir (100 entries)] [Tuple 1-100 packed at end]
         ↑                              ↑
        lower=808                    upper=300 (nearly full!)
        
Once lower ≈ upper, page is FULL → FSM marks category = 0

Next insert searches FSM → finds Page 2 (category > 0)
[Header] [Slot Dir] .......... [Tuple 101][Tuple 102]
```

**Guarantee:** The FSM ensures **sequential fill with minimal wasted seeks**.

### 5.4 The 3-Attempt Insertion Algorithm

Due to fragmentation, insertions use a **3-attempt retry strategy**:

**Attempt 1: Trust FSM's suggestion**
```rust
if let Some(page_id) = fsm_search_avail(min_category) {
    if insert_into_page(page_id, tuple_data).is_ok() {
        return Ok((page_id, slot_id));  // Success!
    }
    // Insertion failed: page has internal fragmentation
    // Update FSM with true contiguous free space
    fsm_set_avail(page_id, actual_free_bytes);
}
```

**Attempt 2: Search again with updated FSM**
```rust
if let Some(page_id2) = fsm_search_avail(min_category) {
    if insert_into_page(page_id2, tuple_data).is_ok() {
        return Ok((page_id2, slot_id));  // Success!
    }
}
```

**Attempt 3: Allocate a brand new page**
```rust
let new_page_id = allocate_new_page();
insert_into_page(new_page_id, tuple_data)
    .expect("Brand new page must have space");
Ok((new_page_id, slot_id))
```

**Why 3 attempts?**
1. Handles 99% of inserts in one disk read (Attempt 1 succeeds)
2. Fallback for fragmentized pages (Attempt 2)
3. Emergency allocation if all tracked pages are fragmented (Attempt 3)

---

## 6. FSM Updates: Delete, Update, and Vacuum

### 6.1 When Tuples Are Deleted

**Heap-Level Delete:**
```rust
delete_tuple(page_id, slot_id):
  │
  ├─ Read page
  ├─ Zero out slot entry at slot_id
  │   (This leaves "dead space" in the slot directory)
  ├─ Mark tuple data as unused
  │   (This leaves "dead space" in the tuple area)
  ├─ Write page back
  │
  └─ FSM is NOT automatically updated (no "hole" tracking yet)
```

**Current FSM Behavior:**
- FSM still reports old contiguous free space
- New inserts ignore the dead space (no compaction during INSERT)
- Dead space remains until Project 10 (VACUUM/Compaction)

### 6.2 Updating FSM After Compaction (Project 10)

**When the Compaction Team Reorganizes a Page:**

```
Project 10 Compaction:
  │
  ├─ Read fragmented page with holes
  ├─ Memory shift: compact all tuples to one end
  ├─ Consolidate slots: rebuild slot directory
  ├─ Result: one large contiguous free space
  │
  └─ Call update_page_free_space(page_id, reclaimed_bytes)
      ├─ Update FSM: fsm_set_avail(page_id, reclaimed_bytes)
      ├─ Bubble up changes through all ancestor nodes
      └─ Next insert sees this page as available again!
```

---

## 7. Contiguous vs. Total Free Space

### 7.1 Current Phase: Contiguous Free Space Only

**Definition:** Contiguous free space = `upper - lower` (the gap between slot directory and tuple data)

**What FSM Tracks:**
```
page_free_bytes = upper - lower

Example Page:
  Offset 0-7:       [Header: lower=400, upper=7500]
  Offset 8-407:     [Slot Directory: 50 slots × 8 bytes = 400 bytes]
  Offset 408-7498:  [Dead space & compressed tuples]
  Offset 7499-8191: [Available space for new data: 692 bytes]
  
Contiguous free = 7500 - 400 = 7100 bytes ← What FSM tracks
Total free = 7100 + (dead_space from deletions) ← What FSM ignores
```

**Why Only Contiguous?**
1. **Heap Manager cannot compact during INSERT** → Cost is too high
2. **If FSM tracked total space:** It would route inserts to fragmented pages where the tuple won't fit
3. **Result:** Failed inserts + wasted disk reads + retry overhead

### 7.2 Future Phase: Total Free Space with On-the-Fly Compaction

**When Project 10 is enabled:**

```
Postgres-Style Compaction:

If insert fails due to fragmentation:
  │
  ├─ Detect: true contiguous space < tuple_size
  │           but total space (including holes) >= tuple_size
  │
  ├─ Trigger inline compaction:
  │   ├─ Memory shift: move all tuples down
  │   ├─ Consolidate slots
  │   └─ Rebuild as one contiguous region
  │
  ├─ Now insert succeeds in the new contiguous space
  │
  └─ FSM updates to reflect true total space
```

**Trade-off:**
- **Now:** Fast inserts, waste fragmented space, need vacuum
- **Future:** Slower inserts (occasional compaction), zero wasted space, optional vacuum

---

## 8. FSM Functions: Detailed Operation Reference

### 8.1 `FSM::build_from_heap(heap_file, fsm_path)`

**Purpose:** Initialize or rebuild FSM from an open heap file

**Signature:**
```rust
pub fn build_from_heap(heap_file: &mut File, fsm_path: PathBuf) -> io::Result<Self>
```

**Inputs:**
- `heap_file`: Mutable reference to an open `<table>.dat` file
- `fsm_path`: Path to `<table>.dat.fsm` file to create/update

**Outputs:**
- Initialized `FSM` struct with loaded `.fsm` fork

**Side Effects:**
- Truncates and overwrites `<table>.dat.fsm` if it exists.
- Scans all heap pages (expensive O(N) operation) reading only the header bytes per page to calculate valid offsets.
- Builds an in-memory mapped hash structure before committing all tree structures cleanly to disk.

**When Called:**
- Table creation
- FSM rebuild after crash
- `.fsm` file corruption/missing

### 8.2 `FSM::fsm_search_avail(min_category)`

**Purpose:** Find a heap page with sufficient free space for insertion

**Signature:**
```rust
pub fn fsm_search_avail(&mut self, min_category: u8) -> io::Result<Option<(u32, FSMPage)>>
```

**Inputs:**
- `min_category`: Minimum required free-space category (0–255)

**Outputs:**
- `Some((page_id, fsm_page))`: Found a suitable page. Returns the modified Level-0 FSM page alongside the ID to save I/O during eventual updates.
- `None`: No page has enough free space (need to allocate)

**Algorithm (O(log M) disk reads max):**

```
1. Determine active root level:
   ├─ Based on heap_page_count, establish whether to read Level 0, 1, or 2 as root.

2. Start recursive search `search_tree_for_available_page(root_level, page_no)`:
   ├─ Read active root FSM page.
   ├─ If root_value < min_category: return None.

3. Traverse active FSM pages:
   ├─ Sequentially scan children preferring the left branch (greedy tree).
   ├─ Drill down Level_N → Level_N-1.

4. Reach Level-0 leaf page:
   ├─ Compute starting heap_page_id = fsm_page_no × FSM_SLOTS_PER_PAGE.
   ├─ Check leaf. If `heap_page_id == 0`, skip (page 0 is a DB header page).
   
5. Return Some((heap_page_id, Level0_FSMPage))
```

**Guarantees:**
- Returns a page whose category >= `min_category`.
- Does NOT guarantee the page will have enough contiguous space (due to internal page fragmentation in the database).
  - → Insertion may still fail (handled by 3-attempt retry).

**I/O Cost:**
- **Best case:** 1-3 disk reads (depending on tree depth and cache).

### 8.3 `FSM::fsm_set_avail(heap_page_id, new_free_bytes, cached_page)`

**Purpose:** Update FSM after insertion or deletion changes available space

**Signature:**
```rust
pub fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32, cached_page: Option<&mut FSMPage>) -> io::Result<()>
```

**Inputs:**
- `heap_page_id`: Which heap page changed
- `new_free_bytes`: New contiguous free space (bytes)
- `cached_page`: Optional Level-0 cached FSM page returned from `fsm_search_avail` (saves a disk read)

**Outputs:**
- FSM tree updated; changes propagated to disk if necessary

**Algorithm:**

```
1. Quantize: category = floor(new_free_bytes / 32)

2. Locate Level-0 FSM page:
   ├─ fsm_page_no = heap_page_id / FSM_SLOTS_PER_PAGE
   ├─ slot_in_l0 = heap_page_id % FSM_SLOTS_PER_PAGE

3. Retrieve Level-0 FSM page:
   ├─ Use `cached_page` if provided, else read from disk.

4. Check existing category:
   ├─ If tree[leaf_index] == category:
   │   └─ Return early (Category unchanged, no FSM writes needed)

5. Update leaf: tree[leaf_index] = category

6. Bubble up strictly within Level-0 page:
   ├─ Recompute maximum paths backwards up to the root of Level-0.
   ├─ Write updated Level-0 FSM page to disk.

7. Parent Propagation:
   ├─ If the Level-0 root changed, propagate the new maximum upwards.
   ├─ Read parent (Level-1), bubble up. If root changed, read Level-2, etc.
```
   │   ├─ old_value = tree[parent_index]
   │   ├─ new_value = max(tree[left_child], tree[right_child])
   │   ├─ If new_value != old_value:
   │   │   └─ tree[parent_index] = new_value
   │   │       (parent changed, continue bubbling)
   │   └─ Else:
   │       └─ Stop (no further ancestors change)

8. Write updated Level-0 FSM page to disk

9. All FSM pages updated; tree is consistent
```

**I/O Cost:**
- **Typical:** 1–3 disk writes (Level 0, Level 1, Level 2 pages) depending on whether the highest local roots changed.
- **Why:** Bubble-up only modifies ancestors whose values actually change. Short-circuiting keeps edits localized.

### 8.4 `FSM::fsm_vacuum_update(heap_page_id, reclaimed_bytes)`

**Purpose:** Wrapper called by Project 10 (Compaction) to update FSM after reclaiming space

**Signature:**
```rust
pub fn fsm_vacuum_update(&mut self, heap_page_id: u32, reclaimed_bytes: u32) -> io::Result<()>
```

**Inputs:**
- `heap_page_id`: Page that was just compacted
- `reclaimed_bytes`: Newly available contiguous space

**Behavior:**
- Delegates to `fsm_set_avail(heap_page_id, reclaimed_bytes, None)`
- Triggers bubble-up as normal
- **No special handling:** Treated as a regular free-space update

---

## 9. Compaction Team Integration APIs

The Heap Manager exposes **3 high-level APIs** for the Compaction Team (Project 10) to use when reorganizing pages:

### 9.1 API 1: `insert_raw_tuple`

**Purpose:** Insert a tuple without going through normal FSM search (for tuple relocation)

**Signature:**
```rust
pub fn insert_raw_tuple(db_name: &str, table_name: &str, tuple_data: &[u8]) 
    -> io::Result<(u32, u32)>
```

**Use Case:**
- Moving a tuple from one page to another during compaction
- Don't search FSM; just find any page with space and insert

**Example:**
```
Compaction workflow:
  │
  ├─ Read tuple from fragmented page X
  ├─ Call insert_raw_tuple(..., tuple_bytes)
  │   └─ Internally searches FSM for available page
  │   └─ Inserts tuple into that page
  ├─ Call delete_tuple(X, slot_id)
  │   └─ Marks original slot as unused
  └─ Result: Tuple moved, fragmentation reduced
```

### 9.2 API 2: `update_page_free_space`

**Purpose:** Notify FSM that a page's contiguous free space has changed

**Signature:**
```rust
pub fn update_page_free_space(db_name: &str, table_name: &str, page_id: u32, 
    reclaimed_bytes: u32) -> io::Result<()>
```

**Use Case:**
- After in-place compaction (same page reorganized)
- Updating FSM with newly available contiguous space

**Example:**
```
Compaction workflow:
  │
  ├─ Read fragmented page P
  ├─ Compact tuples in-place (memory shift)
  ├─ Consolidate slots
  ├─ Measure new contiguous free space: 5,000 bytes
  ├─ Call update_page_free_space(..., page_id, 5000)
  │   └─ FSM updates: category = floor(5000 × 255 / 8192) = 156
  │   └─ Bubble-up propagates to all ancestors
  └─ Next insert sees page P as available again
```

### 9.3 API 3: `rebuild_table_fsm`

**Purpose:** Full table FSM rebuild (after large-scale reorganization or corruption)

**Signature:**
```rust
pub fn rebuild_table_fsm(db_name: &str, table_name: &str) -> io::Result<()>
```

**Use Case:**
- After full table rewrite (e.g., all pages reorganized)
- Repairing corrupted FSM
- Performance optimization (periodic rebuild)

**Example:**
```
Compaction workflow:
  │
  ├─ Perform full-table reorganization:
  │   ├─ Compact every page in sequence
  │   ├─ Move fragmented tuples
  │   └─ Update each page's free space individually
  │
  ├─ Call rebuild_table_fsm(..., table_name)
  │   ├─ Opens heap file
  │   ├─ Scans all pages (O(N) operation)
  │   ├─ Rebuilds FSM from scratch
  │   ├─ Writes updated .fsm fork
  │   └─ Ensures FSM is accurate
  │
  └─ System is back to optimal state
```

---

## 10. Instrumentation & Monitoring

### 10.1 FSM Function Call Counters

RookDB tracks how many times each FSM function is called to verify correctness:

**Tracked Functions:**
- `fsm_search_avail`: How many times did we search for available pages?
- `fsm_search_tree`: How many tree traversals occurred?
- `read_fsm_page`: How many FSM pages read from disk?
- `write_fsm_page`: How many FSM pages written to disk?
- `serialize_fsm_page`: How many times did we encode an FSM page?
- `deserialize_fsm_page`: How many times did we decode an FSM page?
- `fsm_set_avail`: How many times did we update free-space categories?

### 10.2 Viewing Instrumentation Data

**Command:**
```bash
CHECK_HEAP <table_name>
```

**Example Output for 1 insert tuple:**
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

**Example Output for 5000 insert tuple:**
```
Total Heap Pages:  15
FSM Fork Pages:    1
Total Tuples:      5000

╔══════════════════════════════════════════════════════════════╗
║                    OPERATION METRICS                         ║
╠══════════════════════════════════════════════════════════════╣
║ FSM Operations:                                              ║
║  - fsm_search_avail:         5026 calls                      ║
║  - fsm_search_tree:          5026 calls                      ║
║  - fsm_read_page:            5052 calls                      ║
║  - fsm_write_page:           3450 calls                      ║
║  - fsm_serialize_page:       3450 calls                      ║
║  - fsm_deserialize_page:     5052 calls                      ║
║  - fsm_set_avail:            5013 calls                      ║
║  - fsm_vacuum_update:           0 calls                      ║
╠══════════════════════════════════════════════════════════════╣
║ Heap Operations:                                             ║
║  - insert_tuple:             5000 calls                      ║
║  - get_tuple:                   0 calls                      ║
║  - allocate_page:              13 calls                      ║
║  - write_page:               5000 calls                      ║
║  - read_page:                5000 calls                      ║
║  - page_free_space:             0 calls                      ║
╚══════════════════════════════════════════════════════════════╝
```


**How to Verify Correctness:**


---

## 11. Tree Variable (`tree`) Explanation

### 11.1 What the `tree` Variable Holds

**Definition:** `tree` is a byte array (0–255 values) representing the binary max-tree

**Structure:**
```rust
pub struct FSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],  // 4,080 bytes

}
```

**Layout within `tree[]`:**
```
Index 0:           Root node (max of entire subtree)
Index 1-3:         Level 1 of tree (parents of leaves)
Index 4-2039:      Leaves (direct free-space categories for heap pages)
Index 4000+:       Unused (padding to fill 8 KB page)
```

### 11.2 How `tree[]` is Indexed

**Example (FSM_SLOTS_PER_PAGE = 4000):**

```
tree[0]            = root
tree[1], tree[2]   = children of root
tree[3]..tree[6]   = children of tree[1] and tree[2]
...
tree[1020]..tree[2039] = leaves (free-space categories)
```

**Parent-Child Relationship:**
```
For any node at index I:
  left_child_index = 2*I + 1
  right_child_index = 2*I + 2
  parent_index = (I - 1) / 2  (integer division)
```

### 11.3 Example Tree State

```
heap_page_0 has 500 free bytes → category = 15
heap_page_1 has 8000 free bytes → category = 250
heap_page_2 has 1000 free bytes → category = 31
heap_page_3 has 2000 free bytes → category = 62

tree[1020] = 15   (heap page 0)
tree[1021] = 250  (heap page 1)
tree[1022] = 31   (heap page 2)
tree[1023] = 62   (heap page 3)

tree[510] = max(tree[1020], tree[1021]) = max(15, 250) = 250   (parent)
tree[511] = max(tree[1022], tree[1023]) = max(31, 62) = 62    (parent)

tree[255] = max(tree[510], tree[511]) = max(250, 62) = 250    (root)
```

When someone searches for a page with category >= 200:
```
Check tree[0] (root) = 250 >= 200 ✓
→ Traverse tree; find tree[1021] = 250 >= 200
→ Return heap_page_1
```

---

## 12. Summary: FSM Guarantees

| Property | Guarantee | Why |
|----------|-----------|-----|
| **Page Find Time** | O(log N) = O(\log N) | 3-level tree, constant height |
| **Page Found Correctness** | category >= requested | Binary max-tree bubble-up |
| **Contiguous Free Tracking** | Accurate within category resolution | Updated after every insert/delete |
| **Sequential Fills** | Pages fill in order, then wrap | Sequential traversal |
| **No Implicit Scanning** | FSM never scans all pages | O(log N) traversal only |
| **Crash Recovery** | FSM can be rebuilt from heap | Treated as hint, not durability critical |
| **Multi-Page Scalability** | Scales to 4M+ pages | Tree height stays constant |

---

## 13. Compaction Team Integration Checklist

- [ ] Call `insert_raw_tuple()` when moving tuples between pages
- [ ] Call `update_page_free_space()` after in-place compaction
- [ ] Call `rebuild_table_fsm()` after full-table rewrite
- [ ] Verify FSM counters match expected operation counts
- [ ] Test that compacted pages become available again for new inserts

---

### Future Work: Concurrent Access (fp_next_slot)

To optimize concurrent insertions and reduce contention on FSM pages, an `fp_next_slot` pointer could be introduced in the `FSMPage` struct as future work.

Currently, the FSM uses a purely greedy search to traverse the binary max-heap. All concurrent backends looking for free space traverse from the root downwards in exactly the same way, always finding the first block (leftmost branch usually) with enough space. This causes heavy contention, as multiple transactions might attempt to lock and insert tuples into the same heap page.

By adding `fp_next_slot`, we could keep track of where the last successful search ended or round-robin requests. 
For instance, a backend could start searching the tree from `fp_next_slot` instead of the root, effectively spreading out insertions across different heap pages, minimizing lock waits and maximizing write throughput. This would be implemented efficiently by caching the `fp_next_slot` hint, falling back to a full tree search only when no sufficient space is found from the hint onwards.

### Optimization :
- **Early Exit**: If the insert does not changes the category (e.g., from 15 to 15), we can skip the bubble-up entirely, This means for 95% of small inserts, fsm_set_avail will gracefully short-circuit, sparing you an unnecessary disk rewrite and bubble-up traversal.

### Future Work: Delayed Updates and Pinning

1. **Delayed Updates**: PostgreSQL typically doesn't update the FSM immediately after every single small insert. The FSM is mostly updated during VACUUM. We could implement similar delayed updates to reduce FSM write contention during burst inserts.
2. **Pinning and State Retention**: Instead of releasing the page after `fsm_search_tree`, the page should be kept "pinned" in memory if we know we are about to call `fsm_set_avail`. By holding the reference, we avoid needing to re-read or re-deserialize it to update the value, reducing the deserializations from 6 to 3.

