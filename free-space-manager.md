# Free Space Manager (FSM) - Deep Dive & Integrations

**Project:** 6. Free Space Manager and Heap File Manager  
**Date:** 13th March, 2026  
**Scope:** FSM tree mechanics, integration APIs, and operational guarantees

---

## 1. FSM Tree Structure Overview

### 1.1 The 3-Level Binary Max-Tree

RookDB uses a **PostgreSQL-style 3-level binary max-tree** to efficiently track free space availability across all heap pages. Each level serves a distinct purpose:

#### Level 2 (Root)
- **How Many Pages:** Exactly **1 page**
- **What It Stores:** A single binary max-tree array that holds the maximum free-space category across the entire heap
- **Tree Structure:** `tree[0]` = root node = max(all descendants)
- **Coverage:** Up to ~64 billion heap pages (practical limit for most databases)

#### Level 1 (Internal/Intermediate)
- **How Many Pages:** Multiple pages (grows as heap grows)
- **Typical Range:** 1–2,000+ pages depending on heap size
- **What Each Page Stores:** A binary max-tree with up to 2,040 leaf nodes
- **What Each Leaf Represents:** Pointer to a Level-0 FSM page or heap page group
- **Tree Structure:** Each Level-1 page contains `tree[0]` (root of that page's subtree) and internal nodes pointing to children

#### Level 0 (Leaves)
- **How Many Pages:** Multiple pages (1 Level-0 page per 2,040 heap pages)
- **Example:** 1M heap pages → ~500 Level-0 FSM pages
- **What Each Page Stores:** Direct free-space categories (0–255) for heap pages
- **Mapping:** Each leaf slot in a Level-0 page corresponds to one heap page
  - Slot 0 → Heap page 0
  - Slot 1 → Heap page 1
  - ...
  - Slot 2,039 → Heap page 2,039

### 1.2 Visual Tree Diagram

```
┌─────────────────────────────────────────────────────────┐
│  Level 2 (Root): 1 page                                 │
│  ┌──────────────────────────────────────────────────────┐
│  │ tree[0] = 200  (max free-space category in heap)    │
│  │ tree[1] = 180  ┐                                     │
│  │ tree[2] = 150  │ Internal nodes (parents of L1)     │
│  └──────────────────────────────────────────────────────┘
└─────────────────────────────────────────────────────────┘
       │                           │
       │                           │
  ┌────▼─────────────┐       ┌─────▼──────────────┐
  │  Level 1: Page 0 │       │  Level 1: Page 1   │
  │ tree[0] = 180    │       │ tree[0] = 150      │
  │ tree[1] = 200    │       │ tree[1] = 145      │
  │ tree[2] = 175    │       │ tree[2] = 150      │
  └────┬──────┬──────┘       └─────┬──────┬───────┘
       │      │                    │      │
  ┌────▼──┐ ┌─▼────┐          ┌───▼──┐ ┌─▼────┐
  │ L0:0  │ │ L0:1 │          │ L0:2 │ │ L0:3 │
  │ 200   │ │ 150  │          │ 145  │ │ 150  │
  │ 180   │ │ 170  │          │ 120  │ │ 140  │
  │ 175   │ │ 155  │          │ 115  │ │ 150  │
  └───┬───┘ └──┬───┘          └───┬──┘ └──┬───┘
      │        │                  │       │
   [0-2,039] [2,040-4,079]   [4,080-6,119] [6,120-8,159]
   Heap Pages    Heap Pages      Heap Pages   Heap Pages
```

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
category = floor(free_bytes × 255 / PAGE_SIZE)
```

**For an 8 KB page:**
```
category = floor(free_bytes × 255 / 8192)
```

**Resolution:** Each category step represents ~32 bytes (8192 / 256 ≈ 32)

### 2.2 Quantization Table (8 KB Page)

| Free Bytes | Calculation | Category | Interpretation |
|------------|-------------|----------|------------------|
| 0          | 0 × 255 / 8192 = 0 | 0 | Page is full |
| 500        | 500 × 255 / 8192 ≈ 15.6 → 15 | 15 | ~2% free |
| 1,024      | 1024 × 255 / 8192 ≈ 31.9 → 31 | 31 | ~12.5% free |
| 2,048      | 2048 × 255 / 8192 ≈ 63.8 → 63 | 63 | ~25% free |
| 4,096      | 4096 × 255 / 8192 ≈ 127.5 → 127 | 127 | ~50% free |
| 7,000      | 7000 × 255 / 8192 ≈ 218.8 → 218 | 218 | ~85.5% free |
| 8,176      | 8176 × 255 / 8192 ≈ 254.4 → 254 | 254 | ~99.8% free (nearly empty) |
| 8,184      | 8184 × 255 / 8192 ≈ 255.0 → 255 | 255 | Page is completely empty |

### 2.3 Why Quantization?

1. **Memory Efficiency:** 1 byte per page vs. 4–8 bytes if storing exact free bytes
   - For 1M heap pages: 1 MB FSM overhead instead of 4–8 MB
   
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

**How Many Pages Are Created Initially?**

For a heap with `N` pages:
- **Level-0 FSM pages:** ⌈N / 2,040⌉
  - Example: 10,000 heap pages → 5 Level-0 pages
- **Level-1 FSM pages:** ⌈(N / 2,040) / 2,040⌉
  - Example: 10,000 heap pages → 1 Level-1 page
- **Level-2 FSM pages:** 1 (always)

### 3.2 How Pages Are Linked Together

The FSM uses **logical addressing**, not physical pointers:

**Level Mapping:**
```
Logical Address (level, page_no, slot) → Physical Disk Block
```

**Calculation:**
```
For a heap page H:
  level_0_page_no = H / FSM_SLOTS_PER_PAGE         (≈ H / 4000)
  slot_in_l0 = H mod FSM_SLOTS_PER_PAGE             (≈ H mod 4000)
  
For a Level-1 page tracking L0:
  level_1_page_no = level_0_page_no / FSM_SLOTS_PER_PAGE
  slot_in_l1 = level_0_page_no mod FSM_SLOTS_PER_PAGE

And so on up to Level-2.
```

**No Physical Pointers:** Pages don't store file offsets or addresses. Instead, the address is computed deterministically from the page number, enabling easy FSM reconstruction if files are corrupted.

---

## 4. The `next_slot` Variable: Sequential Slot Tracking

### 4.1 What `next_slot` Does (NOT Load Balancing)

**Clarification:** `next_slot` is used to **avoid rescanning from slot 0 every time** when searching for available pages. It provides a **sequential tracking hint**, NOT load balancing.

**Data Structure:**
```rust
pub struct FSMPage {
    tree: [u8; FSM_NODES_PER_PAGE],  // 4,080 bytes: binary max-tree

}
```

### 4.2 How `next_slot` Works

**Current Implementation (Sequential Within Page):**

```
FSM Search for a page:
  │
  ├─ Start at Level 2 root, read tree[0]
  ├─ Descend to Level 1:


  │       │   └─ Use this page
  │       └─ Else:
  │           └─ Wrap around and scan from 0
  │
  ├─ Reach Level 0 leaf:
  │   └─ Repeat sequential logic
  │
  └─ On successful find:

         (for next search, start from next sequential position)
```

**Example Trace:**

```
Page 1 Insert (searching for category ≥ 50):

  ├─ Check tree[0] = 60 ✓ >= 50 → Use this page


Page 2 Insert (same search):

  ├─ Check tree[1] = 45 ✗ < 50
  ├─ Check tree[2] = 55 ✓ >= 50 → Use this page


Page 3 Insert (same search):

  ├─ Check tree[3] = 40 ✗ < 50
  ├─ ... (wrap around, rescan from 0)
  ├─ Check tree[0] = 60 ✓ >= 50 → Use this page

```

### 4.3 Future Use: Concurrent Insert Optimization

**For Future Phases (Multi-Threaded Inserts):**



```




```

This would naturally spread load across pages without explicit coordination.

---

## 5. Sequential Insertion Logic: Why Pages Fill Sequentially

### 5.1 Current Phase: Contiguous Free Space Only

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

### 8.1 `FSM::build_from_heap(heap_path)`

**Purpose:** Initialize or rebuild FSM from heap file

**Signature:**
```rust
pub fn build_from_heap(heap_path: PathBuf) -> io::Result<Self>
```

**Inputs:**
- `heap_path`: Path to `<table>.dat` file

**Outputs:**
- Initialized `FSM` struct with loaded `.fsm` fork

**Side Effects:**
- Creates or overwrites `<table>.dat.fsm`
- Scans all heap pages (expensive O(N) operation)
- Computes and stores all free-space categories

**When Called:**
- Table creation
- FSM rebuild after crash
- `.fsm` file corruption/missing

### 8.2 `FSM::fsm_search_avail(min_category)`

**Purpose:** Find a heap page with sufficient free space for insertion

**Signature:**
```rust
pub fn fsm_search_avail(&mut self, min_category: u8) -> io::Result<Option<u32>>
```

**Inputs:**
- `min_category`: Minimum required free-space category (0–255)

**Outputs:**
- `Some(page_id)`: Found a suitable page
- `None`: No page has enough free space (need to allocate)

**Algorithm (O(log N) = O(\log N) disk reads):**

```
1. Read Level-2 (root) FSM page
   ├─ If tree[0] < min_category: return None (entire heap is full)
   
2. Traverse Level 2 → Level 1:
   ├─ For each child slot:
   │   ├─ If tree[slot] >= min_category:
   │   │   ├─ Search slots sequentially from 0
   │   │   ├─ If not available, try next sequential slot
   │   │   └─ Descend to Level 1 page
   │   
3. Traverse Level 1 → Level 0:
   ├─ Repeat same logic: find first child with category >= min_category

   
4. Reach Level-0 leaf page:
   ├─ Compute heap_page_id = (fsm_page_no × FSM_SLOTS_PER_PAGE) + slot

   ├─ Mark all visited FSM pages as dirty (for eventual flush)
   
5. Return Some(heap_page_id)
```

**Guarantees:**
- Returns a page whose category >= min_category
- Does NOT guarantee the page will have enough contiguous space (due to fragmentation)
  - → Insertion may still fail (handled by 3-attempt retry)

**I/O Cost:**
- **Best case:** 3 disk reads (all FSM pages cached)
- **Worst case:** 3 disk reads + 1 heap page read

### 8.3 `FSM::fsm_set_avail(heap_page_id, new_free_bytes)`

**Purpose:** Update FSM after insertion or deletion changes available space

**Signature:**
```rust
pub fn fsm_set_avail(&mut self, heap_page_id: u32, new_free_bytes: u32) -> io::Result<()>
```

**Inputs:**
- `heap_page_id`: Which heap page changed
- `new_free_bytes`: New contiguous free space (bytes)

**Outputs:**
- FSM tree updated; all changes propagated to disk

**Algorithm:**

```
1. Quantize: category = floor(new_free_bytes × 255 / PAGE_SIZE)

2. Locate Level-0 FSM page:
   ├─ level_0_page_no = heap_page_id / FSM_SLOTS_PER_PAGE
   ├─ slot_in_l0 = heap_page_id mod FSM_SLOTS_PER_PAGE

3. Read Level-0 FSM page

4. Update leaf: tree[leaf_index(slot_in_l0)] = category

5. Bubble up within Level-0 page:
   ├─ For each parent node:
   │   ├─ old_value = tree[parent_index]
   │   ├─ new_value = max(tree[left_child], tree[right_child])
   │   ├─ If new_value != old_value:
   │   │   └─ tree[parent_index] = new_value
   │   │       (parent changed, continue bubbling)
   │   └─ Else:
   │       └─ Stop (no further ancestors change)

6. Write updated Level-0 FSM page to disk

7. If Level-0 root changed:
   ├─ Iteratively propagate the new maximum category up to the parent nodes using a loop.
   │   (This propagates up to Level 1, then Level 2)

8. All FSM pages updated; tree is consistent
```

**I/O Cost:**
- **Typical:** 2–4 disk writes (Level 0, Level 1, Level 2 pages)
- **Why:** Bubble-up only modifies ancestors whose values actually change

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
- Delegates to `fsm_set_avail(heap_page_id, reclaimed_bytes)`
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

**Example Output:**
```
=== Instrumentation: users ===
fsm_search_avail calls:        1,234
fsm_search_tree calls:         1,234
read_fsm_page calls:           3,702
write_fsm_page calls:          2,105
serialize_fsm_page calls:      2,105
deserialize_fsm_page calls:    3,702
fsm_set_avail calls:           1,234

=== FSM Statistics ===
Total Heap Pages:              10,240
FSM Fork Pages:                8
FSM Root Value:                200/255
Avg Free Category:             120/255
Avg Disk I/O per Insert:       ~3.0 reads + ~2.0 writes
```

**How to Verify Correctness:**

1. **Check fsm_search_avail calls:** Should match number of successful inserts
2. **Check read/write balance:** Each search should trigger ~3 reads; each update ~2 writes
3. **Check root value:** If > 0, heap has free space; if = 0, heap is full
4. **Check average category:** Higher = more wasted space / fragmentation

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

**Example (FSM_SLOTS_PER_PAGE = 2,040):**

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

## 13. Checklists for Integration Teams

### 13.1 Compaction Team Integration Checklist

- [ ] Call `insert_raw_tuple()` when moving tuples between pages
- [ ] Call `update_page_free_space()` after in-place compaction
- [ ] Call `rebuild_table_fsm()` after full-table rewrite
- [ ] Verify FSM counters match expected operation counts
- [ ] Test that compacted pages become available again for new inserts

### 13.2 Query Executor Integration Checklist

- [ ] Inserts use `FSM::fsm_search_avail()` for page selection
- [ ] Sequential scans use `HeapScanIterator` (not random access)
- [ ] Deletion calls `delete_tuple()` which updates FSM
- [ ] Retry logic handles insertion failures from fragmentation

---

## References

- **Design Doc:** [design-doc.md](Design-Doc.md) for system overview
- **Heap Manager Doc:** [heap-manager.md](heap-manager.md) for page layout details
- **Tests:** [tests.md](tests.md) for FSM correctness verification
### Future Work: Concurrent Access (fp_next_slot)

To optimize concurrent insertions and reduce contention on FSM pages, an `fp_next_slot` pointer could be introduced in the `FSMPage` struct as future work.

Currently, the FSM uses a purely greedy search to traverse the binary max-heap. All concurrent backends looking for free space traverse from the root downwards in exactly the same way, always finding the first block (leftmost branch usually) with enough space. This causes heavy contention, as multiple transactions might attempt to lock and insert tuples into the same heap page.

By adding `fp_next_slot`, we could keep track of where the last successful search ended or round-robin requests. 
For instance, a backend could start searching the tree from `fp_next_slot` instead of the root, effectively spreading out insertions across different heap pages, minimizing lock waits and maximizing write throughput. This would be implemented efficiently by caching the `fp_next_slot` hint, falling back to a full tree search only when no sufficient space is found from the hint onwards.

