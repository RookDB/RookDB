# Heap Manager & Page Layout

## Overview

The Heap Manager is responsible for managing data storage at the page level. It handles tuple insertion, retrieval, deletion, and pagination using a slotted page architecture. The heap file stores all table data sequentially, with each page containing a fixed-size 8 KB block.

**Key Responsibilities:**
- Allocating and managing heap pages
- Inserting, deleting, and retrieving tuples
- Tracking free space via the FSM
- Maintaining 20-byte header metadata
- Integrating with the Free Space Manager for efficient page selection

---


## 1. Slotted Page Layout

### The Page Structure

Every heap page (except Page 0, which is metadata) follows the **slotted page format**, a space-efficient architecture that minimizes fragmentation and maximizes tuple density:

```
┌─────────────────────────────────────────────────────────────┐
│                     Page 0: Metadata                        │
│              (20-byte HeaderMetadata)                       │
│  - page_count (u32): Total heap pages                       │
│  - fsm_page_count (u32): Total FSM pages                    │
│  - total_tuples (u64): 64-bit tuple count                   │
│  - last_vacuum (u32): Reserved for future VACUUM tracking   │
└─────────────────────────────────────────────────────────────┘

Pages 1+: Slotted Pages (8192 bytes each)

┌──────────────────────────────────────────────────────────────┐
│ Header (8 bytes)                                             │
│  - lower (u32): Offset of next free slot in directory        │
│  - upper (u32): Start of free space for tuple data           │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Slot Directory (growing downward from offset 8)              │
│ - Each slot: 8 bytes = (offset: u32, length: u32)            │
│ - Slot 0 at offset 8, Slot 1 at offset 16, etc.              │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Contiguous Free Space                                        │
│ (gap between directory and tuple data)                       │
│ Size = upper - lower                                         │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│ Tuple Data Region (growing upward from PAGE_SIZE)            │
│ - Tuples stored sequentially from end of page                │
│ - T1: bytes [upper, upper+len1)                              │
│ - T2: bytes [upper+len1, upper+len1+len2)                    │
│ - Etc...                                                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Why Slotted Pages?

1. **Flexibility:** Supports variable-length tuples efficiently
2. **Directness:** Slot directory provides O(1) access to any tuple by slot ID
3. **Space Efficiency:** Only stores one length entry per tuple in the directory
4. **Compaction-Ready:** Logical slot IDs remain valid even if physical tuple data is rearranged 
---

## 2. Contiguous vs. Total Free Space

### Current Phase: Contiguous Free Space Only

RookDB's Heap Manager currently tracks **only the contiguous free space** between `lower` and `upper`. This is a deliberate architectural choice:

```
Contiguous Free Space = upper - lower
```

#### Example: Fragmentation & Holes

Consider a page with tuples and holes:

```
Before Deletion:
┌─ Header ─┐ Slot 0 │ Slot 1 │ Slot 2 │ ... free gap ... │ T3 │ T2 │ T1 │
                                                     ↑               
                                                  upper=100     
lower=24, upper=100, contiguous_free = 76 bytes

After Deleting T2 (mark Slot 1 as invalid):
┌─ Header ─┐ Slot 0 │ Slot 1* │ Slot 2 │ ... free gap ... │ T3 │ XX │ T1 │
                                                     ↑               
                                                  upper=100     
lower=24, upper=100, contiguous_free = 76 bytes  (UNCHANGED!)

Dead space exists but is NOT tracked:
- T2 data (50 bytes) is now "dead" but still occupies space
- FSM reports: 76 bytes contiguous free
- FSM does NOT report: 76 + 50 = 126 bytes total free
```

#### Why Not Track Total Free Space?

**Reason : Insertion Complexity**
- If FSM advertised "126 bytes total," an insert requiring 100 bytes would land on this page
- But only 76 contiguous bytes are available for insertion
- The system would need to:
  1. Compact the page in-place during INSERT
  2. Move tuples around to merge holes
  3. Update all slot offsets
- This is **too expensive** to do during every insertion

---

### Future Phase: Total Free Space with On-The-Fly Compaction

```
When a tuple is deleted:
  ├─ Mark slot as invalid
  └─ FSM tracks: total_free = 76 (contiguous) + 50 (hole) = 126

When an insert requires 100 bytes:
  ├─ FSM routes to this page (Category suggests 126 bytes)
  ├─ Heap Manager detects fragmentation
  ├─ Quick in-place compact (Postgres-style):
  │  └─ Shift T3 left to eliminate hole
  │  └─ Update Slot 2 offset
  ├─ Now contiguous free = 126 bytes ✓
  └─ Insert succeeds without retry
```

**Benefits:**
- Zero wasted space
- Simpler insertion logic (fewer retries)

**Tradeoff:**
- Slower insertions (occasional compaction overhead)
- More CPU usage during active updates

---

## 3. The 3-Attempt Insertion Algorithm

### Overview

`HeapManager::insert_tuple()` implements a **robust 3-attempt retry strategy**.

### Algorithm Flowchart

```
insert_tuple(tuple_data):
  │
  ├─ Calculate min_category:
  │   min_category = ceil(tuple_size × 255 / PAGE_SIZE)
  │
  ├─ ATTEMPT 1: Trust FSM Suggestion (99% success rate)
  │   │
  │   ├─ fsm_search_avail(min_category)
  │   │   └─ Returns Some(page_id) or None
  │   │
  │   ├─ If None: Skip to ATTEMPT 3 (no suitable page)
  │   │
  │   ├─ Try insert_into_page(page_id, tuple_data)
  │   │   └─ Check: upper - lower ≥ tuple_size?
  │   │
  │   ├─ If SUCCESS:
  │   │   ├─ Update FSM: fsm_set_avail(page_id, new_free_bytes)
  │   │   └─ Return (page_id, slot_id) ✓ DONE
  │   │
  │   └─ If FAIL (internal fragmentation):
  │       ├─ Add page_id to failed_pages set
  │       ├─ Update FSM with true free space
  │       └─ Continue to ATTEMPT 2
  │
  ├─ ATTEMPT 2: Retry with Corrected FSM (handles 99.9% of cases)
  │   │
  │   ├─ fsm_search_avail(min_category)
  │   │   (FSM now has corrected categories from Attempt 1)
  │   │
  │   ├─ If None: Skip to ATTEMPT 3
  │   │
  │   ├─ Try insert_into_page(new_page_id, tuple_data)
  │   │
  │   ├─ If SUCCESS:
  │   │   ├─ Update FSM
  │   │   └─ Return (page_id, slot_id) ✓ DONE
  │   │
  │   └─ If FAIL (entire FSM tree fragmented):
  │       └─ Continue to ATTEMPT 3
  │
  ├─ ATTEMPT 3: Allocate New Page (guaranteed success)
  │   │
  │   ├─ allocate_new_page()
  │   │   ├─ Append new page to heap file
  │   │   ├─ Update header.page_count
  │   │   ├─ Register with FSM (Category = 255, fully empty)
  │   │   └─ Return new_page_id
  │   │
  │   ├─ insert_into_page(new_page_id, tuple_data)
  │   │   (This CANNOT fail; page is brand new)
  │   │
  │   └─ Return (new_page_id, 0) ✓ GUARANTEED SUCCESS
  │
  └─ Error handling: If all attempts fail, propagate error
```


---

## 4. Backend Functions & API

### High-Level Functions (Public API)

#### `HeapManager::insert_tuple(tuple_data) -> (page_id, slot_id)`

```rust
pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> io::Result<(u32, u32)>
```

**Purpose:** Insert a tuple using the 3-attempt algorithm with FSM guidance

**Inputs:**
- `tuple_data`: The bytes to insert (variable-length)

**Outputs:**
- `(page_id, slot_id)`: Location of inserted tuple
- Error if all attempts fail (should never happen in practice)

**Implementation:**
1. Calculate `min_category` from tuple size
2. Attempt 1: `fsm_search_avail(min_category)` → insert
3. Attempt 2: Search again with corrected FSM
4. Attempt 3: `allocate_new_page()` → insert (guaranteed)
5. Update FSM with new page state
6. Increment `header.total_tuples`
7. Persist header to disk

---

#### `HeapManager::get_tuple(page_id, slot_id) -> Vec<u8>`

```rust
pub fn get_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<Vec<u8>>
```

**Purpose:** Retrieve a single tuple by coordinates

**Inputs:**
- `page_id`: Page number in heap file
- `slot_id`: Slot index within the page

**Outputs:**
- Tuple data as `Vec<u8>`
- Error if slot is invalid or tuple doesn't exist

**Implementation:**
1. Read heap page
2. Locate slot directory entry
3. Extract (offset, length) from slot
4. Read tuple from `page.data[offset..offset+length]`
5. Return tuple bytes

---

#### `HeapManager::delete_tuple(page_id, slot_id) -> u32`

```rust
pub fn delete_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<u32>
```

**Purpose:** Delete a tuple and update FSM

**Inputs:**
- `page_id`, `slot_id`: Location of tuple to delete

**Outputs:**
- Number of bytes freed

**Implementation:**
1. Read heap page
2. Mark slot as invalid (length=0, offset=0)
3. Decrement `header.total_tuples`
4. Recalculate page free space
5. Call `fsm_set_avail(page_id, new_free_bytes)`
6. Write page and header back to disk

**Side Effect:** Creates dead space (fragmentation) that can be recovered later via VACUUM

---

#### `HeapManager::scan() -> HeapScanIterator`

```rust
pub fn scan(&mut self) -> HeapScanIterator
```

**Purpose:** Create an iterator for sequential table scan

**Outputs:**
- Iterator yielding `(page_id, slot_id, tuple_data)`

**Implementation:**
1. Initialize iterator at page 1, slot 0
2. Lazily load pages on demand
3. Skip invalid slots (length=0)
4. Stop at last page (from header.page_count)


---

#### `HeapManager::allocate_new_page() -> u32`

```rust
fn allocate_new_page(&mut self) -> io::Result<u32>
```

**Purpose:** Extend heap file with a new empty page

**Outputs:**
- ID of new page

**Implementation:**
1. Compute `new_page_id = header.page_count`
2. Create empty `Page::new()`
3. Append page to heap file (extend file size)
4. Update `header.page_count += 1`
5. Check if FSM needs to grow; update `header.fsm_page_count` if needed
6. Register new page with FSM: `fsm_set_avail(new_page_id, PAGE_SIZE - 8)`
7. Persist header
8. Return `new_page_id`

---

### Low-Level Functions (Internal)

#### `insert_into_page(page_id, tuple_data) -> (page_id, slot_id)`

```rust
fn insert_into_page(&mut self, page_id: u32, tuple_data: &[u8]) -> io::Result<(u32, u32)>
```

**Purpose:** Insert tuple into a specific page (no FSM search)

**Behavior:**
- Direct insertion without FSM involvement
- Fails if page doesn't have enough contiguous space
- Used by Attempt 1, 2, and 3 of the insertion algorithm

**Implementation:**
1. Read page from disk
2. Check: `page.upper - page.lower ≥ tuple_data.len()`
3. If no: return `Err("Not enough space")`
4. Create slot directory entry: `(page.upper - tuple_data.len(), tuple_data.len())`
5. Write tuple data to `page.upper - tuple_data.len()`
6. Update page header: `page.lower += 8` (new slot takes 8 bytes)
7. Persist page to disk
8. Return `(page_id, slot_id)`

---

#### `page_free_space(page_id) -> u32`

```rust
fn page_free_space(&mut self, page_id: u32) -> io::Result<u32>
```

**Purpose:** Calculate contiguous free space in a page

**Implementation:**
1. Read page header
2. Return `page.upper - page.lower`

**Why Inline?**
- Simple calculation
- Called frequently
- No I/O needed

---

### Compaction Team Integration APIs (Public)

These 3 facade functions allow Project 10 (Compaction) to safely integrate:

#### 1. `insert_raw_tuple(db, table, tuple_data) -> (page_id, slot_id)`

```rust
pub fn insert_raw_tuple(
    db_name: &str,
    table_name: &str,
    tuple_data: &[u8]
) -> io::Result<(u32, u32)>
```

**Purpose:** Insert a tuple without going through the normal 3-attempt algorithm

**Use Case:** Relocating a tuple during page compaction

**Example:**
```rust
// Compaction reads a tuple from a fragmented page
let tuple_data = get_tuple(page_5, slot_2)?;

// Relocate it to a better location
let (new_page, new_slot) = insert_raw_tuple("mydb", "users", &tuple_data)?;

// Update any indexes/references to point to (new_page, new_slot)
```

---

#### 2. `update_page_free_space(db, table, page_id, reclaimed_bytes) -> ()`

```rust
pub fn update_page_free_space(
    db_name: &str,
    table_name: &str,
    page_id: u32,
    reclaimed_bytes: u32
) -> io::Result<()>
```

**Purpose:** Notify FSM that a page gained free space (after compaction)

**Use Case:** After in-place compaction, convert dead space into contiguous free space

**Example:**
```rust
// Before compaction: Page 5 has 76 bytes contiguous + 50 bytes dead
// After compaction: Page 5 has 126 bytes contiguous (merged)

update_page_free_space("mydb", "users", 5, 126)?;
// FSM updates Category from 2 to 4
```

---

#### 3. `rebuild_table_fsm(db, table) -> ()`

```rust
pub fn rebuild_table_fsm(
    db_name: &str,
    table_name: &str
) -> io::Result<()>
```

**Purpose:** Full FSM rebuild after large-scale reorganization

**Use Case:** After extensive compaction/VACUUM of multiple pages

**Example:**
```rust
// After compacting 1000 pages:
rebuild_table_fsm("mydb", "users")?;
// Scans all heap pages, recalculates all FSM categories
// Ensures FSM accuracy
```

---

## 8. Data Structures
---

### `HeaderMetadata` Structure

```rust
#[derive(Debug, Clone, Copy)]
pub struct HeaderMetadata {
    pub page_count: u32,      // Total heap pages allocated
    pub fsm_page_count: u32,  // Total FSM pages
    pub total_tuples: u64,    // 64-bit tuple count for COUNT(*)
    pub last_vacuum: u32,     // Reserved for VACUUM tracking
}
```

**Located on:** Page 0 (first 20 bytes of heap file)

**Persistence:** Updated atomically after:
- Each `insert_tuple()` (total_tuples incremented)
- Each `delete_tuple()` (total_tuples decremented)
- Each `allocate_new_page()` (page_count incremented)

---

### `HeapManager` Structure

```rust
pub struct HeapManager {
    file_path: PathBuf,
    file_handle: File,
    fsm: FSM,
}
```

**Ownership:** HeapManager owns the FSM instance for this table

**Initialization:**
```rust
let mut hm = HeapManager::open(table_path)?;
// Internally calls FSM::build_from_heap() to recover FSM state
```

---

### `HeapScanIterator` Structure

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
    
    fn next(&mut self) -> Option<Self::Item> {
        // Iterate pages 1 to total_pages
        // Yield (page_id, slot_id, tuple_data) for valid slots
        // Skip deleted slots (length==0)
    }
}
```

---

## 9. Deletion & FSM Updates

### Deletion Workflow

```
HeapManager::delete_tuple(page_id, slot_id):
  │
  ├─ Read page from disk
  │
  ├─ Read slot entry: (offset, length)
  │   └─ Validate: length > 0 (slot is valid)
  │
  ├─ Mark slot as deleted:
  │   └─ Write (0, 0) to slot entry
  │
  ├─ Decrement header.total_tuples
  │
  ├─ Calculate page free space:
  │   └─ new_free = page.upper - page.lower
  │       (dead space from deleted tuple not tracked)
  │
  ├─ Update FSM:
  │   └─ fsm_set_avail(page_id, new_free)
  │       └─ Bubble-up changes through tree
  │
  ├─ Write page to disk
  │
  ├─ Write header to disk
  │
  └─ Return freed_bytes
```

### FSM Bubble-Up After Deletion

When free space increases (deletion):

```
Page 47 freed 100 bytes:
  ├─ Old FSM Category: 2 (≈64 bytes advertised)
  ├─ New free: 164 bytes
  ├─ New FSM Category: 5 (≈160 bytes)
  │
  ├─ FSM Level-0 leaf updated: tree[47] = 5
  │   └─ Parent recalculated: max(left, right)
  │   └─ If parent changed, mark parent dirty
  │
  ├─ FSM Level-1 internal updated
  │   └─ Propagate upward if root changed
  │
  ├─ FSM Level-2 root updated
  │   └─ If root increased, page becomes more searchable
  │
  └─ Next fsm_search_avail() may return this page
```

---

## 10. Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `insert_tuple()` | O(log N) avg | 3 attempts, each O(log N) for FSM search |
| `get_tuple()` | O(1) | Direct page read + slot lookup |
| `delete_tuple()` | O(log N) | FSM bubble-up propagation |
| `allocate_new_page()` | O(1) | Append-only, O(log N) for FSM registration |
| `scan()` | O(N) | N = number of pages |

### I/O Complexity

| Operation | Disk Reads | Disk Writes |
|-----------|-----------|-------------|
| Insert (Attempt 1, success) | 2 (1 FSM root + 1 heap page) | 2 (1 heap page + 1 header) |
| Insert (Attempt 1, fail + Attempt 2) | 4 | 3 |
| Delete | 2 (1 heap page + FSM bubble-up) | 2+ (heap + FSM propagation) |
| Scan (table of P pages) | P | 0 |

---


## 12. Integration with FSM

### Insert Flow with FSM

```
User calls: hm.insert_tuple(50_bytes)
  │
  ├─ Calculate min_category = ceil(50 × 255 / 8192) = 2
  │
  ├─ FSM::fsm_search_avail(2)
  │   ├─ Read FSM Level-0 root
  │   ├─ Compute heap_page_id from level-0 slot
  │   └─ Return Some(page_id=47)
  │
  ├─ insert_into_page(47, 50_bytes)
  │   ├─ Read heap page 47
  │   ├─ Check free_space: 76 ≥ 50? YES
  │   ├─ Write slot directory entry
  │   ├─ Write tuple data at upper offset
  │   ├─ Update page header (lower/upper)
  │   └─ Return (47, 3)
  │
  ├─ FSM::fsm_set_avail(47, 26)  // 76 - 50 = 26 bytes left
  │   ├─ Calculate new category: ceil(26 × 255 / 8192) = 1
  │   ├─ Update Level-0 leaf: tree[47] = 1
  │   ├─ Bubble-up: Level-0 internal parents
  │   ├─ Propagate to Level-1 and Level-2 if needed
  │   └─ Mark FSM pages dirty
  │
  ├─ Update header.total_tuples += 1
  │
  ├─ Persist header to disk
  │
  └─ Return (47, 3)
```

### Delete Flow with FSM

```
User calls: hm.delete_tuple(page_id=47, slot_id=2)
  │
  ├─ Read page 47 from disk
  │
  ├─ Mark slot 2 as invalid: (0, 0)
  │
  ├─ Recalculate page_free_space(47)
  │   └─ free = upper - lower = 76 bytes (dead space not included)
  │
  ├─ FSM::fsm_set_avail(47, 76)
  │   ├─ Calculate new category: ceil(76 × 255 / 8192) = 2
  │   ├─ Update Level-0 leaf: tree[47] = 2 (from previous 1)
  │   ├─ Bubble-up with new max values
  │   └─ Write modified FSM pages
  │
  ├─ Decrement header.total_tuples
  │
  ├─ Persist page and header to disk
  │
  └─ Return freed_bytes
```

---

## Summary

The Heap Manager implements a sophisticated page-oriented storage system that:

1. **Maximizes Insertion Speed:** 3-attempt algorithm with FSM guidance ensures 99% first-attempt success
2. **Minimizes Overhead:** Contiguous free space tracking keeps insertions simple and fast
3. **Handles Fragmentation Gracefully:** Dead space is acceptable now; Project 10 will optimize later
4. **Integrates with FSM:** Every heap operation updates the free space tree for future searches
5. **Provides Clean APIs:** Compaction team can safely integrate via 3 facade functions

The architecture balances **performance** (fast inserts) with **simplicity** (minimal per-insert logic), deferring optimization (compaction) to future phases.

### Advanced Optimization Details

**Phantom Yields for Deleted Tuples**:
- `HeapScanIterator::next()` ignores `offset == 0 && length == 0` during scans, incrementing `current_slot` automatically.
- `HeapManager::get_tuple()` handles retrieving dead tuples proactively by throwing `io::ErrorKind::NotFound`.

**Slot Directory Exhaustion & Tuple Leak**:
- `insert_into_page()` loops through `0..tuple_count` looking for a dead slot. If a slot void exists, it breaks early replacing the newly appended tuple over the dead slot id (`reused_slot_id`).
- Reduces unbounded index array expansion, saving `ITEM_ID_SIZE` bytes and subtracting solely real payload lengths from free space calculation.

**Tail Pointer Rollback Optimization (`delete_tuple`)**:
- **Data Reclaiming**: Rolls continuous space backwards manually (`upper += length`) if the deletion perfectly abuts the `upper` margin.
- **Slot Reclaiming**: Rolls the `lower` bound downwards (`lower -= ITEM_ID_SIZE`) obliterating dead slots if perfectly positioned at the tail end of the storage directory.
