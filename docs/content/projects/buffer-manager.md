---
sidebar_position: 3
title: Buffer Manager
---

# Buffer Manager

The Buffer Manager is responsible for managing an in-memory cache of
database pages in order to minimize disk I/O and improve query
performance. It sits between the Page Layer and the underlying disk
storage and ensures that frequently accessed pages remain in memory.

This component is part of the **Storage Manager architecture of
RookDB**, which follows a layered design.

------------------------------------------------------------------------

# 1. Database and Architecture Design Changes

## 1.1 Position in Existing Architecture

RookDB follows a layered architecture consisting of:

-   Catalog Layer
-   Table Layer
-   Page Layer
-   Buffer Manager Layer

Originally, the **Page Layer directly interacted with disk storage**
through `read_page()` and `write_page()` APIs.

The Buffer Manager introduces an intermediate caching layer between the
Page Layer and disk.

### Architecture

    Table / Page Layer
            ↓
    Buffer Manager
    (Buffer Pool + Page Table)
            ↓
    Replacement Policy
    (LRU / Clock)
            ↓
    Disk Storage (.dat files)

The Buffer Manager intercepts all page access operations and decides
whether the page should be served from memory or loaded from disk.

------------------------------------------------------------------------

## 1.2 Page Layout Constraint

The Buffer Manager **does not modify the page structure**.

The existing **slotted page layout** remains unchanged.

The following components remain untouched:

-   Page header structure
-   Tuple storage format
-   Table file layout

This ensures full compatibility with the current Page Layer
implementation.

------------------------------------------------------------------------

## 1.3 Architectural Modifications

### 1.3.1 Disk Access Interception

All disk operations will be routed through the Buffer Manager.

The following APIs will be introduced:

    fetch_page()
    unpin_page()
    flush_page()
    flush_all_pages()

Direct calls to `read_page()` and `write_page()` will only occur
internally within the Buffer Manager.

------------------------------------------------------------------------

### 1.3.2 Logical Page Identification

Pages must be uniquely identifiable across multiple tables.

``` rust
pub struct PageId {
    pub table_name: String,
    pub page_number: u32,
}
```

This ensures global uniqueness for every page in the database.

------------------------------------------------------------------------

## 1.4 Justification of Design

The proposed Buffer Manager design provides several advantages:

-   **Scalability**\
    A fixed-size buffer pool supports large datasets without excessive
    memory usage.

-   **Performance**\
    Reduces disk I/O by caching frequently accessed pages and delaying
    writes through dirty-page tracking.

-   **Modularity**\
    Supports pluggable replacement policies such as LRU and Clock.

-   **Extensibility**\
    The architecture can be extended later to support WAL logging and
    concurrency control.

------------------------------------------------------------------------

## 1.5 Assumptions

The following assumptions are made for this implementation:

-   Fixed page size of **8 KB**
-   Static buffer pool size
-   Single-threaded execution environment
-   No crash recovery logging (WAL not implemented yet)

------------------------------------------------------------------------

# 2. Backend Data Structures

## 2.1 Data Structures to be Created

### 2.1.1 PageId

``` rust
pub struct PageId {
    pub table_name: String,
    pub page_number: u32,
}
```

**Purpose**

Unique identification of disk pages.

**Justification**

Multiple tables exist in the database, therefore page numbers alone are
insufficient.

------------------------------------------------------------------------

### 2.1.2 FrameMetadata

``` rust
pub struct FrameMetadata {
    pub page_id: Option<PageId>,
    pub dirty: bool,
    pub pin_count: u32,
    pub usage_count: u32,
    pub last_used: u64,
}
```

**Purpose**

Stores runtime metadata for each frame in the buffer pool.

**Justification**

Required for:

-   Replacement policy decisions
-   Dirty page tracking
-   Pin tracking

------------------------------------------------------------------------

### 2.1.3 BufferFrame

``` rust
pub struct BufferFrame {
    pub page: Page,
    pub metadata: FrameMetadata,
}
```

**Purpose**

Represents a single frame in the buffer pool containing:

-   A page in memory
-   Associated metadata

------------------------------------------------------------------------

### 2.1.4 ReplacementPolicy Trait

``` rust
pub trait ReplacementPolicy {
    fn victim(&mut self, frames: &Vec<BufferFrame>) -> Option<usize>;
    fn record_access(&mut self, frame_id: usize);
}
```

**Purpose**

Provides an abstraction for page replacement strategies.

**Justification**

Allows interchangeable policies such as:

-   LRU
-   Clock
-   Future algorithms

------------------------------------------------------------------------

### 2.1.5 BufferPool

``` rust
pub struct BufferPool {
    pub frames: Vec<BufferFrame>,
    pub page_table: std::collections::HashMap<PageId, usize>,
    pub pool_size: usize,
    pub policy: Box<dyn ReplacementPolicy>,
    pub stats: BufferStats,
}
```

**Purpose**

Central structure that manages the buffer pool.

Responsibilities include:

-   Tracking pages in memory
-   Managing page-to-frame mappings
-   Handling page replacement

------------------------------------------------------------------------

### 2.1.6 BufferStats

``` rust
pub struct BufferStats {
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
    pub dirty_flush_count: u64,
}
```

Tracks runtime statistics for the buffer manager.

------------------------------------------------------------------------

# 3. Backend Functions

## 3.1 Functions to be Created

### fetch_page()

**Input** - table name - page number

**Output** - mutable reference to Page

**Workflow**

1.  Construct `PageId`
2.  Check page table for the page

**If page exists (Hit)**

-   Increment pin_count
-   Update replacement policy metadata
-   Increment hit counter
-   Return page reference

**If page does not exist (Miss)**

1.  Increment miss counter
2.  Find free frame or select victim using replacement policy
3.  Flush dirty victim page if needed
4.  Remove victim mapping
5.  Read requested page from disk
6.  Insert into buffer pool
7.  Initialize metadata
8.  Return page

------------------------------------------------------------------------

### unpin_page()

**Inputs**

-   PageId
-   dirty flag

**Steps**

1.  Locate frame via page table
2.  Validate pin count
3.  Decrement pin count
4.  If dirty flag true → mark dirty
5.  If pin count becomes zero → eligible for eviction

------------------------------------------------------------------------

### flush_page()

Writes a page to disk if dirty.

Steps:

1.  Locate frame
2.  If not dirty → return
3.  Write page to disk
4.  Reset dirty flag
5.  Update statistics

------------------------------------------------------------------------

### flush_all_pages()

1.  Iterate through all frames
2.  Write all dirty pages to disk
3.  Reset dirty flags

------------------------------------------------------------------------

### new_page()

1.  Allocate new page on disk
2.  Load it into buffer pool using fetch_page
3.  Mark page dirty
4.  Return PageId and reference

------------------------------------------------------------------------

### delete_page()

1.  Check if page is pinned
2.  Remove from buffer pool
3.  Delete from disk
4.  Update table metadata

------------------------------------------------------------------------

# 4. Frontend Changes (CLI)

## New Commands

### Set Buffer Size

    SET BUFFER SIZE `<N>`;

Example:

    SET BUFFER SIZE 50;

Initializes buffer pool with N frames.

------------------------------------------------------------------------

### Set Replacement Policy

    SET POLICY LRU;
    SET POLICY CLOCK;

Changes replacement strategy.

------------------------------------------------------------------------

### Show Buffer Stats

    SHOW BUFFER STATS;

Displays:

-   Hit count
-   Miss count
-   Hit ratio
-   Evictions
-   Dirty flush count

------------------------------------------------------------------------

# 5. Overall Component Workflow

## Insert Workflow

1.  User runs INSERT
2.  Table layer calls fetch_page()
3.  Page updated in memory
4.  unpin_page(dirty=true)
5.  Dirty page written during eviction

------------------------------------------------------------------------

## Eviction Workflow

1.  No free frame available
2.  Replacement policy selects victim
3.  Dirty page flushed
4.  New page loaded
# 6. Benchmark Results

## 📌 Setup

The benchmark was conducted on the RookDB storage manager to evaluate the performance of different buffer replacement policies under varying buffer pool sizes.

### Configuration

- **Database Name:** `iiit`
- **Table Name:** `ug1`
- **Total Pages in Table:** 1005 pages  
- **Workload:** 10,000 random page fetch requests  
- **Access Pattern:** Random (from `random_numbers.txt`)
- **Policies Evaluated:**
  - LRU (Least Recently Used)
  - Clock Replacement
- **Buffer Pool Sizes Tested:**
  - 1, 2, 32, 128, 256, 1024

### Metrics Collected

- Average Fetch Time (ms)
- Hit Count
- Miss Count
- Hit Ratio
- Evictions
- Dirty Flushes
- Throughput (derived)
- Miss Penalty (estimated)

---

## 📊 Results & Analysis

---

### 1️⃣ Buffer Size vs Average Fetch Time

![1](/assets/buffer_vs_time.png)

- Fetch time generally decreases as buffer size increases.
- LRU shows instability at mid-range sizes (128–256), likely due to eviction overhead.
- At buffer size 1024, both policies show minimal latency → **working set fits in memory**.

---

### 2️⃣ Buffer Size vs Hit Ratio

![2](/assets/buffer_vs_hitratio.png)

- Hit ratio increases almost linearly with buffer size.
- At buffer size 1024, hit ratio reaches ~0.9.
- Both policies perform nearly identically → workload is uniformly random.

---

### 3️⃣ Buffer Size vs Evictions

![3](/assets/buffer_vs_evictions.png)

- Evictions decrease sharply as buffer size increases.
- At 1024, evictions drop to **zero**.
- Indicates buffer pool can fully accommodate active working set.

---

### 4️⃣ Buffer Size vs Misses

![4](/assets/buffer_vs_misses.png)

- Misses decrease proportionally with increasing buffer size.
- Mirrors hit ratio behavior.
- Confirms correctness of buffer manager implementation.

---

### 5️⃣ Eviction Pressure vs Buffer Size

![5](/assets/eviction_pressure.png)

- Eviction pressure is extremely high at small buffer sizes (~1.0).
- Drops steadily and reaches zero at large buffer size.
- Shows system transitions from **I/O bound → memory bound**.

---

### 6️⃣ Hit vs Miss Distribution

![6](/assets/hit_vs_miss_stacked.png)

- Small buffers are dominated by misses.
- Larger buffers show a clear shift toward hits.
- At 1024, majority of accesses are cache hits.

---

### 7️⃣ Buffer Size vs Throughput

![7](/assets/throughput.png)

- Throughput increases with buffer size.
- Clock consistently shows slightly higher throughput.
- At large buffer sizes, throughput peaks → minimal disk access.

---

### 8️⃣ Estimated Miss Penalty

![8](/assets/miss_penalty.png)

- Miss penalty increases with buffer size for LRU.
- Clock shows more stable behavior.
- Indicates LRU may incur additional overhead under certain conditions.

---

### 9️⃣ Buffer Size vs Time (Log Scale)

![9](/assets/log_buffer_vs_time.png)

- Log scale highlights performance trends clearly.
- Shows non-linear improvement in latency.
- Sudden drop at large buffer confirms working set fit.

---

### 🔟 Policy vs Hit Ratio

![10](/assets/policy_vs_hitratio.png)

- Both policies achieve nearly identical hit ratios.
- Confirms that under random workloads, replacement policy impact is minimal.

---

### 1️⃣1️⃣ Policy vs Average Time

![11](/assets/policy_vs_time.png)

- Clock outperforms LRU in average latency.
- Indicates lower overhead in replacement decision-making.

---

## 🧠 Key Observations

1. **Working Set Fit Effect**
   - At buffer size 1024, evictions drop to zero and performance stabilizes.
   - Indicates working set size is close to buffer capacity.

2. **Policy Comparison**
   - LRU and Clock perform similarly in terms of hit ratio.
   - Clock shows better latency and throughput → lower overhead.

3. **Scalability**
   - System scales well with increasing buffer size.
   - Performance improves due to reduced disk I/O.

4. **Disk I/O Dominance**
   - At small buffer sizes, performance is dominated by disk accesses.
   - High eviction and miss rates confirm this.

5. **Correctness Validation**
   - Linear relationship between buffer size and hit ratio confirms correct implementation.

---

# 8. Future Work

This work can be extended in several directions:

- **LRU-K Policy:** Implement LRU-K to better capture access patterns by considering multiple past references, improving performance under skewed workloads.
- **Multiple Buffer Pools:** Introduce multiple buffer pools (per table or database) to reduce contention and improve cache locality.
- **Advanced Workloads:** Evaluate performance under realistic distributions such as Zipfian access patterns.
- **Concurrency:** Extend the buffer manager to support multi-threaded access with proper synchronization.

These enhancements will improve scalability, adaptability, and real-world applicability of the system.


# 9. Conclusion

The Buffer Manager introduces a modular and extensible caching layer
within RookDB. By intercepting disk accesses and maintaining an
in-memory buffer pool, it significantly reduces disk I/O and improves
system performance. The design supports pluggable replacement policies
and runtime monitoring, while remaining compatible with the existing
page storage layout.