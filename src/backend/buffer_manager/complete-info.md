---
title: Buffer Manager
sidebar_position: 1
---

# Buffer Manager - Complete Documentation

## Overview

The **Buffer Manager** is a critical component of RookDB's storage subsystem that manages in-memory caching of database pages. It sits between the execution layer and disk storage, significantly reducing I/O operations by maintaining a pool of frequently accessed pages in memory.

### Key Responsibilities

- **Page Caching**: Maintain an in-memory cache of database pages
- **I/O Optimization**: Reduce disk accesses through intelligent page buffering
- **Memory Management**: Allocate and manage fixed-size memory frames
- **Eviction Strategy**: Implement replacement policies for page eviction
- **Multi-file Support**: Handle pages from multiple database tables
- **Dirty Page Tracking**: Monitor and flush modified pages to disk
- **Performance Monitoring**: Track cache hits, misses, and other metrics

---

## Architecture Overview

### System Position

The Buffer Manager operates as a critical middleware in the database storage stack:

```
┌─────────────────────────────────────┐
│   Query Layer / Execution Engine    │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      Buffer Manager                 │ ◄─ This Component
│  (Caching + Replacement Policy)     │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        Disk Manager                 │
│   (Page Read/Write Operations)      │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      Disk Storage                   │
│   (.dat files on filesystem)        │
└─────────────────────────────────────┘
```

### High-Level Architecture

The Buffer Manager implements a **Buffer Pool** pattern—a fixed-size array of memory frames where each frame can hold one database page.

```
┌─────────────────────────────────────────────────────┐
│               Buffer Pool (Total)                   │
│           128 MB (configurable size)                │
├─────────────────────────────────────────────────────┤
│  Reserved Region │ Data Region                      │
│  (Frames 0-128)  │ (Frames 129+)                    │
│  [Catalog Pages] │ [Table Pages - Managed by Policy]│
├─────────────────────────────────────────────────────┤
│ Page | Page | Page | ... | Page | Page | ... | Page│
├─────────────────────────────────────────────────────┤
│  8KB   8KB    8KB          8KB    8KB         8KB   │
└─────────────────────────────────────────────────────┘
```

---

## Core Data Structures

### 1. PageId

Uniquely identifies a page in the database system.

```rust
pub struct PageId {
    pub table_name: String,    // Name of the table (file)
    pub page_number: u32,      // Page number within the table
}
```

**Purpose**: Enables the buffer manager to locate and track pages across multiple table files.

**Characteristics**:
- Implements `Hash + Eq + PartialEq` for use in HashMap
- Composed of table name and page number
- Used as key in the page_table for O(1) lookups

---

### 2. FrameMetadata

Stores metadata about each buffer frame and its resident page.

```rust
pub struct FrameMetadata {
    pub page_id: Option<PageId>,  // Page currently in this frame (None if empty)
    pub dirty: bool,               // Has this page been modified?
    pub pin_count: u32,            // Number of active users holding this frame
    pub usage_count: u32,          // Used by Clock policy (0 or 1)
    pub last_used: u64,            // Timestamp for LRU policy
}
```

**Field Explanations**:

| Field | Purpose | Usage |
|-------|---------|-------|
| `page_id` | Identifies the resident page | Determines if frame is empty; used for eviction lookup |
| `dirty` | Tracks modifications | Determines if page must be flushed before eviction |
| `pin_count` | Prevents eviction | Frames with pin_count > 0 cannot be evicted |
| `usage_count` | Clock policy state | Used by Clock replacement policy (0 or 1) |
| `last_used` | Access timestamp | Used by LRU replacement policy |

---

### 3. BufferFrame

The fundamental unit of the buffer pool—combines page data with its metadata.

```rust
pub struct BufferFrame {
    pub page: Page,                    // 8 KB page data
    pub metadata: FrameMetadata,       // Associated metadata
}
```

**Layout**:

```
┌────────────────────────────────────┐
│        BufferFrame (8KB+)          │
├────────────────────────────────────┤
│  Page Data (8 KB)                  │
│  [Header | Row Data | Free Space]  │
├────────────────────────────────────┤
│  FrameMetadata                     │
│  - page_id                         │
│  - dirty                           │
│  - pin_count                       │
│  - usage_count                     │
│  - last_used                       │
└────────────────────────────────────┘
```

---

### 4. BufferStats

Tracks performance metrics of the buffer pool.

```rust
pub struct BufferStats {
    pub hit_count: u64,          // Pages found in buffer
    pub miss_count: u64,         // Pages not found in buffer
    pub eviction_count: u64,     // Pages evicted from buffer
    pub dirty_flush_count: u64,  // Dirty pages flushed to disk
}
```

**Metrics**:

- **Hit Ratio**: `hit_count / (hit_count + miss_count)`
  - Indicates buffer effectiveness
  - Higher is better (target: 80-95%)

- **Eviction Rate**: Tracks how often pages are removed
  - High eviction may indicate undersized buffer

- **Dirty Flush Count**: Number of write-back operations
  - Impacts disk I/O performance

---

### 5. BufferPool

The main data structure managing the entire buffer system.

```rust
pub struct BufferPool {
    pub frames: Vec<BufferFrame>,                    // Array of buffer frames
    pub page_table: HashMap<PageId, usize>,         // Page ID → Frame index mapping
    pub files: HashMap<String, File>,               // Open file handles
    pub num_frames: usize,                          // Total number of frames
    pub policy: Box<dyn ReplacementPolicy>,         // Pluggable replacement policy
    pub stats: BufferStats,                         // Performance statistics
}
```

**Components**:

| Component | Purpose |
|-----------|---------|
| `frames` | Stores all buffer frames (capacity = BUFFER_SIZE / PAGE_SIZE) |
| `page_table` | Hash map for O(1) page lookup (Frame location cache) |
| `files` | Cache of open file handles for multi-file support |
| `num_frames` | Total frame count (e.g., 16,384 for 128 MB buffer) |
| `policy` | Runtime-selectable replacement strategy (Clock/LRU/LRU-K) |
| `stats` | Counters for monitoring and profiling |

---

## Core Operations

### 1. fetch_page() - Fetch or Load a Page

The primary operation for accessing a page.

```rust
pub fn fetch_page(
    &mut self,
    table_name: String,
    page_number: u32,
) -> io::Result<&mut Page>
```

**Algorithm**:

```
1. CREATE PageId from (table_name, page_number)

2. CHECK PAGE_TABLE for page_id
   ├─ IF FOUND (BUFFER HIT)
   │  ├─ Increment pin_count
   │  ├─ Increment usage_count
   │  ├─ Record access in replacement policy
   │  ├─ Record cache hit in statistics
   │  └─ RETURN page reference
   │
   └─ IF NOT FOUND (BUFFER MISS)
      ├─ Record cache miss in statistics
      │
      ├─ SEARCH for free frame
      │  ├─ Scan data frames (from RESERVED_FRAMES onwards)
      │  ├─ Find first frame with page_id == None
      │  │
      │  └─ IF NO FREE FRAME
      │     ├─ INVOKE EVICTION
      │     │  ├─ Call policy.victim() to select victim
      │     │  ├─ IF victim is pinned, find first unpinned frame
      │     │  └─ IF VICTIM is dirty
      │     │     ├─ Write page to disk (flush)
      │     │     ├─ Record dirty flush in statistics
      │     │     └─ Update page_table
      │     │
      │     └─ REUSE evicted frame
      │
      ├─ LOAD PAGE FROM DISK
      │  ├─ Get file handle from self.files[table_name]
      │  ├─ Call read_page(file, page_number)
      │  └─ Place page data in selected frame
      │
      ├─ UPDATE METADATA
      │  ├─ Set frame.metadata.page_id = Some(page_id)
      │  ├─ Set frame.metadata.pin_count = 1
      │  ├─ Set frame.metadata.dirty = false
      │  └─ Update page_table[page_id] = frame_index
      │
      └─ RETURN page reference
```

**Step-by-Step Walkthrough**:

1. **Buffer Hit** (Page already in memory):
   - Look up page in `page_table` HashMap
   - If found, increment `pin_count` and `usage_count`
   - Notify replacement policy of access
   - Return mutable reference to page

2. **Find Free Frame**:
   - Search data region (frames 129+) for empty slot
   - Frame is empty if `metadata.page_id == None`

3. **Eviction** (No free frame available):
   - Call `policy.victim()` to select victim
   - If victim is pinned, scan for first unpinned frame
   - If victim is dirty, flush to disk and update statistics
   - Clear victim frame metadata

4. **Disk Load**:
   - Get file handle from `files` HashMap (or error)
   - Use disk manager's `read_page()` function
   - Copy page data into selected frame

5. **Metadata Update**:
   - Set `page_id` to identify resident page
   - Initialize `pin_count = 1` (caller is holding it)
   - Set `dirty = false` (freshly loaded)
   - Add entry to `page_table` for future lookups

**Return Value**: Mutable reference to the page (`&mut Page`)

---

### 2. unpin_page() - Release a Page

Decrements the pin count when done using a page.

```rust
pub fn unpin_page(
    &mut self,
    page_id: &PageId,
    is_dirty: bool,
) -> io::Result<()>
```

**Algorithm**:

```
1. LOOKUP frame index from page_table[page_id]
   └─ IF NOT FOUND, return error

2. GET frame at index

3. VALIDATE
   ├─ IF pin_count == 0
   │  └─ Return error (double unpin)
   │
   └─ DECREMENT pin_count

4. SET DIRTY FLAG
   └─ If is_dirty, set frame.metadata.dirty = true

5. RETURN success
```

**Purpose**:
- Releases lock on the page
- Marks if page was modified
- Makes frame eligible for eviction (when pin_count = 0)

**Important**: Page cannot be evicted while `pin_count > 0`

---

### 3. register_file() - Register a Table File

Makes a table's file available to the buffer manager.

```rust
pub fn register_file(
    &mut self,
    table_name: &str,
) -> io::Result<()>
```

**Purpose**:
- Open file handle for table
- Store in `files` HashMap
- Enable subsequent fetch_page calls

**Notes**:
- Must be called before any fetch_page for that table
- File path determined by layout constants (e.g., `database/base/{db}/{table}.dat`)

---

### 4. flush_all() - Force Write All Dirty Pages

Writes all modified pages to disk.

```rust
pub fn flush_all(&mut self) -> io::Result<()>
```

**Algorithm**:

```
FOR each frame in frames:
  IF frame.metadata.dirty
    ├─ Get file handle from files[table_name]
    ├─ Call write_page(file, page_data, page_number)
    ├─ Set frame.metadata.dirty = false
    └─ Record dirty flush in statistics
```

**Use Cases**:
- Shutdown (ensure durability)
- Transaction commit
- Checkpoint operations

---

## Replacement Policies

The buffer manager supports pluggable replacement policies to decide which page to evict when the buffer is full.

### Overview

All policies implement the `ReplacementPolicy` trait:

```rust
pub trait ReplacementPolicy {
    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize>;
    fn record_access(&mut self, frame_id: usize);
}
```

| Method | Purpose |
|--------|---------|
| `victim()` | Select a frame to evict (must skip pinned frames) |
| `record_access()` | Notify policy of page access for tracking |

### 1. Clock Replacement Policy

**Philosophy**: "Give pages a second chance before eviction"

**Algorithm Overview**:
- Maintains a clock hand pointer that sweeps through frames
- Each frame has a usage bit (0 or 1)
- On access: set usage = 1
- On eviction search:
  1. If usage = 0 → evict immediately
  2. If usage = 1 → give second chance (set to 0), advance hand
  3. Continue circular sweep until evictable frame found

**Characteristics**:

| Aspect | Details |
|--------|---------|
| **Memory Overhead** | Minimal (one pointer) |
| **Time Complexity** | O(n) worst case, O(1) average |
| **Cache Locality** | Good (sequential sweep) |
| **Implementation Complexity** | Low |
| **Hit Ratio** | Good for most workloads |

**Code Implementation**:

```rust
pub struct ClockPolicy {
    pub hand: usize,  // Current hand position
}

impl ReplacementPolicy for ClockPolicy {
    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {
        // Sweep through frames in circular order
        // Give second chance to accessed pages
        // Select first frame with usage_count == 0
    }

    fn record_access(&mut self, frame_id: usize) {
        // Called on each access
        // Sets usage_count = 1 in frame metadata
    }
}
```

**When to Use**:
- General-purpose workloads
- Limited memory overhead required
- Good overall performance needed
- Sequential or batch processing

---

### 2. LRU (Least Recently Used) Policy

**Philosophy**: "Evict the page accessed longest ago"

**Algorithm**:
- Track timestamp of last access for each frame
- On victim selection: find frame with minimum timestamp
- On access: update timestamp to current_time

**Characteristics**:

| Aspect | Details |
|--------|---------|
| **Memory Overhead** | High (HashMap of timestamps) |
| **Time Complexity** | O(n) for victim selection |
| **Implementation Complexity** | Medium |
| **Hit Ratio** | Very good for working set fits |
| **Behavior** | Excellent temporal locality |

**Code Implementation**:

```rust
pub struct LRUPolicy {
    timestamps: HashMap<usize, u64>,  // frame_id → last_access_time
    current_time: u64,                // Logical clock
}

impl ReplacementPolicy for LRUPolicy {
    fn victim(&mut self, frames: &mut Vec<BufferFrame>) -> Option<usize> {
        // Find frame with minimum timestamp
        // Skip pinned frames
        // Return index with oldest access time
    }

    fn record_access(&mut self, frame_id: usize) {
        self.current_time += 1;
        self.timestamps.insert(frame_id, self.current_time);
    }
}
```

**When to Use**:
- Working set fits in buffer (high hit ratio expected)
- Memory is available for metadata
- Temporal locality is strong
- Need predictable performance

---

### 3. LRU-K Policy

**Philosophy**: "Consider the last K accesses for smarter decisions"

**Algorithm**:
- Track the last K access times for each frame
- On victim selection: evict frame with oldest "distance to K-th access"
- Effectively: find page accessed least frequently in recent history

**Characteristics**:

| Aspect | Details |
|--------|---------|
| **Memory Overhead** | High (K timestamps per frame) |
| **Time Complexity** | O(n) + O(k) per access |
| **Implementation Complexity** | High |
| **Hit Ratio** | Excellent, handles mixed workloads |
| **Behavior** | Distinguishes hot vs cold pages |

**Advantages over LRU**:
- Resists flooding from one-time full table scans
- Better handles mixed hot/cold workloads
- Effective for cache pollution resistance

**When to Use**:
- Mixed temporal patterns (some hot, some cold pages)
- Need cache pollution resistance
- Can afford extra memory per frame
- Sequential + random access patterns

---

### Policy Comparison

```
┌──────────────┬────────────┬──────────┬────────────┬──────────────┐
│ Policy       │ Memory     │ Speed    │ Hit Ratio  │ Best Case    │
├──────────────┼────────────┼──────────┼────────────┼──────────────┤
│ Clock        │ Very Low   │ Very Fast│ Good       │ Sequential   │
│ LRU          │ High       │ Medium   │ Excellent  │ Working Set  │
│ LRU-K        │ Very High  │ Slow     │ Excellent* │ Mixed        │
└──────────────┴────────────┴──────────┴────────────┴──────────────┘
  * Best handles mixed access patterns
```

---

## Reserved Region and Multi-File Support

### Reserved Frames

The buffer pool reserves frames 0-128 (129 frames total) for system catalog pages:

```
┌─────────────────────────────────────┐
│ Reserved Region (Frames 0-128)      │
│ Catalog Pages - NEVER EVICTED       │
├─────────────────────────────────────┤
│ pg_database │ pg_table │ pg_column  │
│ pg_constraint│ pg_index│ pg_type   │
└─────────────────────────────────────┘
```

**Purpose**:
- Catalog must always be accessible
- Prevents catalog pages from being evicted
- Guarantees availability for schema lookups

**Behavior**:
- Replacement policy is not applied to reserved region
- Reserved frames are never candidates for eviction
- Each reserved frame can hold one catalog page

### Multi-File Support

The buffer manager handles pages from multiple table files:

```
files HashMap:
┌────────────────────────────────────┐
│ "users" → File(database/base/..)   │
│ "orders" → File(database/base/..)  │
│ "products" → File(database/base/..)│
└────────────────────────────────────┘

page_table HashMap:
┌────────────────────────────────────┐
│ PageId("users", 0) → frame_idx: 150│
│ PageId("orders", 3) → frame_idx: 42│
│ PageId("products", 1) → frame_idx:10│
└────────────────────────────────────┘
```

**Key Operations**:

1. **File Registration**:
   ```rust
   buffer_pool.register_file("users")?;
   buffer_pool.register_file("orders")?;
   ```

2. **Page Access** (same API):
   ```rust
   let page = buffer_pool.fetch_page("users", 0)?;
   let page = buffer_pool.fetch_page("orders", 3)?;
   ```

3. **File Handle Resolution**:
   - fetch_page looks up file by table_name
   - Enables reading from correct table file
   - Supports tables in different physical files

---

## Configuration Constants

The buffer manager is configured via constants in `mod.rs`:

```rust
pub const PAGE_SIZE: usize = 8192;              // 8 KB per page
pub const BUFFER_SIZE: usize = 128 * 1024 * 1024; // 128 MB total
pub const RESERVED_FRAMES: usize = 129;        // Frames 0-128 for catalog
```

**Calculations**:
- Total frames: `BUFFER_SIZE / PAGE_SIZE` = 128 MB / 8 KB = 16,384 frames
- Data frames: 16,384 - 129 = 16,255 frames available for table data
- Maximum data in buffer: ~128 MB - (129 × 8 KB) ≈ 127 MB

---

## Integration with System

### Initialization

```rust
// Create buffer pool with selected replacement policy
let policy = Box::new(ClockPolicy::new());
let mut buffer_pool = BufferPool::new(policy);

// Register table files
buffer_pool.register_file("users")?;
buffer_pool.register_file("orders")?;
```

### Usage Flow

```
Execution Engine
       │
       ├─ fetch_page("users", 0)
       │  │
       │  ├─ [Buffer Hit] → Return page reference
       │  │
       │  └─ [Buffer Miss] → 
       │     ├─ Read from disk
       │     ├─ Possibly evict victim
       │     └─ Return page reference
       │
       ├─ Modify page data
       │
       └─ unpin_page("users_0", is_dirty=true)
          └─ Page marked for flush on eviction
```

### Catalog Page Management

Catalog pages occupy the reserved region and are managed separately:

```
BufferPool (Frames 0-128)
├─ pg_database pages (reserved)
├─ pg_table pages (reserved)
├─ pg_column pages (reserved)
├─ pg_constraint pages (reserved)
├─ pg_index pages (reserved)
└─ pg_type pages (reserved)
```

These pages:
- Are never evicted
- Have their own management protocol
- Are flushed at shutdown or checkpoints

---

## Performance Monitoring

### Statistics Available

```rust
buffer_stats.hit_count         // Pages found in buffer
buffer_stats.miss_count        // Pages loaded from disk
buffer_stats.eviction_count    // Pages removed from buffer
buffer_stats.dirty_flush_count // Dirty pages written to disk

// Calculated metric:
let hit_ratio = buffer_stats.hit_ratio();  // hit_count / (hit_count + miss_count)
```

### Monitoring Recommendations

**Healthy Buffer Usage**:
- Hit ratio > 80% (95% is excellent)
- Low eviction rate relative to workload size
- Dirty flush rate proportional to write activity

**Red Flags**:
- Hit ratio < 50% (buffer may be too small)
- High eviction rate (working set > buffer size)
- Frequent "All frames pinned" errors (concurrent access bottleneck)

---

## Error Handling

### Common Errors

| Error | Cause | Resolution |
|-------|-------|-----------|
| "File not found" | Table not registered | Call `register_file()` first |
| "All frames are pinned" | Pin count leak | Ensure every fetch has matching unpin |
| "Page not found" in unpin | Wrong page ID | Verify PageId construction |
| I/O Error on flush | Disk full/permission | Check disk space and permissions |

### Pin Count Correctness

The pin count is critical for correctness:

```rust
// CORRECT USAGE
let page = buffer_pool.fetch_page("users", 0)?;  // pin_count = 1
// ... modify page ...
buffer_pool.unpin_page(&page_id, true)?;         // pin_count = 0

// INCORRECT (Pin Count Leak)
let page = buffer_pool.fetch_page("users", 0)?;  // pin_count = 1
let page2 = buffer_pool.fetch_page("users", 0)?; // pin_count = 2
buffer_pool.unpin_page(&page_id, true)?;         // pin_count = 1 (STILL PINNED!)
// ❌ page remains pinned, won't be evicted
```

---

## Best Practices

### 1. Always Unpin Pages

```rust
// GOOD: Use RAII pattern or explicit unpinning
let page = buffer_pool.fetch_page("users", 0)?;
// ... use page ...
buffer_pool.unpin_page(&page_id, is_modified)?;

// BETTER: Scope-based cleanup (when available)
{
    let page = buffer_pool.fetch_page("users", 0)?;
    // ... use page ...
} // Automatically unpinned here
```

### 2. Register Files Early

```rust
// Register all tables at startup
pub fn init_database() -> io::Result<()> {
    let mut buffer = BufferPool::new(Box::new(ClockPolicy::new()));
    
    buffer.register_file("users")?;
    buffer.register_file("orders")?;
    buffer.register_file("products")?;
    
    // Now can safely fetch pages from any table
    Ok(())
}
```

### 3. Choose Appropriate Replacement Policy

```rust
// Sequential workload
let policy = Box::new(ClockPolicy::new());

// Working set fits in buffer
let policy = Box::new(LRUPolicy::new());

// Mixed hot/cold access patterns
let policy = Box::new(LRUKPolicy::new());
```

### 4. Monitor Statistics

```rust
// Log statistics periodically
eprintln!("Buffer Hit Ratio: {:.2}%", 
    buffer_pool.stats.hit_ratio() * 100.0);
eprintln!("Evictions: {}", buffer_pool.stats.eviction_count);
eprintln!("Dirty Flushes: {}", buffer_pool.stats.dirty_flush_count);
```

### 5. Flush Before Shutdown

```rust
// Ensure all dirty pages written to disk
buffer_pool.flush_all()?;

// Verify statistics
println!("Final hit ratio: {:.2}%", 
    buffer_pool.stats.hit_ratio() * 100.0);
```

---

## Advanced Topics

### Pin Count Management

The pin count mechanism prevents pages from being evicted while in use:

```
Scenario: Concurrent access to same page

Thread 1: fetch_page("users", 5)  → pin_count = 1
Thread 2: fetch_page("users", 5)  → pin_count = 2 (same frame)
Thread 1: unpin_page()            → pin_count = 1 (still pinned!)
Thread 2: unpin_page()            → pin_count = 0 (now evictable)

Result: Frame won't be evicted until BOTH threads unpin
```

### Dirty Page Handling

When a frame is evicted:

```
IF frame.metadata.dirty:
  1. Get file handle for this table
  2. Write page data to disk at correct offset
  3. Clear dirty flag
  4. Increment dirty_flush_count

Then:
  1. Clear frame metadata (page_id = None)
  2. Clear page_table entry
  3. Reuse frame for new page
```

### Buffer Size Calculation

For a given workload:

```
Required Buffer Size = Working Set Size × 1.5 (safety margin)

Example:
- Average query touches 10 pages
- Concurrent queries: 50
- Total working set: 500 pages
- Page size: 8 KB
- Required: 500 × 8 KB × 1.5 = 60 MB

Current Default: 128 MB (sufficient for moderate workloads)
```

---

## Summary

The Buffer Manager is a sophisticated component that:

1. **Caches pages** in memory to reduce disk I/O
2. **Manages memory** through eviction policies
3. **Supports multiple tables** via file registration
4. **Tracks performance** with comprehensive statistics
5. **Ensures correctness** through pin counts and dirty tracking
6. **Offers flexibility** with pluggable replacement policies

By understanding its architecture and using it correctly, you can build database systems that effectively balance memory usage with disk I/O performance.

---

## Related Components

- **[Disk Manager](../../disk/disk_manager.rs)**: Handles actual disk read/write operations
- **[Page Structure](../../page/mod.rs)**: Defines page format and layout
- **[Catalog Manager](../../catalog/catalog.rs)**: Uses reserved buffer frames for system metadata
- **[Table Manager](../../table/table_file.rs)**: Issues buffer manager requests for data access
- **[Replacement Policies](./replacement_policies.md)**: Detailed policy documentation

---

## References

- **Buffer Pool Concept**: Commonly used in DBMS implementations (PostgreSQL, MySQL)
- **Clock Algorithm**: Originally from Corbató et al., Second-chance page replacement
- **LRU Behavior**: Foundation of cache replacement theory
- **LRU-K**: From "The LRU-K page replacement algorithm for database disk buffering" (O'Neil et al.)
