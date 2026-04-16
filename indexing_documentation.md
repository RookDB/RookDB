# RookDB — System Documentation

**Team:** 1 &nbsp;|&nbsp; **Members:** George, Mithun, Sujay

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [New Database Files](#2-new-database-files)
3. [Modifications to Database Structure](#3-modifications-to-database-structure)
4. [Page Layout and File Structure](#4-page-layout-and-file-structure)
5. [Algorithms](#5-algorithms)
6. [New Data Structures](#6-new-data-structures)
7. [Backend Functions](#7-backend-functions)
8. [Frontend and CLI Changes](#8-frontend-and-cli-changes)
9. [Potential Future Work](#9-potential-future-work)

---

## 1. Project Overview

RookDB is a disk-oriented relational database management system. Prior to this work, RookDB relied entirely on **generic sequential table scans**, which produced degraded performance on large datasets.

This release integrates a **generalized Indexing Subsystem** positioned between the Storage layer and the Catalog layer. The subsystem exposes a unified `IndexTrait` interface backed by nine distinct index implementations, spanning both hash-based and tree-based families. All existing table operations — inserts, deletes, and searches — were extended to maintain index consistency transparently.

**System constraints that remain unchanged:**

| Constraint | Detail |
|---|---|
| Execution model | Single-threaded; no concurrency control or locking on tables or indexes |
| Transactions | Operations are immediately persistent; no ACID rollbacks or Write-Ahead Logging |
| Deletions | Tuples are marked deleted in-place (`length = 0`) and removed from indexes; space is not reclaimed without a full rebuild |
| Supported key types | `INT`, `FLOAT`, `TEXT` — all natively hashable and comparable |

---

## 2. New Database Files

### 2.1 Index Files (`.idx`)

Each index created on a table column is persisted as a dedicated binary file on disk. These files are written and read by the Indexing Subsystem and are separate from the table's `.dat` file.

**Location:**
```
database/base/{db_name}/{table_name}_{column_name}.idx
```

**Contents:**

An `.idx` file encodes the complete serialized state of a single `AnyIndex` instance. This includes:

- A header identifying the index variant (e.g., `BPlusTree`, `ExtendibleHash`, `LinearHash`, etc.)
- All internal node or bucket structures, serialized in the format native to that variant
- For tree-based indexes: internal node keys, child pointers, and leaf-level `(key → RecordId)` mappings, including `next_leaf` chain pointers where applicable
- For hash-based indexes: directory structures, bucket contents, depth metadata, and overflow chain linkages

**Loading:** At query time, `AnyIndex::load()` reconstructs the full in-memory index object from the `.idx` file before any search is performed.

**Saving:** After every index mutation (insert or delete triggered by a heap operation), the updated index is re-persisted to its `.idx` file.

### 2.2 Updated Catalog File (`catalog.json`)

The existing `catalog.json` file was extended to track index metadata per table. No new catalog file is introduced; the schema of the existing file is modified (see [Section 3](#3-modifications-to-database-structure)).

---

## 3. Modifications to Database Structure

### 3.1 Catalog Schema — `IndexEntry` Addition

Each table entry in `catalog.json` now carries an `indexes` array alongside the existing `columns` array. Each element of `indexes` is an `IndexEntry` object.

**Previous table structure (abbreviated):**
```json
{
  "name": "orders",
  "columns": [
    { "name": "id",     "data_type": "INT"  },
    { "name": "amount", "data_type": "FLOAT" }
  ]
}
```

**Updated table structure:**
```json
{
  "name": "orders",
  "columns": [
    { "name": "id",     "data_type": "INT"  },
    { "name": "amount", "data_type": "FLOAT" }
  ],
  "indexes": [
    {
      "column_name":      "id",
      "algorithm":        "BPlusTree",
      "is_clustered":     true,
      "is_primary":       true,
      "include_columns":  []
    },
    {
      "column_name":      "amount",
      "algorithm":        "LinearHash",
      "is_clustered":     false,
      "is_primary":       false,
      "include_columns":  ["amount"]
    }
  ]
}
```

**`IndexEntry` fields:**

| Field | Type | Description |
|---|---|---|
| `column_name` | string | The indexed column |
| `algorithm` | string | One of the 9 supported index variants |
| `is_clustered` | bool | Whether this index defines the physical read ordering |
| `is_primary` | bool | Whether uniqueness is enforced |
| `include_columns` | string[] | Additional columns stored in index nodes for covering index queries |

**Constraints enforced at write time:**
- At most one `is_clustered: true` entry may exist per table.
- `is_primary: true` entries reject duplicate key insertions.

---

## 4. Page Layout and File Structure

### 4.1 Table Data Files (`.dat`) — Unchanged

The slotted page layout used by table `.dat` files is **not modified** by this release. All pages remain 8 KB (8,192 bytes) and follow the existing structure:

```
┌──────────────────────────────────────────────┐  ← Page start (8,192 bytes)
│  Header (8 bytes)                            │
│    lower: u32  |  upper: u32                 │
├──────────────────────────────────────────────┤
│  Metadata Array  ↓ grows downward            │
│    [ (offset: u32, length: u32), ... ]       │
│    (one entry per live or deleted tuple)     │
├──────────────────────────────────────────────┤
│                                              │
│              Free Space                      │
│                                              │
├──────────────────────────────────────────────┤
│  Tuple Array  ↑ grows upward                 │
│    [ raw tuple bytes, ... ]                  │
└──────────────────────────────────────────────┘  ← Page end
```

- `lower` advances downward as Item IDs are appended to the metadata array.
- `upper` advances upward as variable-length tuple payloads are written.
- A deleted tuple is identified by its metadata entry having `length = 0`; its byte region is not immediately reclaimed.

### 4.2 Tuple Layout — Unchanged

The tuple binary layout within pages is not modified. Tuples continue to be encoded as contiguous byte sequences per the column schema, parsed using type-specific deserialization (`INT` → `i32` little-endian, `FLOAT` → `f64` little-endian, `TEXT` → length-prefixed UTF-8).

### 4.3 Index Files (`.idx`) — New

Index files share the same **8 KB fixed page size** as table data files. Internal encoding is variant-specific and opaque to the page layer — the index implementations handle their own serialization within this page boundary.

### 4.4 Record Identification

A tuple's physical location is encoded as:

```
RecordId {
    page_no: u32,   // which 8 KB page in the .dat file
    item_id: u32,   // which slot in that page's metadata array
}
```

`RecordId` values are stored as the value payload inside every index entry, mapping a key to its physical disk location.

---

## 5. Algorithms

### 5.1 Hash-Based Index Algorithms

All hash-based indexes target **O(1)** average-case exact-match lookups. Range scans are not supported by any hash variant.

#### Static Hashing

Keys are assigned to one of a fixed number of buckets using a hash function. The bucket count is set at index creation and never changes. Suitable for datasets with a known, bounded cardinality.

#### Chained Hashing

Uses the same fixed-bucket layout as static hashing. On collision, overflowing keys are appended to an **external linked chain of overflow buffers** attached to the colliding bucket. Degrades toward O(n) in the chain length as the load factor grows.

#### Extendible Hashing

A **directory-based dynamic hash table** that avoids full table rehashing on growth:

1. A global directory maps hash prefixes to physical buckets.
2. Each bucket tracks a `local_depth`; the directory tracks a `global_depth`.
3. When a bucket overflows:
   - The affected bucket is **split** and its `local_depth` is incremented.
   - If `local_depth == global_depth` after the split, the **directory doubles** in size (only pointers are duplicated, not bucket data).
4. Only the split bucket's data is redistributed — all other buckets are unaffected.

This constrains the growth penalty to the split bucket rather than the entire table.

#### Linear Hashing

A dynamic hash scheme using **round-robin incremental splits** managed by an internal `split_pointer`:

1. A load factor threshold determines when to trigger a split.
2. On trigger, the bucket at `split_pointer` is split (not necessarily the overflowing bucket).
3. `split_pointer` advances after each split; when it reaches the end of the current round, a new round begins.
4. Avoids the sudden O(N) cost of directory doubling — splits are spread smoothly over many insertions.

### 5.2 Tree-Based Index Algorithms

All tree-based indexes maintain sorted key order and support both O(log n) point lookups and range scans.

#### B-Tree

A classic balanced multi-level tree where **both internal nodes and leaf nodes** store keys and `RecordId` pointers. On overflow, nodes split and the median key is promoted to the parent. Range scans may require upward traversal back to ancestor nodes to continue across siblings.

#### B+ Tree *(default for Primary Key indexes)*

A variant of the B-Tree where **all `RecordId` pointers are stored exclusively in leaf nodes**. Internal nodes store only routing keys.

Key property: leaf nodes are chained via a `next_leaf` pointer, forming a **dense linked list of all keys in sorted order**. This enables range scans without any upward traversal:

- **Point query:** O(log n) — traverse from root to the target leaf.
- **Range scan:** O(log n + k) — reach the start leaf, then follow `next_leaf` links until the upper bound is exceeded.

#### Radix Tree

A **compressed prefix trie** operating on the bit-level representation of keys. Shared key prefixes are merged into single internal nodes, yielding compact storage for string-heavy columns. Efficient for prefix-range queries on `TEXT` data.

- **Lookup complexity:** O(|key|) — proportional to key length, not dataset size.

#### Skip List

A **probabilistic multi-layer linked list**. Each key is assigned a random height; higher levels act as express lanes skipping over large portions of the base list. Rebalancing after insert or delete is probabilistic and lightweight, making it well-suited for workloads with frequent mutations.

- **Expected complexity:** O(log n) for search, insert, and delete.

#### LSM Tree (Log-Structured Merge Tree)

Optimized for **write-intensive workloads** using immutable sorted runs across generational tiers:

1. Writes are buffered in an in-memory structure (memtable).
2. When the memtable is full, it is flushed as an immutable sorted run to the first on-disk tier (L0).
3. Background **compaction** merges runs across tiers, maintaining sorted order and removing deleted entries.
4. Reads must check all tiers (memtable + disk levels), which adds read amplification compared to tree indexes.

Delivers high sequential write throughput at the cost of higher read latency relative to B+ Trees.

### 5.3 Index Scan with Clustered Sort

When a query uses a **Clustered B+ index**, the executor does not immediately fetch pages as `RecordId`s are found. Instead:

1. All matching `RecordId`s from the index search are accumulated into a list.
2. The list is **sorted by `page_no` and then `item_id`** before any disk access.
3. Pages are fetched in sorted order.

This ensures sequential page access patterns, maximizing OS page cache locality without physically reorganizing the `.dat` file.

### 5.4 Range Scan Traversal

Range scans are restricted to tree-based index types. The procedure:

1. Traverse the tree from the root to the **first leaf containing a key ≥ START**.
2. Collect all `RecordId`s from that leaf where `key ≤ END`.
3. Follow the `next_leaf` (or equivalent sibling) pointer to the next leaf.
4. Repeat until a key exceeding `END` is encountered or no further leaves exist.

**Complexity:** O(log n + k), where k is the number of keys in the range.

---

## 6. New Data Structures

### 6.1 `IndexTrait`

A Rust trait defining the required interface for all index implementations. Every index backend must implement:

| Method | Signature (conceptual) | Description |
|---|---|---|
| `insert` | `(key: IndexKey, rid: RecordId)` | Insert a key-to-record mapping |
| `search` | `(&key: IndexKey) → Vec<RecordId>` | Return all records matching the key |
| `delete` | `(&key: IndexKey)` | Remove a key and its associated records |
| `entry_count` | `() → usize` | Return the number of stored entries |
| `save` | `(path: &Path)` | Persist the index to a `.idx` file |

### 6.2 `AnyIndex`

A type-erased enum that wraps all nine index variants. Because Rust trait objects (`Box<dyn IndexTrait>`) do not support static constructors needed for deserialization, `AnyIndex` acts as a **type dispatcher** — it holds the concrete variant and forwards all `IndexTrait` calls to it.

```
AnyIndex
├── HashBasedIndex
│   ├── StaticHash
│   ├── ChainedHash
│   ├── ExtendibleHash
│   └── LinearHash
└── TreeBasedIndex
    ├── BTree
    ├── BPlusTree
    ├── RadixTree
    ├── SkipList
    └── LSMTree
```

`AnyIndex::load(path)` reads a `.idx` file, identifies the variant from the file header, and reconstructs the correct concrete type.

### 6.3 `HashBasedIndex`

An intermediate enum grouping the four hash implementations. Forwards `IndexTrait` operations to the selected variant.

### 6.4 `TreeBasedIndex`

An intermediate enum grouping the five tree implementations. Forwards `IndexTrait` operations to the selected variant. This grouping also enables the executor to restrict range scan eligibility — only arms of `TreeBasedIndex` are dispatched to the range scan path.

### 6.5 `IndexEntry`

A catalog metadata struct (stored in `catalog.json`) describing a single index on a table. Fields: `column_name`, `algorithm`, `is_clustered`, `is_primary`, `include_columns`.

### 6.6 `IndexKey`

A typed enum wrapping the three supported key types (`INT`, `FLOAT`, `TEXT`). Used uniformly across all index variants so that insert, search, and delete operations are type-safe without requiring the caller to know the underlying key representation.

### 6.7 `RecordId`

```rust
struct RecordId {
    page_no: u32,
    item_id: u32,
}
```

The canonical physical address of a tuple. Stored as the value payload in every index entry. Used by the index scan executor to fetch tuples from the `.dat` file.

---

## 7. Backend Functions

### 7.1 Heap Manager — `src/backend/heap/mod.rs`

#### `insert_tuple_with_index_maintenance`
Wraps the raw tuple insertion path. After writing the tuple to the heap page, it extracts the value of every indexed column for that table from the new tuple's bytes, constructs the appropriate `IndexKey`, and calls `insert` on each applicable `AnyIndex` in memory. Updated index structures are then saved to their `.idx` files.

#### `remove_tuple_from_all_indexes`
Called during delete operations. Accepts the `RecordId` of the deleted tuple and the tuple's column values. Iterates over all `IndexEntry` records for the table and calls `delete` on each corresponding `AnyIndex`, removing the dangling `(key → RecordId)` mapping.

#### `page_free_space`
Returns the number of free bytes remaining in a given page by computing `upper - lower` from the page header. Used before every insertion to decide whether the current page has capacity or whether a new page must be allocated from disk.

### 7.2 Index Subsystem — `src/backend/index/`

#### `AnyIndex::load(path)`
Reads a `.idx` file from disk. Inspects the variant tag in the file header and delegates deserialization to the corresponding concrete type's loader. Returns a fully reconstructed `AnyIndex` ready for queries.

#### `AnyIndex::save(path)`
Serializes the current in-memory index state to a `.idx` file. Delegates to the concrete variant's serialization logic.

#### `AnyIndex::insert(key, rid)`
Forwards an insert call to the wrapped index variant.

#### `AnyIndex::search(&key) → Vec<RecordId>`
Forwards a point lookup to the wrapped index variant. Returns all `RecordId`s associated with the key.

#### `AnyIndex::delete(&key)`
Forwards a delete call to the wrapped index variant, removing the key and its record pointers.

#### `AnyIndex::entry_count() → usize`
Returns the number of key-record pairs currently stored in the index.

### 7.3 Executor — `src/backend/executor/`

#### `seq_scan` — `seq_scan.rs`
Legacy sequential scan. Iterates page by page over the table's `.dat` file. For each page, reads the `lower` pointer from the header, iterates over all `(offset, length)` metadata pairs, skips entries with `length = 0` (deleted), deserializes remaining tuples, and applies the query predicate. Returns all matching tuples.

#### `index_scan` — `index_scan.rs`
Index-accelerated scan. Resolves the appropriate `.idx` file from the `Catalog`, calls `AnyIndex::load()`, invokes `.search(&key)` to retrieve a `Vec<RecordId>`, optionally sorts the list (for clustered indexes), fetches the corresponding 8 KB pages, and returns the matching tuples.

### 7.4 Buffer Manager — `src/backend/buffer_manager/`

#### `flush_to_disk()`
Writes all dirty in-memory `Page` buffers to their corresponding positions in the `.dat` or `.idx` files on disk. Called after bulk load operations and after index mutations.

### 7.5 Catalog — `src/backend/catalog/`

Index creation modifies the catalog via the existing catalog write path — a new `IndexEntry` is appended to the table's `indexes` array and `catalog.json` is rewritten atomically.

---

## 8. Frontend and CLI Changes

**Source:** `src/frontend/menu.rs`

The CLI was extended with commands to interact with the new Indexing Subsystem:

### Index Creation Command

Users can define an index by specifying the index type, target column, and optional flags:

```
INDEX TYPE <algorithm> ON <table>(<column>) [CLUSTERED] [PRIMARY]
```

- `<algorithm>` — one of: `StaticHash`, `ChainedHash`, `ExtendibleHash`, `LinearHash`, `BTree`, `BPlusTree`, `RadixTree`, `SkipList`, `LSMTree`
- `CLUSTERED` — marks this index as defining the physical read order (at most one per table)
- `PRIMARY` — enforces key uniqueness

### Search Command

```
SEARCH <table> WHERE <column> = <value>
```

The executor checks the Catalog for a matching `IndexEntry`. If one exists, an index scan is used. Otherwise, a sequential scan is performed automatically.

### Range Scan Command

```
SEARCH <table> WHERE <column> BETWEEN <start> AND <end>
```

Restricted to tables with a tree-based index on the target column. Falls back to sequential scan if no tree index is present.

### Data Load Hook

The CLI exposes a bulk CSV ingestion command used for benchmarking and seeding:

```
LOAD CSV <filepath> INTO <table>
```

Internally, this triggers `load_csv_into_pages`, which formats data into slotted pages, caches them in the Buffer Manager, and flushes to disk. Index maintenance runs after the bulk load completes.

### Benchmark Command

```
BENCHMARK <table> <column> <value>
```

Runs the same point query twice — once via sequential scan and once via index scan — and reports the elapsed time for each in milliseconds, producing a direct comparison log.

---

## 9. Potential Future Work

The following limitations and design gaps in the current system represent natural directions for future development:

**Concurrency Control**
The system is entirely single-threaded. Adding reader-writer locking at the page or index level would be required before any multi-threaded or multi-client workload could be supported safely.

**ACID Transactions and WAL**
There is no Write-Ahead Logging or rollback capability. A crash mid-operation can leave the database in an inconsistent state. Introducing a WAL and recovery manager would bring the system closer to production-grade durability guarantees.

**Space Reclamation (VACUUM)**
Deleted tuples are marked in-place but their byte regions are never reused within a page. A compaction or VACUUM operation that rewrites pages and removes dead slots would reclaim this wasted space.

**Composite Index Keys**
Current indexes map a single column to `RecordId`s. Supporting multi-column composite keys (e.g., `INDEX ON orders(customer_id, date)`) would enable more selective index scans for queries with multiple equality predicates.

**Index-Only Scans for Covering Indexes**
While `include_columns` is stored in `IndexEntry`, the executor could be extended to detect cases where all columns required by a query are present in the index node — completely eliminating the secondary page fetch from the `.dat` file.

**Dynamic Index Selection (Query Planner)**
Currently, the executor uses an index if one exists for the queried column, with no cost-based selection. A lightweight query planner that estimates selectivity and chooses between available indexes (or prefers a sequential scan for very low-selectivity queries on small tables) would improve overall query performance.

**LSM Tree Compaction Scheduling**
The LSM Tree implementation relies on background compaction to maintain read performance. A more sophisticated compaction scheduler (e.g., leveled or tiered compaction policies) and explicit compaction triggers from the CLI would give operators more control over the read/write trade-off.

**Persistent B+ Tree Paging**
The current B+ Tree is serialized monolithically to a `.idx` file. Mapping tree nodes directly onto 8 KB pages (consistent with the page layer) would enable partial node loads and reduce memory pressure for very large indexes.