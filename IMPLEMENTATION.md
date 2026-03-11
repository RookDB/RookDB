# RookDB — Index Subsystem: Implementation Status

## What Is Implemented

### Milestone 1 — Trait Hierarchy & Shared Types
| File | Contents |
|------|----------|
| `code/src/backend/index/index_trait.rs` | `IndexTrait`, `TreeBasedIndex`, `HashBasedIndex`; `IndexKey` (Int/Float/Text), `RecordId` |
| `code/src/backend/index/config.rs` | `HashIndexType`, `TreeIndexType` enums; all tuning constants; `DEFAULT_HASH_INDEX`, `DEFAULT_TREE_INDEX` |

**To switch algorithms**: change `DEFAULT_HASH_INDEX` or `DEFAULT_TREE_INDEX` in `config.rs` and recompile. No other changes needed.

---

### Milestone 2 — George: Hash Indices
| File | Algorithm | Status |
|------|-----------|--------|
| `code/src/backend/index/hash/static_hash.rs` | Static Hash Index | ✅ Complete |
| `code/src/backend/index/hash/extendible_hash.rs` | Extendible Hashing | ✅ Complete |
| `code/src/backend/index/hash/linear_hash.rs` | Linear Hashing | ✅ Complete |

**Static Hash**: Fixed `N` primary buckets (configurable via `STATIC_HASH_NUM_BUCKETS`). Each bucket holds up to `STATIC_HASH_BUCKET_CAPACITY` entries. Overflow is handled via chained `OverflowSegment`s appended to each bucket. No resizing occurs.

**Extendible Hash**: Directory of `2^global_depth` slots, each pointing to a physical bucket with a `local_depth`. Bucket overflow triggers a split; if `local_depth == global_depth` the directory doubles first. Multiple directory slots may share one physical bucket (shared-page model). Starting global depth is configurable; defaults to initial depth 1 (2 buckets).

**Linear Hash**: Begins with `N₀` buckets. Hash function at level `l` is `h_l(k) = hash(k) mod (N₀ · 2^l)`. If `h_l(k) < split_ptr` (the bucket has already been split this round), `h_{l+1}` is used instead. A split is triggered when the global load factor exceeds `LINEAR_HASH_LOAD_FACTOR_THRESHOLD` (default 0.75). Overflow within buckets uses the same chained-segment design as the static index.

---

### Milestone 2 — Sujay: Tree Indices
| File | Algorithm | Status |
|------|-----------|--------|
| `code/src/backend/index/tree/btree.rs` | B-Tree | ✅ Complete |
| `code/src/backend/index/tree/bplus_tree.rs` | B+ Tree | ✅ Complete |

Both tree implementations use an **arena** (`Vec<Node>` + `usize` indices) to avoid Rust ownership / lifetime issues with recursive tree structures.

**B-Tree**: Classic Knuth/CLRS B-Tree. Values (RecordId lists) stored at every node level. Insert: proactive split on the way down (root split handled before descent). Delete: full CLRS algorithm — Case 2a (replace with predecessor), 2b (successor), 2c (merge); Case 3 with `fix_child` (rotate-right, rotate-left, or merge). Structurally removes a key only when its last `RecordId` is deleted. Range scan: recursive in-order DFS.

**B+ Tree**: Values only at leaf level; internal nodes carry routing keys. Leaves linked via `next_leaf` pointers for O(log n + k) range scans. Insert: bottom-up split propagation through a path stack. Delete: leaf deletion with sibling borrow or merge propagated upward through `fix_leaf_underflow` and `fix_internal_underflow`. Range scan: find start leaf by descent, then walk the linked leaf chain.

---

### Milestone 2 — Mithun: Radix Tree + Linear Hash
| File | Algorithm | Status |
|------|-----------|--------|
| `code/src/backend/index/tree/radix_tree.rs` | Radix Tree | ✅ Complete |
| `code/src/backend/index/hash/linear_hash.rs` | Linear Hash | ✅ Complete (see George section) |

**Radix Tree**: Compressed trie (Patricia tree). Keys are converted to bytes via `IndexKey::as_bytes()` with encoding that preserves sort order for all key types (sign-bit flip for Int, IEEE-754 reordering for Float, raw UTF-8 for Text). Each `RadixNode` carries a compressed edge prefix and a `BTreeMap<u8, Box<RadixNode>>` for children (sorted for in-order iteration). Supports insert, search, delete with path compression on delete. Range scan implemented via `collect_range` DFS respecting byte-order bounds.

---

### Supporting Infrastructure

| File | Purpose |
|------|---------|
| `code/src/backend/index/manager.rs` | `AnyIndex` enum dispatcher (wraps all 6 concrete types); `build_from_table` (scan existing heap and populate index); `index_file_path` helper |
| `code/src/backend/index/mod.rs` | Module root; re-exports all public types |
| `code/src/backend/catalog/types.rs` | Added `IndexAlgorithm` enum, `IndexEntry` struct, `indexes: Vec<IndexEntry>` field on `Table` |
| `code/src/backend/catalog/catalog.rs` | Added `create_index`, `drop_index`, `list_indexes` |
| `code/src/backend/layout.rs` | Added `INDEX_FILE_TEMPLATE` (`database/base/{db}/{table}_{index}.idx`) |
| `code/src/frontend/index_cmd.rs` | CLI handlers: Create Index, Drop Index, List Indexes, Search by Index, Range Scan |
| `code/src/frontend/menu.rs` | Menu options 9–14 for index operations |

---

## What Is Not Yet Implemented

| Feature | Notes |
|---------|-------|
| **Index maintenance on insert** | When new rows are inserted via `load_csv` or `insert_tuple`, existing secondary indices are not automatically updated. The index is only populated at creation time via `build_from_table`. |
| **Index maintenance on delete** | Heap-level tuple deletion is not yet implemented in the storage engine, so index delete paths are not exercised end-to-end. |
| **Persistent B-Tree compaction** | Tombstoned (merged) arena nodes are not reclaimed. For long-running workloads this wastes memory; a periodic rebuild would be needed. |
| **Benchmarking harness** | Comparative benchmarks between algorithms are deferred to a later milestone. The `DEFAULT_HASH_INDEX` / `DEFAULT_TREE_INDEX` constants in `config.rs` are the intended switch points. |
| **Float column indexing** | `extract_key` in `manager.rs` only decodes INT and TEXT columns (matching the current `load_csv` encoder). Float support requires adding float encoding to both systems. |
| **Unique index enforcement** | All indices are non-unique (multiple RecordIds per key are allowed). Unique-index constraint checking is not implemented. |
| **Composite (multi-column) indices** | Only single-column secondary indices are supported. |

---

## Algorithm Selection Config

Edit `code/src/backend/index/config.rs`:

```rust
// Change to select which algorithm is used when "hash" or "tree" is chosen:
pub const DEFAULT_HASH_INDEX: HashIndexType = HashIndexType::Extendible; // or Static / Linear
pub const DEFAULT_TREE_INDEX: TreeIndexType = TreeIndexType::BPlusTree;  // or BTree / RadixTree

// Tuning parameters:
pub const BTREE_MIN_DEGREE: usize = 4;
pub const EXTENDIBLE_HASH_BUCKET_CAPACITY: usize = 4;
pub const STATIC_HASH_NUM_BUCKETS: usize = 64;
pub const LINEAR_HASH_LOAD_FACTOR_THRESHOLD: f64 = 0.75;
```

## File Layout

```
code/src/backend/index/
├── config.rs               ← algorithm selector + tuning constants
├── index_trait.rs          ← IndexTrait, TreeBasedIndex, HashBasedIndex, IndexKey, RecordId
├── manager.rs              ← AnyIndex dispatcher, build_from_table, index_file_path
├── mod.rs
├── hash/
│   ├── static_hash.rs      ← George
│   ├── extendible_hash.rs  ← George
│   ├── linear_hash.rs      ← Mithun
│   └── mod.rs
└── tree/
    ├── btree.rs            ← Sujay
    ├── bplus_tree.rs       ← Sujay
    ├── radix_tree.rs       ← Mithun
    └── mod.rs

Index files on disk: database/base/{db}/{table}_{index}.idx  (JSON-serialised)
Catalog: database/global/catalog.json  (now includes "indexes" array per table)
```
