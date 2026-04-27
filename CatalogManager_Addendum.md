# Catalog Manager – Implementation Addendum

This document records design decisions and corrections that are **not present in
the original CatalogManager.txt** design document. Each entry explains what the
original document left unspecified or incorrect, and how it was resolved in the
implementation.

---

## 1. Catalog File Header Must Be a Full 8 192-Byte Page

### Problem (not in original doc)
The original document states:

> *page 0 – table header (first 4 bytes = total page count)*

It does not specify the byte length of page 0.  The disk manager implementation
in `disk_manager.rs` computes page offsets as:

```
offset = page_num * PAGE_SIZE   (PAGE_SIZE = 8 192)
```

If page 0 is written as fewer than 8 192 bytes, every subsequent `read_page`
call seeks to an offset that is out of bounds for the actual file size, causing
`UnexpectedEof` on every catalog scan.  The entire page-backend would
silently return empty results.

### Decision
Catalog files must use the **identical layout as user-table files**: page 0 is
a full 8 192-byte header page (first 4 bytes = page count = 1), followed by an
8 192-byte empty slotted data page.

### Implementation
`CatalogPageManager::create_catalog_file` delegates to `init_table()`, the same
function used for user tables, instead of writing a custom short header.

---

## 2. Removed `CatalogBuffer` – Page Caching Is Delegated to `CatalogCache`

### Problem (not in original doc)
The original document specifies `CatalogPageManager` as:

```
struct CatalogPageManager {
    buffer_manager: BufferManager,
    catalog_file_paths: Map<CatalogName, FilePath>,
}
```

The implementation added a private `CatalogBuffer` struct and
`buffers: HashMap<String, CatalogBuffer>` inside `CatalogPageManager` as a
stub for a per-catalog page buffer.  This field was never populated — every
CRUD method opened the file directly — making it pure dead code that only
added confusion.

### Decision
`CatalogPageManager` holds **only file-path mappings**.  In-memory caching of
catalog entries is the exclusive responsibility of `CatalogCache`
(`catalog/cache.rs`), which already provides LRU eviction over complete
deserialized entries.  Duplicating a raw page cache inside the page manager
would introduce a second, inconsistent view of the same data.

### Implementation
`CatalogBuffer` struct and the `buffers` field are removed entirely from
`CatalogPageManager`.

---

## 3. `update_catalog_tuple` Uses Delete-Then-Reinsert

### Problem (not in original doc)
The original document describes:

> *`update_catalog_tuple` – fetch page, update tuple, mark page dirty*

It does not address variable-length catalog tuples.  A naive in-place overwrite
only works when the serialized length of the new data is identical to the old
data.  Because all six system catalogs store variable-length strings (names,
expressions, owner fields), the length can and does change — e.g. renaming a
database or column.  Enforcing an exact-length precondition would silently
corrupt or reject valid DDL.

### Decision
`update_catalog_tuple` employs a **delete-then-reinsert** strategy:

1. **Logical delete**: zero the slot's length field in the slot directory.
   The offset is left intact; the space is not reclaimed immediately (deferred
   compaction, not yet implemented).
2. **Reinsert**: append the new tuple at the end of the last data page via
   `insert_tuple`, exactly as a fresh insert.

The method returns `(new_page_num, new_slot_id)` so callers can update any
cached location.

### Tradeoffs
- Leaves gaps (logically deleted slots) in pages; a future `vacuum_catalog`
  operation can compact them.
- The returned location must be used if the caller caches `(page, slot)` pairs;
  stale pairs remain readable but their length field is zero so `scan_catalog`
  skips them correctly.

### Signature change (from original)
```rust
// Original: returns ()
fn update_catalog_tuple(catalog_name, page_num, slot_id, new_data) -> Result<()>

// Revised: returns new location
fn update_catalog_tuple(catalog_name, page_num, slot_id, new_data) -> Result<(u32, u32)>
```

---

## 4. `insert_catalog_tuple` Returns the Exact Slot Index

### Problem (not in original doc)
The original document states `insert_catalog_tuple` returns `(page_num, slot_id)`.
The initial implementation returned `(total_pages - 1, 0)` — hardcoding the slot
to `0`, which is always the *first* slot on the page, not the one just written.

If a caller used this return value for a subsequent `delete_catalog_tuple` or
`read_catalog_tuple`, it would operate on the wrong tuple.

### Decision
After `insert_tuple` returns, re-read the page's `lower` pointer (which has
already been advanced by `insert_tuple`) and compute the exact slot:

```
slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1
```

This is correct because `insert_tuple` appends slots left-to-right: the slot
just written is always the last one (`num_slots - 1`).

---

## 5. `Catalog::alloc_oid` Persists the Counter When the Page Backend Is Active

### Problem (not in original doc)
The original document shows OID allocation through `OidCounter::allocate_oid`,
which calls `persist_counter()` on every allocation.  The `Catalog` struct holds
a plain `oid_counter: u32` field used by `Catalog::alloc_oid()` for convenience.
This in-memory increment was never written back to `pg_oid_counter.dat`.

Consequence: after any DDL session, the counter file retains the value from the
previous restart (`USER_OID_START = 10 000`).  On the next start,
`load_catalog_from_pages` restores `oid_counter` from the file, resetting it to
10 000 and causing every new object to receive an OID that collides with an
already-assigned one.

### Decision
`Catalog::alloc_oid` writes the incremented `next_oid` directly to
`OID_COUNTER_FILE` whenever `page_backend_active == true`.  In legacy JSON mode
the counter is captured implicitly inside `catalog.json` and this write is
skipped to avoid creating a stale counter file.

### Implementation
```rust
pub fn alloc_oid(&mut self) -> u32 {
    let oid = self.oid_counter;
    self.oid_counter += 1;
    if self.page_backend_active {
        // Write self.oid_counter (= next available) to pg_oid_counter.dat
        ...
    }
    oid
}
```

The write uses `seek(Start(0))` + `write_all` so the 4-byte counter is always
at offset 0, matching the format read by `OidCounter::load()`.

---

## Summary of Changes

| # | Component | Original doc says | What was wrong | Fix applied |
|---|-----------|-------------------|----------------|-------------|
| 1 | `page_manager.rs` – `create_catalog_file` | page 0 = header | 8-byte header vs 8 192-byte page breaks all seeks | delegate to `init_table()` |
| 2 | `page_manager.rs` – `CatalogBuffer` | use `BufferManager` | dead stub struct never populated | removed; caching is `CatalogCache`'s job |
| 3 | `page_manager.rs` – `update_catalog_tuple` | fetch + update in place | fails on any length change | delete-then-reinsert; returns new `(page, slot)` |
| 4 | `page_manager.rs` – `insert_catalog_tuple` | returns `(page_num, slot_id)` | slot hardcoded to 0 | compute from `lower` pointer after insert |
| 5 | `catalog/types.rs` – `Catalog::alloc_oid` | use `OidCounter` | counter never persisted; OIDs restart on every launch | write to `pg_oid_counter.dat` when `page_backend_active` |
| 6 | `indexes.rs` – `drop_index` | delete tuple at stored (page, slot) | used fabricated `(page_num_acc=1, i as u32)` — `page_num_acc` never incremented, `i` was the Vec index not the page slot | rewrote to use `find_catalog_tuple` which returns the real `(page_num, slot_id)` |
| 7 | `cache.rs` – `CatalogCache` | wired into `Catalog`; invalidated on DDL | fully implemented but never instantiated or called | added `cache: CatalogCache` field to `Catalog` struct; added `default_instance()` constructor; added `#[derive(Debug)]`; added `invalidate_*` calls in every DDL mutation in `catalog.rs`, `constraints.rs`, and `indexes.rs` |

---

## Issue 6 Detail — `drop_index` wrong (page, slot)

**Root cause:** The original loop tracked a `page_num_acc` variable that was
initialised to `1` and never incremented.  The slot was taken from `i`, the
position in the `scan_catalog` result `Vec`, which is unrelated to the actual
slot number on the storage page.  Calling `delete_catalog_tuple` with these
fabricated coordinates would zero a random unrelated slot.

**Fix:** Replaced the manual scan with `pm.find_catalog_tuple(CAT_INDEX, |b|
deserialize_index_tuple(b).map(|idx| idx.index_oid == index_oid).unwrap_or(false))`.
The page manager returns the real `(page_num, slot_id, raw_bytes)` tuple.  The
index metadata is deserialized from `raw_bytes` (avoids a second scan), and the
correct coordinates are passed to `delete_catalog_tuple`.

---

## Issue 7 Detail — `CatalogCache` dead module

**Root cause:** `cache.rs` implemented a fully functional LRU cache (`CatalogCache`)
but the `Catalog` struct had no `cache` field, so the cache was never allocated
and `invalidate_*` / `insert_*` were never called.

**Fix summary:**

1. Added `#[derive(Debug)]` to `CatalogCache` (required by `Catalog`'s `Debug` derive).
2. Added `pub fn default_instance() -> Self { Self::new(256) }` to `CatalogCache`.
3. In `types.rs` — added `use crate::catalog::cache::CatalogCache` and:
   ```rust
   #[serde(skip, default = "CatalogCache::default_instance")]
   pub cache: CatalogCache,
   ```
   plus `cache: CatalogCache::default_instance()` in `Catalog::new()`.
4. Added `catalog.cache.invalidate_*` calls at every DDL mutation point:

| Function | File | Invalidation added |
|---|---|---|
| `create_database` (legacy) | `catalog.rs` | `invalidate_database(db_name)` |
| `create_database` | `catalog.rs` | `invalidate_database(db_name)` |
| `drop_database` | `catalog.rs` | `invalidate_database(db_name)` |
| `create_table` (legacy) | `catalog.rs` | `invalidate_table(db_oid, table_name)` |
| `create_table` | `catalog.rs` | `invalidate_table(db_oid, table_name)` |
| `drop_table` | `catalog.rs` | `invalidate_table`, `invalidate_constraints`, `invalidate_indexes` |
| `alter_table_add_column` | `catalog.rs` | `invalidate_constraints(table_oid)` |
| `add_primary_key_constraint` | `constraints.rs` | `invalidate_constraints(table_oid)` |
| `add_foreign_key_constraint` | `constraints.rs` | `invalidate_constraints(table_oid)` |
| `add_unique_constraint` | `constraints.rs` | `invalidate_constraints(table_oid)` |
| `add_not_null_constraint` | `constraints.rs` | `invalidate_constraints(table_oid)` |
| `create_index` | `indexes.rs` | `invalidate_indexes(table_oid)` |
| `drop_index` | `indexes.rs` | `invalidate_indexes(index.table_oid)` |

`cargo check` passes with exit 0 (4 pre-existing unused-import warnings only).
