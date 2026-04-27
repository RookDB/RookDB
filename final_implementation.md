# RookDB Catalog Manager — Complete Technical Implementation Report

**Project:** RookDB — A Page-Based Relational Database Engine in Rust  
**Component:** Self-Hosting System Catalog Manager  
**Crate:** `storage_manager`

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Physical Storage Layer](#3-physical-storage-layer)
4. [Buffer Manager](#4-buffer-manager)
5. [Catalog File Layout](#5-catalog-file-layout)
6. [The Six System Catalogs](#6-the-six-system-catalogs)
7. [OID (Object Identifier) System](#7-oid-object-identifier-system)
8. [CatalogPageManager — CRUD Engine](#8-catalogpagemanager--crud-engine)
9. [Catalog Cache (LRU Layer)](#9-catalog-cache-lru-layer)
10. [Binary Serialization Protocol](#10-binary-serialization-protocol)
11. [High-Level Catalog Operations](#11-high-level-catalog-operations)
12. [Constraint System](#12-constraint-system)
13. [B-Tree Index Engine](#13-b-tree-index-engine)
14. [Query Executor Integration](#14-query-executor-integration)
15. [Heap File Manager](#15-heap-file-manager)
16. [Enhancements and Bug Fixes Applied](#16-enhancements-and-bug-fixes-applied)
17. [Test Suite](#17-test-suite)
18. [End-to-End Data Flow](#18-end-to-end-data-flow)
19. [Directory and File Reference](#19-directory-and-file-reference)

---

## 1. Project Overview

RookDB is a from-scratch relational database engine written in Rust. It implements the core storage and catalog machinery that underpins any production RDBMS: a **page-based buffer pool**, a **self-hosting system catalog**, a **slotted-page heap file**, a **B-Tree index**, and a **query executor** capable of sequential scans and CSV bulk-loads.

The **Catalog Manager** is the centerpiece. It mirrors PostgreSQL's approach: instead of storing metadata in a separate in-memory structure, metadata is stored in regular page files using the exact same slotted-page format as user tables. The catalog reads and writes itself through the same buffer manager that user data uses.

### What the Catalog Manager does

| Capability | Detail |
|---|---|
| Database lifecycle | CREATE / DROP / LIST databases |
| Table lifecycle | CREATE / DROP / ALTER TABLE ADD COLUMN |
| Type system | 10 built-in types with OID registry in `pg_type` |
| Constraint enforcement | PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL, CHECK |
| Index management | Create / Drop B-Tree indexes, catalog entry in `pg_index` |
| Statistics tracking | Row count and page count updated on every bulk load |
| Persistence | All metadata durable on disk, survives process restart |
| Cache | LRU in-memory layer avoids repeated page scans |

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend (CLI)                        │
│  menu.rs  database_cmd.rs  table_cmd.rs  data_cmd.rs    │
└──────────────────────┬──────────────────────────────────┘
                       │ calls
┌──────────────────────▼──────────────────────────────────┐
│                Catalog Manager (High Level)              │
│  catalog.rs  constraints.rs  indexes.rs                  │
│  ┌──────────────────────────────────────────────────┐   │
│  │               CatalogCache (LRU)                 │   │
│  └──────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────┐   │
│  │           CatalogPageManager (CRUD)              │   │
│  │  insert_catalog_tuple   scan_catalog             │   │
│  │  find_catalog_tuple     delete_catalog_tuple     │   │
│  │  update_catalog_tuple   compact_page_data        │   │
│  └────────────────────┬─────────────────────────────┘   │
│                       │ pin_page / unpin_page            │
│  ┌────────────────────▼─────────────────────────────┐   │
│  │              BufferManager (LRU Pool)            │   │
│  │  frames: Vec<Page>   page_table: HashMap         │   │
│  │  lru_order: Vec      max_pages: 256              │   │
│  └────────────────────┬─────────────────────────────┘   │
│                       │ read_page / write_page           │
│  ┌────────────────────▼─────────────────────────────┐   │
│  │              Disk Manager                        │   │
│  │  create_page  read_page  write_page              │   │
│  └────────────────────┬─────────────────────────────┘   │
└──────────────────────┬──────────────────────────────────┘
                       │
                 ┌─────▼──────┐
                 │  .dat files│
                 │  .idx files│
                 └────────────┘
```

### Module Map

| Path | Role |
|---|---|
| `src/backend/layout.rs` | Physical path constants, OID ranges, built-in type OIDs |
| `src/backend/page/mod.rs` | `Page` struct, `PAGE_SIZE=8192`, `init_page`, `page_free_space` |
| `src/backend/disk/` | `create_page`, `read_page`, `write_page` — raw file I/O |
| `src/backend/buffer_manager/` | LRU buffer pool, `pin_page`, `unpin_page`, `flush_pages` |
| `src/backend/heap/mod.rs` | `init_table`, `insert_tuple` — slotted-page heap writes |
| `src/backend/table/` | `page_count`, `TABLE_HEADER_SIZE` — table file header utilities |
| `src/backend/catalog/types.rs` | All catalog data structures and error types |
| `src/backend/catalog/oid.rs` | Persistent OID counter (`pg_oid_counter.dat`) |
| `src/backend/catalog/page_manager.rs` | Low-level CRUD over catalog `.dat` files |
| `src/backend/catalog/cache.rs` | LRU in-memory metadata cache |
| `src/backend/catalog/serialize.rs` | Binary encode/decode for all 6 catalog tuple types |
| `src/backend/catalog/catalog.rs` | High-level DDL: init, bootstrap, create/drop DB/table |
| `src/backend/catalog/constraints.rs` | ADD PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL |
| `src/backend/catalog/indexes.rs` | CREATE/DROP index + full page-based B-Tree |
| `src/backend/executor/seq_scan.rs` | Sequential scan + tuple decode + print |
| `src/backend/executor/load_csv.rs` | CSV bulk-load with constraint check + index update |

---

## 3. Physical Storage Layer

### Page Format

Every file in RookDB — user tables and system catalogs alike — is composed of **8 KB pages**.

```
┌─────────────────────────────────────────────────────────────┐
│ PAGE (8192 bytes)                                           │
├──────────┬──────────┬─────────────────────────────────────┤
│ lower(4) │ upper(4) │ slot[0] slot[1] … slot[N-1]  ▶ … ◀ tuple data │
│ (u32 LE) │ (u32 LE) │ ← slot array grows right          tuple data grows left │
└──────────┴──────────┴─────────────────────────────────────┘
  byte 0     byte 4     byte 8
```

| Field | Bytes | Meaning |
|---|---|---|
| `lower` | 0–3 | Offset just past the last slot entry (slot array end) |
| `upper` | 4–7 | Offset of the topmost (lowest-address) tuple |
| Slot `i` | `8 + i*8` | `offset(4) + length(4)` pointing into tuple data area |
| Tuple data | grows from page end inward | Raw serialized tuple bytes |

**Constants:**
- `PAGE_SIZE = 8192` bytes
- `PAGE_HEADER_SIZE = 8` bytes (the two pointers)
- `ITEM_ID_SIZE = 8` bytes per slot entry

**Free space** = `upper - lower`. A new tuple of size `N` requires `N + 8` bytes of free space (data + one slot entry).

### File Layout

Every `.dat` file (both catalog and user table) has this structure:

```
Page 0  [8192 bytes]  Table Header — first 4 bytes = total page count (u32 LE)
Page 1  [8192 bytes]  First slotted data page
Page 2  [8192 bytes]  Second slotted data page
…
```

`init_table` writes page 0 with `page_count = 1` and immediately appends page 1 via `create_page`. A freshly initialized file is always exactly 16 384 bytes (2 pages).

---

## 4. Buffer Manager

**File:** `src/backend/buffer_manager/buffer_manager.rs`

The buffer manager maintains a fixed-size pool of in-memory page frames and mediates all disk I/O. No code in the catalog or executor ever calls `read_page` / `write_page` directly — everything goes through the buffer manager.

### Data Structures

```rust
struct PageId {
    file_path: String,   // absolute or relative path to the .dat file
    page_num:  u32,      // zero-based page index within that file
}

struct PageMetadata {
    page_id:      PageId,
    pin_count:    u32,    // number of active holders
    is_dirty:     bool,   // written since last flush?
    last_accessed: u64,   // for LRU eviction ordering
    frame_index:  usize,  // index into frames Vec
}

struct BufferManager {
    frames:     Vec<Page>,                    // raw page data, max_pages slots
    page_table: HashMap<String, PageMetadata>,// key = "path:page_num"
    lru_order:  Vec<String>,                  // LRU eviction queue
    max_pages:  usize,                        // default 256
}
```

### API

| Method | Description |
|---|---|
| `pin_page(id) -> usize` | Load page into a frame if not already cached. Increment pin_count. Return frame index. |
| `unpin_page(id, is_dirty)` | Decrement pin_count. If `is_dirty=true`, mark frame dirty. |
| `flush_pages()` | Write every dirty frame back to disk. |
| `evict_page()` (private) | Find oldest unpinned frame in LRU order, flush if dirty, release slot. |

### Usage Pattern

Every catalog CRUD operation follows this pattern:
```rust
let fi = bm.pin_page(PageId::new(&path, page_num))?;   // get frame index
let page = &mut bm.frames[fi];                          // access page data
// ... read or modify page.data ...
bm.unpin_page(&PageId::new(&path, page_num), is_dirty)?; // release
```

The `is_dirty` flag on unpin is `true` whenever the page was written, `false` for read-only access. This ensures dirty pages are eventually flushed to disk.

---

## 5. Catalog File Layout

The catalog lives in `database/global/catalog_pages/`. Six files, one per system catalog:

```
database/
├── global/
│   ├── catalog_pages/
│   │   ├── pg_database.dat     ← database records
│   │   ├── pg_table.dat        ← table records
│   │   ├── pg_column.dat       ← column records
│   │   ├── pg_constraint.dat   ← constraint records
│   │   ├── pg_index.dat        ← index records
│   │   └── pg_type.dat         ← type records
│   └── pg_oid_counter.dat      ← 4-byte persistent OID counter
└── base/
    └── {database}/
        ├── {table}.dat         ← user table heap files
        └── indexes/
            └── {index}.idx     ← B-Tree index files
```

Each `.dat` file uses the identical page format as user tables. There is nothing structurally special about catalog files — they are just slotted-page files accessed through the same buffer manager as everything else.

---

## 6. The Six System Catalogs

### 6.1 `pg_database` — Database Registry

One record per database. Created by `CREATE DATABASE`, deleted by `DROP DATABASE`.

| Field | Type | Description |
|---|---|---|
| `db_oid` | u32 | Globally unique OID |
| `db_name` | varchar | Database name |
| `db_owner` | varchar | Owner string |
| `created_at` | u64 | Unix timestamp |
| `encoding` | u8 | 1=UTF8, 2=ASCII |

The built-in `system` database is written during bootstrap with `SYSTEM_DB_OID = 11`.

### 6.2 `pg_table` — Table Registry

One record per user-created table.

| Field | Type | Description |
|---|---|---|
| `table_oid` | u32 | Globally unique OID |
| `table_name` | varchar | Table name |
| `db_oid` | u32 | Foreign key → pg_database |
| `table_type` | u8 | 0=UserTable, 1=SystemCatalog |
| `row_count` | u64 | Updated by load_csv |
| `page_count` | u32 | Updated by load_csv |
| `created_at` | u64 | Unix timestamp |

### 6.3 `pg_column` — Column Registry

One record per column. Columns are ordered by `column_position`.

| Field | Type | Description |
|---|---|---|
| `column_oid` | u32 | Globally unique OID |
| `table_oid` | u32 | Foreign key → pg_table |
| `column_name` | varchar | Column name |
| `column_pos` | u16 | 1-based position |
| `type_oid` | u32 | Foreign key → pg_type |
| `type_length` | i16 | Fixed byte size, or -1 for variable |
| `type_align` | u8 | Alignment requirement (1/2/4/8) |
| `type_category` | u8 | Numeric/String/DateTime/Boolean/Binary |
| `type_name` | varchar | e.g. "INT", "TEXT", "VARCHAR(64)" |
| `is_builtin` | u8 | 1 if a system-defined type |
| `type_modifier` | optional | VarcharLen or Precision+Scale |
| `is_nullable` | u8 | 0 = NOT NULL |
| `default_value` | optional | Encoded default expression |
| `constraint_oids` | u32[] | OIDs of constraints on this column |

### 6.4 `pg_constraint` — Constraint Registry

One record per constraint (PK, FK, UNIQUE, NOT NULL, CHECK).

| Field | Type | Description |
|---|---|---|
| `constraint_oid` | u32 | Globally unique OID |
| `constraint_name` | varchar | Human-readable name |
| `constraint_type` | u8 | 1=PK, 2=FK, 3=UNIQUE, 4=NOT NULL, 5=CHECK |
| `table_oid` | u32 | Table this constraint belongs to |
| `column_oids` | u32[] | Columns covered |
| `is_deferrable` | u8 | Deferral flag |
| metadata | type-specific | See below |

**Type-specific metadata:**
- PK / UNIQUE: `index_oid(4)` — OID of backing B-Tree index
- FK: `referenced_table_oid(4)`, `referenced_column_oids[]`, `on_delete(1)`, `on_update(1)`
- NOT NULL: no extra bytes
- CHECK: `check_expression(varchar)`

### 6.5 `pg_index` — Index Registry

One record per index.

| Field | Type | Description |
|---|---|---|
| `index_oid` | u32 | Globally unique OID |
| `index_name` | varchar | Used to locate the `.idx` file |
| `table_oid` | u32 | Foreign key → pg_table |
| `index_type` | u8 | 1=BTree, 2=Hash |
| `column_oids` | u32[] | Indexed columns |
| `is_unique` | u8 | Uniqueness guarantee |
| `is_primary` | u8 | Set for PK-backing indexes |
| `index_pages` | u32 | Page count of index file |

### 6.6 `pg_type` — Type Registry

One record per data type. Populated during bootstrap with all 10 built-in types.

| OID | Name | Length | Align | Category |
|---|---|---|---|---|
| 1 | INT | 4 | 4 | Numeric |
| 2 | BIGINT | 8 | 8 | Numeric |
| 3 | FLOAT | 4 | 4 | Numeric |
| 4 | DOUBLE | 8 | 8 | Numeric |
| 5 | BOOL | 1 | 1 | Boolean |
| 6 | TEXT | -1 | 1 | String |
| 7 | VARCHAR(255) | -1 | 1 | String |
| 8 | DATE | 4 | 4 | DateTime |
| 9 | TIMESTAMP | 8 | 8 | DateTime |
| 10 | BYTES | -1 | 1 | Binary |

---

## 7. OID (Object Identifier) System

**File:** `src/backend/catalog/oid.rs`, `src/backend/catalog/types.rs`

### Design

Every database object — database, table, column, constraint, index, type — receives a globally unique 32-bit unsigned integer called an OID. OIDs are monotonically increasing and never reused.

### OID Ranges

| Range | Reserved for |
|---|---|
| 1–10 | Built-in type OIDs (compile-time constants in `layout.rs`) |
| 11 | System database OID (`SYSTEM_DB_OID`) |
| 12–9 999 | Reserved for future system objects |
| 10 000+ | All user-created objects (`USER_OID_START`) |

### Allocation — `Catalog::alloc_oid`

```rust
pub fn alloc_oid(&mut self) -> u32 {
    let oid = self.oid_counter;       // return current, then advance
    self.oid_counter += 1;
    if self.page_backend_active {     // persist immediately when live
        // write self.oid_counter (next value) to pg_oid_counter.dat
    }
    oid
}
```

The counter file always stores the **next** value to be allocated, so on restart `load_catalog` reads the file into `catalog.oid_counter` and the first `alloc_oid()` call continues without any gap or collision.

### `OidCounter` struct

`OidCounter` manages the counter file at `database/global/pg_oid_counter.dat`:
- `OidCounter::initialize()` — called at bootstrap: creates the file with `USER_OID_START` if absent
- `OidCounter::load()` — called at startup: reads the persisted value into `next_oid`
- `OidCounter::persist()` — writes `next_oid` back to disk at offset 0

---

## 8. CatalogPageManager — CRUD Engine

**File:** `src/backend/catalog/page_manager.rs`

`CatalogPageManager` owns a `HashMap<&str, &str>` mapping catalog names (`"pg_database"`, etc.) to their file paths. All six CRUD operations take `&mut BufferManager` and a catalog name string.

### `insert_catalog_tuple`

```
1. pin page 0 → read total_pages
2. pin last data page (page total_pages - 1)
3. if free_space(last_page) < len(data) + ITEM_ID_SIZE:
       unpin last_page (clean)
       create_page() → append new page to file
       increment total_pages in page 0 (dirty)
       unpin page 0 (dirty), pin new last page
   else:
       unpin page 0 (clean)
4. write tuple bytes at upper pointer (growing downward)
5. write slot entry at lower pointer: [offset, length]
6. slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE   ← before incrementing
7. lower += ITEM_ID_SIZE
8. update lower and upper pointers in page header
9. unpin page (dirty)
10. return (page_num, slot_id)
```

### `scan_catalog`

Reads every live tuple across all data pages:
```
for page_num in 1..total_pages:
    pin page
    read lower → num_slots = (lower - 8) / 8
    for slot in 0..num_slots:
        read offset and length from slot entry
        if length == 0: skip (tombstone)
        collect page.data[offset..offset+length]
    unpin page (clean)
```

### `find_catalog_tuple`

Same as scan but short-circuits on the first tuple satisfying a predicate. Returns `(page_num, slot_id, bytes)`.

### `delete_catalog_tuple`

```
1. pin page
2. zero the length field in slot entry (tombstone)
3. compact_page_data(page)   ← reclaim data space
4. unpin (dirty)
```

### `compact_page_data` — Space Reclamation

After any deletion, the data area of the page is compacted:

```
1. collect all slots where length > 0 (live)
2. new_upper = PAGE_SIZE
3. for each live (slot, data):
       new_upper -= len(data)
       copy data to page[new_upper .. new_upper+len]
       update slot.offset = new_upper
4. page.upper = new_upper
```

This reclaims the data bytes occupied by deleted tuples on every delete operation. The slot array is not shrunk (tombstone entries remain), but the entire data area is made available for new inserts.

### `update_catalog_tuple`

Implemented as delete + re-insert. The deleted slot becomes a tombstone and is compacted; the new version is appended to the last data page.

---

## 9. Catalog Cache (LRU Layer)

**File:** `src/backend/catalog/cache.rs`

`CatalogCache` is a pure in-memory LRU cache that sits above `CatalogPageManager`. It avoids redundant page scans for hot objects.

### Cached Collections

| Cache | Key | Value | Invalidated by |
|---|---|---|---|
| `databases` | `db_name: String` | `Database` | `drop_database` |
| `tables` | `(db_oid, table_name)` | `Table` | `drop_table`, `alter_table` |
| `constraints` | `table_oid: u32` | `Vec<Constraint>` | any constraint change |
| `indexes` | `table_oid: u32` | `Vec<Index>` | `create_index`, `drop_index` |
| `types` | `type_name: String` | `DataType` | (never, types are immutable) |

### LRU Eviction

Each cache uses an internal access counter. When the cache reaches its capacity, the entry with the smallest `last_accessed` counter is evicted.

### Usage in DDL

Every `get_database` / `get_table` call checks the cache first:
```rust
if let Some(db) = catalog.cache.get_database(db_name) {
    return Ok(db.clone());
}
// cache miss → scan pg_database pages, insert result into cache
```

---

## 10. Binary Serialization Protocol

**File:** `src/backend/catalog/serialize.rs`

All catalog records are stored as variable-length byte slices inside slotted pages. The protocol uses little-endian encoding throughout.

### Encoding Rules

| Field type | Encoding |
|---|---|
| `u8` | 1 byte |
| `u16` | 2 bytes LE |
| `u32` | 4 bytes LE |
| `i16` | 2 bytes LE |
| `u64` | 8 bytes LE |
| `String` / `&str` | `u16 length` + raw UTF-8 bytes |
| `Vec<u32>` | `u16 count` + N×4 bytes |
| Optional field | `u8 flag (0=absent, 1=present)` + payload if present |

### Column Tuple Wire Format

```
column_oid (4)
table_oid  (4)
column_name (varchar)
column_pos  (2)
type_oid    (4)
type_length (2, signed)
type_align  (1)
type_category (1)
type_name  (varchar)
is_builtin  (1)             ← stores actual value, not hardcoded
type_mod_flag (1):
    0 = no modifier
    1 = VarcharLen → u16 max_length
    2 = Precision  → u8 precision + u8 scale
is_nullable (1)
has_default (1):
    0 = no default
    1 = default → tag(1) + payload
        tag 1 = Integer(i32)   tag 5 = Str(varchar)
        tag 2 = BigInt(i64)    tag 6 = Boolean(u8)
        tag 3 = Float(f32)     tag 7 = Null
        tag 4 = Double(f64)    tag 8 = CurrentTimestamp
constraint_oids (u32[])
```

### Constraint Tuple Wire Format

```
constraint_oid (4)
constraint_name (varchar)
constraint_type (1): 1=PK, 2=FK, 3=UNIQUE, 4=NOT_NULL, 5=CHECK
table_oid (4)
column_oids (u32[])
is_deferrable (1)
--- type-specific payload ---
PK/UNIQUE: index_oid (4)
FK: referenced_table_oid (4) + referenced_column_oids (u32[])
    + on_delete (1) + on_update (1)
NOT NULL: (empty)
CHECK: check_expression (varchar)
```

---

## 11. High-Level Catalog Operations

**File:** `src/backend/catalog/catalog.rs`

### Bootstrap (`bootstrap_catalog`)

Called once on first startup:
1. Create `database/global/` and `database/base/`
2. `OidCounter::initialize()` — write `USER_OID_START` to counter file
3. `CatalogPageManager::initialize_files()` — create all 6 `.dat` files with `init_table`
4. Insert all 10 built-in `DataType` records into `pg_type`
5. Insert the system database record into `pg_database` with `SYSTEM_DB_OID = 11`

### Load (`load_catalog`)

Called every startup after the first:
1. `OidCounter::load()` — read persisted counter from disk
2. Set `catalog.oid_counter = oid_ctr.next_oid`
3. Set `catalog.page_backend_active = true`
4. Return the `Catalog` handle (all metadata is lazy-loaded on demand)

### `create_database`

1. Reject empty name or existing name
2. `alloc_oid()` → `db_oid`
3. `fs::create_dir_all(database/base/{db_name})`
4. Serialize and insert into `pg_database`
5. Insert into `CatalogCache`

### `drop_database`

1. Find all tables with `db_oid` in `pg_table`
2. `drop_table` each one (cascades to indexes and constraints)
3. Delete database record from `pg_database`
4. `fs::remove_dir_all(database/base/{db_name})`
5. Invalidate cache

### `create_table`

1. Verify database exists
2. Reject duplicate table name in same database
3. `alloc_oid()` → `table_oid`
4. For each column definition:
   - Resolve type via `DataType::from_name` (or `pg_type` scan)
   - `alloc_oid()` → `column_oid`
   - Serialize and insert into `pg_column`
5. `init_table` → create `database/base/{db}/table.dat`
6. Insert into `pg_table`
7. Apply each `ConstraintDefinition` (PK triggers `add_primary_key_constraint`, etc.)

### `drop_table`

1. Check for FK dependencies from other tables (reject if found)
2. Drop all indexes (`drop_index` each)
3. Locate and delete `pg_table` record
4. `fs::remove_file(table.dat)`
5. Invalidate cache for table, constraints, indexes

### `alter_table_add_column`

1. Reject NOT NULL column without a default value
2. Reject duplicate column name
3. `alloc_oid()` → `column_oid`
4. Insert new `pg_column` record with `column_position = existing_count + 1`

---

## 12. Constraint System

**File:** `src/backend/catalog/constraints.rs`

### `add_primary_key_constraint`

1. Scan `pg_constraint` — reject if table already has a PK
2. For each PK column name: look up `column_oid` in `pg_column`
3. Create a B-Tree index via `create_index` (unique=true, primary=true)
4. Serialize `Constraint { PrimaryKey { index_oid } }` → insert into `pg_constraint`
5. Update each PK column: set `is_nullable = false` → `update_catalog_tuple` on `pg_column`
6. Invalidate constraint and column cache

### `add_foreign_key_constraint`

1. Validate referencing column count == referenced column count
2. Look up referenced table OID
3. Verify referenced columns are covered by a PK or UNIQUE constraint
4. Insert FK constraint record into `pg_constraint`

### `add_unique_constraint`

1. Resolve column names → OIDs
2. Create backing B-Tree index (unique=true, primary=false)
3. Insert UNIQUE constraint record into `pg_constraint`

### `add_not_null_constraint`

1. Verify column OID exists in table
2. Find current column record in `pg_column`, set `is_nullable = false`
3. `update_catalog_tuple` the modified column record
4. Insert NOT NULL constraint record into `pg_constraint`

### `validate_constraints`

Called by `load_csv` before every row insert:
- For each NOT NULL constraint: verify the column OID has a `Some(value)` in the tuple map
- Returns `ConstraintViolation::NotNullViolation` on failure

---

## 13. B-Tree Index Engine

**File:** `src/backend/catalog/indexes.rs`

RookDB implements a full page-based B-Tree. Each index is stored in a dedicated `.idx` file under `database/base/{db}/indexes/`.

### Page Format (Index Pages)

Index pages have a different header from slotted pages:

```
Byte 0:     node_type  (1=leaf, 0=internal)
Bytes 1–2:  num_keys   (u16 LE)
Bytes 3–4:  lower      (u16 LE, slot array end)
Bytes 5–6:  upper      (u16 LE, tuple data start)
Bytes 7–10: right_sibling (u32 LE, leaf pages only; 0 for internal)
Bytes 11+:  slot array (4 bytes each: u16 offset + u16 length)
Data area:  key bytes + payload (grows from page end inward)
```

**Leaf node payload:** `key_bytes || page_num(4) || slot_id(4)` (8-byte suffix)  
**Internal node payload:** `key_bytes || child_page_num(4)` (4-byte suffix)

### B-Tree Operations

#### `index_lookup`

```
current_page = 0 (root)
loop:
    pin page, check node_type
    if leaf:
        binary search for key
        unpin, return found/not_found
    else:
        child = get_internal_child(page, key)
        unpin, current_page = child
```

#### `insert_index_entry`

```
1. Traverse from root to leaf, recording path
2. Try insert_into_page(leaf, key, [page_num, slot_id])
   If fits: unpin dirty, done
   If full:
     a. allocate_index_page() — append new empty page
     b. split_page(old, new, is_leaf) → returns promoted_key
     c. Route pending insert: key >= promoted_key → new page, else → old page
     d. Propagate split up path: insert promoted_key + child_ptr into parent
     e. If root splits: allocate old_root_page, copy root data there,
        write new root with two children
```

#### `split_page`

Splits a full page at the midpoint:
1. Copy entries `[0..mid)` back to old page (reinitialised)
2. Copy entries `[mid..N)` to new page
3. For leaf splits: first entry of new page becomes the promoted key; linked-list pointer `right_sibling` is updated
4. For internal splits: first entry of new page becomes promoted separator; it is moved out (not kept in new page)
5. Returns the promoted key bytes

---

## 14. Query Executor Integration

### Sequential Scan (`seq_scan.rs`)

`show_tuples` reads the table's schema from the catalog, then iterates every slotted-page tuple and decodes each field according to its type:

| Type | Decoding |
|---|---|
| INT / INTEGER | 4-byte LE i32 |
| BIGINT | 8-byte LE i64 |
| FLOAT / REAL | 4-byte LE f32 |
| DOUBLE | 8-byte LE f64 |
| BOOL / BOOLEAN | 1-byte u8 (0/1) |
| TEXT / STRING | `u16 length` + UTF-8 bytes |
| VARCHAR(n) | `u16 length` + UTF-8 bytes |

### CSV Bulk Load (`load_csv.rs`)

`load_csv` performs a schema-aware insert for every CSV row:

```
1. Fetch table schema (columns + indexes) from catalog
2. Open CSV, skip header line
3. For each row:
   a. Parse values, encode each field:
      INT        → 4 bytes LE
      BIGINT     → 8 bytes LE
      FLOAT      → 4 bytes LE
      DOUBLE     → 8 bytes LE
      BOOL       → 1 byte
      TEXT       → u16 length + bytes   ← variable-length
      VARCHAR(n) → u16 length + bytes (truncated to max_len)
   b. validate_constraints(tuple_map) → reject row on violation
   c. insert_tuple(file, &tuple_bytes) → write to heap file
   d. For each index: insert_index_entry(bm, key_bytes, page_num, slot_id)
4. update_table_statistics(row_count, page_count)
```

---

## 15. Heap File Manager

**File:** `src/backend/heap/mod.rs`

### `init_table`

Writes a 8 KB page-0 header (first 4 bytes = `1u32` page count) then calls `create_page` to append the first empty data page (page 1). The resulting file is 16 384 bytes.

### `insert_tuple`

```
1. page_count(file) → total_pages
2. read_page(file, last_page_num)
3. if free_space < len(data) + 8: create_page, total_pages++, read new page
4. write data at upper pointer (decreasing)
5. write slot entry at lower pointer
6. slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE   ← before incrementing lower
7. lower += ITEM_ID_SIZE
8. write_page(file, page, last_page_num)
9. return (last_page_num, slot_id)
```

The page update for the heap file goes directly through `read_page` / `write_page` (not the buffer manager), because user-table inserts during `load_csv` do not need the buffer pool's LRU semantics — they are streaming writes.

---

## 16. Enhancements and Bug Fixes Applied

Seven correctness and quality issues were identified and fixed:

---

### Fix 1 — B-Tree Split: Dead Outer If/Else Removed

**File:** `src/backend/catalog/indexes.rs`

**Problem:** After a page split, the code for routing the pending insert had this structure:
```rust
if insert_key >= promoted_key && (!is_leaf && insert_key >= promoted_key) {
    if insert_key >= promoted_key { new_page ... } else { old_page ... }
} else {
    if insert_key >= promoted_key { new_page ... } else { old_page ... }
}
```
The outer condition simplifies to `!is_leaf && insert_key >= promoted_key`, but **both branches contained identical inner `if/else` blocks**. The outer condition was entirely dead — the routing decision was always determined solely by `insert_key >= promoted_key`, making the outer if/else a no-op that obscured the real logic.

**Fix:** Collapsed to a single correct `if/else`:
```rust
if insert_key >= promoted_key {
    insert_into_page(&mut new_page, &insert_key, &insert_suffix, is_leaf).unwrap();
} else {
    insert_into_page(&mut bm.frames[fi], &insert_key, &insert_suffix, is_leaf).unwrap();
}
```

---

### Fix 2 — TEXT Stored as Fixed 10 Bytes → Variable-Length

**Files:** `src/backend/executor/seq_scan.rs`, `src/backend/executor/load_csv.rs`

**Problem:** The `TEXT` type has `type_length = -1` (variable-length), but both the CSV loader and the sequential scan fell into a default catch-all that treated TEXT as a fixed 10-byte field, silently truncating any string longer than 10 characters and padding shorter ones with spaces.

**Fix:** Added an explicit `"TEXT" | "STRING"` match arm that uses the same `u16 length prefix + bytes` encoding as VARCHAR:

- **`load_csv.rs`:** `TEXT` now writes `[u16 len LE][raw UTF-8 bytes]`
- **`seq_scan.rs`:** `TEXT` now reads `[u16 len LE]` then reads that many bytes

TEXT columns now store and retrieve strings of arbitrary length, consistent with their declared `type_length = -1`.

---

### Fix 3 — `is_builtin` Hardcoded in Column Tuple Deserialization

**File:** `src/backend/catalog/serialize.rs`

**Problem:** `deserialize_column_tuple` always set `is_builtin: true` regardless of the stored value. The corresponding `serialize_column_tuple` did not write `is_builtin` to the byte stream at all. Any user-defined type embedded in a column tuple would be incorrectly classified as a built-in type upon reload.

**Fix:** 
- `serialize_column_tuple` now writes `dt.is_builtin as u8` after `type_name`
- `deserialize_column_tuple` reads it back: `let is_builtin = read_u8(&mut c)? != 0;`

The column tuple binary format now fully round-trips the `DataType.is_builtin` flag.

---

### Fix 4 — Dead `OidCounter::allocate_oid` Method Removed

**File:** `src/backend/catalog/oid.rs`

**Problem:** `OidCounter` had an `allocate_oid()` method that incremented `next_oid` and persisted it. This method was **never called** by any DDL code. All OID allocation goes through `Catalog::alloc_oid()`. The existence of this dead method created a maintenance hazard: if any future code called `OidCounter::allocate_oid()` directly, the two counters would silently diverge, leading to OID reuse.

**Fix:** Removed `OidCounter::allocate_oid()`. The single authoritative OID allocation path is now exclusively `Catalog::alloc_oid()`. `OidCounter` retains `new`, `load`, `persist`, and `initialize` which are all actively used for counter file management.

---

### Fix 5 — `SYSTEM_DB_OID` No Longer Aliases `OID_TYPE_INT`

**File:** `src/backend/layout.rs`

**Problem:** `SYSTEM_DB_OID = 1` was identical to `OID_TYPE_INT = 1`. Although they live in different catalog tables (`pg_database` vs `pg_type`) and no existing code confused them, the collision violated the invariant that OIDs are globally unique across all objects. Any future cross-catalog join or global OID map would silently produce incorrect results.

**Fix:** Changed `SYSTEM_DB_OID` from `1` to `11`:
```rust
// Before:
pub const SYSTEM_DB_OID: u32 = 1;

// After:
pub const SYSTEM_DB_OID: u32 = 11;  // 1–10 reserved for built-in type OIDs
```

Built-in type OIDs occupy `1–10`; `SYSTEM_DB_OID = 11` is the next available system-range OID.

---

### Fix 6 — Delete Now Reclaims Page Space (Compaction)

**File:** `src/backend/catalog/page_manager.rs`

**Problem:** `delete_catalog_tuple` only zeroed the `length` field in the slot entry (marking it as a tombstone). The actual tuple bytes in the data area were never reclaimed. Repeated CREATE/DROP cycles would fill pages with unreachable dead bytes, eventually requiring new pages to be allocated even with significant dead space in existing ones.

**Fix:** Added `compact_page_data(page: &mut Page)`, called at the end of every `delete_catalog_tuple`:

```rust
fn compact_page_data(page: &mut Page) {
    // 1. Collect all live (slot_idx, data_bytes) pairs
    // 2. Repack data tightly from page end downward
    // 3. Update each live slot's offset to the new location
    // 4. Update page.upper to the new upper pointer
}
```

After compaction, the data area is fully defragmented: all live tuples are packed tightly at the top of the page, and the entire gap between `lower` and the packed data is available for new inserts.

---

### Fix 7 — Slot ID Calculation Consistent with Heap Layer

**File:** `src/backend/catalog/page_manager.rs`

**Problem:** `insert_catalog_tuple` incremented `lower` before computing `slot_id`:
```rust
lower += ITEM_ID_SIZE;
let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1;  // post-increment, subtract 1
```
While arithmetically equivalent, this differed in style and order from `heap/mod.rs::insert_tuple`:
```rust
let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;  // pre-increment
lower += ITEM_ID_SIZE;
```

**Fix:** Reordered to match `heap/mod.rs`:
```rust
let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;  // compute BEFORE incrementing
lower += ITEM_ID_SIZE;
page.data[0..4].copy_from_slice(&lower.to_le_bytes());
```

Both the catalog and heap layers now use the same, consistent pattern.

---

## 17. Test Suite

The project includes 90+ test cases across 16 test files, all in `tests/`. Tests run single-threaded (`RUST_TEST_THREADS=1` via `.cargo/config.toml`) to avoid filesystem races.

| Test File | What It Tests |
|---|---|
| `test_init_page.rs` | `init_page` sets correct lower/upper pointers |
| `test_page_free_space.rs` | `page_free_space` returns correct values |
| `test_create_page.rs` | `create_page` appends and updates page count |
| `test_read_page.rs` / `test_write_page.rs` | Disk I/O roundtrip |
| `test_init_table.rs` | `init_table` creates 2-page file |
| `test_page_count.rs` | `page_count` reads correct value from page 0 |
| `test_catalog_bootstrap.rs` | 6 catalog files created, types registered, OID persists |
| `test_init_catalog.rs` | `init_catalog` creates `catalog_pages/` directory |
| `test_load_catalog.rs` | `load_catalog` returns a valid `Catalog` |
| `test_catalog_operations.rs` | Full DDL lifecycle: create/drop DB and table, ALTER TABLE, persistence across restart, error cases |
| `test_catalog_cache.rs` | Cache hit/miss, invalidation, LRU eviction with access-order |
| `test_constraints.rs` | PK creation/duplicate rejection, UNIQUE, NOT NULL + NULL validation, composite PK, FK column-count mismatch |
| `test_indexes.rs` | B-Tree create/drop, single insert+lookup, 100-key insert+lookup, non-existent key, file/directory creation |
| `test_serialization.rs` | Full roundtrip for all 6 tuple types, all `DefaultValue` variants, all enum helpers |
| `test_type_system.rs` | 10 built-in types, OIDs/lengths/aligns, case-insensitive name lookup, invalid names |

---

## 18. End-to-End Data Flow

### Example: `CREATE TABLE students (id INT PRIMARY KEY, name TEXT)`

```
1. create_table("mydb", "students", [id INT, name TEXT], [PK id])
   ├── get_database("mydb")          → scan pg_database, cache hit/miss
   ├── alloc_oid() = 10001           → table_oid, persisted to pg_oid_counter.dat
   ├── alloc_oid() = 10002           → column_oid for "id"
   │   └── serialize_column_tuple → insert into pg_column via buffer manager
   ├── alloc_oid() = 10003           → column_oid for "name"
   │   └── serialize_column_tuple → insert into pg_column
   ├── init_table("database/base/mydb/students.dat")
   ├── serialize_table_tuple → insert into pg_table
   └── add_primary_key_constraint(table=10001, columns=["id"])
       ├── alloc_oid() = 10004       → index_oid
       │   ├── create index file students_pk.idx (single root leaf page)
       │   └── insert into pg_index
       ├── alloc_oid() = 10005       → constraint_oid
       │   └── serialize_constraint(PK { index_oid=10004 }) → insert into pg_constraint
       └── update "id" column: is_nullable=false → update_catalog_tuple on pg_column
```

### Example: `LOAD CSV students.csv INTO mydb.students`

```
1. load_csv("mydb", "students", file, "students.csv")
   ├── get_table_metadata("mydb", "students")
   │   ├── columns: [{id INT, is_nullable=false}, {name TEXT, is_nullable=true}]
   │   └── indexes: [{idx_students_pk, columns=[10002], is_primary=true}]
   └── for each CSV row:
       ├── encode: id=42 → [42,0,0,0]  (4 bytes LE)
       │          name="Alice" → [5,0,'A','l','i','c','e']  (u16 len + bytes)
       ├── validate_constraints → check NOT NULL on id column → pass
       ├── insert_tuple(file, tuple_bytes) → (page=1, slot=0)
       └── insert_index_entry(bm, "idx_students_pk", [42,0,0,0], 1, 0)
           └── B-Tree insert: traverse root → leaf, insert key, mark dirty
```

---

## 19. Directory and File Reference

```
RookDB/
├── Cargo.toml                          crate: storage_manager
├── .cargo/config.toml                  RUST_TEST_THREADS=1
├── src/
│   ├── main.rs                         entry point → frontend::menu::run()
│   ├── lib.rs                          crate root, re-exports
│   └── backend/
│       ├── mod.rs
│       ├── layout.rs                   ★ all path constants + OID constants
│       ├── page/mod.rs                 Page, PAGE_SIZE, init_page, page_free_space
│       ├── disk/                       create_page, read_page, write_page
│       ├── buffer_manager/             BufferManager, PageId, pin/unpin/flush
│       ├── heap/mod.rs                 init_table, insert_tuple
│       ├── table/                      page_count, TABLE_HEADER_SIZE
│       ├── statistics/mod.rs           print page count
│       ├── executor/
│       │   ├── seq_scan.rs             ★ show_tuples (fixed TEXT handling)
│       │   └── load_csv.rs             ★ load_csv (fixed TEXT + constraints)
│       └── catalog/
│           ├── mod.rs                  public re-exports
│           ├── types.rs                all structs + CatalogError
│           ├── oid.rs                  ★ OidCounter (allocate_oid removed)
│           ├── cache.rs                CatalogCache LRU
│           ├── serialize.rs            ★ binary encode/decode (is_builtin fixed)
│           ├── page_manager.rs         ★ CRUD + compaction + slot_id fix
│           ├── catalog.rs              init/bootstrap/load + DDL
│           ├── constraints.rs          PK/FK/UNIQUE/NOT NULL/CHECK
│           └── indexes.rs              ★ B-Tree (split fix)
├── tests/                              16 test files, 90+ test cases
└── database/                           created at runtime
    └── global/
        ├── pg_oid_counter.dat
        └── catalog_pages/
            ├── pg_database.dat
            ├── pg_table.dat
            ├── pg_column.dat
            ├── pg_constraint.dat
            ├── pg_index.dat
            └── pg_type.dat
```

Files marked ★ were modified in this implementation cycle.

---

*End of Report*
