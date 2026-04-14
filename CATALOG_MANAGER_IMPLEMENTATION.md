# RookDB Catalog Manager — Complete Implementation Guide

> This document describes every aspect of how the Catalog Manager was designed,
> built, and tested. It covers the full transition from a legacy JSON-based
> metadata system to a production-grade, self-hosting, page-based catalog
> inspired by PostgreSQL's system catalogs.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [On-Disk Storage Layout](#2-on-disk-storage-layout)
3. [Data Structures (types.rs)](#3-data-structures-typesrs)
4. [Binary Serialization (serialize.rs)](#4-binary-serialization-serializrs)
5. [Page Manager (page_manager.rs)](#5-page-manager-page_managerrs)
6. [Buffer Manager Integration](#6-buffer-manager-integration)
7. [OID System (oid.rs)](#7-oid-system-oidrs)
8. [Catalog Cache (cache.rs)](#8-catalog-cache-cachrs)
9. [Core Catalog Operations (catalog.rs)](#9-core-catalog-operations-catalogrs)
10. [Constraint System (constraints.rs)](#10-constraint-system-constraintsrs)
11. [Index System (indexes.rs)](#11-index-system-indexesrs)
12. [Frontend Integration](#12-frontend-integration)
13. [Module Exports (mod.rs)](#13-module-exports-modrs)
14. [Layout Constants (layout.rs)](#14-layout-constants-layoutrs)
15. [Test Suite](#15-test-suite)
16. [How Everything Ties Together](#16-how-everything-ties-together)

---

## 1. Architecture Overview

### What Changed

**Before (Legacy System):**
- All metadata was stored in a single `catalog.json` file
- On every DDL change, the entire JSON file was serialized and rewritten
- No OIDs, no constraints, no indexes, only basic INT and TEXT types
- Column metadata was minimal (just name and type string)

**After (New Catalog Manager):**
- Metadata is stored in 6 separate page-based system catalog files (`.dat`)
- Each catalog file uses the same slotted page layout (8KB pages) as user tables
- Full constraint system: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL
- B-Tree index infrastructure for constraint enforcement
- Extended type system with 10 built-in types
- Persistent OID counter for unique object identification
- LRU catalog cache for performance
- Dual-mode operation: page-based (primary) + JSON (legacy fallback)

### Design Principles

1. **Self-hosting**: The catalog describes itself — system catalog tables are
   themselves entries in `pg_table` and `pg_column`
2. **PostgreSQL-inspired**: Naming follows PostgreSQL conventions (`pg_database`,
   `pg_table`, `pg_column`, `pg_constraint`, `pg_index`, `pg_type`)
3. **Buffer manager integration**: Catalog pages go through the same pin/unpin
   buffer pool as user data pages
4. **Consistency**: Every DDL operation updates both the page-based catalog AND
   the in-memory representation, invalidating the cache

---

## 2. On-Disk Storage Layout

```
database/
├── global/
│   ├── catalog_pages/              # Page-based catalog storage
│   │   ├── pg_database.dat         # System catalog: databases
│   │   ├── pg_table.dat            # System catalog: tables
│   │   ├── pg_column.dat           # System catalog: columns
│   │   ├── pg_constraint.dat       # System catalog: constraints
│   │   ├── pg_index.dat            # System catalog: indexes
│   │   └── pg_type.dat             # System catalog: data types
│   ├── pg_oid_counter.dat          # Persistent OID counter (4 bytes)
│   └── catalog.json                # Legacy format (kept for migration)
└── base/
    └── {database}/
        ├── {table}.dat             # User table data files
        └── indexes/
            └── {index_name}.idx    # B-Tree index files
```

Each `.dat` file uses RookDB's standard **slotted page layout** (8KB pages):
- **Page header** (8 bytes): item count, lower pointer, upper pointer, special
- **Item ID array** (grows downward from header): 4 bytes each (offset + length)
- **Tuple data area** (grows upward from end of page): variable-length tuples

This layout is consistent with user table pages, so the same buffer manager
and disk I/O code works for both user data and catalog metadata.

---

## 3. Data Structures (types.rs)

**File**: `src/backend/catalog/types.rs` (485 lines)

This is the foundational module. Every struct that represents a catalog entity
is defined here. The spec (§2) lists exactly what fields each struct needs.

### 3.1 Type System

```rust
enum TypeCategory { Numeric, String, DateTime, Boolean, Binary }

struct DataType {
    type_oid: u32,           // Unique identifier (1-10 for built-ins)
    type_name: String,       // "INT", "VARCHAR(255)", etc.
    type_category: TypeCategory,
    type_length: i16,        // Fixed byte length, or -1 for variable
    type_align: u8,          // Alignment: 1, 2, 4, or 8
    is_builtin: bool,
}
```

**10 Built-in Types** (allocated OIDs 1-10 in `layout.rs`):

| OID | Name | Category | Length | Align |
|-----|------|----------|--------|-------|
| 1 | INT | Numeric | 4 | 4 |
| 2 | BIGINT | Numeric | 8 | 8 |
| 3 | FLOAT | Numeric | 4 | 4 |
| 4 | DOUBLE | Numeric | 8 | 8 |
| 5 | BOOL | Boolean | 1 | 1 |
| 6 | TEXT | String | -1 | 1 |
| 7 | VARCHAR | String | -1 | 1 |
| 8 | DATE | DateTime | 4 | 4 |
| 9 | TIMESTAMP | DateTime | 8 | 8 |
| 10 | BYTES | Binary | -1 | 1 |

**Key methods on DataType:**
- `DataType::from_name("VARCHAR(50)")` → resolves a SQL type string
  (case-insensitive, with aliases like INTEGER→INT, REAL→FLOAT, BYTEA→BYTES)
- `DataType::all_builtins()` → returns all 10 built-in types
- Individual constructors: `DataType::int()`, `DataType::text()`, etc.

### 3.2 Column

```rust
struct Column {
    column_oid: u32,
    name: String,
    column_position: u16,              // 1-based position in table
    data_type: DataType,
    type_modifier: Option<TypeModifier>,  // e.g. VARCHAR(50) → VarcharLen(50)
    is_nullable: bool,
    default_value: Option<DefaultValue>,
    constraints: Vec<u32>,             // OIDs of constraints on this column
}

enum TypeModifier {
    VarcharLen(u16),
    Precision { precision: u8, scale: u8 },
}

enum DefaultValue {
    Integer(i32), BigInt(i64), Float(f32), Double(f64),
    Str(String), Boolean(bool), Null, CurrentTimestamp,
}
```

`ColumnDefinition` is the DDL-facing struct used during CREATE TABLE / ALTER TABLE.

### 3.3 Constraint System

```rust
enum ConstraintType { PrimaryKey, ForeignKey, Unique, NotNull, Check }

enum ConstraintMetadata {
    PrimaryKey { index_oid: u32 },
    ForeignKey {
        referenced_table_oid: u32,
        referenced_column_oids: Vec<u32>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
    Unique { index_oid: u32 },
    NotNull,
    Check { check_expression: String },
}

enum ReferentialAction { NoAction, Cascade, SetNull, Restrict }

struct Constraint {
    constraint_oid: u32,
    constraint_name: String,
    constraint_type: ConstraintType,
    table_oid: u32,
    column_oids: Vec<u32>,
    metadata: ConstraintMetadata,
    is_deferrable: bool,
}
```

All enums have `to_u8()`/`from_u8()` methods for binary serialization.

`ConstraintDefinition` is the DDL-facing enum used during CREATE TABLE:
```rust
enum ConstraintDefinition {
    PrimaryKey { columns: Vec<String>, name: Option<String> },
    ForeignKey { columns, referenced_table, referenced_columns, on_delete, on_update, name },
    Unique { columns: Vec<String>, name: Option<String> },
    NotNull { column: String },
    Check { expression: String, name: Option<String> },
}
```

`ConstraintViolation` is returned when a constraint is violated during data ops:
```rust
enum ConstraintViolation {
    NotNullViolation { column: String },
    UniqueViolation { constraint: String },
    ForeignKeyViolation { constraint: String },
    CheckViolation { constraint: String },
}
```

### 3.4 Index Metadata

```rust
enum IndexType { BTree, Hash }

struct Index {
    index_oid: u32,
    index_name: String,
    table_oid: u32,
    index_type: IndexType,
    column_oids: Vec<u32>,
    is_unique: bool,
    is_primary: bool,
    index_pages: u32,
}
```

### 3.5 Table & Database

```rust
enum TableType { UserTable, SystemCatalog }

struct TableStatistics {
    row_count: u64,
    page_count: u32,
    created_at: u64,
    last_modified: u64,
}

struct Table {
    table_oid: u32,
    table_name: String,
    db_oid: u32,
    columns: Vec<Column>,
    constraints: Vec<Constraint>,
    indexes: Vec<u32>,           // OIDs of indexes on this table
    table_type: TableType,
    statistics: TableStatistics,
}

enum Encoding { UTF8, ASCII }

struct Database {
    db_oid: u32,
    db_name: String,
    tables: HashMap<String, Table>,
    owner: String,
    encoding: Encoding,
    created_at: u64,
}
```

### 3.6 Catalog (Top-Level)

```rust
struct Catalog {
    databases: HashMap<String, Database>,
    oid_counter: u32,           // Next OID to allocate (starts at 10,000)
    bootstrap_mode: bool,       // True during initial setup
    page_backend_active: bool,  // True when page-based storage is running
    cache: CatalogCache,        // LRU metadata cache
}
```

The `oid_counter`, `bootstrap_mode`, `page_backend_active`, and `cache` fields
are `#[serde(skip)]` — they are not stored in the legacy JSON file but are
reconstructed at load time.

### 3.7 Error Types

```rust
enum CatalogError {
    DatabaseNotFound(String),
    DatabaseAlreadyExists(String),
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    TypeNotFound(String),
    IndexNotFound(String),
    ConstraintNotFound(String),
    AlreadyHasPrimaryKey,
    ReferencedKeyMissing,
    ColumnCountMismatch,
    TypeMismatch { column: String },
    ForeignKeyDependency(String),
    InvalidOperation(String),
    IoError(std::io::Error),
}
```

All variants have human-readable `Display` implementations and `From<io::Error>`
for ergonomic error propagation with `?`.

---

## 4. Binary Serialization (serialize.rs)

**File**: `src/backend/catalog/serialize.rs` (416 lines)

Every catalog tuple type has a `serialize_*` and `deserialize_*` function pair.
The format is a compact binary encoding: fixed-length fields followed by
length-prefixed variable-length strings/arrays.

### Encoding Rules

- **u32/u64/u16/i16/u8**: Little-endian byte order
- **Strings**: `u16 length + UTF-8 bytes`
- **Vec<u32>** (column OIDs, etc.): `u16 count + (count × u32)`
- **Booleans**: 1 byte (0 or 1)
- **Enums**: Converted to u8 via `to_u8()`/`from_u8()`

### Tuple Formats

**Database Tuple**: `[db_oid:4][owner_len:2][owner:N][name_len:2][name:N][created_at:8][encoding:1]`

**Table Tuple**: `[table_oid:4][name_len:2][name:N][db_oid:4][table_type:1][row_count:8][page_count:4][created_at:8]`

**Column Tuple**: `[column_oid:4][table_oid:4][name_len:2][name:N][pos:2][type_oid:4][type_name_len:2][type_name:N][type_cat:1][type_len:2][type_align:1][is_builtin:1][type_mod_flag:1][type_mod?:2][is_nullable:1][default_flag:1][default?][constraint_count:2][constraints:N×4]`

**Constraint Tuple**: `[constraint_oid:4][name_len:2][name:N][type:1][table_oid:4][col_count:2][cols:N×4][is_deferrable:1][metadata...]`
- PrimaryKey metadata: `[index_oid:4]`
- ForeignKey metadata: `[ref_table:4][ref_col_count:2][ref_cols:N×4][on_delete:1][on_update:1]`
- Unique metadata: `[index_oid:4]`
- NotNull: empty
- Check: `[expr_len:2][expr:N]`

**Index Tuple**: `[index_oid:4][name_len:2][name:N][table_oid:4][type:1][col_count:2][cols:N×4][is_unique:1][is_primary:1][pages:4]`

**Type Tuple**: `[type_oid:4][name_len:2][name:N][category:1][length:2][align:1][is_builtin:1]`

### Helper Functions

- `calculate_tuple_size(columns, type_map) → (fixed_size, has_variable)`:
  Computed from column types for storage planning
- `TypeCategory::to_u8()`/`from_u8()`: Numeric→1, String→2, DateTime→3,
  Boolean→4, Binary→5

---

## 5. Page Manager (page_manager.rs)

**File**: `src/backend/catalog/page_manager.rs` (289 lines)

This is the CRUD interface between catalog operations and physical storage.
It abstracts away page-level details.

### Catalog File IDs

```rust
pub const CAT_DATABASE: usize = 0;
pub const CAT_TABLE: usize = 1;
pub const CAT_COLUMN: usize = 2;
pub const CAT_CONSTRAINT: usize = 3;
pub const CAT_INDEX: usize = 4;
pub const CAT_TYPE: usize = 5;
```

### CatalogPageManager

```rust
struct CatalogPageManager {
    file_paths: [String; 6],  // Paths to the 6 catalog .dat files
}
```

### Key Operations

1. **`initialize_files()`**: Creates `catalog_pages/` directory and all 6 `.dat`
   files. Each file is initialized with a slotted page (header page 0 + first
   data page 1).

2. **`insert_catalog_tuple(bm, catalog_id, bytes) → Result<(page, slot)>`**:
   Finds a page with sufficient free space (or allocates a new page), then
   inserts the tuple bytes using the slotted page item ID array.

3. **`scan_catalog(bm, catalog_id) → Result<Vec<Vec<u8>>>`**:
   Iterates all pages in the catalog file. For each page, reads the item count
   and collects all live tuples. Returns a vector of raw byte arrays.

4. **`delete_catalog_tuple(bm, catalog_id, page_num, slot_id)`**:
   Marks the item ID at `slot_id` as deleted (sets offset and length to 0).

5. **`find_catalog_tuple(bm, catalog_id, predicate) → Result<Option<(page, slot, bytes)>>`**:
   Scans all pages and returns the first tuple matching the predicate function.

6. **`update_catalog_tuple(bm, catalog_id, page_num, slot_id, new_bytes)`**:
   For variable-length updates: deletes the old tuple, then inserts the new
   bytes (potentially on a different page). This "delete-then-reinsert" approach
   handles the case where the new tuple is larger than the old one.

All operations go through the `BufferManager` for page access.

---

## 6. Buffer Manager Integration

**File**: `src/backend/buffer_manager/buffer_manager.rs` (343 lines)

The buffer manager was enhanced to support the catalog system:

### Key APIs

```rust
struct PageId { file_path: String, page_num: u32 }

impl BufferManager {
    fn pin_page(page_id) → Result<usize>    // Load into buffer, return frame index
    fn unpin_page(page_id, is_dirty) → Result<()>  // Release, mark dirty if modified
    fn flush_pages() → Result<()>           // Write all dirty pages to disk
}
```

**How catalog operations use it:**
1. `pin_page()` to get a page into memory
2. Read/modify the page data at `bm.frames[frame_index].data`
3. `unpin_page()` with `is_dirty=true` if modified
4. Buffer manager handles LRU eviction when the pool is full

---

## 7. OID System (oid.rs)

**File**: `src/backend/catalog/oid.rs` (110 lines)

Every database object (database, table, column, constraint, index) gets a
unique OID (Object Identifier) — a `u32` that is never reused.

### How It Works

- **Counter file**: `database/global/pg_oid_counter.dat` stores a single `u32`
  (little-endian) — the next available OID.
- **Built-in types** use OIDs 1–10 (defined in `layout.rs`)
- **User objects** start at OID 10,000 (defined by `USER_OID_START`)
- `Catalog::alloc_oid()` increments the counter and writes it to disk
  immediately when the page backend is active, ensuring OIDs survive crashes

```rust
struct OidCounter {
    next_oid: u32,
    counter_file_path: String,
}

impl OidCounter {
    fn initialize() → Result<()>  // Create file with initial value 10,000
    fn load() → Result<()>        // Read counter from disk
    fn allocate_oid() → Result<u32>   // Increment and persist
    fn persist_counter() → Result<()> // Write to disk
}
```

---

## 8. Catalog Cache (cache.rs)

**File**: `src/backend/catalog/cache.rs` (236 lines)

An in-memory LRU cache that stores copies of frequently-accessed catalog
entries. Reduces disk I/O on repeated metadata lookups.

### Structure

```rust
struct CatalogCache {
    databases: HashMap<String, Database>,
    tables: HashMap<(u32, String), Table>,       // (db_oid, table_name)
    constraints: HashMap<u32, Vec<Constraint>>,  // table_oid → constraints
    indexes: HashMap<u32, Vec<Index>>,           // table_oid → indexes
    types: HashMap<u32, DataType>,               // type_oid → type
    type_names: HashMap<String, u32>,            // type_name → type_oid
    access_order: Vec<CacheKey>,                 // LRU tracking
    max_cache_size: usize,                       // Default: 256 entries
}
```

### Cache Operations

| Operation | Description |
|-----------|-------------|
| `get_database(name)` | Return cached database, update LRU |
| `insert_database(name, db)` | Add to cache, evict if needed |
| `invalidate_database(name)` | Remove from cache |
| `get_table(db_oid, name)` | Return cached table |
| `insert_table(db_oid, name, table)` | Add table to cache |
| `invalidate_table(db_oid, name)` | Remove table from cache |
| `get_constraints(table_oid)` | Return cached constraints |
| `invalidate_constraints(table_oid)` | Remove constraints |
| `get_indexes(table_oid)` | Return cached indexes |
| `invalidate_indexes(table_oid)` | Remove indexes |
| `get_type_by_oid(oid)` | Return cached type |
| `get_type_by_name(name)` | Lookup type by name |
| `insert_type(dt)` | Add type (both maps) |
| `invalidate_all()` | Clear entire cache |

### LRU Eviction

When `access_order.len() >= max_cache_size`, the oldest entry (front of the
list) is evicted. Every access moves the entry to the back of the list.

### Invalidation Policy

**Every DDL operation** (CREATE, ALTER, DROP) calls the appropriate
`invalidate_*` method to ensure stale data is never served.

---

## 9. Core Catalog Operations (catalog.rs)

**File**: `src/backend/catalog/catalog.rs` (615 lines)

This is the main orchestration module. It ties together all the subsystems.

### 9.1 Bootstrap & Initialization

**`init_catalog(bm)`**: Called at startup.
- If `catalog_pages/` exists → page backend detected
- Else if `catalog.json` exists → legacy mode
- Else → calls `bootstrap_catalog(bm)`

**`bootstrap_catalog(bm)`**: First-time setup.
1. Creates `database/global/` and `database/base/`
2. Initializes the OID counter at 10,000
3. Creates the 6 catalog `.dat` files via `CatalogPageManager::initialize_files()`
4. Inserts all 10 built-in types into `pg_type`
5. Inserts the "system" database record into `pg_database` (OID=1)

**`init_catalog_page_storage()`**: Creates/verifies the CatalogPageManager.

### 9.2 Load & Save

**`load_catalog(bm) → Catalog`**: Builds the full in-memory catalog.
1. If page backend exists: scans `pg_database`, `pg_table`, `pg_column`,
   `pg_constraint`, `pg_index` and builds the nested `Catalog` struct
2. Fallback: reads `catalog.json`
3. Last resort: returns empty `Catalog`

**`save_catalog(catalog)`**: Legacy JSON export to `catalog.json`.

### 9.3 Type Helpers

**`register_builtin_types(pm, bm)`**: Idempotent — checks which types already
exist in `pg_type` before inserting.

**`lookup_type_by_name(pm, bm, name)`**: First tries `DataType::from_name()`
(in-memory), then scans `pg_type` on disk.

### 9.4 Database Operations

**`create_database(catalog, pm, bm, name, owner, encoding) → Result<db_oid>`**:
1. Validates name (non-empty, not duplicate)
2. Allocates OID
3. Creates `database/base/{name}/`
4. Serializes and inserts into `pg_database`
5. Updates in-memory catalog
6. Invalidates cache

**`drop_database(catalog, pm, bm, name)`**:
1. Drops all tables in the database (via `drop_table`)
2. Deletes the `pg_database` tuple
3. Removes the database directory
4. Cleans up in-memory state and cache

**`show_databases(catalog, pm, bm)`**: Scans `pg_database` directly from pages
and prints a formatted table (name | owner | created_at).

**`create_database(catalog, name)`**: Legacy JSON-mode create (still works).

### 9.5 Table Operations

**`create_table(catalog, pm, bm, db_name, table_name, col_defs, constraint_defs) → Result<table_oid>`**:
1. Validates database exists and table name is unique
2. Allocates table OID
3. For each column definition:
   - Resolves type via `DataType::from_name()`
   - Allocates column OID
   - Serializes and inserts into `pg_column`
4. Creates the table data file and initializes it
5. Serializes and inserts into `pg_table`
6. Applies constraints (PK, FK, UNIQUE, NOT NULL) via their respective functions
7. Updates in-memory catalog and cache

**`drop_table(catalog, pm, bm, table_oid)`**:
1. Checks for FK dependencies from other tables → error if found
2. Drops all indexes on the table
3. Deletes the table data file
4. Deletes the `pg_table` tuple
5. Cleans up in-memory state and cache

**`alter_table_add_column(catalog, pm, bm, table_oid, col_def) → Result<col_oid>`**:
1. Resolves type
2. Validates: NOT NULL without default → error; duplicate name → error
3. Allocates column OID
4. Serializes and inserts into `pg_column`
5. Updates in-memory table

**`show_tables(catalog, pm, bm, db_name)`**: Scans `pg_table` filtered by
`db_oid`, prints formatted table.

### 9.6 Metadata Queries

**`get_table_metadata(catalog, pm, bm, db_name, table_name) → Result<TableMetadata>`**:
Returns a flat view with columns, constraints, and indexes.

---

## 10. Constraint System (constraints.rs)

**File**: `src/backend/catalog/constraints.rs` (411 lines)

### 10.1 add_primary_key_constraint

```
add_primary_key_constraint(catalog, pm, bm, table_oid, column_names, name) → Result<constraint_oid>
```

1. Scans `pg_constraint` to ensure no existing PK on this table
2. Resolves column names → column OIDs via `pg_column`
3. Sets `is_nullable = false` on all PK columns
4. Creates a backing B-Tree index (`is_unique=true, is_primary=true`)
5. Builds and persists the `Constraint` in `pg_constraint`
6. Updates the in-memory table's constraint list

### 10.2 add_foreign_key_constraint

```
add_foreign_key_constraint(catalog, pm, bm, table_oid, column_names,
                           ref_table_oid, ref_column_names,
                           on_delete, on_update, name) → Result<constraint_oid>
```

1. Validates column counts match
2. Resolves both sets of column names to OIDs
3. Verifies referenced columns are covered by a PK or UNIQUE constraint
4. Checks type compatibility between referencing and referenced columns
5. Builds and persists the FK `Constraint`
6. Updates in-memory state

### 10.3 add_unique_constraint

```
add_unique_constraint(catalog, pm, bm, table_oid, column_names, name) → Result<constraint_oid>
```

Creates a unique B-Tree index (`is_unique=true, is_primary=false`) and persists
the UNIQUE constraint.

### 10.4 add_not_null_constraint

```
add_not_null_constraint(catalog, pm, bm, table_oid, column_oid) → Result<()>
```

Sets `is_nullable = false` on the target column (both in `pg_column` on disk
and in the in-memory `Table.columns`).

### 10.5 validate_constraints

```
validate_constraints(catalog, pm, bm, table_oid, tuple_values) → Result<(), ConstraintViolation>
```

Called before INSERT/UPDATE to enforce constraints:
1. Checks NOT NULL: if any non-nullable column has a NULL value → `NotNullViolation`
2. Fetches all constraints for the table
3. For UNIQUE/PK: does an index lookup → `UniqueViolation` if duplicate found
4. For FK: does an index lookup on the referenced table → `ForeignKeyViolation`

### 10.6 get_constraints_for_table

Scans `pg_constraint` filtering by `table_oid`. Uses cache if available.

---

## 11. Index System (indexes.rs)

**File**: `src/backend/catalog/indexes.rs` (440 lines)

### 11.1 create_index

```
create_index(catalog, pm, bm, table_oid, column_oids, is_unique, is_primary, name) → Result<index_oid>
```

1. Resolves the database name for the table
2. Generates a name if none provided: `idx_{table_oid}_{col_oids}`
3. Creates `database/base/{db}/indexes/` directory
4. Creates the `.idx` file and initializes a B-Tree root page
5. Allocates index OID
6. Serializes and inserts into `pg_index`
7. Updates the table's index list

### 11.2 drop_index

```
drop_index(catalog, pm, bm, index_oid) → Result<()>
```

1. Finds the index in `pg_index`
2. Checks if any constraint references this index → error if so
3. Deletes the `.idx` file
4. Removes the `pg_index` tuple
5. Updates the table's index list

### 11.3 B-Tree Implementation

The B-Tree uses the same slotted page layout as catalog pages:

**Page format:**
- Byte 0: `is_leaf` flag (1=leaf, 0=internal)
- Bytes 1-2: `num_keys` (u16)
- Bytes 3-4: `lower` pointer (u16)
- Bytes 5-6: `upper` pointer (u16)
- Bytes 7-10: `right_sibling` (u32, leaf pages only)
- Bytes 11+: Item ID array (4 bytes per entry: offset + length)
- End of page: key+payload data area

**Leaf entries**: `[key_bytes][page_num:4][slot_id:4]`
**Internal entries**: `[key_bytes][child_page_num:4]`

**Key operations:**

```
insert_index_entry(bm, db_name, index_name, key_bytes, page_num, slot_id) → Result<()>
```
- Walks tree to find leaf
- Inserts into leaf; if full, splits the page
- Promotes the split key up the tree (possibly splitting parent)
- If root splits, creates a new root

```
index_lookup(bm, db_name, index_name, key_bytes) → Result<bool>
```
- Walks tree from root to leaf
- Returns `true` if key found (binary search within each node)

**Internal functions:**
- `search_node()`: Binary search within a page
- `get_internal_child()`: Navigate internal nodes
- `insert_into_page()`: Insert with space check
- `split_page()`: Split full page, return promoted key
- `allocate_index_page()`: Extend the index file with a new page

---

## 12. Frontend Integration

The frontend CLI commands were updated to use the enhanced catalog APIs:

### table_cmd.rs
- **CREATE TABLE**: Now accepts constraint definitions in the column spec
  (e.g., `id:INT:PRIMARY KEY`, `email:TEXT:UNIQUE`)
- Calls `create_table()` instead of `create_table()`
- Passes both `ColumnDefinition` and `ConstraintDefinition` vectors

### database_cmd.rs
- **CREATE DATABASE**: Calls `create_database()` with owner and encoding
- **SHOW DATABASES**: Uses `show_databases()` which reads from `pg_database`
- **SHOW TABLES**: Uses `show_tables()` which reads from `pg_table`
- **DROP DATABASE**: Uses `drop_database()` with cascading table drops

The user experience (UX) remains largely unchanged — the changes are primarily
in the backend storage and validation layer.

---

## 13. Module Exports (mod.rs)

**File**: `src/backend/catalog/mod.rs` (41 lines)

This module re-exports all public APIs so callers can use
`storage_manager::catalog::*` instead of deep paths:

```rust
// Sub-modules
pub mod types;
pub mod oid;
pub mod cache;
pub mod serialize;
pub mod page_manager;
pub mod constraints;
pub mod indexes;
pub mod catalog;

// Re-exports: core data types
pub use types::{
    Catalog, CatalogError, Column, ColumnDefinition, Constraint,
    ConstraintDefinition, ConstraintMetadata, ConstraintType,
    ConstraintViolation, DataType, Database, DefaultValue, Encoding,
    Index, IndexType, ReferentialAction, Table, TableMetadata,
    TableStatistics, TableType, TypeCategory, TypeModifier,
};

// Re-exports: catalog operations
pub use catalog::{
    bootstrap_catalog, create_database, create_database,
    create_table, create_table, drop_database, drop_table,
    alter_table_add_column, get_table_metadata, init_catalog,
    init_catalog_page_storage, load_catalog, lookup_type_by_name,
    register_builtin_types, save_catalog, show_databases, show_tables,
};

// Re-exports: constraint operations
pub use constraints::{
    add_foreign_key_constraint, add_not_null_constraint,
    add_primary_key_constraint, add_unique_constraint,
    get_constraints_for_table, validate_constraints,
};

// Re-exports: index operations
pub use indexes::{
    create_index, drop_index, get_indexes_for_table,
    index_lookup, insert_index_entry,
};

pub use oid::OidCounter;
pub use page_manager::CatalogPageManager;
```

---

## 14. Layout Constants (layout.rs)

**File**: `src/backend/layout.rs` (75 lines)

All file paths and OID ranges are centralized here:

```rust
// Directories
DATA_DIR           = "database"
GLOBAL_DIR         = "database/global"
DATABASE_DIR       = "database/base"
CATALOG_PAGES_DIR  = "database/global/catalog_pages"

// File paths
CATALOG_FILE       = "database/global/catalog.json"
TABLE_DIR_TEMPLATE = "database/base/{database}"
TABLE_FILE_TEMPLATE= "database/base/{database}/{table}.dat"
OID_COUNTER_FILE   = "database/global/pg_oid_counter.dat"
INDEX_DIR_TEMPLATE = "database/base/{database}/indexes"
INDEX_FILE_TEMPLATE= "database/base/{database}/indexes/{index}.idx"

// System catalog file paths
PG_DATABASE_FILE   = "database/global/catalog_pages/pg_database.dat"
PG_TABLE_FILE      = "database/global/catalog_pages/pg_table.dat"
PG_COLUMN_FILE     = "database/global/catalog_pages/pg_column.dat"
PG_CONSTRAINT_FILE = "database/global/catalog_pages/pg_constraint.dat"
PG_INDEX_FILE      = "database/global/catalog_pages/pg_index.dat"
PG_TYPE_FILE       = "database/global/catalog_pages/pg_type.dat"

// OID ranges
SYSTEM_OID_START   = 1        // Built-in types start here
USER_OID_START     = 10,000   // User objects start here
SYSTEM_DB_OID      = 1        // The "system" database

// Built-in type OIDs
OID_TYPE_INT       = 1
OID_TYPE_BIGINT    = 2
OID_TYPE_FLOAT     = 3
OID_TYPE_DOUBLE    = 4
OID_TYPE_BOOL      = 5
OID_TYPE_TEXT       = 6
OID_TYPE_VARCHAR   = 7
OID_TYPE_DATE      = 8
OID_TYPE_TIMESTAMP = 9
OID_TYPE_BYTES     = 10
```

---

## 15. Test Suite

103 tests total, all passing. Run with: `cargo test -- --test-threads=1`

### Test Files

| File | Tests | What It Covers |
|------|-------|----------------|
| `test_catalog_bootstrap.rs` | 5 | Bootstrap creates all 6 files, valid page structure, built-in types, OID counter persistence |
| `test_type_system.rs` | 9 | All 10 built-in types, from_name resolution, aliases, case-insensitivity, catalog lookup |
| `test_serialization.rs` | 26 | Roundtrip serialize+deserialize for all 6 tuple types, all DefaultValue/ConstraintType/IndexType variants |
| `test_catalog_cache.rs` | 14 | Hit/miss for all 5 entity types, DDL invalidation (per-entity + invalidate_all), LRU eviction with access order |
| `test_constraints.rs` | 9 | PK creation + backing index, duplicate PK rejected, UNIQUE + backing index, NOT NULL + violation, composite PK, FK column mismatch |
| `test_indexes.rs` | 8 | B-Tree create + file on disk, default naming, drop + cleanup, insert/lookup, 100-key bulk test, non-existent file handling |
| `test_catalog_operations.rs` | 22 | E2E database + table CRUD, persistence across restart, ALTER TABLE ADD COLUMN, FK dependency blocks drop, invalid type rejection, metadata queries, error messages |
| `test_init_catalog.rs` | 1 | Legacy init_catalog function |
| `test_load_catalog.rs` | 1 | Legacy load_catalog function |
| `test_save_catalog.rs` | 1 | Legacy save_catalog function |
| Other pre-existing tests | 7 | Page read/write, table init, page count, show tuples |

### Test Isolation

All tests that touch the filesystem use a `cleanup()` helper that:
1. Deletes `database/global/catalog_pages/`
2. Deletes `database/global/pg_oid_counter.dat`
3. Deletes `database/global/catalog.json`
4. Deletes any test database directories

Tests **must** run with `--test-threads=1` because they share the filesystem.

---

## 16. How Everything Ties Together

### Example: CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)

Here's the complete flow:

```
1. Frontend parses the command, builds:
   - ColumnDefinitions: [{name:"id", type:"INT"}, {name:"name", type:"TEXT"}]
   - ConstraintDefinitions: [PrimaryKey{cols:["id"]}, NotNull{col:"name"}]

2. create_table() is called:
   a. Validates database exists, table name is unique
   b. Allocates table_oid = 10003 (from OID counter)

3. Column processing:
   a. "id" → DataType::from_name("INT") → INT type (OID=1, 4 bytes)
      → Allocates col_oid = 10004
      → serialize_column_tuple(...) → binary bytes
      → pm.insert_catalog_tuple(bm, CAT_COLUMN, bytes) → written to pg_column.dat
   b. "name" → same process, col_oid = 10005

4. Table file creation:
   → Creates "database/base/mydb/users.dat"
   → Initializes with header + first data page

5. Table metadata:
   → serialize_table_tuple(10003, "users", db_oid, ...) → binary bytes
   → pm.insert_catalog_tuple(bm, CAT_TABLE, bytes) → written to pg_table.dat

6. Constraint #1: PrimaryKey{cols:["id"]}
   → add_primary_key_constraint(catalog, pm, bm, 10003, ["id"], None)
   → Checks no existing PK
   → Resolves "id" → col_oid 10004
   → Sets id column is_nullable = false
   → create_index(unique=true, primary=true) → creates .idx file, writes to pg_index.dat
   → Builds Constraint struct with PrimaryKey metadata
   → serialize_constraint_tuple(...) → written to pg_constraint.dat

7. Constraint #2: NotNull{col:"name"}
   → add_not_null_constraint(catalog, pm, bm, 10003, 10005)
   → Sets name column is_nullable = false (in pg_column and in-memory)

8. In-memory update:
   → Table struct with columns, constraints, indexes added to catalog.databases["mydb"]
   → Cache invalidated

9. Returns Ok(10003) (the table OID)
```

### Example: INSERT with Constraint Validation

```
1. Before inserting a row into the users table:
   → validate_constraints(catalog, pm, bm, table_oid=10003, values)

2. NOT NULL check:
   → Column "id" (PK) is_nullable=false → check value is not NULL
   → Column "name" is_nullable=false → check value is not NULL

3. PRIMARY KEY check:
   → Get constraint: PrimaryKey{index_oid: 10006}
   → Extract key value for "id" column
   → index_lookup(bm, "mydb", "pk_users_id", key_bytes)
   → B-Tree traversal: root → leaf → binary search
   → If found → return UniqueViolation error
   → If not found → continues

4. After validation passes:
   → Insert the row into users.dat
   → insert_index_entry(bm, "mydb", "pk_users_id", key, page, slot)
```

### Example: Catalog Persistence Across Restart

```
Session 1:
  → bootstrap_catalog() creates everything
  → create_database("mydb", ...) writes to pg_database.dat
  → create_table("users", ...) writes to pg_table, pg_column, pg_constraint, pg_index
  → All writes go through BufferManager → pages marked dirty → flushed to disk

Session 2 (restart):
  → load_catalog(bm) called
  → Scans pg_database.dat → finds "mydb"
  → Scans pg_table.dat → finds "users" (matched to mydb via db_oid)
  → Scans pg_column.dat → finds "id" and "name" (matched to users via table_oid)
  → Scans pg_constraint.dat → finds PK and NOT NULL
  → Scans pg_index.dat → finds the PK index
  → Rebuilds complete Catalog struct in memory
  → Everything is exactly as it was before the restart
```

---

## Summary of All Changed/New Files

### New Backend Files (src/backend/catalog/)

| File | Lines | Purpose |
|------|-------|---------|
| `types.rs` | 485 | All data structures (enhanced from original) |
| `serialize.rs` | 416 | Binary serialization for all 6 tuple types |
| `page_manager.rs` | 289 | CRUD operations on system catalog pages |
| `cache.rs` | 236 | LRU catalog cache |
| `indexes.rs` | 440 | Index metadata management + B-Tree implementation |
| `constraints.rs` | 411 | Constraint creation, validation, querying |
| `catalog.rs` | 615 | Core operations (bootstrap, CRUD, load/save) |
| `oid.rs` | 110 | Persistent OID counter |
| `mod.rs` | 41 | Module re-exports |

### Modified Backend Files

| File | Changes |
|------|---------|
| `layout.rs` | Added all catalog file paths, OID constants, index templates |
| `buffer_manager.rs` | Enhanced with pin/unpin, dirty tracking, LRU eviction |

### Modified Frontend Files

| File | Changes |
|------|---------|
| `table_cmd.rs` | Enhanced CREATE TABLE with constraint support |
| `database_cmd.rs` | Enhanced CREATE/DROP/SHOW DATABASE |

### New Test Files (tests/)

| File | Tests |
|------|-------|
| `test_catalog_bootstrap.rs` | 5 |
| `test_type_system.rs` | 9 |
| `test_serialization.rs` | 26 |
| `test_catalog_cache.rs` | 14 |
| `test_constraints.rs` | 9 |
| `test_indexes.rs` | 8 |
| `test_catalog_operations.rs` | 22 |

**Total: ~3,000 lines of new code + 103 passing tests**
