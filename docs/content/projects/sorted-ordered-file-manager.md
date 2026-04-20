---
title: Sorted and Ordered File Manager
sidebar_position: 5
---

# Sorted and Ordered File Manager

**Team:** Ayush Rawal (2024201036), Priyanshu Sharma (2024201046)

## Overview

The Sorted and Ordered File Manager adds sorting, ordered storage, range queries, and ORDER BY processing to RookDB. Prior to this component, RookDB only supported heap files where tuples are appended in insertion order with no regard for sort order. This made sorted output and range-based lookups impossible without a full table scan.

This component introduces a new layer between the Buffer Manager and the Executor that provides:

- **In-memory and external merge sorting** of heap tables into ordered files
- **Ordered file maintenance** with sorted insertion and page splitting
- **Deferred insertion (delta store)** for write-heavy ordered-table ingestion
- **Ordered scan** for full sequential iteration in sort order
- **Range scan** with binary search-based seek for efficient key-range queries
- **ORDER BY execution** that detects pre-sorted tables to avoid redundant work

The implementation supports multi-column sort keys with mixed ASC/DESC directions on INT (4 bytes) and TEXT (10 bytes, fixed-width) data types.

---

## Design

### Architecture

The sorting and ordered file layer sits between the existing Buffer Manager and Executor layers:

```
+-------------------+
|   CLI / Frontend   |  Option 5 table-type menu + options 10, 12, 13
+-------------------+
         |
+-------------------+
|     Executor       |  order_by_execute, create_ordered_file_from_heap
+-------------------+
         |
+-------------------+
|  Sorting / Ordered |  comparator, in_memory_sort, external_sort,
|    File Manager    |  sorted_insert, ordered_scan, range_scan
+-------------------+
         |
+-------------------+
|  Buffer Manager    |  page cache, CSV loading (with sorted mode)
+-------------------+
         |
+-------------------+
|  Disk Manager      |  read_page, write_page, create_page
+-------------------+
```

All existing layers (Page, Disk Manager, Heap Manager) are used as-is without modification. The Catalog and Buffer Manager receive minor extensions for sort metadata. The Executor gains two new functions.

### Table Header Page Extension

The existing table header page (page 0, 8192 bytes) uses only the first 4 bytes for page count. We extend it to store ordered file metadata:

| Byte Range      | Field              | Type       | Description                                |
|-----------------|--------------------|------------|--------------------------------------------|
| `[0..4]`        | `page_count`       | `u32` (LE) | Existing total page count                  |
| `[4..5]`        | `file_type`        | `u8`       | `0 = heap`, `1 = ordered`                  |
| `[5..9]`        | `sort_key_count`   | `u32` (LE) | Number of sort columns (`0` for heap)      |
| `[9..9+N*5]`    | `sort_key_entries` | array      | `N` entries, 5 bytes each                  |
| remaining bytes | reserved           | -          | Zeroed                                     |

Each sort key entry (5 bytes):
- `column_index` (`u32` LE): 0-based index in schema columns
- `direction` (`u8`): `0 = ASC`, `1 = DESC`

Backward compatibility is maintained: existing heap files have `file_type = 0` (zeroed bytes default).

### Ordered File Page Layout

Ordered files use the **same slotted page layout** as heap files. The difference is purely logical:

- In **heap files**, the ItemId array reflects insertion order (append-only).
- In **ordered files**, the ItemId array is maintained in **sorted order** -- `ItemId[0]` points to the smallest sort key, `ItemId[N-1]` to the largest.

Physical tuple bytes in the payload region may still be in insertion order; sorted order is enforced through the ItemId indirection.

**Cross-page sort invariant:** All tuples on page P are `<=` all tuples on page P+1. This enables:
- Binary search across pages: O(log P) to find the correct page for a key
- Efficient range scans: start at the matching page, scan forward, stop when exceeding the end key
- Sequential ordered scan: iterate pages 1..N, reading tuples in ItemId order

### Page Splitting

When a sorted insertion targets a full page, a **50/50 page split** occurs:

1. Extract all tuples from the full page, add the new tuple, sort them
2. Write the lower half to the original page, upper half to a new page
3. Shift all subsequent pages forward by one position in the file
4. Update `page_count` in the header

### Deferred Insertion (Delta Store)

For ordered tables, inserts can be deferred instead of doing immediate in-place sorted insertion.

- New tuples are first appended to an unsorted sidecar file: `database/base/{db_name}/{table_name}.delta`
- The main ordered file (`{table_name}.dat`) stays sorted and is used as the base run
- Merge policy is tuple-count based: merge when `delta_current_tuples >= 500`

Merge workflow:
1. Read tuples from base ordered file + delta sidecar
2. Sort merged tuples with `TupleComparator`
3. Rewrite base ordered file in sorted order
4. Truncate/reset the `.delta` file

Files created/used in this process:
- `database/base/{db_name}/{table_name}.dat` (base table file, persistent)
- `database/base/{db_name}/{table_name}.delta` (deferred-insert buffer, persistent sidecar that is emptied after merge)

### External Merge Sort

For tables that exceed available memory, a two-phase **external multi-way merge sort** is used:

**Phase 1 -- Run Generation:**
- Read `B` pages at a time (B = buffer pool size)
- Sort tuples in memory using Rust's `sort_by`
- Write each sorted batch to a temporary run file
- Produces `R = ceil(total_pages / B)` sorted runs

**Phase 2 -- K-Way Merge (k = B-1):**
- Reserve 1 output buffer page, use up to B-1 input pages
- Use a min-heap to repeatedly select the smallest tuple across runs
- Flush output pages as they fill
- Repeat merge passes until one run remains

Temporary files are stored as `database/base/{db_name}/.sort_tmp_{table_name}_run_{N}.dat` and are cleaned up after the sort completes. These are external-sort artifacts and are different from the deferred-insertion sidecar `database/base/{db_name}/{table_name}.delta`.

### Catalog Changes

The catalog JSON is extended with two new optional fields on each table:

```json
{
  "columns": [ ... ],
  "sort_keys": [{ "column_index": 0, "direction": "ASC" }],
  "file_type": "ordered"
}
```

Heap tables have `sort_keys: null` and `file_type: "heap"` (or absent, via `#[serde(default)]`). This allows the query executor to check sort order without opening the data file.

---

## Data Structures

### New Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `SortKey` | `catalog/types.rs` | Column index + sort direction (ASC/DESC) |
| `SortDirection` | `catalog/types.rs` | Enum: `Ascending`, `Descending` |
| `OrderedFileHeader` | `ordered/ordered_file.rs` | Page 0 metadata: page count, file type, sort keys |
| `FileType` | `ordered/ordered_file.rs` | Enum: `Heap` (0), `Ordered` (1) |
| `SortKeyEntry` | `ordered/ordered_file.rs` | On-disk sort key (5 bytes: column_index u32 + direction u8) |
| `TupleComparator` | `sorting/comparator.rs` | Precomputed column offsets for efficient tuple comparison |
| `SortedRun` | `sorting/external_sort.rs` | State of a single sorted run file during merge |
| `ExternalSortState` | `sorting/external_sort.rs` | Orchestrates external sort: runs, temp files, buffer size |
| `MergeEntry` | `sorting/external_sort.rs` | Min-heap entry: tuple data + source run index |
| `OrderedScanIterator` | `ordered/scan.rs` | Sequential scan over all tuples in sort order |
| `RangeScanIterator` | `ordered/scan.rs` | Range-based scan with binary search seek |

**Key structure -- TupleComparator:**

```rust
pub struct TupleComparator {
    pub columns: Vec<Column>,
    pub sort_keys: Vec<SortKey>,
    pub tuple_size: usize,
    pub column_offsets: Vec<usize>,
}
```

Precomputes byte offsets for each column (INT = 4 bytes, TEXT = 10 bytes) at construction time, enabling O(1) field extraction during comparison. Used by all sorting, insertion, and scan operations.

### Modified Structures

| Structure | File | Change |
|-----------|------|--------|
| `Table` | `catalog/types.rs` | Added `sort_keys: Option<Vec<SortKey>>`, `file_type: Option<String>` |
| `Column` | `catalog/types.rs` | Added `#[derive(Clone)]` |
| `BufferManager` | `buffer_manager.rs` | Added `pool_size: usize` |

---

## APIs

### Comparator

```rust
pub fn TupleComparator::new(columns: Vec<Column>, sort_keys: Vec<SortKey>) -> Self
```
Constructs a comparator with precomputed column byte offsets.

```rust
pub fn TupleComparator::compare(&self, tuple_a: &[u8], tuple_b: &[u8]) -> Ordering
```
Compares two tuples by sort keys in priority order, respecting ASC/DESC.

```rust
pub fn TupleComparator::compare_key(&self, tuple: &[u8], key_index: usize, key_value: &[u8]) -> Ordering
```
Compares a specific column of a tuple against a raw key value (for range scan boundaries).

```rust
pub fn TupleComparator::extract_key(&self, tuple: &[u8], key_index: usize) -> Vec<u8>
```
Extracts the sort key bytes from a tuple for the given sort key index.

### Ordered File Header

```rust
pub fn read_ordered_file_header(file: &mut File) -> io::Result<OrderedFileHeader>
```
Reads and parses page 0 into an `OrderedFileHeader`.

```rust
pub fn write_ordered_file_header(file: &mut File, header: &OrderedFileHeader) -> io::Result<()>
```
Serializes an `OrderedFileHeader` into a zeroed 8192-byte buffer and writes to page 0.

```rust
pub fn init_ordered_table(file: &mut File, sort_keys: &[SortKeyEntry]) -> io::Result<()>
```
Creates a new ordered table file: writes header page + one empty data page.

### Sorting

```rust
pub fn in_memory_sort(
    catalog: &mut Catalog, db_name: &str, table_name: &str,
    sort_keys: Vec<SortKey>, file: &mut File,
) -> io::Result<()>
```
Sorts a table that fits in memory. Loads all pages, extracts and sorts tuples, rewrites pages with sorted ItemId arrays, updates header and catalog.

```rust
pub fn external_sort(
    catalog: &mut Catalog, db_name: &str, table_name: &str,
    sort_keys: Vec<SortKey>, buffer_pool_size: usize,
) -> io::Result<()>
```
Full external merge sort: run generation, k-way merge, final file write, temp cleanup, catalog update.

```rust
pub fn generate_sorted_runs(
    file: &mut File, state: &mut ExternalSortState,
) -> io::Result<()>
```
Phase 1: reads B pages at a time, sorts tuples in memory, writes sorted runs to temp files.

```rust
pub fn merge_runs(state: &mut ExternalSortState) -> io::Result<String>
```
Phase 2: k-way merge using min-heap until one sorted run remains. Returns final run file path.

### Sorted Insertion

```rust
pub fn sorted_insert(
    file: &mut File, tuple_data: &[u8], comparator: &TupleComparator,
) -> io::Result<()>
```
Inserts a tuple into an ordered file maintaining sort invariant. Triggers page split if target page is full.

```rust
pub fn find_insert_page(
    file: &mut File, total_pages: u32, tuple_data: &[u8], comparator: &TupleComparator,
) -> io::Result<u32>
```
Binary search across pages to find the correct page for insertion. O(log P) page reads.

```rust
pub fn find_insert_slot(
    page: &Page, tuple_data: &[u8], comparator: &TupleComparator,
) -> u32
```
Binary search within a page's ItemId array to find the correct slot position.

```rust
pub fn split_page(
    file: &mut File, page_num: u32, page: &Page,
    tuple_data: &[u8], comparator: &TupleComparator, total_pages: u32,
) -> io::Result<()>
```
Splits a full page 50/50, inserts the new tuple, shifts subsequent pages forward.

### Scanning

```rust
pub fn ordered_scan(
    file: &mut File, catalog: &Catalog, db_name: &str, table_name: &str,
) -> io::Result<Vec<Vec<u8>>>
```
Full sequential scan of an ordered file in sort order. Iterates page by page, slot by slot.

```rust
pub fn range_scan(
    file: &mut File, catalog: &Catalog, db_name: &str, table_name: &str,
    key_column_name: &str, start_value: Option<&str>, end_value: Option<&str>,
) -> io::Result<Vec<Vec<u8>>>
```
Scans an ordered file for tuples within `[start_key, end_key]`. Uses binary search to seek to start position, then scans forward until end key is exceeded. Range column must be the leading sort key.

### Executor

```rust
pub fn order_by_execute(
    catalog: &mut Catalog, db_name: &str, table_name: &str,
    sort_keys: Vec<SortKey>, buffer_pool_size: usize,
) -> io::Result<()>
```
Executes ORDER BY. If the table is already sorted on the requested keys, performs a direct ordered scan. Otherwise, sorts first, then scans.

```rust
pub fn create_ordered_file_from_heap(
    catalog: &mut Catalog, db_name: &str, table_name: &str,
    sort_keys: Vec<SortKey>, buffer_pool_size: usize,
) -> io::Result<()>
```
Converts a heap table to an ordered file. Uses in-memory sort if the table fits in the buffer pool, otherwise falls back to external sort.

### Modified APIs

```rust
pub fn create_table(
    catalog: &mut Catalog, db_name: &str, table_name: &str,
    columns: Vec<Column>, sort_keys: Option<Vec<SortKey>>,  // NEW parameter
)
```
Now accepts optional sort keys. If provided, calls `init_ordered_table()` instead of `init_table()`.

**`load_csv_into_pages`** -- After loading CSV data, if the target table is ordered, extracts all tuples, sorts them via `TupleComparator`, and rewrites pages with sorted ItemId arrays before flushing.

**`show_tuples`** -- Reads the file header and prints `[Ordered by: ...]` if the file is an ordered file.

---

## CLI Options

Top-level sort menu options are available as 10, 12, and 13.
Table creation type (sorted/unsorted) is selected inside option 5.

```
========== RookDB Storage Manager ==========
Choose an option:
1. Show Databases
2. Create Database
3. Select Database
4. Show Tables
5. Create Table
6. Load CSV
7. Show Tuples
8. Show Table Statistics
9. Exit
10. Sort Table
12. Range Scan
13. ORDER BY Query
=============================================
Enter your choice:
```

Create Table flow:

```
> 5
Enter table name: employees
Enter columns in the format:- column_name:data_type
Press Enter on an empty line to finish
Enter column (name:type): id:INT
Enter column (name:type): name:TEXT
Enter column (name:type):

Select table type:
1. Sorted Table
2. Unsorted Table
Enter your choice (1/2): 1
Enter sort columns (format: col1:ASC,col2:DESC): id:ASC
```

### Option 10: Sort Table

Sorts an existing heap table into an ordered file using in-memory or external sort.

```
> 10
Enter table name: employees
Enter sort columns (format: col1:ASC,col2:DESC): id:ASC
```

Output: `Table 'employees' sorted by [id ASC]. File type changed to ordered.`

### Option 12: Range Scan

Queries tuples within a key range on an ordered file.

```
> 12
Enter table name: employees_sorted
Enter column name for range: id
Enter start value (or leave empty for unbounded): 5
Enter end value (or leave empty for unbounded): 20
```

Output: formatted table of matching tuples. The range column must be the leading sort key.

### Option 13: ORDER BY Query

Displays all tuples sorted by specified columns. Works on both heap and ordered tables.

```
> 13
Enter table name: employees
Enter sort columns (format: col1:ASC,col2:DESC): name:ASC,id:DESC
```

Output: all tuples displayed in the specified sort order.

---

## Testing

### Test Summary

| Test File | Tests | Covers |
|-----------|-------|--------|
| `test_tuple_comparator.rs` | 20 | INT/TEXT comparison, ASC/DESC, multi-column keys, ties, extract_key |
| `test_in_memory_sort.rs` | 7 | Small table sort, already sorted, reverse order, single tuple, empty table |
| `test_external_sort.rs` | 6 | Large table sort, buffer_pool_size=4, temp file cleanup, run generation |
| `test_ordered_file_header.rs` | 5 | Header read/write round-trip, multi-key headers, FileType enum |
| `test_sorted_insert.rs` | 10 | Random order insertion, page split, cross-page invariant, duplicates |
| `test_scan.rs` | 10 | Ordered scan, range scan, unbounded ranges, page boundary crossing |
| `test_order_by.rs` | 4 | ORDER BY on heap, pre-sorted detection, multi-column ORDER BY |
| `test_catalog_sort_keys.rs` | 3 | Catalog persistence of sort metadata, backward compatibility |
| `test_integration.rs` | 6 | End-to-end: heap->sort->scan, sorted insert, 10K rows external sort, TEXT sort, multi-column sort |
| *Phase 1 tests (10 files)* | 10 | Existing page/table/catalog tests (unchanged) |
| **Total** | **81** | **All passing** |

### Edge Cases Covered

- Empty table sort (0 tuples)
- Single tuple sort
- Duplicate sort keys (all-equal values)
- Multi-column sort with mixed directions (e.g., name ASC, id DESC)
- TEXT column as primary sort key (lexicographic ordering)
- DESC sorting (reverse order)
- Range scans crossing page boundaries
- External sort with very small buffer pool (4 pages)

---

## Implementation Progress

| Feature | Status |
|---------|--------|
| TupleComparator (INT, TEXT, ASC/DESC, multi-column) | Complete |
| In-memory sort | Complete |
| External merge sort (run generation + k-way merge) | Complete |
| Ordered file header (read/write/init) | Complete |
| Sorted insertion with page splitting | Complete |
| Ordered scan (full sequential) | Complete |
| Range scan (binary search seek + forward scan) | Complete |
| Executor: order_by_execute | Complete |
| Executor: create_ordered_file_from_heap | Complete |
| Catalog extensions (sort_keys, file_type) | Complete |
| Buffer manager: sorted CSV loading | Complete |
| CLI table-type menu + options 10, 12, 13 | Complete |
| Unit tests (71 new tests) | Complete |
| Integration tests (6 end-to-end scenarios) | Complete |
| Temp file cleanup | Complete |
