# RookDB Robustness Improvements - Implementation Summary

## Overview
Implemented major improvements to make RookDB more robust, reliable, modular, and user-friendly.

---

## 1. Data Type Validation Module (`src/backend/types_validator.rs`)
- **Case-Insensitive Type Checking**: "INT", "int", "InT" work correctly.
- **Type Safety**: `DataType` enum structure rather than hardcoded definitions.
- **Comprehensive Validation**: Proper checks for values based on limits and dimensions.
- **Serialization/Deserialization**: Dedicated functions preventing repetitive memory layout casting logic.
- **Extensible Architecture**: Easy interface to drop new schema constructs.

### Supported Types:
- `INT`: 32-bit signed integers (4 bytes)
- `TEXT`: Variable-length strings (10 bytes, padded/truncated)

### Functions:
```rust
pub fn from_str(type_str: &str) -> Result<Self, String>      // Parse type
pub fn validate_value(&self, value: &str) -> Result<(), String>   // Validate value
pub fn serialize_value(&self, value: &str) -> Result<Vec<u8>, String>  // Convert to bytes
pub fn deserialize_value(&self, bytes: &[u8]) -> Result<String, String>  // Convert from bytes
```

## 2. Error Handling Module (`src/backend/error_handler.rs`)
- **Custom Error Types**: Handled natively mapping explicit contexts.
- **Graceful Fault Reporting**: Friendly guidance over panics/crashes.
- **Path Checking Validation**: Safety wrappers over filesystem operations, directory paths, and file availability checks.
### Error Types:
- `FileNotFound`: File or path doesn't exist
- `InvalidPath`: Directory provided instead of file
- `InvalidDataType`: Unsupported data type encountered
- `ValidationError`: Data doesn't match schema
- `DiskFull`: Disk space or permission issues

### Functions:
```rust
pub fn validate_file_path(path: &str) -> RookResult<()>
pub fn verify_csv_path(csv_path: &str) -> RookResult<()>
pub fn print_error_with_guidance(error: &RookDBError)
pub fn safe_read_file(path: &str) -> RookResult<String>
pub fn safe_write_file(path: &str, content: &str) -> RookResult<()>
```

## 3. Page API Abstraction Layer (`src/backend/page_api.rs`)
- **Safe Page Operands**: `get_lower`, `get_upper`, `set_lower`, `set_upper`.
- **Page Meta Information Check**: `get_tuple_count`, `get_free_space`, `can_fit_tuple`.
- **Integrity Validation**: Header validation checking layout overlapping or unbounded data frames recursively.
### Functions:
```rust
pub fn get_lower(page: &Page) -> io::Result<u32>                    // Get lower pointer
pub fn get_upper(page: &Page) -> io::Result<u32>                    // Get upper pointer
pub fn set_lower(page: &mut Page, value: u32) -> io::Result<()>     // Set lower safely
pub fn set_upper(page: &mut Page, value: u32) -> io::Result<()>     // Set upper safely
pub fn get_tuple_count(page: &Page) -> io::Result<u32>              // Count tuples
pub fn get_free_space(page: &Page) -> io::Result<u32>               // Available space
pub fn can_fit_tuple(page: &Page, tuple_size: u32) -> io::Result<bool>
pub fn validate_page_header(page: &Page) -> io::Result<()>          // Verify integrity
pub fn get_page_stats(page: &Page) -> io::Result<String>            // Formatted stats
pub fn reset_page(page: &mut Page) -> io::Result<()>                // Empty page
```

## 4. Enhanced CSV Loading (`src/backend/executor/load_csv.rs`)
- **Pre-Load Data Profiling**: Header/schema compatibility test BEFORE mass operations begin, not aborting halfway.
- **Per-Tuple Row Injection Validation**: Validation triggers inline. Rejects malformed strings but preserves valid insertions indicating line limits.
- **String Dimension Handiling**: Handles length 10 padding natively, raises inline truncate warnings.
### Functions:
```rust
pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<u32>  // Returns count of inserted rows

pub fn insert_single_tuple(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    values: &[&str],
) -> io::Result<bool>  // Returns success/failure
```

### Validation Process:
1. **Schema Validation**: Check all column types exist and are supported
2. **File Validation**: Ensure CSV file is readable
3. **Column Count Validation**: Check each row has correct column count
4. **Value Validation**: Validate each value against its data type
5. **Serialization**: Convert values to bytes safely
6. **Insertion**: Insert into heap using insert_tuple


## 5. Improved Catalog Persistence (`src/backend/catalog/catalog.rs`)
- **Safe State Syncs**: `save_catalog` yields `std::io::Result<()>` and recovers gracefully.
- **Fault-Tolerant Reloads**: Corrupt `.json` or deleted state files construct cleanly upon reload via `load_catalog()` rather than crashing execution.

## 6. Layout Abstractions & Table Display (`src/backend/executor/seq_scan.rs`)
### Features:
- **Professional Table Format**: Box drawing characters (┌─┬─┐)
- **Single Header Row**: Column names shown once, not repeated
- **ID Column**: Sequential tuple numbering
- **Data Type Info**: Shows type in header
- **Error Handling**: Graceful handling of deserialization errors

### Output Format:
```
╔════════════════════════════════════════════╗
║   Tuples in 'mydb.users'                   ║
║   Total pages: 2                           ║
╚════════════════════════════════════════════╝

[TABLE DISPLAY] Columns:
  1: id (INT)
  2: name (TEXT)

┌─────┬──────────────────────────────────────────────────┐
│ ID  │ id: INT                       │ name: TEXT       │
├─────┼──────────────────────────────────────────────────┤
│   1 │ 1                             │ Alice            │
│   2 │ 2                             │ Bob              │
│   3 │ 3                             │ Charlie          │
└─────┴──────────────────────────────────────────────────┘

Total tuples displayed: 3
```

## 7. Frontend & REPL Operations (`src/frontend/data_cmd.rs` & `src/frontend/menu.rs`)
- Manual single-tuple ingestion feature mapping commands securely into native layout layers (`insert_tuple_cmd`).
- Upgraded nested boxes UI for operations categorizations visually dividing Menu selections over operations boundaries.

## 8. Heap File Management (HM) (`src/backend/heap/heap_manager.rs`)
- **FSM-Backed Tuple Insertion**: `insert_tuple` intelligently maps new data directly to pages with free space by consulting the FSM tree instead of blindly appending.
- **Dynamic Growth**: `allocate_new_page` safely expands table boundaries horizontally and notifies FSM mappings only when capacity is exhausted.
- **Header Metadata Integrity**: `HeaderMetadata` persists schema state (total tuples securely updated natively, `page_count`, `fsm_page_count`) ensuring state integrity preventing sequential misalignment.
- **Coordinate Lookups**: Added `get_tuple(page_id, slot_id)` for O(1) constant-time direct tuple coordinate fetching bypassing global scans natively.
- **Lazy Evaluation Iterator Scans**: Implemented `HeapScanIterator` yielding page/slot structures lazily preventing memory faults when scanning multi-GB files.

## 9. Free Space Management (FSM) (`src/backend/fsm/fsm.rs`)
- **3-Level Binary Max-Tree**: Tree-based structure replacing O(N) scan layouts. Stored safely inside a `.fsm` persistence sidecar fork avoiding main file cluster intrusion.
- **Constant-Time I/O Space Discovery (`fsm_search_avail`)**: Rapid lookup resolving exact target heap pages fitting arbitrary payloads. Requires exactly 3 bounded page reads (O(1) I/O) while leveraging O(log N) binary max-tree cpu-checks internally, completely avoiding raw header sequence scanning.
- **Load Balancing/Spreading (`fp_next_slot`)**: Incorporating sequential slot hints driving horizontal data ingestion eliminating hot-spots linearly.
- **Auto-Bubble Capacity Resolvers (`fsm_set_avail`)**: Updating a leaf slot capacity recursively updates max parent nodes, notifying the tree roots exactly how much space is left across subsets.
- **Compaction Readiness (`fsm_vacuum_update`)**: Integration hooking natively into vacuuming modules marking pages as refreshed effortlessly.
- **Fault-Tolerant Native Reconstruction (`build_from_heap`)**: If a `.fsm` sidecar drops out of scope or is deleted, FSM rebuilds the layout completely seamlessly from the primary heap data without logging exceptions.

## 10. Diagnostics and Tracing Debug Statements
- Pervasive output streams mapping operation traces recursively `[TYPE_VALIDATOR]`, `[ERROR_HANDLER]`, `[CATALOG]`, `[CSV_LOADER]`, `[FSM_ALLOCATOR]`.

---

## Technical Outcomes
- **Robustness**: Hardened IO interfaces over raw allocations/deallocations.
- **Usability**: Interactive feedback loop provides user prompts matching logical behaviors instead of memory leaks or generic panic tracebacks.
- **Maintainability**: Layered boundary patterns simplify future test assertions mapping and debugging safely.

## 11. Core Bug Fixes & Refactoring
- **`read_all_pages` API Implementation**: Added a new function in `disk_manager.rs` to read all pages (header + data) from a file on disk into memory.
- **Refactoring `load_table_from_disk`**: Modified `BufferManager::load_table_from_disk` in `buffer_manager.rs` to utilize the new `read_all_pages` API, significantly simplifying the reading logic.
- **Cleaned Up Legacy Code**: Removed `load_csv_into_pages` and `load_csv_to_buffer` from the buffer manager. The active bulk loading logic now relies appropriately on `load_csv.rs` through the frontend commands.
- **`load_catalog` Data Sync Issue**: Standardized CSV/Bulk loader to strictly use `insert_tuple` per valid row insertion rather than trying to construct un-validated tuples blindly. 
- **Catalog Failure Problem**: Critical issue is when a accidentally deleted catalog file caused the DB to create a blank slate catalog. Thus data loss of the existing data.


    