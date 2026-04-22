---
title: FSM and Heap Manager
sidebar_position: 6
---

# FSM and Heap Manager

This page documents the current implementation of RookDB's page-level storage stack: heap files, free-space tracking, tuple access, CSV ingest, and diagnostics. It is written from the code in `src/backend/heap`, `src/backend/fsm`, `src/backend/disk`, `src/backend/executor`, and the CLI under `src/frontend`.

## What This Project Does

RookDB stores each table as a heap file in `database/base/<db>/<table>.dat` and keeps free-space state in a sidecar `database/base/<db>/<table>.dat.fsm` file. The heap manager is responsible for creating and opening heap files, inserting and retrieving tuples, scanning pages sequentially, deleting tuples, and updating header metadata. The FSM tracks page-level free space so inserts can find a suitable page without linearly scanning the entire file.

The current implementation is focused on page-level storage. It supports inserts, point lookups, scans, deletion marking, FSM rebuild/recovery, CSV loading, and CLI diagnostics. It does not implement full in-page compaction or tuple relocation as a first-class phase of INSERT.

## High-Level Layout

### File Layout

| File | Purpose |
| --- | --- |
| `<table>.dat` | Heap file containing page 0 metadata plus data pages |
| `<table>.dat.fsm` | Sidecar FSM fork with page-level free-space categories |
| `database/global/catalog.json` | Catalog metadata for databases and tables |

### Heap Pages

| Page | Role |
| --- | --- |
| Page 0 | Header metadata page |
| Page 1+ | Slotted heap data pages |

### Heap Page Format

Each heap page is 8192 bytes.

| Offset | Size | Meaning |
| --- | --- | --- |
| 0..4 | 4 bytes | `lower` pointer, next free slot entry in the slot directory |
| 4..8 | 4 bytes | `upper` pointer, start of tuple data region |
| 8..n | variable | Slot directory, 8 bytes per slot `(offset, length)` |
| ... | variable | Free space between directory and tuple data |
| end | variable | Tuple payloads packed from the end of the page backward |

The free space reported by the page helpers is `upper - lower`, which is the contiguous region that can accept new inserts.

## Header Metadata

Page 0 stores a 20-byte `HeaderMetadata` structure.

| Field | Type | Meaning |
| --- | --- | --- |
| `page_count` | `u32` | Total number of heap pages, including page 0 |
| `fsm_page_count` | `u32` | Total number of FSM pages currently tracked |
| `total_tuples` | `u64` | Total inserted tuples |
| `last_vacuum` | `u32` | Unix timestamp of the last vacuum-style update |

`HeaderMetadata::new()` starts with `page_count = 1`, `fsm_page_count = 0`, `total_tuples = 0`, and `last_vacuum = 0`. When a heap is created, `HeapManager::create()` writes the header page, creates the first data page, and then synchronizes the FSM state.

## Core Modules and Responsibilities

### `src/backend/heap/heap_manager.rs`

This is the high-level table storage API.

| Method | Purpose |
| --- | --- |
| `HeapManager::create()` | Create a new heap file and initialize metadata, page 1, and FSM state |
| `HeapManager::open()` | Open an existing heap file and rebuild/open the FSM sidecar if needed |
| `HeapManager::insert_tuple()` | Insert a tuple using FSM-guided page selection |
| `HeapManager::get_tuple()` | Fetch a tuple by `(page_id, slot_id)` |
| `HeapManager::delete_tuple()` | Mark a tuple deleted and update the heap page/header |
| `HeapManager::scan()` | Return a lazy sequential scan iterator |
| `HeapManager::fsm_search_for_page()` | Expose FSM search for debugging/testing |
| `HeapManager::flush()` | Persist header, heap file, and FSM state |
| `HeapManager::vacuum_page()` | Wrapper that forwards a vacuum-style free-space update to the FSM |

### `src/backend/fsm/fsm.rs`

This module owns the free-space map fork and the tree search/update logic.

| Method | Purpose |
| --- | --- |
| `FSM::open()` | Open or create the FSM sidecar |
| `FSM::build_from_heap()` | Rebuild the FSM by scanning heap pages |
| `FSM::fsm_search_avail()` | Find a heap page with enough free-space category |
| `FSM::fsm_set_avail()` | Update one heap page's free-space category and bubble the change upward |
| `FSM::fsm_vacuum_update()` | Convenience wrapper for compaction/vacuum-style updates |
| `FSM::sync()` | Flush the FSM file to disk |
| `FSM::calculate_fsm_page_count()` | Compute how many FSM pages are needed for a heap size |

### `src/backend/page/mod.rs`

This module provides the raw page format and low-level helpers.

| Function | Purpose |
| --- | --- |
| `Page::new()` | Allocate a zeroed 8 KB page buffer |
| `init_page()` | Initialize the page header pointers (`lower = 8`, `upper = 8192`) |
| `page_free_space()` | Return contiguous free bytes (`upper - lower`) |
| `get_tuple_count()` | Return the number of slot entries currently in use |
| `get_slot_entry()` | Read one slot entry `(offset, length)` safely |

### `src/backend/disk/disk_manager.rs`

This module handles page-level I/O.

| Function | Purpose |
| --- | --- |
| `create_page()` | Low-level append helper that creates a new page on disk |
| `read_page()` | Read one page into memory |
| `write_page()` | Write one page back to disk |
| `read_header_page()` | Deserialize page 0 into `HeaderMetadata` |
| `update_header_page()` | Persist `HeaderMetadata` back to page 0 |
| `read_all_pages()` | Load all pages from disk into memory |

### `src/backend/executor/load_csv.rs`

This module handles CSV ingest and single-tuple insertion.

| Function | Purpose |
| --- | --- |
| `load_csv()` | Validate a CSV file and insert rows through `HeapManager` |
| `insert_single_tuple()` | Validate a row entered from the CLI and insert it through `HeapManager` |

### `src/backend/executor/seq_scan.rs`

This module renders sequential scans as formatted tables.

| Function | Purpose |
| --- | --- |
| `show_tuples()` | Print all tuples in a table using schema-aware decoding |

### `src/frontend/menu.rs` and `src/frontend/data_cmd.rs`

These modules connect the interactive menu to the backend APIs.

| Command | Purpose |
| --- | --- |
| `Load CSV` | Validate a CSV file and bulk insert rows |
| `Insert Single Tuple` | Prompt for values and insert one tuple |
| `Show Tuples` | Display all tuples in a formatted table |
| `Check Heap Health` | Print header metadata, FSM details, and metrics |

## Heap Manager Behavior

### Creation and Open

`HeapManager::create()` removes any existing file at the target path, writes a fresh header page, creates page 1, initializes an FSM sidecar, and sets the in-memory header to reflect the new table. `HeapManager::open()` reads the header, opens the `.fsm` fork if it exists, and rebuilds it from the heap if the fork is missing or empty. After opening, it also reconciles `fsm_page_count` in the header with the calculated FSM size and persists the corrected header if needed.

### Insertion Flow

`insert_tuple()` is the main write path.

1. Validate that the tuple is not empty.
2. Reject tuples larger than a single page can hold.
3. Compute `required_bytes = tuple_len + ITEM_ID_SIZE`.
4. Convert required bytes to a minimum FSM category using ceiling rounding.
5. Ask the FSM for a candidate page.
6. Re-read the page and verify actual contiguous free space.
7. Insert the tuple.
8. Write the page back to disk.
9. Refresh the FSM with the new free-space value.
10. Increment `total_tuples` and persist the header.

The code uses a three-attempt strategy. If the first page suggested by the FSM does not have enough real contiguous space, that page is updated in the FSM and the insert retries. If the FSM returns no candidate on the final attempt, the heap manager allocates a new page and inserts there.

### Slot Reuse

`insert_into_page()` reuses dead slots when possible. If a page already contains deleted slots, the new tuple can reuse the slot entry instead of extending the slot directory. If no dead slot is available, a new slot entry is appended and `lower` advances by `ITEM_ID_SIZE`.

This means the implementation is slightly more efficient than a pure append-only slot directory: deleted slots can be reused before the page grows again.

### Tuple Retrieval

`get_tuple()` validates the page and slot bounds, reads the slot entry, and returns the tuple bytes. A dead slot (`offset == 0 && length == 0`) returns `NotFound`.

### Deletion

`delete_tuple()` marks the slot as dead by writing `(0, 0)` into the slot directory entry. It also performs two local optimizations when possible:

- If the deleted tuple was the last tuple at the `upper` boundary, the `upper` pointer is rolled back.
- If the deleted slot was the last slot in the slot directory, the `lower` pointer is rolled back.

The method decrements `total_tuples`, writes the header back to disk, and returns the nominal freed bytes for the deleted tuple plus the slot entry. The FSM is not automatically refreshed in this path; explicit vacuum-style updates go through `vacuum_page()` / `fsm_vacuum_update()`.

### Sequential Scan

`scan()` returns a `HeapScanIterator` that lazily loads pages one at a time. The iterator starts at page 1, skips dead slots, validates tuple bounds, and yields `io::Result<(page_id, slot_id, Vec<u8>)>`. Only one page is cached at a time, so scan memory usage stays small even on large tables.

### Flush

`flush()` persists the header page, syncs the heap file, and syncs the FSM sidecar.

## FSM Behavior

### Tree Model

The FSM fork uses a fixed-height, three-level tree model.

| Constant | Value | Meaning |
| --- | --- | --- |
| `FSM_NODES_PER_PAGE` | `7999` | Number of bytes stored in one FSM page tree array |
| `FSM_SLOTS_PER_PAGE` | `4000` | Number of leaf slots tracked per level-0 FSM page |
| `FSM_LEVELS` | `3` | Root, internal, and leaf levels |
| `FSM_PAGE_SIZE` | `8192` | Size of one FSM page on disk |

Each `FSMPage` stores a binary max-tree in a fixed-size byte array. `tree[0]` is the root value for that page. Leaves live in the right half of the array, starting at `FSM_NON_LEAF_NODES`.

### Free-Space Categories

The FSM stores free space as a `u8` category in the range `0..=255`.

- Higher values mean more free space.
- Search thresholds are rounded up on the heap side so a tuple never matches a page with less space than it needs.
- Stored categories are derived from current free bytes using the page-size scale.

### Search

`fsm_search_avail(min_category)` does a quick reject at the root. If the root value is below the requested category, it returns `None` immediately. Otherwise it descends through the tree, preferring the left child when both children qualify. At the leaf level it converts the final slot back into a heap page id.

### Updates

`fsm_set_avail(heap_page_id, new_free_bytes)` updates the leaf entry for the target heap page, recomputes parent values inside the level-0 page, writes that page back, and then propagates the new root upward through level 1 and level 2 as needed.

### Rebuild

`FSM::build_from_heap()` is the recovery path for a missing or stale FSM sidecar. It reads page 0 to get `page_count`, scans each heap page's first 8 bytes to compute free bytes, fills the leaf pages in memory, bubbles max values upward, writes the FSM fork, and syncs the file. This is what `HeapManager::open()` uses when the sidecar is absent or empty.

### Vacuum Updates

`fsm_vacuum_update()` is a thin wrapper around `fsm_set_avail()` for compaction or vacuum workflows that reclaim contiguous space later.

## CLI Workflow

The program entry point is `cargo run`, which initializes `env_logger` and starts the interactive menu in `src/frontend/menu.rs`.

### Menu Options

| Option | Action |
| --- | --- |
| 1 | Show databases |
| 2 | Create database |
| 3 | Select database |
| 4 | Show tables |
| 5 | Create table |
| 6 | Load CSV |
| 7 | Insert single tuple |
| 8 | Show tuples |
| 9 | Show table statistics |
| 10 | Check heap health |
| 11 | Exit |

### Table Creation

`create_table_cmd()` collects column definitions from the user, stores them in the catalog, saves the catalog, and initializes the table file on disk. `BufferManager::load_table_from_disk()` then loads the table pages back into memory using `read_all_pages()`.

### CSV Loading

`load_csv()` validates the schema before it reads any data rows. It checks that each column type is supported, then opens the CSV file and processes rows one at a time:

- skip empty rows
- validate column count
- validate each value against its column type
- serialize the row to bytes
- insert it through `HeapManager::insert_tuple()`

This means malformed rows are rejected without aborting the whole load. Valid rows still get inserted.

### Single-Tuple Insertion

`insert_single_tuple()` uses the same validation and serialization path as CSV loading, but it prompts for one value per column in the CLI.

### Sequential Scan Output

`show_tuples()` renders tuples as a formatted table. It reads the schema from the catalog, walks each data page, validates page headers, decodes tuple bytes by column type, and prints a single numbered row per tuple.

### Heap Diagnostics

`check_heap_cmd()` prints:

- heap page count
- FSM fork page count
- total tuples
- last vacuum timestamp or elapsed time
- FSM sidecar file size if present
- the current operation metrics snapshot

This is the quickest way to confirm that the heap header and FSM are in sync.

## Instrumentation

The instrumentation module tracks operation counts with relaxed atomics.

### FSM Counters

- `fsm_search_avail_calls`
- `fsm_search_tree_calls`
- `fsm_read_page_calls`
- `fsm_write_page_calls`
- `fsm_serialize_page_calls`
- `fsm_deserialize_page_calls`
- `fsm_set_avail_calls`
- `fsm_vacuum_update_calls`

### Heap Counters

- `insert_tuple_calls`
- `get_tuple_calls`
- `allocate_page_calls`
- `write_page_calls`
- `read_page_calls`
- `page_free_space_calls`

### Snapshot API

`StatsSnapshot::capture()` reads the current counters, `StatsSnapshot::reset_all()` clears them, and `StatsSnapshot::print_table()` prints a compact diagnostics table. The `CHECK_HEAP` menu command uses this snapshot.

## Testing Coverage

The repository has targeted integration tests under `tests/` that exercise the real storage paths.

### Heap Manager Tests

`tests/test_heap_manager.rs` covers:

- heap creation
- single and multi-row inserts
- point lookup by coordinates
- sequential scans
- header persistence after flush/reopen
- large tuple insertion
- invalid coordinate handling
- empty scans
- multiple-page growth

### FSM Integration Tests

`tests/test_fsm_heavy.rs` covers:

- large insert workloads
- distinct page allocation under heavy writes
- fragmentation and rebuild behavior
- FSM recovery after sidecar removal
- oversize tuple rejection

### Additional Integration Tests

Other tests in `tests/` cover page creation, slot parsing, page free-space calculation, page counting, catalog initialization, and persistence helpers.

## Running the Project

### Normal Run

```bash
cargo run
```

### Logging

```bash
RUST_LOG=off cargo run
RUST_LOG=info cargo run
RUST_LOG=debug cargo run
RUST_LOG=trace cargo run
```

### Tests

```bash
cargo test
cargo test test_heap_manager -- --nocapture
cargo test test_fsm_heavy -- --nocapture
```

### Benchmark Binary

```bash
cargo run --bin benchmark_fsm_heap
```

The benchmark runner writes reports into `benchmark_runs/` and feeds the history files already checked into the repo.

## Benchmark Results (Available)

This section captures the benchmark evidence requested in the submission checklist.

### Latest RookDB FSM/Heap Run

Source: `benchmark_runs/latest_fsm_heap_benchmark.json` (run id `1776859982`)

| Metric | Value |
| --- | ---: |
| Small insert TPS | 21291.09 |
| Large insert TPS | 16930.30 |
| Point lookup OPS | 515331.10 |
| Sequential scan TPS | 2167611.49 |
| FSM rebuild time (sec) | 0.005375 |
| Inserted tuples | 21000 |
| Scanned tuples | 21000 |
| Point lookups passed | 1000 / 1000 |
| Oversized tuple rejected | true |
| FSM rebuild search found page | true |

### Cross-Engine Snapshot

Source: `benchmark_runs/benchmark_comparison.csv`

| Engine | Rows Configured | Insert sec | Update sec | Delete sec | Rows After Delete | Avg Payload Len | Small TPS | Large TPS | Lookup OPS | Scan TPS | FSM Rebuild sec | Avg FSM Free Bytes | pgbench TPS | pgbench Latency ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| rookdb_fsm_heap | 21000 | NA | NA | NA | NA | NA | 21291.09 | 16930.30 | 515331.10 | 2167611.49 | 0.005375 | NA | NA | NA |
| sqlite | 100000 | 0 | 1 | 0 | 90000 | 55.56 | NA | NA | NA | NA | NA | NA | NA | NA |
| mysql | 100000 | 1 | 0 | 0 | 90000 | 55.56 | NA | NA | NA | NA | NA | NA | NA | NA |
| postgres_fsm | 1000 | 0 | 0 | 0 | 900 | 55.56 | NA | NA | NA | NA | NA | 2269.71 | NA | NA |
| pgbench | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | 3867.890925 | 2.068 |

### Recent RookDB Trend (Last 3 Runs)

Source: `benchmark_runs/benchmark_history.csv`

| Run ID | Small TPS | Large TPS | Lookup OPS | Scan TPS | Rebuild sec |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1776596962 | 18852.9578 | 13036.0534 | 43332.4078 | 47035.3673 | 0.009143 |
| 1776620140 | 21265.6556 | 16456.1535 | 77746.6411 | 2166632.8431 | 0.006503 |
| 1776859982 | 21291.0861 | 16930.2971 | 515331.1002 | 2167611.4872 | 0.005375 |

The latest run remains correctness-clean (`scan_matches_insert_count = true`) while maintaining strong insert throughput and very low FSM rebuild time.

## Current Limitations and Future Work

- Free-space management is page-granular, not tuple-granular.
- There is no full vacuum/compaction implementation yet.
- Deleted tuples can leave interior holes until a future compaction pass rewrites the page.
- The FSM search policy is deterministic and left-first; there is no load-spreading hint such as `fp_next_slot` in the current code.
- `last_vacuum` is stored in the header but is not yet part of a full maintenance lifecycle.

## Implementation Notes

A few code-level details are worth keeping in mind when reading or extending the implementation:

- `insert_tuple()` uses ceiling rounding when converting required bytes to an FSM category.
- `page_free_space()` and the FSM rebuild path work from the page header bytes, so the first 8 bytes of every data page matter.
- `read_all_pages()` is used by the buffer manager to load an entire heap file into memory.
- `create_page()` still exists as a low-level disk helper, but `HeapManager::allocate_new_page()` is the stateful path that updates header and FSM state together.

## Summary

RookDB's heap manager and FSM are built around a simple rule: heap pages own tuple storage, while the FSM owns page-level availability. The heap manager performs validation, insertion, lookup, deletion, scanning, and header persistence. The FSM keeps page selection fast and rebuildable. Together they give the database a compact, testable storage layer that can recover from a missing or stale free-space sidecar without losing heap data.

<!-- BENCHMARK_RUN_LOG_START -->
### Auto-updated Benchmark Run Log

Latest run is injected automatically by `cargo run --bin benchmark_fsm_heap ...`.

- Latest run id: `1776859982`
- Latest JSON report: `benchmark_runs/latest_fsm_heap_benchmark.json`
- History CSV: `benchmark_runs/benchmark_history.csv`

| Run ID | Small TPS | Large TPS | Lookup OPS | Scan TPS | Rebuild sec | Correctness | Oversize Reject |
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :---: |
| `1776859982` | 21291.09 | 16930.30 | 515331.10 | 2167611.49 | 0.005375 | ✅ | ✅ |

> Re-run the benchmark command to refresh this section and append to history files.
<!-- BENCHMARK_RUN_LOG_END -->
