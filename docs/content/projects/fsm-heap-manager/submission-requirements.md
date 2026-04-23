---
title: Submission Requirements Coverage
sidebar_position: 1
---

# Submission Requirements Coverage

This page is structured exactly against the requested documentation checklist and provides one separate section for each requirement.

## 1. Details of Newly Introduced Database Files

The FSM and heap manager work introduces and uses the following persistent files:

| File | Structure and Content |
| --- | --- |
| `database/base/<db>/<table>.dat` | Heap file with page 0 header metadata and pages 1..N data pages |
| `database/base/<db>/<table>.dat.fsm` | FSM sidecar file containing tree pages with free-space categories |
| `database/global/catalog.json` | Database and table metadata, schema references, and mapping information |

Intermediate and generated artifacts related to validation and performance are written under `benchmark_runs/`, including `latest_fsm_heap_benchmark.json`, history CSV/JSONL, and comparison outputs.

## 2. Modifications Made to Database Structure

The implementation extends the storage layer from simple heap-only behavior to heap plus free-space sidecar coordination.

Key structural modifications:

- Header metadata expanded to include `page_count`, `fsm_page_count`, `total_tuples`, and `last_vacuum`.
- Heap growth and FSM growth are synchronized.
- FSM rebuild-on-open supports recovery when sidecar state is missing or stale.

## 3. Changes to Page Layout or File Structure

Page layout remains slotted-page based, with clarified semantics and stable helper APIs.

Data page structure:

- `lower` pointer at bytes 0..3
- `upper` pointer at bytes 4..7
- slot directory entries as offset and length pairs
- tuple payloads packed from page tail toward front

FSM page structure is a compact tree-backed byte array model.

### FSM Page Layout Diagram

![FSM Page Layout](/assets/fsm/FSM_Page_Layout.png)

## 4. Algorithms Used

The key algorithms in this phase are:

- FSM tree search for candidate page discovery using category thresholds
- FSM bubble-up updates after free-space changes
- Three-attempt insert fallback strategy for robust progress under fragmentation or stale category mismatch
- Sidecar rebuild from heap pages for recovery

### FSM Search Flow Diagram

![FSM Insertion/Search Flow](/assets/fsm/FSM_Insertion.png)

### FSM Level Mapping Diagram

![FSM Level Tree Structure](/assets/fsm/FSM_Level_Tree_Structure.png)

## 5. Newly Created Data Structures and Their Purpose

| Data Structure | Purpose |
| --- | --- |
| `HeaderMetadata` | Persistent table-level metadata in page 0 |
| `FSMPage` | Encodes free-space tree data inside one FSM page |
| `FSM` | Sidecar manager for search, update, rebuild, and sync |
| `HeapManager` | Core heap API for create/open/insert/get/delete/scan |
| `HeapScanIterator` | Lazy iterator for page-wise sequential scans |

## 6. Backend Functions and Their Purpose

### Heap Layer Functions

- `create`: initialize heap and metadata
- `open`: open heap and reconcile/rebuild FSM
- `insert_tuple`: FSM-guided insert with fallback
- `get_tuple`: coordinate-based lookup
- `delete_tuple`: slot invalidation and metadata update
- `scan`: sequential tuple iteration
- `allocate_new_page`: append and register page state
- `flush`: sync header, heap, and FSM

### FSM Layer Functions

- `open`: open/create sidecar
- `build_from_heap`: reconstruct sidecar from heap
- `fsm_search_avail`: find candidate page by category
- `fsm_set_avail`: update category and propagate maxima
- `fsm_vacuum_update`: reclaim-aware category refresh
- `sync`: persist sidecar changes

### Disk and Page Functions

- `create_page`, `read_page`, `write_page`
- `read_header_page`, `update_header_page`, `read_all_pages`
- `init_page`, `page_free_space`, `get_tuple_count`, `get_slot_entry`

## 7. Frontend and CLI Changes

The interactive flow exposes heap/FSM behavior through:

- CSV load and single tuple insertion commands
- schema-aware tuple display
- heap health diagnostics command showing metadata and instrumentation counters
- improved validation and actionable error messages in data entry paths

## 8. Benchmark Results (Available)

Representative results from the latest benchmark report include:

- strong small and large insert throughput
- high point lookup and sequential scan throughput
- low FSM rebuild latency
- correctness checks such as scan count parity and oversized tuple rejection

Canonical outputs are published under `benchmark_runs/`, including JSON, CSV, and comparison artifacts.

## 9. Potential Future Work

Planned and natural next steps include:

- full compaction-aware insert path
- richer fragmentation reclamation workflows
- expanded type support
- concurrency and contention improvements
- improved benchmark methodology with repeated runs and median/p95 reporting

## 10. No PDF Uploads

This documentation is maintained entirely as Markdown in Docusaurus content files.

No PDF files are required or included for this submission.

## Cross-References

- [Design Overview](./design-doc)
- [Heap Manager and Page Layout](./heap-manager)
- [Free Space Manager Deep Dive](./free-space-manager)
- [Features Implemented](./features-implemented)
- [Benchmark Report](./benchmark-report)
- [Testing Guide](./test)
