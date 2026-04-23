---
title: FSM and Heap Manager Design
sidebar_position: 1
---

# FSM and Heap Manager Design

This page is the architectural overview for the FSM and heap manager implementation.

## Project Scope

The current phase focuses on page-level storage operations:

- Tuple insertion using FSM-guided page selection
- Tuple retrieval by page id and slot id
- Sequential scanning across pages
- Free space tracking at page granularity through an FSM sidecar file

Out of scope for this phase:

- Full in-page compaction
- Tuple relocation as a standard insert behavior
- Advanced tuple-level vacuum lifecycle

## Database Files and Structure

The storage layout introduces and uses the following files:

| File | Purpose |
| --- | --- |
| `database/base/<db>/<table>.dat` | Heap file containing page 0 metadata and data pages |
| `database/base/<db>/<table>.dat.fsm` | Free Space Map sidecar fork |
| database/global/catalog.json | Database and table metadata |

## Header and Page Layout

Page 0 stores header metadata:

- page_count as u32
- fsm_page_count as u32
- total_tuples as u64
- last_vacuum as u32

Data pages are 8192-byte slotted pages with:

- lower pointer at bytes 0 to 3
- upper pointer at bytes 4 to 7
- slot directory entries of offset and length
- tuple payloads packed from the end of page backward

## Algorithms

### FSM Search

Insertion computes a minimum required free-space category and calls FSM search.

Search behavior:

1. Read root category.
2. Early return if root cannot satisfy request.
3. Traverse tree levels toward a qualifying leaf.
4. Convert leaf slot to heap page id.

### FSM Update and Bubble-Up

After insert or reclaim events:

1. Convert free bytes to category.
2. Update leaf value.
3. Recompute parent max values.
4. Propagate root deltas upward.

### Insert Retry Strategy

The heap manager uses a resilient retry flow:

1. Try FSM-provided page.
2. Retry after correcting page category if first attempt fails.
3. Allocate a new page if no page can satisfy insertion.

## Data Structures Introduced

| Data Structure | Purpose |
| --- | --- |
| HeaderMetadata | Persistent heap header state |
| FSMPage | Tree storage page for free-space categories |
| FSM | Sidecar fork manager and tree operations |
| HeapManager | Core heap API for create open insert read delete scan |
| HeapScanIterator | Lazy page-by-page scan iterator |

## Backend Functions

### Heap Layer

- create
- open
- insert_tuple
- get_tuple
- delete_tuple
- scan
- allocate_new_page
- flush

### FSM Layer

- open
- build_from_heap
- fsm_search_avail
- fsm_set_avail
- fsm_vacuum_update
- sync

### Disk and Page Layer

- create_page
- read_page
- write_page
- read_header_page
- update_header_page
- read_all_pages
- init_page
- page_free_space
- get_tuple_count
- get_slot_entry

## Frontend and CLI Changes

The interactive menu supports operations relevant to this project:

- Load CSV
- Insert single tuple
- Show tuples
- Check heap health

These commands are wired through executor and heap manager layers.

## Potential Future Work

- Full compaction-aware insert path
- Stronger fragmentation recovery flow
- Expanded type system
- Enhanced concurrency and contention mitigation
- Deeper benchmark methodology with repeated runs and medians

## Submission Checklist Coverage

This section maps the requested submission requirements to pages in this folder.

| Requirement | Covered In |
| --- | --- |
| Newly introduced database files, structure, contents, intermediate generation | This page and Free Space Manager Deep Dive |
| Modifications to database structure | This page and Heap Manager and Page Layout |
| Page layout, file structure, tuple layout changes | Heap Manager and Page Layout |
| Algorithms used | This page and Free Space Manager Deep Dive |
| Newly created data structures and purpose | This page and Features Implemented |
| Backend functions and purpose | This page and Heap Manager and Page Layout |
| Frontend and CLI changes | This page and Features Implemented |
| Benchmark results | Benchmark Report |
| Potential future work | This page |
| No PDF uploads | All content in this section is Markdown only |

## Related Pages

- [Heap Manager and Page Layout](./heap-manager)
- [Free Space Manager Deep Dive](./free-space-manager)
- [Implemented Features Summary](./features-implemented)
- [Benchmark Report](./benchmark-report)
- [Testing Guide](./test)
