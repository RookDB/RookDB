# RookDB Comprehensive Test Documentation

This document serves as the unified source for all integrated and unit tests implemented in the RookDB database system. It blends design-doc coverage objectives (FSM + Heap functionality) with general validation and schema testing goals.

## 1. Running the Test Suite

To execute all tests within the workspace (unit + integration):
```bash
cargo test --all-targets --all-features
```

For executing documentation tests specifically:
```bash
cargo test --doc
```

## 2. Integration Tests (`tests/` directory)

The integration test suite strictly examines the boundary behaviors and interactions between the components (HeapManager, Free Space Manager (FSM), Catalog, and Layout managers).

### High-Stress FSM and Allocation Tests (`test_fsm_heavy.rs`)
1. **Large Insertions & Timing (`test_large_insertions`)**
   - **Concept**: Throughput limitations and Large Insertions.
   - **Action**: Inserts tens of thousands of records consecutively as fast as possible.
   - **Verification**: Benchmarks elapsed CPU time ensuring operations complete under thresholds and the FSM tree doesn't choke during leaf filling.

2. **Updation and Deallocation Integrity (`test_update_delete_fsm_deallocation`)**
   - **Concept**: Emulating Updates/Deletes and Reclaiming space.
   - **Action**: Inserts heavy tuples (e.g., 4000 bytes) taking half of a page. Clears out the FSM explicitly simulating a compaction/delete event (`fsm_vacuum_update`).
   - **Verification**: Verifies that the FSM's `fsm_search_avail` can successfully locate and repurpose the released page boundary reliably.

3. **Allocation Accuracy & Collision (`test_allocation_accuracy`)**
   - **Concept**: Allocation limits, bounded-tracking.
   - **Action**: Inserts two 8000-byte tuples which occupy ~95% of a page boundary each.
   - **Verification**: Ensures the HeapManager effectively enforces physical page splits for each tuple (`page_id1 != page_id2`).

4. **Fragmentation Management / Leaf Node Bubble Up (`test_fragmentation_management`)**
   - **Concept**: Spatial Fragmentation & Coalescing.
   - **Action**: Inserts small fragmented sequential payloads, opening the FSM mapping files directly to observe spatial distribution limits bubbling up the max-tree bounds.
   - **Verification**: Ensures max-tree index categorization accurately reflects node capacities.

5. **Persistence Recovery (`test_persistence_fsm_recovery`)**
   - **Concept**: Crash Resilience / Recovery.
   - **Action**: Writes records and destructively deletes the `.fsm` runtime side-car mapped to the layout.
   - **Verification**: Asserts that `FSM::build_from_heap` successfully resuscitates the spatial mapping layout by scraping the `.dat` master-file independently.

6. **Boundary Violations (`test_boundary_violations`)**
   - **Concept**: Safe boundaries and pointer overflows.
   - **Action**: Injects a payload mathematically exceeding safe limits (9000 bytes).
   - **Verification**: Asserts explicit `Err(_)` rejections before pointer corruption or Out-Of-Memory anomalies happen.

### Heap Manager General Tests (`test_heap_manager.rs`)
7. **Basic Heap Operations**
   - **`test_heap_create`**: Opening empty heaps vs establishing new ones.
   - **`test_heap_insert_single` / `test_heap_insert_multiple`**: Verifying sequential and chunked payload storage correctly updates internal byte-counters.
   - **`test_heap_get_tuple` / `test_heap_scan`**: Checks tuple exact coordinations (Page + Slot bounds) routing correctly alongside linear unindexed scans over non-contiguous spaces.
   - **`test_heap_header_persistence`**: Saving header counters seamlessly avoiding desync logic.
   - **`test_heap_large_tuples` & `test_heap_invalid_operations`**: Checks safety measures natively against invalid pointers.
   - **`test_heap_multiple_pages`**: Assertions verifying page spanning sequences seamlessly index across 8192 boundaries.

### Smart Leaf FSM Allocations (`test_fsm_page_allocation.rs`)
8. **Optimized FSM Page Density**
   - **`test_fsm_page_allocation`**: Triggers full data injection via Bulk CSV loaders and directly analyzes `HeapManager` distribution layout. Ensures tuples are spread properly into 2-3 packed pages (e.g., using 8184 capacity logic) instead of creating sequential wasted pages iteratively.

### Primitive Page Boundaries (`test_create_page.rs`, `test_read_page.rs`, etc.)
9. **Raw Layout I/O Tests**
   - Validates `.dat` primitive operations, assuring read/write limits map correctly. Sizes verify (`PAGE_HEADER_SIZE` + `PAGE_SIZE`) exactly and no metadata bytes drip recursively.

## 3. Unit Tests (`src/` directory)

### Page & Offset Safeties (`src/backend/page/mod.rs`)
Unit tests placed inside the Page layout module focus exclusively on raw offset integrities:
1. **`test_page_free_space_detects_corrupted_pointers`**
   - Verifies corrupted `lower/upper` pointers are rejected aggressively rather than triggering kernel panics natively.
2. **`test_get_tuple_count_detects_invalid_alignment`**
   - Refuses malformatted slot-directory assignments mathematically out of index bounds.
3. **`test_get_slot_entry_detects_out_of_bounds_tuple`**
   - Protects byte sequences slices out-of-index, securing memory boundary limits effectively.

### Type Verification Tests (`src/backend/types_validator.rs`)
Unit tests targeting database schema type integrities:
1. **Case-Insensitive Types (`INT`, `int`, `InT`)**: Successfully parsed seamlessly over variants.
2. **Types Blocking (`FLOAT`, `VARCHAR`)**: Rejected gracefully reporting Unsupported exceptions.
3. **Data Dimensions**: Checking `INT` over/underflows, `TEXT` truncation paddings under constraints linearly without crash traces.

## 4. Coverage Gaps & Future Additions
The current architecture enjoys strict safety assurances. If strict completion of the historical design document is sought, the following named tests could be formally asserted using explicit assertion bindings:
- Explicit invariant tree bubbling checks (e.g., `test_fsm_set_avail_bubbles_up`).
- Decrement category mapping validations (`test_fsm_set_avail_decreases_category`).
- Explicit metrics benchmarks using automated Rust Criterion tools `bench_insert_throughput`.
