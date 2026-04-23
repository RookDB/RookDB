# RookDB Testing Guide (Comprehensive)

This document is the comprehensive testing reference for RookDB. It is based on the test strategy and detailed scenarios captured in `tests.md`, and it is structured as a practical guide for development, verification, and debugging.

The suite validates both correctness and behavior under load across:

- Heap file manager operations
- Free Space Manager (FSM) tree logic
- Catalog persistence
- Disk/page primitives
- End-to-end integration flows
- Isolation and cleanup safety
- Stress and recovery behavior

## 1. Overview

RookDB ships with a broad test surface that combines:

- Integration tests in `tests/`
- Source-level unit tests inside `src/backend/**`
- Stress/performance validation via heavy insertion tests and benchmark artifacts

The objective is to prove not only that individual functions return expected values, but also that heap, FSM, disk I/O, and catalog stay consistent together across create/insert/read/delete/scan/reopen cycles.

## 2. Test Suite Inventory

### 2.1 Integration Test Files (`tests/`)

- `tests/test_heap_manager.rs`
- `tests/test_fsm_heavy.rs`
- `tests/test_hsm_integration.rs`
- `tests/test_init_catalog.rs`
- `tests/test_create_page.rs`
- `tests/test_fsm_page_allocation.rs`
- `tests/test_init_page.rs`
- `tests/test_init_table.rs`
- `tests/test_load_catalog.rs`
- `tests/test_page_count.rs`
- `tests/test_page_free_space.rs`
- `tests/test_read_page.rs`
- `tests/test_save_catalog.rs`
- `tests/test_write_page.rs`

### 2.2 Source-Level Unit Test Modules (`src/backend/**`)

- `src/backend/heap/heap_manager.rs`
- `src/backend/fsm/fsm.rs`
- `src/backend/page/mod.rs`
- `src/backend/page_api.rs`
- `src/backend/heap/types.rs`
- `src/backend/types_validator.rs`
- `src/backend/error_handler.rs`

These unit tests target local invariants such as page pointer bounds, slot entry safety, serialization behavior, and validation/error handling.

## 3. Core Heap Manager Coverage (`test_heap_manager.rs`)

This category verifies primary heap lifecycle behavior.

### 3.1 `test_heap_create`

Verifies:

- Heap creation succeeds
- Header page and initial data page are initialized
- Initial counters (`page_count`, `total_tuples`) start from expected values

### 3.2 `test_heap_insert_single`

Verifies:

- Single insertion returns valid `(page_id, slot_id)`
- First tuple lands in expected slot position
- Header tuple count updates correctly

### 3.3 `test_heap_insert_multiple`

Verifies:

- Multiple insertions succeed sequentially
- No tuple loss between insert calls
- Aggregate tuple counters remain accurate

### 3.4 `test_heap_get_tuple`

Verifies:

- Coordinate-based retrieval returns exact inserted bytes
- No corruption in write-read roundtrip

### 3.5 `test_heap_scan`

Verifies:

- Full sequential scan returns all inserted tuples
- Iterator-based scan flow is stable

### 3.6 `test_heap_header_persistence`

Verifies:

- Header metadata is flushed to disk
- Reopen/read cycle preserves tuple/page counters

### 3.7 `test_heap_large_tuples`

Verifies:

- Large tuple insert/retrieve path works without truncation
- Slotted-page boundaries remain valid

### 3.8 `test_heap_invalid_operations`

Verifies:

- Invalid page/slot requests return errors
- System handles bad coordinates gracefully

### 3.9 `test_heap_empty_scan`

Verifies:

- Empty heap scan returns no tuples
- No panic on empty iterator traversal

### 3.10 `test_heap_multiple_pages`

Verifies:

- Multi-page growth when pages fill
- Cross-page scan still returns complete dataset

## 4. FSM Heavy and Robustness Coverage (`test_fsm_heavy.rs`)

These tests validate FSM/heap coupling under stress.

### 4.1 `test_large_insertions` (Critical)

Scenario:

- High-volume insertion run (50,000 records in the documented scenario)
- FSM search/update activity exercised repeatedly

Validates:

- FSM-guided allocation keeps operating under load
- Operations remain internally consistent across repeated insert cycles
- Throughput remains within expected performance envelope

### 4.2 `test_update_delete_fsm_deallocation` (Critical Coupling)

Scenario:

- Insert tuples, delete one, then insert again

Validates:

- `delete_tuple` decrements tuple counters correctly
- Heap-side free space changes are propagated to FSM via update path
- Freed space becomes reusable by later inserts

### 4.3 `test_allocation_accuracy`

Validates:

- Large tuples do not overlap on the same page unexpectedly
- FSM allocation decisions avoid page-collision behavior

### 4.4 `test_fragmentation_management`

Validates:

- Fragmented/free-space category behavior remains searchable
- FSM category update and tree propagation remain correct

### 4.5 `test_persistence_fsm_recovery` (Crash Recovery)

Scenario:

- Create data, remove/corrupt FSM sidecar, rebuild from heap

Validates:

- FSM can be rebuilt from heap metadata safely
- Data path remains recoverable even when `.fsm` is missing

### 4.6 `test_boundary_violations`

Validates:

- Oversized tuples are rejected cleanly
- Safety boundaries are enforced without corruption

## 5. Integration and Schema/Isolation Coverage (`test_hsm_integration.rs`)

### 5.1 `test_multiple_columns_insertion`

Scenario:

- Create database and table with multiple typed columns
- Insert rows through executor path
- Reopen and scan

Validates:

- Schema-driven insertion works for mixed INT/TEXT structures
- Multi-column tuple serialization path is stable
- Inserted rows are retrievable and counted correctly

### 5.2 `test_multiple_tables_isolation`

Scenario:

- Create two tables with different schemas
- Interleave inserts across both tables
- Verify independent counts

Validates:

- No cross-table corruption
- Independent heap files/FSM behavior per table
- Shared catalog operations remain consistent

Note:

- This test uses mutex-guarded setup in the documented strategy to avoid parallel interference on shared test artifacts.

## 6. Catalog and Persistence Coverage

Primary checks include:

- `test_init_catalog`
- `test_save_catalog`
- `test_load_catalog`
- `test_init_table`

Validates:

- Catalog initialization and persistence lifecycle
- Save-load roundtrip integrity
- Table schema registration correctness

## 7. Page and Disk Primitive Coverage

Primary checks include:

- `test_create_page`
- `test_init_page`
- `test_page_count`
- `test_page_free_space`
- `test_read_page`
- `test_write_page`
- `test_fsm_page_allocation`

Validates:

- Page initialization bounds (`lower`, `upper`)
- Exact page-level write/read behavior
- Free-space calculations and page-count accounting
- FSM page selection distribution behavior for bulk loading

## 8. Unit Test Focus Areas in `src/`

### 8.1 Memory and Bounds Safety

Examples from `src/backend/page/mod.rs` style tests:

- Detect invalid/corrupted pointer bounds
- Reject out-of-range slot accesses safely

### 8.2 Data Type Validation

Examples from `src/backend/types_validator.rs`:

- Case-insensitive type handling
- Rejection of unsupported types

### 8.3 Error Formatting and Handling

Examples from `src/backend/error_handler.rs`:

- Stable error message and mapping behavior
- Predictable error surfaces for callers

## 9. Running Tests

### 9.1 Run Entire Suite

```bash
cargo test
```

### 9.2 Run One Integration File

```bash
cargo test --test test_heap_manager
cargo test --test test_fsm_heavy
cargo test --test test_hsm_integration
```

### 9.3 Run One Named Test

```bash
cargo test test_page_free_space
cargo test test_multiple_columns_insertion
```

### 9.4 Run with Logs

```bash
RUST_LOG=debug cargo test -- --nocapture
RUST_LOG=trace cargo test -- --nocapture
```

### 9.5 Module-Scoped Logging

```bash
RUST_LOG=storage_manager::backend::fsm=debug cargo test -- --nocapture
RUST_LOG=storage_manager::backend::heap=debug cargo test -- --nocapture
RUST_LOG=storage_manager::backend::fsm=debug,storage_manager::backend::heap=warn cargo test -- --nocapture
```

### 9.6 Run Sequentially (No Test Parallelism)

```bash
cargo test -- --test-threads=1
```

Useful when test artifacts in filesystem paths may conflict under parallel execution.

### 9.7 Run Optimized

```bash
cargo test --release
```

## 10. Operational Metrics and Validation Signals

The documented strategy captures these as key indicators:

- Insert throughput under heavy load
- FSM search/update operation counts
- Page allocation growth behavior
- Rebuild/recovery success after FSM loss
- Oversized tuple rejection behavior

Interpretation guidance:

- Treat exact timings as environment-dependent
- Use trend consistency and pass/fail signals as primary regression checks

## 11. Troubleshooting Guide

### 11.1 Test Hangs / Resource Contention

Symptom:

- Lock contention or apparent hang during filesystem/catalog tests

Action:

```bash
cargo test -- --test-threads=1
```

### 11.2 Permission or Stale Artifact Errors

Symptom:

- File open/create failures due to leftover artifacts

Action:

```bash
rm -rf database/base/test_*
rm -f database/global/catalog.json
cargo test
```

### 11.3 Unexpected Allocation Assertions

Symptom:

- Page growth/allocation assertions fail unexpectedly

Action:

```bash
RUST_LOG=storage_manager::backend::fsm=debug cargo test test_large_insertions -- --nocapture
```

Inspect FSM search and page allocation log lines for regression clues.

## 12. Best Practices

- Use `cargo test` as the default test entrypoint.
- Keep test artifact cleanup strict to avoid false negatives.
- Use `--nocapture` with `RUST_LOG` when debugging behavior, not only failures.
- Run heavy FSM tests separately when profiling.
- Use `--release` when comparing performance runs over time.

## 13. What This Suite Proves

When the suite passes, it provides strong evidence that:

- Heap operations are correct across create/insert/get/scan/delete/reopen
- FSM and heap stay synchronized through allocation and deallocation updates
- Catalog persistence works across save/load cycles
- Page-level binary layout and bounds checks are stable
- End-to-end insertion paths respect schema and table isolation
- Crash-recovery-style FSM rebuild behavior is functional
- Safety constraints (oversized tuple rejection, invalid slot/page handling) hold

## 14. Residual Limits

Even with strong coverage, these limits remain:

- Some scenarios are inherently filesystem-state sensitive
- Benchmark/stress timing can vary by host and cache state
- Cross-engine comparisons are useful context, not strict workload equivalence

## 15. Summary

RookDB testing is layered by design:

- Unit tests protect local invariants and safety checks
- Integration tests validate subsystem interaction and persistence
- Heavy/stress tests validate robustness and operational behavior under load

This layered strategy gives practical confidence in correctness, recoverability, and day-to-day storage behavior.