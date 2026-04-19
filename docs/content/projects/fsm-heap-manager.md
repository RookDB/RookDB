---
title: FSM and Heap Manager
sidebar_position: 6
---

# FSM and Heap Manager

## Project Scope

This project implements page-level space management and tuple storage using:

- A heap file manager for tuple insert, point lookup, and sequential scan.
- A Free Space Map (FSM) sidecar fork to track free space categories per heap page.
- Header metadata persistence for page count and tuple statistics.

Out of scope in this phase: tuple-level delete/update compaction policies and index-assisted access paths.

## Design Summary

### Storage Model

- Page 0 stores table metadata via HeaderMetadata.
- Pages 1..N are slotted heap pages.
- Free space is tracked in a separate .fsm file.

### FSM Model

- Uses a PostgreSQL-style multi-level max-tree concept.
- Leaf entries represent free-space category per heap page (u8, 0-255).
- Internal nodes store max(child_left, child_right).
- Root acts as quick rejection: if root < required category, no page can satisfy insert.

### Rebuild Strategy

The FSM fork is treated as reconstructible state. On rebuild from heap:

1. Read heap header and page count.
2. Initialize empty FSM pages.
3. Scan each heap data page for free bytes.
4. Call fsm_set_avail per page to populate leaf + propagate max values upward.
5. Sync FSM fork.

This allows recovery even if the .fsm file is missing or stale.

## Public APIs (Current)

### HeapManager

- `create(file_path) -> io::Result<Self>`
- `open(file_path) -> io::Result<Self>`
- `insert_tuple(tuple_data) -> io::Result<(u32, u32)>`
- `get_tuple(page_id, slot_id) -> io::Result<Vec<u8>>`
- `scan() -> HeapScanIterator`
- `flush() -> io::Result<()>`

### FSM

- `open(fsm_path, heap_page_count) -> io::Result<Self>`
- `build_from_heap(heap_file, fsm_path) -> io::Result<Self>`
- `fsm_search_avail(min_category) -> io::Result<Option<u32>>`
- `fsm_set_avail(heap_page_id, new_free_bytes) -> io::Result<()>`
- `fsm_vacuum_update(heap_page_id, reclaimed_bytes) -> io::Result<()>`
- `sync() -> io::Result<()>`

## Evaluation Checklist (This Phase)

### Correctness

- Insert path validates tuple size limits and page-space availability before write.
- `get_tuple(page_id, slot_id)` validates page and slot bounds.
- `scan()` provides full sequential coverage over data pages.
- FSM rebuild correctness fixed: `build_from_heap` now rebuilds leaf + internal tree state (not root-only).

### Documentation Quality

- Design assumptions are explicitly documented:
	- Page 0 reserved for metadata.
	- Data pages start from Page 1.
	- `.fsm` fork is recoverable/rebuildable state.
- API surface and implementation status are tracked in this page.
- Dated change-log entries are maintained for traceability.

### Robustness

- Oversized tuple insertion is rejected.
- Missing/stale `.fsm` can be rebuilt from heap data.
- Boundary checks exist for page/slot accesses and tuple bounds.
- Retry logic on insert avoids repeatedly attempting pages that already failed current insert requirements.

### Modular and Clean Code

- Heap and FSM are separated into dedicated modules with reusable APIs.
- Disk/page helper APIs (`read_page`, `write_page`, `read_all_pages`, `get_tuple_count`, `page_free_space`) are reused across tests and benchmarking.
- FSM update logic is centralized in `fsm_set_avail` and reused by insert and rebuild paths.

## Benchmarking (Initial Results)

### Benchmark Runner Script (Cross-Platform)

A dedicated benchmark binary is provided:

- `src/bin/benchmark_fsm_heap.rs`

It runs on macOS, Linux, and Windows through Cargo and produces a JSON report with:

- System details (OS, CPU, core count, memory)
- Insert throughput (small/large tuples)
- Point lookup ops/sec
- Sequential scan throughput
- FSM rebuild time
- Correctness/robustness checks
- Scalability metrics (pages used, tuple density)

### How to Run

From repo root:

```bash
cargo run --bin benchmark_fsm_heap -- \
	--small-tuples 5000 \
	--large-tuples 300 \
	--lookup-samples 500 \
	--output benchmark_runs/initial_phase_results.json
```

Default run (larger workload):

```bash
cargo run --bin benchmark_fsm_heap
```

### Cross-Database Comparison (SQLite, MySQL, pgbench, PostgreSQL FSM)

For standard external comparison runs, use the benchmark kit:

```bash
./benchmarks/run_all_benchmarks.sh
```

Or run each target independently:

```bash
./benchmarks/run_sqlite_bench.sh
./benchmarks/run_mysql_bench.sh
./benchmarks/run_pgbench.sh
./benchmarks/run_postgres_fsm_compare.sh
```

Output artifacts are written to `benchmark_runs/` and can be compared with RookDB's native benchmark JSON and CSV history.

### Initial Measured Output (Phase Baseline)

Source: `benchmark_runs/initial_phase_results.json`

**Run environment**

- OS: Darwin 14.5
- Kernel: 23.5.0
- CPU: Apple M1
- Logical cores: 8
- Arch: aarch64

**Workload config**

- Small inserts: 5000 tuples × 50 bytes
- Large inserts: 300 tuples × 1000 bytes
- Point lookup samples: 500

**Performance**

- Small insert throughput: 2929.15 tuples/sec
- Large insert throughput: 1499.67 tuples/sec
- Point lookup throughput: 20811.37 ops/sec
- Sequential scan throughput: 32206.24 tuples/sec
- FSM rebuild time: 0.0058 sec

<!-- BENCHMARK_RUN_LOG_START -->
### Auto-updated Benchmark Run Log

Latest run is injected automatically by `cargo run --bin benchmark_fsm_heap ...`.

- Latest run id: `1776596962`
- Latest JSON report: `benchmark_runs/latest_fsm_heap_benchmark.json`
- History CSV: `benchmark_runs/benchmark_history.csv`

| Run ID | Small TPS | Large TPS | Lookup OPS | Scan TPS | Rebuild sec | Correctness | Oversize Reject |
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :---: |
| `1776596962` | 18852.96 | 13036.05 | 43332.41 | 47035.37 | 0.009143 | ✅ | ✅ |

> Re-run the benchmark command to refresh this section and append to history files.
<!-- BENCHMARK_RUN_LOG_END -->

## Testing (Initial Results)

### Functional/Correctness Signals

- Inserted tuples: 5300
- Scanned tuples: 5300
- Count match: true
- Point lookups passed: 500 / 500

### Robustness Signals

- Oversized tuple rejected: true
- FSM rebuild search succeeds after rebuild: true

### Scalability Signals (Current Workload)

- Heap page count: 75
- FSM page count: 3
- Pages used with tuples: 74
- Average tuples/page (used pages): 71.62

## Existing Benchmark Standards and Comparison Plan (Bonus)

Reviewed standards to align future comparison:

- YCSB (Yahoo Cloud Serving Benchmark) — throughput/latency for key-value style operations.
- TPC-C / TPC-H families — transactional and analytical workload standards.

Current benchmark is component-level (FSM + heap manager) rather than full SQL workload. Next iteration will map measured metrics to YCSB-like operation classes:

- Insert-heavy workload → current small/large tuple insert throughput
- Read-heavy workload → point lookup + sequential scan throughput
- Recovery behavior → FSM rebuild timing

Planned enhancement: export CSV from JSON and generate comparison graphs (baseline vs. optimized variants).

## Innovative/Experimental Directions (Bonus)

- Compare first-fit vs. hint-guided page selection variants.
- Evaluate sequential insertion without `fp_next_slot` load-spreading under mixed insert sizes.
- Measure impact of different tuple size distributions (uniform vs. skewed).
- Add repeated-run median/p95 reporting for more stable benchmarking.

## In-depth Study References (Working Set)

- PostgreSQL Free Space Map design notes/concepts.
- YCSB benchmark model and workload taxonomy.
- TPC benchmark families (TPC-C, TPC-H) for broader context.

## Implementation Progress

### Completed

- Heap file creation/open with persistent header metadata.
- FSM-aware tuple insertion path (search page, insert, update FSM).
- Sequential scan iterator over heap pages.
- Tuple retrieval by (page_id, slot_id).
- FSM rebuild path now reconstructs full tree state from heap pages.
- Integration coverage for heavy insertion, boundary checks, deallocation simulation, and persistence recovery.

### Current Validation Status

- Heavy FSM integration tests passing.
- Page allocation integration test passing with self-contained CSV fixture generation.
- Full cargo test suite passing.

## Test Coverage Notes

Key integration scenarios validated:

- Large insertion throughput behavior.
- Allocation correctness under large tuple sizes.
- Boundary rejection for oversized tuples.
- Simulated free-space reclamation and rediscovery.
- FSM rebuild after sidecar removal.
- Fragmentation search behavior after rebuild.

## Known Limitations / Next Milestones

- Implement `fp_next_slot` hint for multi-threaded load spreading (currently unused).
- Expand multi-level page addressing beyond current practical ranges.
- Add vacuum timestamp lifecycle updates in header metadata.
- Add graph-based benchmark reporting (throughput and latency trends across runs).

## Change Log

### 2026-03-28

- Fixed FSM rebuild logic to repopulate leaf/internal tree entries (not root-only).
- Updated FSM heavy tests to green.
- Made allocation integration test self-contained by generating CSV fixture during test setup.

### 2026-04-19

- Added cross-database benchmarking scripts for SQLite, MySQL, PostgreSQL (`pgbench`), and PostgreSQL FSM metrics.
- Added benchmark aggregation output at `benchmark_runs/benchmark_comparison.csv`.
- Updated docs to include database file changes, API surfaces, and CLI/benchmark workflow.

## Documentation Submission Checklist Mapping

This section directly maps to the documentation submission requirements.

1. **Details of newly introduced database files**
	- Covered in Database Doc: `.dat`, `.dat.fsm`, benchmark intermediate files and outputs.
2. **Modifications made to the database structure**
	- Covered in Design + Database docs: sidecar FSM fork and heap/FSM separation.
3. **Changes to page layout or file structure / tuple layout**
	- Covered in Database Doc: slotted page structure (`lower`, `upper`, item-id, tuple payload).
4. **Algorithms used**
	- Covered here and in Database Doc: max-tree search, bubble-up max propagation, rebuild-from-heap.
5. **Newly created data structures and purpose**
	- Covered in Database Doc: `FSMPage`, `HeaderMetadata`, `HeapScanIterator`.
6. **Backend functions and purpose**
	- Covered in API Doc: HeapManager + FSM function list and purpose.
7. **Frontend/CLI changes**
	- Covered in User Guide: workflow and command behavior.
8. **Benchmark results**
	- Covered below and in benchmark artifacts under `benchmark_runs/`.
9. **Potential future work**
	- Covered in Known Limitations / Next Milestones.

## Cross-Database Benchmark Snapshot

From `benchmark_runs/benchmark_comparison.csv`:

- `rookdb_fsm_heap`
  - small_insert_tps: 18852.96
  - large_insert_tps: 13036.05
  - lookup_ops_per_sec: 43332.41
  - seq_scan_tps: 47035.37
  - fsm_rebuild_seconds: 0.009142958
- `sqlite`
  - rows_configured: 100000
  - update_seconds: 1
  - rows_after_delete: 90000
- `mysql`
  - rows_configured: 100000
  - update_seconds: 1
  - rows_after_delete: 90000
- `postgres_fsm`
  - avg_fsm_free_bytes: 2269.71
  - rows_after_delete: 900
- `pgbench`
  - pgbench_tps: 3867.890925
  - pgbench_latency_ms: 2.068
