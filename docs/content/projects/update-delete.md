# Update, Delete, Compaction & Autovacuum

## Overview

This project implements the full **UPDATE / DELETE / COMPACTION** pipeline for RookDB.

Current functionality includes:

- SQL-style `WHERE` parsing with nested `AND` / `OR`
- Soft-delete using slot flags
- Physical page compaction to reclaim dead tuple space
- Background autovacuum wake-up based on dead-tuple thresholds
- Per-page write locking for update / delete / compaction safety
- Operation logging for `update`, `delete`, and `compaction`
- Benchmark harness for RookDB vs PostgreSQL

This phase also focused on **correctness**, **testing**, **initial benchmarking**, and **clean integration with other components** such as the heap layer, FSM, and autovacuum manager.

---

## High-level Design

### Soft-delete model

RookDB follows a PostgreSQL-like two-step deletion model:

```text
DELETE / UPDATE(old version)  -> mark slot as deleted
SHOW / SCAN                   -> ignore deleted slots
COMPACTION                    -> rewrite page and physically reclaim space
```

Each slot entry is:

```text
[ offset: u32 ][ length: u16 ][ flags: u16 ]
```

The deleted bit is:

```text
SLOT_FLAG_DELETED = 0x0001
```

### Why UPDATE is implemented as delete + insert

RookDB currently updates rows by:

1. scanning for all matching live tuples,
2. soft-deleting the old tuple versions,
3. reinserting the modified tuple bytes.

This keeps the tuple-writing logic centralized in the heap insertion path, but it also makes `UPDATE` significantly more expensive than `DELETE`.

---

## WHERE clause representation

The parser converts the user condition into **DNF**:

```text
Vec<Vec<Condition>>
outer Vec  = OR groups
inner Vec  = AND conditions
```

This structure is reused by both `DELETE` and `UPDATE`.

Supported operators include:

| Operator | INT | TEXT |
|---|---|---|
| `=` `!=` | ✓ | ✓ |
| `<` `>` `<=` `>=` | ✓ | ✓ lexicographic |
| `BETWEEN` | ✓ | ✓ |
| `IN` / `NOT IN` | ✓ | ✓ |
| `LIKE` / `NOT LIKE` | ✗ | ✓ |

---

## Core APIs implemented by this component

### Update API

```rust
pub fn update_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    assignments: &[SetAssignment],
    condition_groups: &[Vec<Condition>],
    returning: bool,
) -> io::Result<UpdateResult>
```

**Return value:**
- `UpdateResult.updated_count`: number of rows updated
- `UpdateResult.returning_rows`: updated rows when `RETURNING` is enabled

### Delete API

```rust
pub fn delete_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    condition_groups: &[Vec<Condition>],
    returning: bool,
) -> io::Result<DeleteResult>
```

**Return value:**
- `DeleteResult.deleted_count`: number of rows deleted
- `DeleteResult.returning_rows`: deleted rows when `RETURNING` is enabled

### Compaction API

```rust
pub fn compaction_table(
    db_name: &str,
    table_name: &str,
) -> io::Result<usize>
```

**Return value:** number of data pages that were physically compacted.

---

## Reused APIs / functions from other components

This component directly interacts with two reusable APIs from other components.

| API / Function | Used for | Input parameters | Return value |
|---|---|---|---|
| `insert_tuple(file, data)` | Insert updated tuple bytes during UPDATE reinsertion phase | `file: &mut File`, `data: &[u8]` | `io::Result<()>` |
| `fsm_set_avail(file_identity, page_id, free_space)` | Update FSM free-space after compaction and page-space changes | `file_identity: u64`, `page_id: u32`, `free_space: u32` | `()` |

These two APIs are the explicit cross-team integration points used by update/compaction workflows.

Autocompaction path for FSM update is:

`autovacuum worker -> compaction_table(...) -> fsm_set_avail(file_identity, page_num, free)`

---

## Logging verification

Operation logging is currently working and has been verified from generated files under:

- [code/database/logs/bench_db/bench_table/update.log](code/database/logs/bench_db/bench_table/update.log)
- [code/database/logs/bench_db/bench_table/delete.log](code/database/logs/bench_db/bench_table/delete.log)
- [code/database/logs/bench_db/bench_table/compaction.log](code/database/logs/bench_db/bench_table/compaction.log)

Each entry contains:

- top-level timestamp,
- operation name,
- operation-specific details,
- success / failure status.

Example verified timestamp format:

```text
2026-03-30T00:31:36.270Z
```

This confirms that update, delete, and compaction logs are being generated with readable ISO timestamps.

---

## Benchmarking setup

The benchmark harness lives in [code/src/bin/benchmark_compare.rs](code/src/bin/benchmark_compare.rs).

For the current benchmark:

- rows generated: `100000`
- iterations: `5`
- update predicate: `id <= 50000`
- update assignment: `score = score + 10`
- delete predicate: `id <= 20000`

### How the 100k rows are generated

The benchmark creates a synthetic seed table by repeatedly calling heap insertion on deterministic rows:

```text
for id in 1..=100000:
    create tuple(id, age, score, label)
    insert_tuple(...)
```

The seed file is then copied before each benchmark sample so that every iteration starts from the same logical table state.

---

## Initial benchmark results (RookDB, release build)

Measured using:

```text
cargo run --release --bin benchmark_compare -- --rook-only
```

### Summary (100k rows, 5 iterations)

| Operation | Average | Fastest | Slowest | Total |
|---|---:|---:|---:|---:|
| UPDATE | 38721.181 ms | 27699.099 ms | 43002.963 ms | 193605.906 ms |
| DELETE | 26.007 ms | 25.688 ms | 26.654 ms | 130.034 ms |
| COMPACTION | 12.362 ms | 11.964 ms | 13.190 ms | 61.810 ms |

### PostgreSQL baseline (same conditions, previous run)

| Operation | Average | Fastest | Slowest | Total |
|---|---:|---:|---:|---:|
| UPDATE | 309.688 ms | 259.706 ms | 438.599 ms | 1548.439 ms |
| DELETE | 16.512 ms | 13.587 ms | 19.077 ms | 82.561 ms |
| COMPACTION (VACUUM FULL) | 82.720 ms | 61.959 ms | 129.823 ms | 413.602 ms |

### RookDB vs PostgreSQL (average latency comparison)

| Operation | RookDB avg | PostgreSQL avg | Relative result |
|---|---:|---:|---|
| UPDATE | 38721.181 ms | 309.688 ms | PostgreSQL faster (~125x) |
| DELETE | 26.007 ms | 16.512 ms | PostgreSQL faster (~1.58x) |
| COMPACTION | 12.362 ms | 82.720 ms | RookDB faster (~6.69x) |

## Benchmark Comparison

### UPDATE latency (ms) — RookDB vs PostgreSQL

**Results:** RookDB takes **38,721.18 ms** while PostgreSQL takes only **309.69 ms** per UPDATE operation. PostgreSQL is approximately **125x faster**. UPDATE remains the main performance bottleneck for RookDB due to the delete-then-insert implementation.

### DELETE and COMPACTION latency (ms) — RookDB vs PostgreSQL

**Results:**
- **DELETE:** PostgreSQL ~1.58x faster (16.51 ms vs 26.01 ms)
- **COMPACTION:** RookDB ~6.69x faster (12.36 ms vs 82.72 ms)

RookDB's compaction is significantly faster due to its efficient page-level rewriting strategy.

![Benchmark Charts - UPDATE, DELETE, and COMPACTION Latency Comparison](/assets/benchmark-charts.png)

---

## Update scalability diagnosis

To identify the update bottleneck, a phase-level profile was added to the update path and one profiled release run was captured.

### UPDATE phase breakdown (single profiled run)

| Phase | Time |
|---|---:|
| Catalog lookup | 0.001 ms |
| Scan + match | 28.860 ms |
| Soft-delete phase | 160.168 ms |
| Dead tuple header | 0.003 ms |
| Reinsert phase | 38434.766 ms |
| Autovacuum notify | 0.010 ms |
| **Total** | **38623.808 ms** |

`Reinsert phase` is dominated by repeated calls to `insert_tuple(...)` (one call per updated row).
For this profiled run:

- matched rows: `50000`
- reinsert phase time: `38434.766 ms`
- average per reinsert call: ~`0.769 ms`

### Main conclusion

`UPDATE` is currently slower because the operation processes a large matched set and performs the reinsertion path for updated rows.

### Why UPDATE is not scalable yet

Current reasons:

- full table scan to find matches,
- no indexing on the predicate column,
- row-by-row reinsertion after soft-delete,
- repeated page writes and FSM maintenance during insertion.

---

## Testing and robustness

Important edge cases include:

- deleting all rows,
- deleting rows already deleted,
- update with no matching rows,
- nested boolean predicates,
- `LIKE` / `NOT LIKE` with invalid column types,
- compaction idempotence,
- preserving live tuples during compaction,
- concurrent write safety at page level.

### Current test coverage

Three test files were added for this component:

- `code/tests/test_update.rs` — 30 tests covering: condition matching, arithmetic SET, zero-match updates, page-lock acquisition during update, and `RETURNING` correctness
- `code/tests/test_delete.rs` — 57 tests covering: soft-delete correctness, zero-match deletes, delete-all rows, page-lock safety, and WHERE clause edge cases
- `code/tests/test_compaction.rs` — 10 tests covering: live tuple preservation, page-lock held during compaction, idempotency, and post-compaction free-space correctness

Page-locking behaviour is exercised entirely within `test_update.rs` (lock acquire on write), `test_delete.rs` (lock prevent concurrent page writes), and `test_compaction.rs` (lock held for full page rewrite duration).

---

## Folder structure

Final placement for these files:

```text
src/storage/heap/autocompaction.rs
src/storage/log/operation_log.rs
src/storage/page/page_locking.rs
```

Dependency direction used:

- `heap/autocompaction` can call `log` + `page`
- `log` can call `page` if needed
- `page` calls neither `heap` nor `log`

---

## In-depth study (current approach)

### 1) Update/Delete in heap storage (page-level locking)

The update/delete implementation follows a PostgreSQL-style heap pattern where foreground writes are short, page-scoped critical sections.

- `UPDATE` is implemented as version replacement (old tuple invalidated, new tuple inserted), not table-level locking.
- `DELETE` marks tuple state first (soft-delete); physical space reclaim is deferred.
- Page-level write locks isolate conflicting writers on the same page while allowing independent pages to progress concurrently.

This design keeps lock hold-times small and improves concurrency under write-heavy workloads.

### 2) Autocompaction via background workers

The design follows a modern deferred-maintenance model:

- foreground mutation path performs logical changes,
- background workers perform physical reclaim later.

Compaction is page-local and rewrites only pages with dead slots, reducing the amount of time spent in write-critical sections.

This separation of concerns (foreground mutation vs background reclaim) improves robustness and keeps write latency more predictable.

### 3) PostgreSQL scheduling vs this implementation

PostgreSQL autovacuum primarily uses threshold-based eligibility and worker-driven relation selection.

Current implementation introduces an explicit **priority-based scheduling** queue for autocompaction:

- table priority is computed from dead tuples relative to threshold,
- eligible tables are pushed into a max-heap,
- workers pick higher-priority work first.

In this project, priority is effectively:

```text
priority = dead_tuple_count - threshold
threshold = 50 + 0.2 * table_size
```

This makes cleanup effort focus first on relations with higher immediate reclaim pressure.

## PostgreSQL reference baseline and version citation

References used in this study:

- PostgreSQL Source Tree Version: PostgreSQL 19devel (`configure.ac:20`)
- PostgreSQL Documentation (devel): https://www.postgresql.org/docs/devel/
- PostgreSQL Installation (devel): https://www.postgresql.org/docs/devel/installation.html

---

## Feature status summary

| Feature | Status |
|---|---|
| Soft-delete via slot flags | ✅ Done |
| WHERE parser with DNF | ✅ Done |
| UPDATE with arithmetic SET | ✅ Done |
| Compaction | ✅ Done |
| Autovacuum thresholding | ✅ Done |
| Page-level locking | ✅ Done |
| Operation logging with timestamps | ✅ Done |
| Benchmark harness | ✅ Done |