# RookDB Benchmark Report

This report documents the benchmark scripts, how to run them, the outputs they generate, and what the latest results mean for the FSM/heap manager implementation.

## Scope

The benchmark set measures four things:

1. RookDB's own FSM-backed heap manager performance.
2. A SQLite comparison workload.
3. A MySQL comparison workload.
4. PostgreSQL comparison workloads using `pgbench` and `pg_freespacemap`.

The benchmark outputs are stored in the repository-root `benchmark_runs/` directory. Intermediate scratch files are not meant to be committed.

## How To Run The Benchmarks

### Full Suite

Run every benchmark and refresh the comparison CSV:

```bash
./benchmarks/generate_comparison_csv.sh
```

This will, by default, run the full suite first and then generate `benchmark_runs/benchmark_comparison.csv`.

### Run Everything Without Rebuilding the CSV

```bash
./benchmarks/run_all_benchmarks.sh
```

### Aggregate Existing Outputs Only

```bash
RUN_BENCHMARKS=0 ./benchmarks/generate_comparison_csv.sh
```

### Individual Scripts

```bash
./benchmarks/run_sqlite_bench.sh
./benchmarks/run_mysql_bench.sh
./benchmarks/run_pgbench.sh
./benchmarks/run_postgres_fsm_compare.sh
```

### RookDB Native Benchmark Only

```bash
cargo run --bin benchmark_fsm_heap
```

You can also direct the JSON output explicitly:

```bash
cargo run --bin benchmark_fsm_heap -- --output benchmark_runs/latest_fsm_heap_benchmark.json
```

## Script Summary

| Script | Purpose | Main Output |
| --- | --- | --- |
| `benchmarks/run_all_benchmarks.sh` | Runs the full benchmark suite | Multiple files in `benchmark_runs/` |
| `benchmarks/generate_comparison_csv.sh` | Aggregates all result files into one comparison table | `benchmark_runs/benchmark_comparison.csv` |
| `benchmarks/run_sqlite_bench.sh` | Creates, updates, deletes, and reports SQLite metrics | `benchmark_runs/sqlite_benchmark.txt` |
| `benchmarks/run_mysql_bench.sh` | Creates, updates, deletes, and reports MySQL metrics | `benchmark_runs/mysql_benchmark.txt` |
| `benchmarks/run_pgbench.sh` | Runs `pgbench` against PostgreSQL | `benchmark_runs/pgbench_results.txt` |
| `benchmarks/run_postgres_fsm_compare.sh` | Measures PostgreSQL free-space behavior | `benchmark_runs/postgres_fsm_metrics.csv`, `benchmark_runs/postgres_fsm_summary.txt` |
| `cargo run --bin benchmark_fsm_heap` | Benchmarks the RookDB FSM/heap stack | `benchmark_runs/latest_fsm_heap_benchmark.json` |

## Required Environment

### Shared

- Rust toolchain and Cargo
- Repository root as the working directory when invoking the scripts
- A writable `benchmark_runs/` directory

### SQLite

- `sqlite3` available on `PATH`

### MySQL

- `mysql` client available on `PATH`
- A reachable MySQL server
- Optional environment variables:
	- `MYSQL_HOST`
	- `MYSQL_PORT`
	- `MYSQL_USER`
	- `MYSQL_PASSWORD`
	- `MYSQL_DATABASE`

### PostgreSQL / pgbench

- `psql` and `pgbench` available on `PATH`
- A reachable PostgreSQL server
- Optional environment variables:
	- `PGHOST`
	- `PGPORT`
	- `PGUSER`
	- `PGPASSWORD`
	- `PGDATABASE`
	- `PGBENCH_SCALE`
	- `PGBENCH_CLIENTS`
	- `PGBENCH_JOBS`
	- `PGBENCH_TIME`

## Output Files

The canonical benchmark output set is:

- `benchmark_runs/latest_fsm_heap_benchmark.json`
- `benchmark_runs/benchmark_history.csv`
- `benchmark_runs/benchmark_history.jsonl`
- `benchmark_runs/benchmark_comparison.csv`
- `benchmark_runs/sqlite_benchmark.txt`
- `benchmark_runs/mysql_benchmark.txt`
- `benchmark_runs/pgbench_results.txt`
- `benchmark_runs/postgres_fsm_metrics.csv`
- `benchmark_runs/postgres_fsm_summary.txt`
- `benchmark_runs/initial_phase_results.json`

Temporary SQLite files such as `sqlite_bench.db`, `sqlite_bench.db-wal`, and `sqlite_bench.db-shm` are intermediate artifacts and are intentionally not part of the final benchmark deliverables.

## Latest RookDB Benchmark Results

Source: `benchmark_runs/latest_fsm_heap_benchmark.json`

| Metric | Value |
| --- | ---: |
| Run ID | 1776859982 |
| Small inserts | 20000 |
| Large inserts | 1000 |
| Lookup samples | 1000 |
| Inserted total | 21000 |
| Scanned total | 21000 |
| Small insert TPS | 21291.0861 |
| Large insert TPS | 16930.2971 |
| Point lookup OPS | 515331.1002 |
| Sequential scan TPS | 2167611.4872 |
| FSM rebuild seconds | 0.005375 |
| Heap pages | 269 |
| FSM pages | 3 |
| Pages used with tuples | 268 |
| Avg tuples per used page | 78.36 |
| Avg free bytes on used pages | 94.45 |
| Oversized tuple rejected | true |
| FSM rebuild search found page | true |

## Cross-Database Comparison

Source: `benchmark_runs/benchmark_comparison.csv`

| Engine | Rows Configured | Insert sec | Update sec | Delete sec | Rows After Delete | Avg Payload Len | Small TPS | Large TPS | Lookup OPS | Scan TPS | FSM Rebuild sec | Avg FSM Free Bytes | pgbench TPS | pgbench Latency ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| rookdb_fsm_heap | 21000 | NA | NA | NA | NA | NA | 21291.09 | 16930.30 | 515331.10 | 2167611.49 | 0.005375 | NA | NA | NA |
| sqlite | 100000 | 0 | 1 | 0 | 90000 | 55.56 | NA | NA | NA | NA | NA | NA | NA | NA |
| mysql | 100000 | 1 | 0 | 0 | 90000 | 55.56 | NA | NA | NA | NA | NA | NA | NA | NA |
| postgres_fsm | 1000 | 0 | 0 | 0 | 900 | 55.56 | NA | NA | NA | NA | NA | 2269.71 | NA | NA |
| pgbench | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | NA | 3867.890925 | 2.068 |

## Analysis

### Storage Utilization

The latest RookDB run reports `269` heap pages and only `3` FSM pages. Of those heap pages, `268` hold tuples. The average free space on used pages is `94.45` bytes, which means the heap is packed extremely tightly. On an 8192-byte page, that leaves roughly `1.15%` free space and implies about `98.85%` average utilization on active pages.

This is the expected effect of page-level free-space selection: the FSM keeps inserts routed to pages that still have room, and the heap manager reuses available slot space before extending the table.

### Insert Performance

The latest RookDB run shows:

- Small insert throughput: `21291.09` TPS
- Large insert throughput: `16930.30` TPS

Compared with the earlier history point at run `1776596962`, small inserts improved from `18852.96` TPS to `21291.09` TPS, and large inserts improved from `13036.05` TPS to `16930.30` TPS. That is a meaningful gain for both tuple sizes, especially for larger payloads.

Interpretation:

- The FSM search path is finding viable pages efficiently.
- Slot reuse and header/page update logic are not introducing a visible bottleneck.
- Large tuple handling is scaling better than earlier runs, which suggests the current page reuse and allocation path is stable.

### Read Performance

The latest run reports:

- Point lookup throughput: `515331.10` OPS
- Sequential scan throughput: `2167611.49` TPS

These are very strong numbers, but they should be read carefully. This benchmark is single-process and can be sensitive to page cache warmth, so lookup and scan values should be treated as best-case operational performance unless you run repeated cold/warm trials.

Still, the trend is positive: point lookups and scans remain correct and fast while the storage layer is under sustained insert pressure.

### Recovery and Robustness

The FSM rebuild time in the latest run is `0.005375` seconds. That is fast enough to support rebuild-on-open behavior without turning recovery into a user-visible penalty.

The benchmark also confirms that:

- oversized tuples are rejected correctly
- `scanned_total == inserted_total`
- rebuilding the FSM still finds a valid page

This means the storage layer remains correct while the benchmarked performance improves.

### Cross-Engine Context

The comparison CSV is useful as an engineering snapshot, not a perfectly identical workload comparison.

- RookDB reports storage-engine metrics like TPS, OPS, and FSM rebuild time.
- SQLite and MySQL report operation durations and post-delete state.
- PostgreSQL FSM metrics report average free bytes.
- `pgbench` reports throughput and latency for a synthetic transactional workload.

Because the workloads are not identical, the table should be read as a qualitative comparison set rather than a strict apples-to-apples TPC-style benchmark.

## Trend Analysis

Recent RookDB runs in `benchmark_history.csv` show a clear progression:

| Run ID | Small TPS | Large TPS | Lookup OPS | Scan TPS | Rebuild sec |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1776596962 | 18852.9578 | 13036.0534 | 43332.4078 | 47035.3673 | 0.009143 |
| 1776620140 | 21265.6556 | 16456.1535 | 77746.6411 | 2166632.8431 | 0.006503 |
| 1776859982 | 21291.0861 | 16930.2971 | 515331.1002 | 2167611.4872 | 0.005375 |

Read this as:

- insert throughput improved and then stabilized at a stronger level
- rebuild time kept decreasing
- read metrics remained correct and became much faster in the latest runs

## Benchmark Method Notes

- The benchmark suite is single-process for the RookDB path.
- Results can vary with OS cache state and background system load.
- External engine scripts are shell-based and use elapsed second timing.
- For submission-quality reporting, repeated runs and median values are preferable.

## Script Inputs And Outputs

### RookDB Native Benchmark

Command:

```bash
cargo run --bin benchmark_fsm_heap
```

Main outputs:

- `benchmark_runs/latest_fsm_heap_benchmark.json`
- `benchmark_runs/benchmark_history.csv`
- `benchmark_runs/benchmark_history.jsonl`

### Comparison Aggregation

Command:

```bash
./benchmarks/generate_comparison_csv.sh
```

Main output:

- `benchmark_runs/benchmark_comparison.csv`

### SQLite, MySQL, pgbench, PostgreSQL FSM

Commands:

```bash
./benchmarks/run_sqlite_bench.sh
./benchmarks/run_mysql_bench.sh
./benchmarks/run_pgbench.sh
./benchmarks/run_postgres_fsm_compare.sh
```

Main outputs:

- `benchmark_runs/sqlite_benchmark.txt`
- `benchmark_runs/mysql_benchmark.txt`
- `benchmark_runs/pgbench_results.txt`
- `benchmark_runs/postgres_fsm_metrics.csv`
- `benchmark_runs/postgres_fsm_summary.txt`

## Cleanup Policy

Intermediate files are removed after the benchmark summary is produced. Examples include:

- transient SQLite database files and WAL/SHM sidecars
- temporary `fsm_heap_bench_*.dat.fsm` files used during benchmark runs

Final result files remain in `benchmark_runs/` so the documentation and comparison CSV can refer to them directly.

## Conclusion

The benchmark set shows that RookDB's FSM-backed heap manager is behaving as intended:

- heap pages are densely utilized
- insert throughput is strong for both small and large tuples
- point lookups and scans are fast
- FSM rebuild is cheap enough to support recovery
- correctness checks stay green throughout the run

This makes the current documentation package complete for the benchmark portion of the submission requirements.

