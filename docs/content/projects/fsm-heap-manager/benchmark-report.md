---
title: Benchmark Report
sidebar_position: 5
---

# Benchmark Report

This page covers benchmark scripts, outputs, latest results, interpretation, and caveats.

## Benchmark Scope

The benchmark set includes:

1. RookDB native FSM and heap benchmark
2. SQLite comparison script
3. MySQL comparison script
4. PostgreSQL comparisons using pgbench and free-space map data

## How to Run

Full suite and comparison generation:

```bash
./benchmarks/generate_comparison_csv.sh
```

Run full suite only:

```bash
./benchmarks/run_all_benchmarks.sh
```

Native RookDB benchmark:

```bash
cargo run --bin benchmark_fsm_heap
```

## Output Artifacts

Canonical output directory:

- benchmark_runs/latest_fsm_heap_benchmark.json
- benchmark_runs/benchmark_history.csv
- benchmark_runs/benchmark_history.jsonl
- benchmark_runs/benchmark_comparison.csv
- benchmark_runs/sqlite_benchmark.txt
- benchmark_runs/mysql_benchmark.txt
- benchmark_runs/pgbench_results.txt
- benchmark_runs/postgres_fsm_metrics.csv
- benchmark_runs/postgres_fsm_summary.txt

## Latest Result Highlights

From latest_fsm_heap_benchmark.json:

- Strong small and large insert throughput
- High lookup and sequential scan rates
- Very low FSM rebuild time
- Correctness checks remain green, including scan count parity and oversized tuple rejection

## Analysis Summary

- Space utilization is dense across active heap pages.
- Insert throughput trend improved across recent history entries.
- FSM rebuild remains fast enough for practical recovery-on-open behavior.
- Cross-engine table is useful for qualitative context, not strict workload parity.

## Method Caveats

- Single-process RookDB benchmark path
- Cache and host-load sensitivity
- External script timing granularity
- Prefer repeated runs and median reporting for formal comparison

## Conclusion

Current benchmark evidence supports that the FSM-backed heap manager is behaving as intended for performance, recoverability, and correctness under the benchmark workloads.
