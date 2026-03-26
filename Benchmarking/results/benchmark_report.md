# RookDB Primary-Key Benchmark (SQLite Baseline)

## Dataset
- Rows: 20000
- Seed: 42
- Data: controlled synthetic orders with heavy-tail customers, skewed categories, and bursty timestamps

## SQLite Baseline (Measured)
- Total rows: 20000
- Unique primary keys: 20000
- Search p95 latency: 6.292015 us
- Search p99 latency: 15.166996 us
- Search avg latency: 5.812285 us
- Search throughput: 172049.37 ops/s

## Correctness Cross-Verification
- Overall status: PASS
- SQLite miss checks: PASS
- RookDB algorithms tested on primary key: 9

## Artifacts
- Benchmarking/results/rookdb_primary_key_metrics.json
- Benchmarking/results/latency_comparison.csv
- Benchmarking/results/scalability_summary.csv
- Benchmarking/results/correctness_verification.json
- Benchmarking/results/charts/search_p95_comparison.png
- Benchmarking/results/charts/rookdb_insert_p95.png
- Benchmarking/results/charts/scalability_search_p95.png

## Reference Metrics Context
- Latency percentiles (p50/p95/p99) and throughput are standard service-benchmark metrics.
- Reference: Cooper et al., 2010, YCSB (SoCC).