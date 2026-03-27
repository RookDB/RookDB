# RookDB Primary-Key Benchmark (SQLite, DuckDB & Dict Baselines)

## Dataset
- Rows: 50000
- Seed: 42
- Data: controlled synthetic orders with heavy-tail customers, skewed categories, and bursty timestamps

## DuckDB Baseline (Measured)
- Total rows: 50000
- Unique primary keys: 50000
- Search p95 latency: 640.814000 us
- Search p99 latency: 910.599000 us
- Search avg latency: 496.775240 us
- Search throughput: 2012.98 ops/s

## SQLite Baseline (Measured)
- Total rows: 50000
- Unique primary keys: 50000
- Search p95 latency: 14.869000 us
- Search p99 latency: 16.772000 us
- Search avg latency: 11.018893 us
- Search throughput: 90753.22 ops/s

## Python Dict Baseline (Speed of Light limit)
- Unique primary keys: 50000
- Search p95 latency: 0.161000 us
- Search p99 latency: 0.411000 us
- Search avg latency: 0.142150 us
- Search throughput: 7034847.08 ops/s

## Correctness Cross-Verification
- Overall status: PASS
- SQLite miss checks: PASS
- DuckDB miss checks: PASS
- Dict miss checks: PASS
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