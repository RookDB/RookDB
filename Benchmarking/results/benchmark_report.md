# RookDB Primary-Key Benchmark (SQLite, DuckDB & Dict Baselines)

## Dataset
- Rows: 50000
- Seed: 42
- Data: controlled synthetic orders with heavy-tail customers, skewed categories, and bursty timestamps

## DuckDB Baseline (Measured)
- Total rows: 50000
- Unique primary keys: 50000
- Search p95 latency: 422.001001 us
- Search p99 latency: 494.492007 us
- Search avg latency: 370.446729 us
- Search throughput: 2699.44 ops/s

## SQLite Baseline (Measured)
- Total rows: 50000
- Unique primary keys: 50000
- Search p95 latency: 8.746996 us
- Search p99 latency: 12.004006 us
- Search avg latency: 8.442346 us
- Search throughput: 118450.49 ops/s

## Python Dict Baseline (Speed of Light limit)
- Unique primary keys: 50000
- Search p95 latency: 0.159998 us
- Search p99 latency: 0.361004 us
- Search avg latency: 0.116527 us
- Search throughput: 8581708.36 ops/s

## Correctness Cross-Verification
- Overall status: PASS
- SQLite miss checks: PASS
- DuckDB miss checks: PASS
- Dict miss checks: PASS
- RookDB algorithms tested on primary key: 9

## B-Tree Hyperparameter Tuning (Degree x Page Size)
- Total combinations evaluated: 12
- Correctness-valid combinations: 12
- Best by index page count:
  degree=256, page_size=8192 bytes, pages=258, size=2113536 bytes
- Best by index file size:
  degree=512, page_size=4096 bytes, pages=394, size=1613824 bytes

## Artifacts
- Benchmarking/results/rookdb_primary_key_metrics.json
- Benchmarking/results/latency_comparison.csv
- Benchmarking/results/scalability_summary.csv
- Benchmarking/results/correctness_verification.json
- Benchmarking/results/charts/search_p95_comparison.png
- Benchmarking/results/charts/rookdb_insert_p95.png
- Benchmarking/results/charts/scalability_search_p95.png

- Benchmarking/results/btree_hyperparameter_tuning.csv
- Benchmarking/results/btree_hyperparameter_tuning.json
## Reference Metrics Context
- Latency percentiles (p50/p95/p99) and throughput are standard service-benchmark metrics.
- Reference: Cooper et al., 2010, YCSB (SoCC).