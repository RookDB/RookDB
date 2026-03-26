# Benchmark Standards Comparison

## Method
- RookDB values use average p95 latency per workload from this run.
- Existing benchmark standards are represented by normalized latency-index profiles from `Benchmarking/benchmark_standards_baseline.json`.
- Comparison is pattern-oriented baseline matching, not absolute latency equivalence.

## Profiles Compared
- BTree_Engine_Baseline
- LSM_Engine_Baseline

## Workload Mapping
- insert_heavy: YCSB-A/F style write-intensive mix
- read_heavy: YCSB-C style read-mostly
- mixed: YCSB-B style read/update mix
- range_query: Scan-heavy profile analogous to YCSB scan workloads

## RookDB Normalized Workload Shape
- insert_heavy: avg p95 = 13.495 us, normalized index = 6.536
- read_heavy: avg p95 = 2.065 us, normalized index = 1.000
- mixed: avg p95 = 12.583 us, normalized index = 6.095
- range_query: avg p95 = 407.222 us, normalized index = 197.234


## Artifacts
- CSV: Benchmarking/results/standards_comparison.csv
- Graph: Benchmarking/results/charts/standards_latency_baseline_compare.svg

## References
- Cooper et al. (2010), Benchmarking Cloud Serving Systems with YCSB, SoCC.
- YCSB project documentation: https://github.com/brianfrankcooper/YCSB
- Lim et al. (2021), A Comprehensive Evaluation of Key-Value Stores for Cloud Data-Intensive Applications.
