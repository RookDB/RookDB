# RookDB Benchmarking Initial Results

## Run Configuration
- Seed: 7
- Preload rows per case: 20000
- Operations per workload: 8000
- Range width: 64
- Repeats: 1
- Total benchmark scenarios: 36

## Workloads Implemented
- Insert-heavy workload
- Read-heavy workload
- Mixed workload
- Range query workload

## Metrics Implemented
- Query latency: min, max, avg, p50, p95, p99
- Logical operations count (internal metric)
- Persisted index size on disk
- Index build time measurement

## Initial Findings
- Best p95 for insert-heavy: **Skip List** at **0.209 us**
- Best p95 for read-heavy: **Skip List** at **0.167 us**
- Best p95 for mixed: **Skip List** at **0.209 us**
- Best p95 for range-query: **Skip List** at **0.167 us**

Range query was skipped for these hash indexes (expected): Chained Hash, Extendible Hash, Linear Hash, Static Hash.

## Generated Artifacts
- Raw benchmark data: Benchmarking/results/raw_results.json
- Summary by index: Benchmarking/results/summary_by_index.csv
- Summary by workload: Benchmarking/results/summary_by_workload.csv
- Charts:
    - Benchmarking/results/charts/latency_p95_by_workload.svg
    - Benchmarking/results/charts/build_time_ms_by_index.svg
    - Benchmarking/results/charts/index_size_bytes_by_index.svg
    - Benchmarking/results/charts/logical_io_ops_by_workload.svg

## Notes and Assumptions
- Logical operations count is an internal benchmark metric: number of benchmarked index operations plus save/load operations per scenario.
- Hash indexes do not support ordered range scans and are marked as skipped for range workload.
- This phase provides initial results; larger-scale runs can be produced by increasing --preload and --ops.
