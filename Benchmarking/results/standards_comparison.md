# Reference Systems Comparison

## Method
- Comparison engine: `Benchmarking/standards_compare_engine.py`
- Reference systems are measured, not hardcoded:
  - SQLite (B-tree)
  - SortedContainers (tree-like ordered structure)
  - Python dict (hash-like structure)
- Similarity metrics are computed on consistently normalized vectors.
- Raw p95 values are also reported to preserve absolute performance meaning.

## Caveat
- This is still a lightweight proxy benchmark, not a full cross-DBMS publication-grade study.

## RookDB Workload Summary
- insert_heavy: avg p95=13.495 us, std=28.004 us
- read_heavy: avg p95=2.065 us, std=4.722 us
- mixed: avg p95=12.583 us, std=27.372 us
- range_query: avg p95=733.000 us, std=943.637 us

## Similarity Scores (Normalized)
- Python dict (hash-like): cosine=0.9047, rmse=84.7418, mae=73.9369
- SortedContainers (tree-like): cosine=0.4545, rmse=2925.9733, mae=1508.7759
- SQLite (B-tree): cosine=0.2962, rmse=2926.8551, mae=1503.4334

## Artifacts
- CSV: Benchmarking/results/standards_comparison.csv
- Normalized graph: Benchmarking/results/charts/standards_latency_baseline_compare.svg
- Raw graph: Benchmarking/results/charts/standards_raw_p95_by_workload.svg

## References
- Cooper et al. (2010), Benchmarking Cloud Serving Systems with YCSB, SoCC.
- YCSB project documentation: https://github.com/brianfrankcooper/YCSB
- SQLite documentation: https://www.sqlite.org/docs.html
- SortedContainers documentation: https://grantjenks.com/docs/sortedcontainers/
