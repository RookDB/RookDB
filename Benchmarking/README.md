# Benchmarking (Rebuilt)

This benchmark stack uses measured systems only (no hardcoded comparison vectors)
and generates reproducible benchmarking + correctness artifacts.

## Pipeline

1. Generate controlled synthetic data (`synthetic_orders.csv`)
2. Load and benchmark SQLite primary-key index baseline
3. Run RookDB Rust index benchmark on same data
4. Cross-verify correctness between SQLite and RookDB outputs
5. Run scalability sweep across multiple row counts
6. Generate matplotlib charts and report

## Run

From repository root (uv workflow):

```bash
uv python install 3.12
uv venv --python 3.12 .venv-bench
uv pip install --python .venv-bench/bin/python -r Benchmarking/requirements.txt
```

Run benchmark:

```bash
.venv-bench/bin/python Benchmarking/run_benchmarks.py --rows 50000 --seed 42
```

Optional:

```bash
.venv-bench/bin/python Benchmarking/run_benchmarks.py --rows 50000 --seed 42 --scales 10000,30000,50000
```

## Output

- `Benchmarking/data/synthetic_orders.csv`
- `Benchmarking/results/sqlite_baseline.db`
- `Benchmarking/results/rookdb_primary_key_metrics.json`
- `Benchmarking/results/latency_comparison.csv`
- `Benchmarking/results/scalability_summary.csv`
- `Benchmarking/results/correctness_verification.json`
- `Benchmarking/results/benchmark_report.md`
- `Benchmarking/results/charts/search_p95_comparison.png`
- `Benchmarking/results/charts/rookdb_insert_p95.png`
- `Benchmarking/results/charts/scalability_search_p95.png`

## Notes

- No hardcoded comparison JSON vectors are used.
- Benchmarking currently focuses on primary-key index latency and correctness.
- All RookDB index algorithms are tested for primary-key operations.

## Evaluation Coverage

- Correctness: SQLite vs RookDB cross-verification + per-algorithm correctness
- Benchmarking: p50/p95/p99 + throughput
- Testing: multi-scale runs via `--scales`
- Robustness: miss-key checks and strict subprocess failure handling
- Documentation quality: generated markdown + chart artifacts

## Research Metric Context

The benchmark emphasizes latency percentiles and throughput, aligned with
common service-benchmark reporting practice (for example YCSB-style metrics).

Reference:

- Cooper et al. (2010), Benchmarking Cloud Serving Systems with YCSB.
