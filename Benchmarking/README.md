# Benchmarking

This folder contains the Python-driven benchmarking pipeline for RookDB index performance testing.

## Workloads

- Insert-heavy workload
- Read-heavy workload
- Mixed workload
- Range query workload

## Baseline Comparison Against Existing Benchmarks

The framework includes a standards comparison layer using YCSB-inspired workload
profiles as normalized baselines.

- Baseline config: `Benchmarking/benchmark_standards_baseline.json`
- Output report: `Benchmarking/results/standards_comparison.md`
- Output graph: `Benchmarking/results/charts/standards_latency_baseline_compare.svg`

This comparison is intentionally **pattern-oriented** (normalized latency index)
instead of absolute latency matching, because hardware/setup differences make
absolute cross-system latency comparisons misleading.

## Independent .dat Correctness Validation

Reusable validator APIs and CLI are included:

- Reusable APIs: `Benchmarking/dat_validator.py`
- CLI validator: `Benchmarking/validate_dat_files.py`

What it validates:

- Header page count consistency vs actual file pages
- Slotted-page lower/upper bounds per page
- Slot directory integrity and tuple boundaries
- Tombstone semantics (`len=0`)

Index behavior validation is also available (all index algorithms):

- Write (insert)
- Read (load/save)
- Search (point lookup)
- Range search (tree indexes, unsupported check for hash indexes)
- Corruption detection (intentionally invalid index file load)

Dummy-data assurance checks are built in:

- `dummy_valid.dat` must pass
- `dummy_corrupt.dat` must fail

## Metrics Collected

- Query latency: min, max, avg, p50, p95, p99
- Logical I/O operations count
- Index size on disk
- Build time measurement

## Architecture

- Rust benchmark runner: `src/bin/index_benchmark.rs`
- Python orchestrator and visualization: `Benchmarking/run_benchmarks.py`

The Python script runs all index algorithms, stores raw data, computes summaries, and generates visualizations.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r Benchmarking/requirements.txt
```

`requirements.txt` is intentionally minimal because chart generation uses pure-Python SVG output.

## Run Benchmarks

```bash
python Benchmarking/run_benchmarks.py --cargo-profile release
```

Optional parameters:

- `--preload <int>`: preloaded rows per benchmark scenario (default 20000)
- `--ops <int>`: operations per workload (default 8000)
- `--range-width <int>`: key width used in range scans (default 64)
- `--seed <int>`: deterministic seed (default 7)
- `--skip-run`: skip Rust execution and only post-process existing raw JSON
- `--validate-dat`: run independent `.dat` validation and dummy-data checks
- `--validate-index`: run index read/write/search/range validation and corruption checks

## Output

Generated under `Benchmarking/results/`:

- `raw_results.json`
- `summary_by_index.csv`
- `summary_by_workload.csv`
- `analysis_report.md`
- `standards_comparison.csv`
- `standards_comparison.md`
- `dat_validation_report.json`
- `index_validation_report.json`
- `charts/*.svg`

## Notes

- Hash indexes do not support ordered range queries and are expected to be skipped in range workload.
- I/O operation count is tracked as a logical metric for benchmarked operations plus save/load operations.
- Generated temporary index snapshots are written to `Benchmarking/results/index_files/` and ignored from version control.

## Run Only .dat Validation

```bash
python Benchmarking/validate_dat_files.py --root database/base --output Benchmarking/results/dat_validation_report.json
```

## Run Only Index Validation

```bash
cargo run --release --bin index_validation
```
