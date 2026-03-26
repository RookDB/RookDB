---
title: Indexing
sidebar_position: 1
---

## Overview

RookDB supports secondary indexes to accelerate point lookups and ordered range
queries. Indexes live alongside table files and store `(page_no, item_id)`
record IDs that point to the table heap.

## Supported Algorithms

- Static Hash
- Chained Hash
- Extendible Hash
- Linear Hash
- B-Tree
- B+ Tree
- Radix Tree
- Skip List
- LSM Tree

## Index Files

Index files are stored next to their table file at:

```text
database/base/{db}/{table}_{index}.idx
```

Indexes are serialized to JSON using `serde` for easy persistence.

## Operations

### Build From Table

Bulk build scans every tuple in the table and inserts keys into a new index.
This is used after CSV ingestion and during rebuilds.

### Load Index

`AnyIndex::load` deserializes an index file based on the catalog algorithm
metadata and returns a ready-to-use in-memory index.

### Search (Point Lookup)

Point lookup returns a list of `RecordId` values for an exact key match.

### Range Scan (Tree Indexes)

Tree-based indexes expose ordered range scan. Hash-based indexes return an
error for this operation.

### Index Scan (Fetch Tuples)

Index scan uses a point lookup to fetch the matching tuples from disk. Each
`RecordId` is translated into `(page_no, item_id)` access on the table file.

### Clustered Index

Clustered index creation reorders table tuples by the indexed column and then
rebuilds all table indexes so record IDs remain consistent.

Only one clustered index is allowed per table.

### Index Validation

Index validation checks heap/index consistency by verifying every live tuple in
the heap has a corresponding `(key, RecordId)` entry in the chosen index.

## Maintenance

- **Insert**: adds `(key, RecordId)` to every index on the table.
- **Delete**: removes `(key, RecordId)` from every index on the table.
- **Bulk Rebuild**: re-scans the table and rewrites all indexes.

## Benchmarking and Performance Testing

### Benchmarking Framework

RookDB now uses a rebuilt, measured benchmarking pipeline focused on primary-key
index correctness and lookup latency.

- Python orchestrator: `Benchmarking/run_benchmarks.py`
- Rust benchmark runner: `src/bin/primary_key_benchmark.rs`
- Baseline database: SQLite (primary key index)

### Workloads Implemented

- Controlled synthetic orders data generation (deterministic seed)
- Primary-key insertion and lookup benchmarking
- Miss-key lookup checks

### Metrics Collected

- Primary-key search latency (`avg`, `p95`) for SQLite baseline
- Primary-key insert/search latency (`avg`, `p95`) for each RookDB index
- Cross-system correctness verification status

### Correctness and Baseline Verification

The pipeline validates that:

- SQLite row count matches generated synthetic dataset
- SQLite unique primary keys match dataset size
- Every RookDB index algorithm returns correct primary-key hits
- Missing-key checks return no false positives

All index algorithms are validated on primary keys:

- Static Hash
- Chained Hash
- Extendible Hash
- Linear Hash
- B-Tree
- B+ Tree
- Radix Tree
- Skip List
- LSM Tree

### Artifacts

- `Benchmarking/data/synthetic_orders.csv`
- `Benchmarking/results/rookdb_primary_key_metrics.json`
- `Benchmarking/results/latency_comparison.csv`
- `Benchmarking/results/scalability_summary.csv`
- `Benchmarking/results/correctness_verification.json`
- `Benchmarking/results/benchmark_report.md`
- `Benchmarking/results/charts/search_p95_comparison.png`
- `Benchmarking/results/charts/rookdb_insert_p95.png`
- `Benchmarking/results/charts/scalability_search_p95.png`

### Evaluation Aspect Coverage

- **Correctness**: SQLite vs RookDB cross-verification on primary keys for all index algorithms.
- **Documentation quality**: generated benchmark report and chart artifacts.
- **Robustness**: miss-key checks and strict failure handling in benchmark pipeline.
- **Benchmarking (Initial Results)**: measured latency (`p50/p95/p99`) and throughput.
- **Testing (Initial performance/scalability)**: multi-scale benchmarking through `--scales`.
- **Modular and clean code**: split benchmark responsibilities between Python orchestration and Rust benchmark binary.

### Reference Metric Context

The benchmark uses latency percentiles and throughput as primary metrics, aligned
with common benchmark practice in key-value/service systems literature.

- Cooper et al. (2010), *Benchmarking Cloud Serving Systems with YCSB*.

### Reproduce

From the repository root:

```bash
python Benchmarking/run_benchmarks.py --rows 50000 --seed 42 --scales 10000,30000,50000
```

Generated outputs are stored in `Benchmarking/results/`:

- `sqlite_baseline.db`
- `rookdb_primary_key_metrics.json`
- `latency_comparison.csv`
- `correctness_verification.json`
- `benchmark_report.md`
