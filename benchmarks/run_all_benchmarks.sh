#!/usr/bin/env bash
set -euo pipefail

mkdir -p benchmark_runs

echo "[STEP] RookDB internal FSM/Heap benchmark"
cargo run --bin benchmark_fsm_heap -- --output benchmark_runs/latest_fsm_heap_benchmark.json

echo "[STEP] SQLite benchmark"
./benchmarks/run_sqlite_bench.sh || true

echo "[STEP] MySQL benchmark"
./benchmarks/run_mysql_bench.sh || true

echo "[STEP] pgbench benchmark"
./benchmarks/run_pgbench.sh || true

echo "[STEP] PostgreSQL FSM compare"
./benchmarks/run_postgres_fsm_compare.sh || true

echo "[DONE] Benchmark pipeline complete. Check benchmark_runs/ for outputs."