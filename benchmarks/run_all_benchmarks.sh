#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmark_runs"

cd "$ROOT_DIR"
mkdir -p "$BENCH_DIR"

echo "[STEP] RookDB internal FSM/Heap benchmark"
cargo run --bin benchmark_fsm_heap -- --output "$BENCH_DIR/latest_fsm_heap_benchmark.json"

echo "[STEP] SQLite benchmark"
"$ROOT_DIR/benchmarks/run_sqlite_bench.sh" || true

echo "[STEP] MySQL benchmark"
"$ROOT_DIR/benchmarks/run_mysql_bench.sh" || true

echo "[STEP] pgbench benchmark"
"$ROOT_DIR/benchmarks/run_pgbench.sh" || true

echo "[STEP] PostgreSQL FSM compare"
"$ROOT_DIR/benchmarks/run_postgres_fsm_compare.sh" || true

echo "[DONE] Benchmark pipeline complete. Check $BENCH_DIR for outputs."