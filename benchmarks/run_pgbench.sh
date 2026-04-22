#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmark_runs"

if ! command -v pgbench >/dev/null 2>&1; then
  echo "[SKIP] pgbench command not found."
  exit 0
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "[SKIP] psql command not found."
  exit 0
fi

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-rookbench}"

PGBENCH_SCALE="${PGBENCH_SCALE:-10}"
PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-8}"
PGBENCH_JOBS="${PGBENCH_JOBS:-4}"
PGBENCH_TIME="${PGBENCH_TIME:-30}"

OUT_FILE="$BENCH_DIR/pgbench_results.txt"
SCRIPT_FILE="$ROOT_DIR/benchmarks/sql/pgbench_custom.sql"

mkdir -p "$BENCH_DIR"

PSQL_ARGS=(
  "-h" "$PGHOST"
  "-p" "$PGPORT"
  "-U" "$PGUSER"
  "-d" "$PGDATABASE"
  "-v" "ON_ERROR_STOP=1"
  "-t" "-A"
)

echo "[INFO] pgbench initialize: db=$PGDATABASE host=$PGHOST:$PGPORT scale=$PGBENCH_SCALE"
pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -i -s "$PGBENCH_SCALE" "$PGDATABASE" >/tmp/pgbench_init.out 2>/tmp/pgbench_init.err

max_aid=$(psql "${PSQL_ARGS[@]}" -c "SELECT COALESCE(MAX(aid), 1) FROM pgbench_accounts;")

echo "[INFO] pgbench run: clients=$PGBENCH_CLIENTS jobs=$PGBENCH_JOBS seconds=$PGBENCH_TIME"
pgbench -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -n \
  -c "$PGBENCH_CLIENTS" -j "$PGBENCH_JOBS" -T "$PGBENCH_TIME" \
  -D "max_aid=$max_aid" \
  -f "$SCRIPT_FILE" \
  > "$OUT_FILE"

echo "[DONE] pgbench results -> $OUT_FILE"