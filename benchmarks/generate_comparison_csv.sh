#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Ensure Homebrew PostgreSQL tools are reachable on macOS.
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"

OUT_CSV="${1:-benchmark_runs/benchmark_comparison.csv}"
RUN_BENCHMARKS="${RUN_BENCHMARKS:-1}"

mkdir -p benchmark_runs

kv_get() {
  local file="$1"
  local key="$2"
  if [[ -f "$file" ]]; then
    grep -E "^${key}=" "$file" | tail -n1 | cut -d'=' -f2-
  else
    echo "NA"
  fi
}

json_get() {
  local file="$1"
  local key="$2"
  if [[ -f "$file" ]]; then
    grep -E "\"${key}\"" "$file" | head -n1 | sed -E 's/.*: ([^,]+),?$/\1/' | tr -d '"'
  else
    echo "NA"
  fi
}

value_or_na() {
  local value="${1:-}"
  if [[ -z "$value" ]]; then
    echo "NA"
  else
    echo "$value"
  fi
}

if [[ "$RUN_BENCHMARKS" == "1" ]]; then
  ./benchmarks/run_all_benchmarks.sh
fi

rook_json="benchmark_runs/latest_fsm_heap_benchmark.json"
sqlite_txt="benchmark_runs/sqlite_benchmark.txt"
mysql_txt="benchmark_runs/mysql_benchmark.txt"
pg_summary_txt="benchmark_runs/postgres_fsm_summary.txt"
pgbench_txt="benchmark_runs/pgbench_results.txt"

rook_small_tuples="$(json_get "$rook_json" small_tuples)"
rook_large_tuples="$(json_get "$rook_json" large_tuples)"
if [[ "$rook_small_tuples" =~ ^[0-9]+$ && "$rook_large_tuples" =~ ^[0-9]+$ ]]; then
  rook_rows_configured="$((rook_small_tuples + rook_large_tuples))"
else
  rook_rows_configured="NA"
fi

rook_small_tps="$(value_or_na "$(json_get "$rook_json" small_insert_tuples_per_sec)")"
rook_large_tps="$(value_or_na "$(json_get "$rook_json" large_insert_tuples_per_sec)")"
rook_lookup_ops="$(value_or_na "$(json_get "$rook_json" point_lookup_ops_per_sec)")"
rook_scan_tps="$(value_or_na "$(json_get "$rook_json" seq_scan_tuples_per_sec)")"
rook_rebuild_sec="$(value_or_na "$(json_get "$rook_json" fsm_rebuild_seconds)")"

sqlite_rows="$(value_or_na "$(kv_get "$sqlite_txt" rows_configured)")"
sqlite_insert="$(value_or_na "$(kv_get "$sqlite_txt" insert_seconds)")"
sqlite_update="$(value_or_na "$(kv_get "$sqlite_txt" update_seconds)")"
sqlite_delete="$(value_or_na "$(kv_get "$sqlite_txt" delete_seconds)")"
sqlite_after="$(value_or_na "$(kv_get "$sqlite_txt" rows_after_delete)")"
sqlite_avg_payload="$(value_or_na "$(kv_get "$sqlite_txt" avg_payload_len_after_delete)")"

mysql_rows="$(value_or_na "$(kv_get "$mysql_txt" rows_configured)")"
mysql_insert="$(value_or_na "$(kv_get "$mysql_txt" insert_seconds)")"
mysql_update="$(value_or_na "$(kv_get "$mysql_txt" update_seconds)")"
mysql_delete="$(value_or_na "$(kv_get "$mysql_txt" delete_seconds)")"
mysql_after="$(value_or_na "$(kv_get "$mysql_txt" rows_after_delete)")"
mysql_avg_payload="$(value_or_na "$(kv_get "$mysql_txt" avg_payload_len_after_delete)")"

pg_rows="$(value_or_na "$(kv_get "$pg_summary_txt" rows_configured)")"
pg_insert="$(value_or_na "$(kv_get "$pg_summary_txt" insert_seconds)")"
pg_update="$(value_or_na "$(kv_get "$pg_summary_txt" update_seconds)")"
pg_delete="$(value_or_na "$(kv_get "$pg_summary_txt" delete_vacuum_seconds)")"
pg_after="$(value_or_na "$(kv_get "$pg_summary_txt" rows_after_delete)")"
pg_avg_payload="$(value_or_na "$(kv_get "$pg_summary_txt" avg_payload_len_after_delete)")"
pg_avg_fsm_free="$(value_or_na "$(kv_get "$pg_summary_txt" avg_fsm_free_bytes)")"

if [[ -f "$pgbench_txt" ]]; then
  pgbench_tps="$(grep -E "tps = " "$pgbench_txt" | grep -v "including connections establishing" | tail -n1 | awk '{print $3}')"
  pgbench_latency="$(grep -E "latency average = " "$pgbench_txt" | tail -n1 | awk '{print $4}')"
else
  pgbench_tps="NA"
  pgbench_latency="NA"
fi

pgbench_tps="$(value_or_na "$pgbench_tps")"
pgbench_latency="$(value_or_na "$pgbench_latency")"

cat > "$OUT_CSV" <<EOF
engine,rows_configured,insert_seconds,update_seconds,delete_seconds,rows_after_delete,avg_payload_len_after_delete,small_insert_tps,large_insert_tps,lookup_ops_per_sec,seq_scan_tps,fsm_rebuild_seconds,avg_fsm_free_bytes,pgbench_tps,pgbench_latency_ms
rookdb_fsm_heap,${rook_rows_configured},NA,NA,NA,NA,NA,${rook_small_tps},${rook_large_tps},${rook_lookup_ops},${rook_scan_tps},${rook_rebuild_sec},NA,NA,NA
sqlite,${sqlite_rows},${sqlite_insert},${sqlite_update},${sqlite_delete},${sqlite_after},${sqlite_avg_payload},NA,NA,NA,NA,NA,NA,NA,NA
mysql,${mysql_rows},${mysql_insert},${mysql_update},${mysql_delete},${mysql_after},${mysql_avg_payload},NA,NA,NA,NA,NA,NA,NA,NA
postgres_fsm,${pg_rows},${pg_insert},${pg_update},${pg_delete},${pg_after},${pg_avg_payload},NA,NA,NA,NA,NA,${pg_avg_fsm_free},NA,NA
pgbench,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,${pgbench_tps},${pgbench_latency}
EOF

echo "[DONE] Comparison CSV generated at $OUT_CSV"