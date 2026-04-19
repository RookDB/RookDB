#!/usr/bin/env bash
set -euo pipefail

if ! command -v psql >/dev/null 2>&1; then
  echo "[SKIP] psql command not found."
  exit 0
fi

ROWS="${BENCH_ROWS:-100000}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-rookbench}"

CSV_OUT="benchmark_runs/postgres_fsm_metrics.csv"
SUMMARY_OUT="benchmark_runs/postgres_fsm_summary.txt"

mkdir -p benchmark_runs

PSQL_ARGS=(
  "-h" "$PGHOST"
  "-p" "$PGPORT"
  "-U" "$PGUSER"
  "-d" "$PGDATABASE"
  "-v" "ON_ERROR_STOP=1"
)

now_s() {
  date +%s
}

elapsed() {
  local start="$1"
  local end="$2"
  echo $((end - start))
}

echo "[INFO] PostgreSQL FSM compare rows=$ROWS db=$PGDATABASE host=$PGHOST:$PGPORT"

t0=$(now_s)
psql "${PSQL_ARGS[@]}" <<SQL
CREATE EXTENSION IF NOT EXISTS pg_freespacemap;
DROP TABLE IF EXISTS rookbench;
CREATE TABLE rookbench (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL
);
SQL
t1=$(now_s)

psql "${PSQL_ARGS[@]}" <<SQL
INSERT INTO rookbench(id, payload)
SELECT gs, repeat('x', 50)
FROM generate_series(1, ${ROWS}) AS gs;
SQL
t2=$(now_s)

psql "${PSQL_ARGS[@]}" <<SQL
UPDATE rookbench
SET payload = payload || payload
WHERE (id % 5) = 0;
SQL
t3=$(now_s)

psql "${PSQL_ARGS[@]}" <<SQL
DELETE FROM rookbench WHERE (id % 10) = 0;
VACUUM ANALYZE rookbench;
SQL
t4=$(now_s)

psql "${PSQL_ARGS[@]}" --csv -c "
SELECT
  blk AS block_number,
  pg_freespace('rookbench'::regclass, blk) AS free_bytes
FROM generate_series(
  0,
  GREATEST((pg_relation_size('rookbench') / 8192) - 1, 0)
) AS blk
" > "$CSV_OUT"

row_count=$(psql "${PSQL_ARGS[@]}" -t -A -c "SELECT COUNT(*) FROM rookbench;")
avg_payload=$(psql "${PSQL_ARGS[@]}" -t -A -c "SELECT ROUND(AVG(length(payload))::numeric, 2) FROM rookbench;")
heap_pages=$(psql "${PSQL_ARGS[@]}" -t -A -c "SELECT pg_relation_size('rookbench') / 8192;")
avg_free=$(psql "${PSQL_ARGS[@]}" -t -A -c "SELECT ROUND(AVG(pg_freespace('rookbench'::regclass, blk))::numeric, 2) FROM generate_series(0, GREATEST((pg_relation_size('rookbench') / 8192) - 1, 0)) AS blk;")

cat > "$SUMMARY_OUT" <<EOF
engine=postgres
rows_configured=$ROWS
create_seconds=$(elapsed "$t0" "$t1")
insert_seconds=$(elapsed "$t1" "$t2")
update_seconds=$(elapsed "$t2" "$t3")
delete_vacuum_seconds=$(elapsed "$t3" "$t4")
rows_after_delete=$row_count
avg_payload_len_after_delete=$avg_payload
heap_pages=$heap_pages
avg_fsm_free_bytes=$avg_free
fsm_metrics_csv=$CSV_OUT
database=$PGDATABASE
host=$PGHOST
port=$PGPORT
EOF

echo "[DONE] PostgreSQL FSM summary -> $SUMMARY_OUT"
echo "[DONE] PostgreSQL FSM per-page metrics -> $CSV_OUT"