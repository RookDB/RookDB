#!/usr/bin/env bash
set -euo pipefail

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "[SKIP] sqlite3 command not found."
  exit 0
fi

ROWS="${BENCH_ROWS:-100000}"
DB_FILE="${SQLITE_DB_FILE:-benchmark_runs/sqlite_bench.db}"
OUT_FILE="benchmark_runs/sqlite_benchmark.txt"

mkdir -p benchmark_runs
rm -f "$DB_FILE"

now_s() {
  date +%s
}

elapsed() {
  local start="$1"
  local end="$2"
  echo $((end - start))
}

run_sql() {
  local sql="$1"
  sqlite3 "$DB_FILE" "$sql" >/dev/null
}

echo "[INFO] SQLite benchmark rows=$ROWS db=$DB_FILE"

t0=$(now_s)
run_sql "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"
t1=$(now_s)

run_sql "DROP TABLE IF EXISTS rookbench;"
run_sql "CREATE TABLE rookbench(id INTEGER PRIMARY KEY, payload TEXT NOT NULL);"
t2=$(now_s)

run_sql "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < $ROWS) INSERT INTO rookbench(id, payload) SELECT x, substr('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 1, 50) FROM seq;"
t3=$(now_s)

run_sql "UPDATE rookbench SET payload = payload || payload WHERE (id % 5) = 0;"
t4=$(now_s)

run_sql "DELETE FROM rookbench WHERE (id % 10) = 0;"
t5=$(now_s)

count_after=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM rookbench;")
avg_len=$(sqlite3 "$DB_FILE" "SELECT ROUND(AVG(length(payload)), 2) FROM rookbench;")

cat > "$OUT_FILE" <<EOF
engine=sqlite
rows_configured=$ROWS
setup_seconds=$(elapsed "$t0" "$t1")
create_seconds=$(elapsed "$t1" "$t2")
insert_seconds=$(elapsed "$t2" "$t3")
update_seconds=$(elapsed "$t3" "$t4")
delete_seconds=$(elapsed "$t4" "$t5")
rows_after_delete=$count_after
avg_payload_len_after_delete=$avg_len
database_file=$DB_FILE
EOF

echo "[DONE] SQLite benchmark summary -> $OUT_FILE"