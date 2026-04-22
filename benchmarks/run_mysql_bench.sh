#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmark_runs"

if ! command -v mysql >/dev/null 2>&1; then
  echo "[SKIP] mysql command not found."
  exit 0
fi

ROWS="${BENCH_ROWS:-100000}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-rookbench}"
OUT_FILE="$BENCH_DIR/mysql_benchmark.txt"

mkdir -p "$BENCH_DIR"

MYSQL_ARGS=(
  "-h${MYSQL_HOST}"
  "-P${MYSQL_PORT}"
  "-u${MYSQL_USER}"
  "--protocol=tcp"
  "--batch"
  "--skip-column-names"
)

if [[ -n "$MYSQL_PASSWORD" ]]; then
  MYSQL_ARGS+=("-p${MYSQL_PASSWORD}")
fi

now_s() {
  date +%s
}

elapsed() {
  local start="$1"
  local end="$2"
  echo $((end - start))
}

run_mysql() {
  local sql="$1"
  mysql "${MYSQL_ARGS[@]}" -e "$sql"
}

echo "[INFO] MySQL benchmark rows=$ROWS db=$MYSQL_DATABASE host=$MYSQL_HOST:$MYSQL_PORT"

t0=$(now_s)
run_mysql "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE};"
t1=$(now_s)

run_mysql "USE ${MYSQL_DATABASE}; DROP TABLE IF EXISTS rookbench; CREATE TABLE rookbench(id INT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB;"
t2=$(now_s)

run_mysql "USE ${MYSQL_DATABASE}; SET SESSION cte_max_recursion_depth=1000000; INSERT INTO rookbench(id, payload) WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < ${ROWS}) SELECT n, RPAD('x', 50, 'x') FROM seq;"
t3=$(now_s)

run_mysql "USE ${MYSQL_DATABASE}; UPDATE rookbench SET payload = CONCAT(payload, payload) WHERE MOD(id, 5) = 0;"
t4=$(now_s)

run_mysql "USE ${MYSQL_DATABASE}; DELETE FROM rookbench WHERE MOD(id, 10) = 0;"
t5=$(now_s)

count_after=$(mysql "${MYSQL_ARGS[@]}" -e "USE ${MYSQL_DATABASE}; SELECT COUNT(*) FROM rookbench;")
avg_len=$(mysql "${MYSQL_ARGS[@]}" -e "USE ${MYSQL_DATABASE}; SELECT ROUND(AVG(CHAR_LENGTH(payload)), 2) FROM rookbench;")

cat > "$OUT_FILE" <<EOF
engine=mysql
rows_configured=$ROWS
setup_seconds=$(elapsed "$t0" "$t1")
create_seconds=$(elapsed "$t1" "$t2")
insert_seconds=$(elapsed "$t2" "$t3")
update_seconds=$(elapsed "$t3" "$t4")
delete_seconds=$(elapsed "$t4" "$t5")
rows_after_delete=$count_after
avg_payload_len_after_delete=$avg_len
database=${MYSQL_DATABASE}
host=${MYSQL_HOST}
port=${MYSQL_PORT}
EOF

echo "[DONE] MySQL benchmark summary -> $OUT_FILE"