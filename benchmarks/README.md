# Cross-DB Benchmark Kit

This folder contains practical benchmark scripts to compare RookDB with:

- SQLite
- MySQL
- PostgreSQL `pgbench`
- PostgreSQL free-space map metrics (`pg_freespacemap` extension)

All scripts write outputs in `benchmark_runs/`.

## Quick Start

From repository root:

```bash
./benchmarks/run_all_benchmarks.sh
```

Generate a single comparison CSV (and run benchmarks first by default):

```bash
./benchmarks/generate_comparison_csv.sh
```

To only aggregate existing outputs without re-running benchmarks:

```bash
RUN_BENCHMARKS=0 ./benchmarks/generate_comparison_csv.sh
```

This runs:

1. RookDB FSM/Heap benchmark (`cargo run --bin benchmark_fsm_heap`)
2. SQLite benchmark
3. MySQL benchmark (if `mysql` client is available)
4. `pgbench` benchmark (if `pgbench` is available)
5. PostgreSQL FSM comparison (if `psql` is available)

## Individual Runs

```bash
./benchmarks/run_sqlite_bench.sh
./benchmarks/run_mysql_bench.sh
./benchmarks/run_pgbench.sh
./benchmarks/run_postgres_fsm_compare.sh
```

## Environment Variables

### Shared

- `BENCH_ROWS` default: `100000`

### MySQL

- `MYSQL_HOST` default: `127.0.0.1`
- `MYSQL_PORT` default: `3306`
- `MYSQL_USER` default: `root`
- `MYSQL_PASSWORD` default: empty
- `MYSQL_DATABASE` default: `rookbench`

### PostgreSQL (`pgbench` + FSM compare)

- `PGHOST` default: `127.0.0.1`
- `PGPORT` default: `5432`
- `PGUSER` default: `postgres`
- `PGPASSWORD` optional
- `PGDATABASE` default: `rookbench`

### `pgbench` settings

- `PGBENCH_SCALE` default: `10`
- `PGBENCH_CLIENTS` default: `8`
- `PGBENCH_JOBS` default: `4`
- `PGBENCH_TIME` default: `30`

## Output Files

- `benchmark_runs/sqlite_benchmark.txt`
- `benchmark_runs/mysql_benchmark.txt`
- `benchmark_runs/pgbench_results.txt`
- `benchmark_runs/postgres_fsm_metrics.csv`
- `benchmark_runs/postgres_fsm_summary.txt`
- `benchmark_runs/latest_fsm_heap_benchmark.json`
- `benchmark_runs/benchmark_comparison.csv`

## Notes

- MySQL and PostgreSQL scripts assume a running server and valid credentials.
- PostgreSQL FSM metrics require extension `pg_freespacemap`.
- The scripts are intentionally shell-only and dependency-light.