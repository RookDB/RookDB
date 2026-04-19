# User Guide

## Show Databases

Displays all databases available in the catalog.

---

## Create Database

Creates a new database and updates the catalog.

Steps:
1. Enter a database name when prompted

Example:
```
users
```

---

## Select Database

Sets the active database for performing operations.

Steps:
1. Enter a database name from the displayed list

---

## Show Tables

Displays all tables in the selected database.

---

## Create Table

Creates a new table with a schema.

Steps:
1. Enter table name
2. Enter columns using format:

```
column_name:data_type
```

3. Press Enter on an empty line to finish

Supported Types:
- INT
- TEXT

Example:
```
id:INT
name:TEXT
```

---

## Load CSV

Loads CSV data into an existing table.

Steps:
1. Enter table name
2. Enter CSV file path

Example:
```
examples/example.csv
```

---

## Show Tuples

Displays tuples stored in table pages along with page metadata such as pointers and tuple count.

---

## Show Table Statistics

Displays storage statistics like total number of pages.

---

## Exit

Exit from RookDB.

---

## Frontend / CLI Changes

The CLI now exposes a stable flow for working with Heap + FSM-backed table storage:

1. Create/select database.
2. Create table with supported schema types (`INT`, `TEXT`).
3. Load data (`LOAD CSV`) into heap pages.
4. Show tuples (sequential scan path).
5. Show table statistics (page-level state).

These commands route into backend APIs that update both heap pages and FSM metadata.

## Benchmark Commands

### Internal FSM + Heap Benchmark

Run from repo root:

```bash
cargo run --release --bin benchmark_fsm_heap
```

Outputs are written under `benchmark_runs/` including history and latest JSON snapshots.

### Cross-Database Benchmark Suite

Run from repo root:

```bash
./benchmarks/run_all_benchmarks.sh
```

This orchestrates SQLite, MySQL, PostgreSQL (`pgbench`), and PostgreSQL FSM metrics collection and writes a combined report to `benchmark_runs/benchmark_comparison.csv`.