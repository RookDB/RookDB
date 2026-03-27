# Selection Operator — Test Suite

This directory contains the complete test infrastructure for the **Selection Operator** of RookDB.  
It is organized into three independent components, each runnable in isolation.

---

## Directory Structure

```
tests/test_selection/
├── benchmark/           Standalone benchmark binary crate
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── cli/                 Interactive & automated test CLI binary crate
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── common/              Shared helpers (tuple builder, accessor)
├── functional/          Cargo-integrated unit tests (cargo test)
└── README.md            This file
```

---

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable, 1.70+)
- The `rookdb` root crate must be present at `../../../..` relative to either binary crate (it is — no extra steps needed)

---

## 1 · CLI — Interactive & Automated Tests

The CLI supports two modes of operation:

| Mode | Description |
|------|-------------|
| `0`  | Interactive query mode — enter predicates manually |
| `1`  | Automated test suite — runs all registered test cases |

### Navigate to the CLI folder

```bash
cd "tests/test_selection/cli"
```

> **Full path example**
> ```bash
> cd "/home/surjit/Desktop/data system/submisssion/4-phase 2 submission(27.02.2027)/test/test1/rookdb/tests/test_selection/cli"
> ```

### Build & run

```bash
cargo run
```

You will be prompted to select a mode:

```
Select mode:
  0 → Interactive query mode
  1 → Run automated tests

Enter mode (0 or 1):
```

### Interactive mode (mode 0)

Type SQL-style predicates at the prompt. The executor evaluates them against the in-memory tuple set.

```
> id > 500 AND name = 'Alice'
```

### Automated tests (mode 1)

A numbered menu appears. Select a test category or individual test, or choose **Run Full Test Suite** to execute all tests sequentially.

### Clean build

```bash
cargo clean && cargo run
```

---

## 2 · Benchmark — Performance Suite

The benchmark is a standalone binary crate that measures the selection executor across **24 predicate patterns** and **3 dataset sizes** (1 k, 10 k, 100 k tuples).

### Navigate to the benchmark folder

```bash
cd "tests/test_selection/benchmark"
```

> **Full path example**
> ```bash
> cd "/home/surjit/Desktop/data system/submisssion/4-phase 2 submission(27.02.2027)/test/test1/rookdb/tests/test_selection/benchmark"
> ```

### Build & run

```bash
cargo run
```

### Predicate categories covered

| Category | Examples |
|----------|---------|
| Basic comparisons | `id > 500`, `amount > 500` |
| Selectivity sweep | high / medium / low selectivity |
| Logical operators | `AND`, `OR`, short-circuit evaluation |
| Arithmetic expressions | `id + 10 > 500`, `amount * 2 > 1000` |
| NULL handling | `IS NULL`, `IS NOT NULL` |
| Pattern matching | `LIKE 'A%'`, `LIKE '%li%'` |
| Set membership | `IN (...)` |
| Range queries | `BETWEEN ... AND ...` |
| Execution modes | `filter_tuples`, `filter_tuples_streaming`, `count_matching_tuples` |

### Release mode (faster numbers)

```bash
cargo run --release
```

---

## 3 · Functional Unit Tests (cargo test)

These are standard Rust unit tests integrated into the main workspace.

```bash
# From the rookdb root
cargo test --test test_selection
```

To see printed output:

```bash
cargo test --test test_selection -- --nocapture
```

---

## Quick Reference

| Task | Command (run from the indicated folder) |
|------|-----------------------------------------|
| Run CLI | `cd tests/test_selection/cli && cargo run` |
| Run Benchmark | `cd tests/test_selection/benchmark && cargo run` |
| Run Benchmark (optimised) | `cd tests/test_selection/benchmark && cargo run --release` |
| Run Unit Tests | `cargo test --test test_selection` (from rookdb root) |
| Run Unit Tests (verbose) | `cargo test --test test_selection -- --nocapture` |
