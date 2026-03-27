Here is a **formal, polished README** suitable for submission or documentation:

---

# Selection Operator — CLI Test Suite

## Overview

This directory provides a **Command-Line Interface (CLI)–based testing framework** for the Selection Operator of RookDB. The CLI enables interactive evaluation and validation of predicate execution over a generated dataset.

The system is designed to support:

* Manual execution of SQL-like predicates
* Validation of predicate correctness
* Testing across a wide range of logical, arithmetic, and edge-case scenarios

---

## Directory Structure

```
tests/test_selection/
├── cli/                 Interactive query CLI
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── common/              Shared utilities (tuple builder, accessor)
├── functional/          Cargo-integrated unit tests
└── README.md            Documentation
```

---

## Prerequisites

* Rust toolchain (stable, version 1.70 or higher)
* RookDB project properly cloned and accessible

---

## CLI — Query Execution Interface

The CLI serves as the **primary interface** for testing the Selection Operator.

At runtime, the system:

* Generates a dataset of random tuples
* Stores the dataset in:

  * `tuple_storage.bin` (binary format)
  * `tuple_rows.txt` (human-readable format)
* Evaluates user-provided predicates over the generated data

---

## Usage

### Navigate to CLI Directory

```bash
cd tests/test_selection/cli
```

### Build and Run

```bash
cargo run
```

---

## Execution Modes

Upon running the CLI, the user is prompted to select a mode:

```
Select mode:
  0 → Interactive query mode
  1 → Run automated tests
```

---

## Interactive Mode (Mode 0)

Interactive mode is the **recommended workflow** for testing and experimentation.
Users can directly input SQL-like predicates, which are evaluated against the dataset.

### Example Queries

```sql
id > 500
amount < 1000
name = 'Alice'
date >= '2023-01-01'
```

---

## Query Categories for Testing

To ensure comprehensive validation, the following categories of queries should be tested:

### 1. Basic Comparisons

```sql
id > 500
id <= 100
amount >= 250.5
```

### 2. Logical Expressions

```sql
id > 500 AND amount < 1000
id < 100 OR name = 'Bob'
(id > 200 AND amount < 800) OR name = 'Alice'
```

### 3. NULL Handling

```sql
name IS NULL
amount IS NOT NULL
name IS NULL OR id > 500
```

### 4. String Comparisons

```sql
name = 'Alice'
name != 'Bob'
```

 pattern matching is supported:

```sql
name LIKE 'A%'
name LIKE '%li%'
```

### 5. Arithmetic Expressions

```sql
id + 10 > 500
amount * 2 > 1000
```

### 6. Edge Cases

```sql
id > 1000000     -- No matching tuples
id >= 0          -- All tuples match
```

---

## Advanced Predicate Testing

For stress testing and validation of complex predicate trees:

```sql
(id > 500 AND amount < 1000) OR (name = 'Alice' AND date > '2022-01-01')
```

```sql
name IS NULL AND (amount > 500 OR id < 100)
```

---

## Automated Mode (Mode 1)

Automated mode executes a predefined suite of test cases, including:

* Comparison predicates
* Logical operators
* NULL semantics
* Edge-case scenarios
* Parser validation

This mode is primarily intended for **verification**, whereas interactive mode is better suited for **exploration and debugging**.

---

## Clean Build

To rebuild the project from scratch:

```bash
cargo clean && cargo run
```

---

## Functional Unit Tests

From the RookDB root directory:

```bash
cargo test --test test_selection
```

To display detailed output:

```bash
cargo test --test test_selection -- --nocapture
```

---

## Implementation Notes

* The dataset is regenerated on every execution of the CLI
* Predicate evaluation includes:

  * Expression parsing
  * Tuple access via offset-based accessor
  * Three-valued logic (TRUE, FALSE, UNKNOWN)
* Execution is performed entirely in-memory (no runtime disk I/O)

---

## Quick Reference

| Task           | Command                                    |
| -------------- | ------------------------------------------ |
| Run CLI        | `cd tests/test_selection/cli && cargo run` |
| Clean and run  | `cargo clean && cargo run`                 |
| Run unit tests | `cargo test --test test_selection`         |

---


