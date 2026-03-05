---
title: Projection
sidebar_position: 2
---

# RookDB

RookDB is a lightweight storage manager for a relational database management system.

## Documentation

- **Project Documentation:** [Project Documentation](https://rookdb.github.io/RookDB/)

- **Rust Code Documentation:** [Rust Code Documentation](https://rookdb.github.io/RookDB/rust-docs/storage_manager/all.html)


## Getting Started

- Install Rust: [Rust Documentation](https://www.rust-lang.org/tools/install)
- Run the project: `cargo run`


## Contributing

Please see the [Contribution Guidelines](../../../../.github/CONTRIBUTING.md) for more information.

## Testing
Run all tests: `cargo test`
Run a specific test: `cargo test --test <file_name>`

---

## Projection Operator

RookDB now includes a **projection operator** as part of the executor module. This operator allows queries to select a subset of columns from a table or result set, implementing the traditional relational 'projection' capability. Key points:

- Implemented in `src/executor/projection.rs` and integrated with the executor pipeline.
- Works alongside `seq_scan` and other operators to form simple query plans.
- Supports expressions on columns (e.g. selecting `a + b` though only basic column references are currently handled).
- Tested via new unit tests (`cargo test --test test_projection`).

Usage examples and details are available in the project documentation under [executor operators](https://rookdb.github.io/RookDB/).

> Note: RookDB currently supports only INT and TEXT data types. Ensure that table schemas are created using only these data types. Accordingly, the CSV file used for loading data (e.g., examples/example.csv) must contain only INT or TEXT columns. If you are running the system for the first time, it is recommended to load examples/example.csv to understand the expected format.


# Projection