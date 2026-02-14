# RookDB

RookDB is a lightweight storage manager for a relational database management system.

## Documentation

- **Project Documentation:** [Project Documentation](https://rookdb.github.io/RookDB/)

- **Rust Code Documentation:** [Rust Code Documentation](https://rookdb.github.io/RookDB/rust-docs/storage_manager/all.html)


## Getting Started

- Install Rust: [Rust Documentation](https://www.rust-lang.org/tools/install)
- Run the project: `cargo run`


## Contributing

Please see the [Contribution Guidelines](.github/CONTRIBUTING.md) for more information.

## Testing
Run all tests: `cargo test`
Run a specific test: `cargo test --test <file_name>`

---

> Note: RookDB currently supports only INT and TEXT data types. Ensure that table schemas are created using only these data types. Accordingly, the CSV file used for loading data (e.g., examples/example.csv) must contain only INT or TEXT columns. If you are running the system for the first time, it is recommended to load examples/example.csv to understand the expected format.
