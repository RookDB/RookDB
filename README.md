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

> Note: RookDB supports INT (fixed-length), and TEXT and VARCHAR (variable-length) data types. Use `INT`, `TEXT`, `VARCHAR`, or `VARCHAR(n)` (e.g. `VARCHAR(255)`) when creating columns. TEXT and VARCHAR store a 2-byte length prefix followed by the string bytes, so row size varies with content. Ensure CSV files match the table schema. For first-time use, try loading examples/example.csv to see the expected format.
