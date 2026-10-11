# RookDB


RookDB is a lightweight storage manager for a relational database management system.

## Documentation

- **Project Documentation:** [Project Documentation](https://rookdb.github.io/RookDB/)

- **Rust Code Documentation:** [Rust Code Documentation](https://rookdb.github.io/RookDB/rust-docs/storage_manager/all.html)


## Getting Started

- Install Rust: [Rust Documentation](https://www.rust-lang.org/tools/install)
- This crate is the storage and execution library. The SQL shell lives in the [`rookdb-cli`](https://github.com/RookDB/rookdb-cli) crate: run `cargo run --bin rookdb` there.
- Run the tests: `cargo test`
- Set `RUST_LOG=trace` to view detailed trace-level logs for all operations and commands.


## Contributing

Please see the [Contribution Guidelines](.github/CONTRIBUTING.md) for more information.

## Testing
Run all tests: `cargo test`
Run a specific test: `cargo test --test <file_name>`