# RookDB


RookDB is a lightweight storage manager for a relational database management system.

## Documentation

- **Project Documentation:** [Project Documentation](https://rookdb.github.io/RookDB/)

- **Rust Code Documentation:** [Rust Code Documentation](https://rookdb.github.io/RookDB/rust-docs/storage_manager/all.html)


## Scope

RookDB aims at broad SQL-99 query coverage on a real storage engine. It is not a transactional database, by design:

- **No transactions.** Every statement commits on its own; `BEGIN`, `COMMIT`, `ROLLBACK` and `SAVEPOINT` are rejected. A multi-row `INSERT` or `UPDATE` that fails a constraint changes no rows.
- **No write-ahead log.** Table and index pages are written to their files at statement boundaries and fsynced within 200 ms (the shell also fsyncs before waiting for input and on exit). A crash can lose the latest statements or leave a table partly written; pages carry no checksums.
- **Atomic catalog.** Schema changes save the system tables by building a new copy and swapping it in, so a crash leaves the old catalog or the new one, never a mix.
- **One process per data directory.** The first process locks `database/rookdb.lock`; a second one is refused. Within a process there is no concurrency control: statements run one at a time.

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