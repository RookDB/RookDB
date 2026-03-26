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

## Index Features Implemented

RookDB now supports a complete index lifecycle across hash-based and tree-based algorithms:

- Secondary index creation and on-disk persistence (`save`/`load`)
- Clustered index declaration (single clustered index per table)
- Clustered physical table reordering by indexed key
- Automatic index rebuild after CSV ingestion
- Exact index-to-heap consistency validation

### Secondary and Clustered Behavior

- Secondary indexes can be created on non-key columns.
- Clustered indexes physically reorder table tuples by the clustered key.
- After CSV ingest, clustered layout maintenance is applied so physical order remains aligned with clustered metadata.
- Index scans prioritize clustered access locality by reading clustered RID hits in physical `(page_no, item_id)` order.

### Validation and Consistency Checking

Each index implementation provides:

- `validate_structure()`: verifies internal algorithm invariants.
- `all_entries()`: emits all `(IndexKey, RecordId)` pairs for exact verification.

`validate_index_consistency` now performs:

- Structure validation (`validate_structure`)
- Full map comparison of expected heap-derived key/RID pairs vs. actual index entries
- Missing-entry and stale-entry detection
- Entry-count validation

For table-wide checks, `validate_all_table_indexes` validates every registered index on the table.

---

> Note: RookDB currently supports only INT and TEXT data types. Ensure that table schemas are created using only these data types. Accordingly, the CSV file used for loading data (e.g., examples/example.csv) must contain only INT or TEXT columns. If you are running the system for the first time, it is recommended to load examples/example.csv to understand the expected format.
