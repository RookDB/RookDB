# RookDB

RookDB is a lightweight storage manager for a relational database management system.

---

## Documentation

- **Design Document:**  [Design-Doc.pdf](docs/Design-Doc.pdf)

- **Database Documentation:** [Database-Doc.pdf](docs/Database-Doc.pdf)

- **API Documentation:**  [API-Doc.pdf](docs/API-Doc.pdf)

---

## Prerequisites
Install Rust: [Rust Documentation](https://www.rust-lang.org/tools/install)

## Run the Code
1. Navigate to the `code` directory.
2. Execute:
`cargo run`

---

## 🧪 Running Tests

RookDB includes unit and integration tests located in the `tests/` directory.  
Each test file in this directory is compiled and executed independently by Cargo.


### Run All Tests

To run **all test cases** present in the project:

```bash
cargo test
```
* This command executes all test files in the `tests/` folder.

To run a specific test file present in the tests/ folder, use:
```bash
cargo test --test <file_name>
```

---

## Contribution

Please check contribution details here:  
[CONTRIBUTING.md](.github/CONTRIBUTING.md)

---

### Auto-Generated Code Documentation

The Rust API documentation for the current codebase is auto-generated using **`rustdoc`** and hosted via GitHub Pages.

🔗 **Code Documentation:**  
https://rookdb.github.io/RookDB/storage_manager/

This documentation is generated directly from Rust source code and doc comments and is automatically updated on each push to the `main` branch.

---

> Note: RookDB currently supports only INT and TEXT data types. Ensure that table schemas are created using only these data types. Accordingly, the CSV file used for loading data (e.g., examples/example.csv) must contain only INT or TEXT columns. If you are running the system for the first time, it is recommended to load examples/example.csv to understand the expected format.

check
