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

> Note: RookDB currently supports only INT and TEXT data types. Ensure that table schemas are created using only these data types. Accordingly, the CSV file used for loading data (e.g., examples/example.csv) must contain only INT or TEXT columns. If you are running the system for the first time, it is recommended to load examples/example.csv to understand the expected format.

> The CLI-level operations are currently functional; however, some components require further organization and minor modifications to improve structure and consistency. will update soon.
