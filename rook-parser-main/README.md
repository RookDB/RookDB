# Rook Parser

- Rook Parser provides the **query parsing layer for RookDB**. It parses SQL queries and generates an **Abstract Syntax Tree (AST)** using **Apache DataFusion's `sqlparser`**.

- The generated AST is then converted into a structured JSON format that can be used by the **Storage Engine**.

- Crate available at: [Rook Parser Crate](https://crates.io/crates/rook-parser)

- For documentation about Rook Parser, please visit: [Rook Parser Docs](https://rookdb.github.io/docs/Rook-Parser)

---

## Getting Started

Run the interactive SQL parser:

```bash
cargo run
```