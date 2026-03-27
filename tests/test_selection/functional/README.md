# Selection Operator — Functional Test Suite

Standalone integration tests for the RookDB **Selection Operator** (`storage_manager::executor::selection`).  
Each file is an independent integration test that compiles directly against the library crate.

---

## 📁 Structure

```
functional/
├── Cargo.toml          # Crate manifest (depends on storage_manager)
├── README.md           # This file
└── tests/
    ├── basic.rs        # Comparison operators and boundary values
    ├── logic.rs        # AND / OR / NOT and nested predicate trees
    ├── null.rs         # NULL semantics and three-valued logic (TVL)
    ├── execution.rs    # filter_tuples, count_matching_tuples, filter_tuples_detailed
    └── edge.rs         # TupleAccessor validation, type mismatches, TEXT / DATE types
```

---

## ▶️ How to Run

### Run all tests
```bash
cd tests/test_selection/functional
cargo test
```

### Run with printed output
```bash
cargo test -- --nocapture
```

### Run a specific test file
```bash
cargo test --test basic
cargo test --test logic
cargo test --test null
cargo test --test execution
cargo test --test edge
```

### Run a single test by name
```bash
cargo test --test null test_null_or_true
```

---

## 🧪 Test File Summary

| File | Tests | What It Covers |
|---|---|---|
| `basic.rs` | 12 | `=`, `≠`, `<`, `>`, `≤`, `≥` on INT / FLOAT; boundary values |
| `logic.rs` | 10 | `AND`, `OR`, nested trees up to 5 levels deep |
| `null.rs` | 13 | NULL comparisons → `Unknown`; TVL truth tables for AND / OR |
| `execution.rs` | 6 | `filter_tuples`, `count_matching_tuples`, `filter_tuples_detailed`, empty input |
| `edge.rs` | 20 | Malformed tuples, non-monotonic offsets, schema mismatches, TEXT / DATE |

**Total: 61 tests**

---

## 🔗 Dependency

```toml
storage_manager = { path = "../../../" }
```

The crate resolves to the root `storage_manager` library at `rookdb/`.  
No other external dependencies are required.

---

## ✅ Expected Output

```
test result: ok. 12 passed; 0 failed  (basic)
test result: ok. 20 passed; 0 failed  (edge)
test result: ok.  6 passed; 0 failed  (execution)
test result: ok. 10 passed; 0 failed  (logic)
test result: ok. 13 passed; 0 failed  (null)
```
