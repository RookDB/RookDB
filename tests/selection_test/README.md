# Selection Test Suite

This folder contains the SelectionExecutor test suite, which is automatically registered as a Cargo integration test target because it resides in the `tests/` directory:

- Test target name: `selection_test`
- Test entry file: `tests/selection_test/main.rs`

## Test Files Covered

- `test_basic.rs`
- `test_null_logic.rs`
- `test_arithmetic.rs`
- `test_in_like.rs`
- `test_short_circuit.rs`
- `test_varlen.rs`
- `test_streaming.rs`

## Run From Project Root

Make sure you are at the root of the `RookDB` project.

Run all selection tests:

```bash
cargo test --test selection_test
```

This command runs all test files listed above.

Run all selection tests with output:

```bash
cargo test --test selection_test -- --nocapture
```

## Run Specific Test Module

```bash
cargo test --test selection_test -- test_basic
cargo test --test selection_test -- test_null_logic
cargo test --test selection_test -- test_arithmetic
cargo test --test selection_test -- test_in_like
cargo test --test selection_test -- test_short_circuit
cargo test --test selection_test -- test_varlen
cargo test --test selection_test -- test_streaming
```

## Run a Single Test Case

Use a test name filter after `--`.

```bash
cargo test --test selection_test -- and_truth_table_all_nine_cases
```

## Notes

- Because this suite is located inside `tests/selection_test/main.rs`, Cargo automatically discovers it as the integration test `selection_test`. No explicit configuration in `Cargo.toml` is needed.
- This suite validates selection behavior (basic filters, NULL logic, arithmetic, IN/LIKE, short-circuit, varlen extraction, streaming APIs).

## Verify Tests Are Running Correctly

1. Run all tests:

```bash
cargo test --test selection_test
```

2. Confirm Cargo reports success with a summary like:

```text
test result: ok. 118 passed
```

3. Optional: run one module filter and confirm only that module's tests run:

```bash
cargo test --test selection_test -- test_varlen
```
