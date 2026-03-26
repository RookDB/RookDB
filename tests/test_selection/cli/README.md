# Selection Operator Interactive Testing Environment

This directory contains a standalone interactive CLI for testing the Selection Operator independently from the main database system.

## Overview

The testing environment simulates the execution pipeline:

```
Tuple Generator → Access Operator → Selection Operator → Output File
```

This mirrors the actual database workflow: `Access Method → Selection → Output`

## Architecture

### Modules

1. **tuple_generator.rs** - Generates test tuples in the correct storage format
2. **tuple_accessor.rs** - Simulates the Access Operator with sequential tuple streaming
3. **main.rs** - Interactive CLI menu system

### Tuple Format

All tuples follow the storage layer format:

```
| Header (8 bytes) | NULL Bitmap | Offset Array | Field Data |
```

- **Header**: 
  - [0..4] total length (u32 little-endian)
  - [4..8] column count (u32 little-endian)
- **NULL Bitmap**: ceil(columns / 8) bytes, 1 = NULL
- **Offset Array**: num_columns × 4 bytes (absolute byte offsets from tuple start)
- **Field Data**: Raw column values

## How to Run

### Prerequisites

- Rust toolchain installed (cargo and rustc)
- Run from within the RookDB-main project structure (this CLI is located at tests/test_selection_operator/cli-test_selection_operator/)

### Quick Start

1. **Navigate to the project directory:**
   ```bash
   cd tests/test_selection_operator/cli-test_selection_operator
   ```

2. **Build the project:**
   ```bash
   cargo build --release
   ```

3. **Run the interactive test CLI:**
   ```bash
   cargo run --release
   ```
   
   Or run the compiled binary directly:
   ```bash
   ./target/release/selection_test
   ```

### What Happens When You Run

- The program automatically generates 100 random test tuples at startup
- Two files are created:
  - `tuple_storage.bin` - Binary tuple data
  - `tuple_rows.txt` - Human-readable version of the tuples
- An interactive menu appears with all available tests

### Interactive Menu

The menu shows all available tests organized by category:

```
========================================
Selection Operator Interactive Test CLI
========================================

Select a test:
========================================

Basic Comparison Tests
----------------------------------------
1. Equals Operator (=)
2. Not Equals Operator (≠)
3. Less Than Operator (<)
...

Logical Predicate Tests
----------------------------------------
10. Logical AND: Range Predicate
11. Logical OR: Extreme Values
...

Exit
----------------------------------------
N. Exit
```

Simply enter the number of the test you want to run and press Enter.

### Output

Test results are automatically saved to the `output/` directory. Each test creates a file with:

- Test description
- Predicate being tested
- Statistics (tuples processed and matched)
- Result status

Example:
```
output/comparison_equals.txt
output/logical_and_range.txt
output/datatype_int_gt.txt
```

You can also check `tuple_rows.txt` to see what random data was generated.

## Example Run

```bash
$ cargo run --release
Generating 100 random tuples...
PASS: Tuples generated and stored:
  - Binary storage: tuple_storage.bin
  - Human readable: tuple_rows.txt

========================================
Selection Operator Interactive Test CLI
========================================

Select a test:
...
Enter your choice: 1

COMPLETE: Comparison Operator: Equals (=) complete. Results written to output/comparison_equals.txt

Enter your choice: 28
Exiting. Goodbye!
```

Then check the results:
```bash
$ cat output/comparison_equals.txt
TEST: Comparison Operator: Equals (=)
============================================================

Predicate: id = 500
...
...
------------------------------------------------------------

Statistics:
  Total tuples processed: 100
  Matched tuples: 3
```

## Running Specific Tests

If you want to run all tests at once, choose the "Run Full Test Suite" option from the menu. This will execute all tests sequentially and save results for each one.

## Troubleshooting

**Problem: `cargo: command not found`**
- Install Rust from https://rustup.rs/

**Problem: `storage_manager` dependency not found**
- Make sure you're running from within the RookDB-main project structure
- The correct path should be: RookDB-main/tests/test_selection_operator/cli-test_selection_operator/
- Check that `Cargo.toml` has the correct path dependency: `storage_manager = { path = "../../.." }`

**Problem: Permission denied when running binary**
- Run: `chmod +x target/release/selection_test`

## Test Data

### INT Tuples
- Values: 5, 10, 15, 20, 30
- With NULL: 5, NULL, 15, 20, NULL

### FLOAT Tuples
- Values: 5.5, 10.2, 15.8, 20.1
- With NULL: 5.5, NULL, 15.8

### DATE Tuples
- Values: 2024-01-10, 2024-02-15, 2024-03-20

### STRING Tuples
- Values: Alice, Bob, Charlie

## Integration

The testing environment imports and uses:
- `SelectionExecutor` - Main selection operator
- `Predicate` - Predicate tree structures
- `ColumnReference` - Column references
- `Constant` - Constant values
- `TriValue` - Three-valued logic results

No modifications are made to the core Selection Operator implementation.

## Notes

- This is a standalone testing tool - it does NOT modify the main database system
- All tuples are manually constructed to match the exact storage format
- The environment tests execution layer only (no disk I/O)
- Output file is overwritten on each test run
