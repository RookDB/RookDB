---
title: Fixed Length Data Types
sidebar_position: 1
---

## Overview

This module implements typed value handling for RookDB, including:

- SQL type parsing and metadata
- Value serialization and deserialization
- Type validation
- Comparison semantics
- NULL-aware row encoding
- Built-in type functions

Main public exports are provided from src/backend/types/mod.rs.

## Supported Types

Current DataType variants:

- SmallInt
- Int
- BigInt
- Real
- DoublePrecision
- Numeric { precision, scale }
- Decimal { precision, scale }
- Bool
- Char(n)
- Character(n)
- Varchar(n)
- Date
- Time
- Timestamp
- Bit(n)

Notes:

- Character(n) is implemented as an alias-compatible type with Char(n)-equivalent storage behavior.
- Numeric and Decimal use exact decimal encoding with precision/scale constraints.

## Core API

### Type Metadata

From src/backend/types/datatype.rs:

- DataType::alignment() -> u32
: Returns alignment rule used by layout logic.
- DataType::fixed_size() -> Option<u32>
: Returns fixed byte width where applicable.
- DataType::min_storage_size() -> u32
: Minimum storage bytes for a value of this type.
- DataType::is_variable_length() -> bool
: True for variable-width types.
- DataType::encoded_len(bytes: &[u8]) -> Result<usize, String>
: Computes encoded field length from raw bytes.

Parsing and formatting:

- impl FromStr for DataType
- impl Display for DataType
- impl Serialize/Deserialize for DataType

### Typed Values and Encoding

From src/backend/types/value.rs:

- enum DataValue
: Runtime typed value container.
- struct NumericValue { unscaled: i128, scale: u8 }
: Exact-decimal internal representation.
- DataValue::to_bytes() -> Vec<u8>
: Generic byte encoding.
- DataValue::from_bytes(ty: &DataType, bytes: &[u8]) -> Result<DataValue, String>
: Decode typed value from bytes.
- DataValue::parse_and_encode(ty: &DataType, input: &str) -> Result<Vec<u8>, String>
: Parse text input for target type and return encoded bytes.
- DataValue::to_bytes_for_type(ty: &DataType) -> Result<Vec<u8>, String>
: Type-aware encoding path for cases that require DataType metadata (for example Numeric/Decimal and fixed-width Char/Character padding).

### Validation

From src/backend/types/validation.rs:

- validate_smallint
- validate_int
- validate_bigint
- validate_real
- validate_double
- validate_numeric
- validate_bool
- validate_char
- validate_varchar
- validate_date
- validate_time
- validate_timestamp
- validate_bit
- validate_value(ty, input)

All validators return Result<(), TypeValidationError>.

### Comparison

From src/backend/types/comparison.rs:

- trait Comparable
: compare and is_equal for typed values.
- compare_nullable(left, right) -> Result<Option<Ordering>, ComparisonError>
- nullable_equals(left, right) -> Result<Option<bool>, ComparisonError>

Mixed integer promotion is supported across SmallInt, Int, and BigInt.

### Row and NULL Encoding

From src/backend/types/row.rs:

- serialize_nullable_row(schema, values) -> Result<Vec<u8>, String>
- deserialize_nullable_row(schema, row_bytes) -> Result<Vec<Option<DataValue>>, String>
- struct Row
	- Row::new(schema)
	- Row::set_value
	- Row::set_null
	- Row::get_value
	- Row::serialize
	- Row::deserialize

Row byte layout:

- [NULL bitmap bytes][encoded field bytes...]

Schema is catalog-level metadata and is not embedded in each row payload.

### Built-in Type Functions

From src/backend/types/functions.rs:

String functions:

- length
- substring
- upper
- lower
- trim
- ltrim
- rtrim

Numeric functions:

- abs
- round
- floor
- ceiling

Null and conversion:

- cast
- coalesce
- nullif

Temporal functions:

- extract(DatePart, value)
- current_date
- current_time
- current_timestamp

## Error Types

- TypeValidationError
- ComparisonError
- FunctionError

All errors are formatted with descriptive messages suitable for CLI feedback.

## Test Coverage Summary

The backend types suite includes validation, round-trip encoding, comparison, NULL behavior, row operations, and function-level tests.

Run tests:

```bash
cargo test --lib
```

Comprehensive Phase 10 integration tests:

```bash
cargo test --test test_types_basic
cargo test --test test_type_serialization
cargo test --test test_null_handling
cargo test --test test_type_comparison
cargo test --test test_typed_rows
cargo test --test test_type_functions
cargo test --test test_type_constraints
```

## Testing and Benchmarking Phase (Initial Results)

This section documents initial evidence for correctness, robustness, and performance/scalability required in the testing and benchmarking phase.

### Correctness Evidence

- Unit and integration tests pass for all implemented SQL-99 reduced fixed-length datatypes.
- Round-trip encode/decode is verified for SmallInt, Int, BigInt, Real, DoublePrecision, Numeric, Decimal, Bool, Char, Character, Varchar, Date, Time, Timestamp, and Bit.
- Type function behavior is validated for string, numeric, temporal, cast, and NULL-aware helper functions.

Run complete suite:

```bash
cargo test
```

### Robustness Evidence

Edge and failure-path tests currently include:

- Integer bounds (SmallInt/Int/BigInt overflow and min/max boundaries)
- Floating-point invalid literals
- Numeric/Decimal precision and scale violations
- Char/Varchar length constraints
- Date/Time/Timestamp format and range checks
- Bit-length and symbol validation
- NULL bitmap correctness in nullable row encoding
- Truncated/invalid encoded payload decode rejection

### Benchmark Methodology

- Tooling: integration benchmark test harness in tests/test_type_benchmarks.rs
- Runner script: scripts/run_type_benchmarks.sh
- Build profile: test (debug)
- Host (sample run): Linux 6.6.87.2-microsoft-standard-WSL2 x86_64
- Rust toolchain (sample run): rustc 1.94.0, cargo 1.94.0
- Benchmark date: 2026-03-26

Run benchmark suite:

```bash
./scripts/run_type_benchmarks.sh
```

### Initial Performance and Scalability Results

Sample run output file:

- /tmp/rookdb_type_bench_20260326_182407.log

#### Numeric Comparison Throughput

| Operations | Scale | Seconds | Ops/sec |
| ---: | :--- | ---: | ---: |
| 100,000 | Small | 0.004022 | 24,863,245.93 |
| 1,000,000 | Medium | 0.043570 | 22,951,709.67 |
| 5,000,000 | Large | 0.178152 | 28,065,868.93 |

#### Numeric Function Throughput (ABS, ROUND, FLOOR, CEILING)

| Operations | Scale | Seconds | Ops/sec |
| ---: | :--- | ---: | ---: |
| 20,000 | Small | 0.002120 | 9,432,511.79 |
| 200,000 | Medium | 0.020727 | 9,649,435.52 |
| 1,000,000 | Large | 0.119461 | 8,370,923.03 |

#### Typed Row Round-Trip (serialize + deserialize)

| Rows | Scale | Seconds | Rows/sec |
| ---: | :--- | ---: | ---: |
| 2,000 | Small | 0.120591 | 16,584.93 |
| 20,000 | Medium | 1.166918 | 17,139.17 |
| 100,000 | Large | 5.949959 | 16,806.84 |

### Key Observations

- Numeric comparison and numeric function workloads sustain multi-million operations/sec in this environment.
- Typed row round-trip throughput remains stable across increasing input sizes (around 16k-17k rows/sec), indicating linear scaling behavior for this initial benchmark.
- Throughput values are environment-sensitive; this phase focuses on initial trends and reproducibility rather than absolute peak numbers.

### Reproducibility Notes

- Re-run the benchmark script 3-5 times and report median values for submission snapshots.
- Keep CPU load minimal during benchmark runs.
- Use the same input scales and test-thread setting to maintain comparability.

## Example Usage

```rust
use storage_manager::types::{DataType, DataValue, serialize_nullable_row, deserialize_nullable_row};

let schema = vec![
		DataType::Int,
		DataType::Varchar(16),
		DataType::Date,
		DataType::Bool,
];

let bytes = serialize_nullable_row(
		&schema,
		&[Some("42"), Some("alice"), Some("2026-03-13"), Some("true")],
)?;

let values = deserialize_nullable_row(&schema, &bytes)?;
assert_eq!(values.len(), 4);
# Ok::<(), String>(())
```