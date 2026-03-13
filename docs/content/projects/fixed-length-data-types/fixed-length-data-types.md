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