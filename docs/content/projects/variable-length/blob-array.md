---
title: Blob and Array
sidebar_position: 2
---

# Blob and Array

## Overview

This implementation adds variable-length type support to RookDB, focused on:

- SQL type parsing for `BLOB` and `ARRAY<type>`
- Typed runtime values for fixed and variable-length data
- Binary encoding and decoding of values
- Tuple serialization with NULL-aware row layout
- Variable-field metadata for mixed-width tuples
- TOAST-style handling for large values

The main public pieces are spread across:

- `src/backend/catalog/data_type.rs`
- `src/backend/storage/value_codec.rs`
- `src/backend/storage/tuple_codec.rs`
- `src/backend/storage/row_layout.rs`
- `src/backend/storage/toast.rs`

## Supported Types

Current `DataType` variants:

- `Int32`
- `Boolean`
- `Text`
- `Blob`
- `Array { element_type: Box<DataType> }`

Notes:

- `Text`, `Blob`, and `Array` are treated as variable-length types.
- Nested arrays are rejected by `DataType::parse()` in the current implementation.
- `VARCHAR` is accepted as an alias for `TEXT`.
- `BYTEA` is accepted as an alias for `BLOB`.

## Core API

### Type Metadata

From `src/backend/catalog/data_type.rs`:

- `DataType::parse(type_str: &str) -> Result<DataType, String>`
- `DataType::is_variable_length(&self) -> bool`
- `DataType::fixed_size(&self) -> Option<usize>`
- `DataType::to_string(&self) -> String`

What these provide:

- Parsing of declarations such as `INT`, `BOOLEAN`, `TEXT`, `BLOB`, and `ARRAY<INT>`
- Fixed-width size lookup for `Int32` and `Boolean`
- Variable-length detection for tuple layout decisions
- String formatting for schema and display use

### Typed Values

Also from `src/backend/catalog/data_type.rs`:

- `enum Value`
- `Value::is_null(&self) -> bool`
- `Value::data_type(&self) -> DataType`
- `Value::to_display_string(&self) -> String`

Current `Value` variants:

- `Null`
- `Int32(i32)`
- `Boolean(bool)`
- `Text(String)`
- `Blob(Vec<u8>)`
- `Array(Vec<Value>)`

Notes:

- `Value::to_display_string()` renders blobs as `BLOB(<n>bytes)`.
- Arrays are displayed in bracket form such as `["a","b"]` or `[1,2]`.

## Value Encoding

From `src/backend/storage/value_codec.rs`:

- `ValueCodec::encode(value: &Value, ty: &DataType) -> Result<Vec<u8>, String>`
- `ValueCodec::decode(bytes: &[u8], ty: &DataType) -> Result<Value, String>`

Internal helpers used by the codec:

- `decode_int32(bytes)`
- `decode_boolean(bytes)`
- `encode_text(text)`
- `decode_text(bytes)`
- `encode_blob(blob)`
- `decode_blob(bytes)`
- `encode_array(values, element_type)`
- `decode_array(bytes, element_type)`

Encoding behavior:

- `Int32`: 4 bytes, little-endian
- `Boolean`: 1 byte
- `Text`: 4-byte length prefix followed by UTF-8 bytes
- `Blob`: 4-byte length prefix followed by raw bytes
- `Array`: 4-byte element count, followed by encoded elements

Array details:

- Fixed-length element arrays store element bytes directly after the count.
- Variable-length element arrays store a 4-byte element length before each encoded element.
- `NULL` values encode to an empty byte vector in the current implementation.

## Row and Tuple Layout

From `src/backend/storage/row_layout.rs`:

- `TupleHeader::new(column_count, null_bitmap_bytes, var_field_count) -> Self`
- `TupleHeader::size() -> usize`
- `TupleHeader::to_bytes() -> Vec<u8>`
- `TupleHeader::from_bytes(bytes) -> Result<Self, String>`

- `VarFieldEntry::new(offset, length, is_toast) -> Self`
- `VarFieldEntry::size() -> usize`
- `VarFieldEntry::is_toast() -> bool`
- `VarFieldEntry::to_bytes() -> Vec<u8>`
- `VarFieldEntry::from_bytes(bytes) -> Result<Self, String>`

- `ToastPointer::new(value_id, total_bytes, chunk_count) -> Self`
- `ToastPointer::size() -> usize`
- `ToastPointer::to_bytes() -> Vec<u8>`
- `ToastPointer::from_bytes(bytes) -> Result<Self, String>`

- `ToastChunk::new(value_id, chunk_no, data) -> Self`
- `ToastChunk::header_size() -> usize`
- `ToastChunk::to_bytes() -> Vec<u8>`
- `ToastChunk::from_bytes(bytes) -> Result<Self, String>`

Tuple byte layout:

- `[TupleHeader]`
- `[NULL bitmap]`
- `[variable-field directory]`
- `[fixed-width field region]`
- `[variable payload region]`

This layout allows fixed and variable-width columns to coexist in a single tuple while keeping offsets explicit for variable-length fields.

## Tuple Serialization

From `src/backend/storage/tuple_codec.rs`:

- `TupleCodec::encode_tuple(values, schema, toast_manager) -> Result<Vec<u8>, String>`
- `TupleCodec::decode_tuple(tuple_bytes, schema) -> Result<Vec<Value>, String>`

Schema format expected by the codec:

- `&[(String, DataType)]`

Behavior:

- Builds a NULL bitmap from `Value::Null`
- Counts variable-length columns from schema metadata
- Stores fixed-width values in the fixed region
- Stores variable-width values in the payload region with `VarFieldEntry` offsets
- Reconstructs values by walking the header, bitmap, directory, and payload during decode

## TOAST Support

From `src/backend/storage/toast.rs`:

- `const TOAST_CHUNK_SIZE: usize = 4096`
- `const TOAST_THRESHOLD: usize = 8192`
- `ToastManager::new() -> Self`
- `ToastManager::store_large_value(payload: &[u8]) -> Result<ToastPointer, String>`
- `ToastManager::read_large_value(toast_file, ptr) -> Result<Vec<u8>, String>`
- `ToastManager::should_use_toast(value_size: usize) -> bool`
- `ToastManager::to_bytes() -> Vec<u8>`
- `ToastManager::from_bytes(bytes: &[u8]) -> Result<Self, String>`

Current behavior:

- Values larger than `TOAST_THRESHOLD` are redirected to TOAST during tuple encoding.
- `store_large_value()` splits payloads into chunks and returns a `ToastPointer`.
- `read_large_value()` is currently a placeholder and returns zero-filled bytes sized from the pointer metadata.


## Test Coverage Summary

The implemented tests cover:

- Data type parsing
- Variable-length detection
- Value encode/decode round trips
- Tuple header and field directory serialization
- Tuple encode/decode with NULL, `BLOB`, and `ARRAY`
- TOAST pointer, chunk, and manager metadata behavior

Run tests:

```bash
cargo test --lib
```
