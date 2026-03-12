---
title: Select Operators
sidebar_position: 1
---

---

# Selection Operator – Implementation Documentation

## File Location

```
src/backend/executor/selection.rs
```

This file contains the **complete implementation of the Selection Operator** in RookDB.
The operator is responsible for **filtering tuples based on predicate conditions** during query execution.

It operates **after tuple retrieval (Sequential Scan / Access Operator)** and **before downstream operators or output**.

---

# 1. Role in Query Execution

Execution pipeline:

```
Access Operator (Seq Scan / Iterator)
        ↓
Selection Operator
        ↓
Downstream Operators / Output
```

Responsibilities:

* Evaluate predicates on each tuple
* Implement SQL **three-valued logic**
* Return only tuples where predicate evaluates to **TRUE**

Important characteristics:

* **No disk access**
* **No storage modifications**
* Works on **tuple byte slices (`&[u8]`)**
* Fully **streaming** (no buffering of results)

---

# 2. Main Structures

## 2.1 Predicate

Represents the **predicate expression tree** used for filtering.

```rust
enum Predicate {
    Equals(ColumnReference, Constant),
    LessThan(ColumnReference, Constant),
    GreaterThan(ColumnReference, Constant),
    LessOrEqual(ColumnReference, Constant),
    GreaterOrEqual(ColumnReference, Constant),
    NotEquals(ColumnReference, Constant),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>)
}
```

### Purpose

Defines conditions similar to a SQL `WHERE` clause.

Example:

```
age > 25 AND city = "NYC"
```

is represented as

```
And(
    GreaterThan(age, 25),
    Equals(city, "NYC")
)
```

### Design Choice

* `enum` allows **pattern matching**
* `Box<Predicate>` enables **recursive predicate trees**
* Easily extensible for new operators

---

## 2.2 ColumnReference

Identifies a column used in a predicate.

```rust
struct ColumnReference {
    column_name: String,
    column_index: usize
}
```

### Purpose

Allows predicates to refer to columns.

### Important Optimization

`column_index` is **resolved once during initialization**.

This avoids repeated schema lookups during tuple evaluation.

---

## 2.3 Constant

Represents literal values used in predicates.

```rust
enum Constant {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null
}
```

### Supported Types

* INT
* FLOAT
* DATE
* TEXT
* NULL

Used for **type-safe comparisons** during predicate evaluation.

---

## 2.4 TriValue

Implements **SQL three-valued logic**.

```rust
enum TriValue {
    True,
    False,
    Unknown
}
```

### Why Needed

SQL comparisons with NULL produce **UNKNOWN**, not TRUE/FALSE.

Example:

```
NULL = 5 → UNKNOWN
```

Logical operators must propagate this correctly.

---

## 2.5 SelectionExecutor

Main execution component.

```rust
struct SelectionExecutor {
    predicate: Predicate,
    schema: Vec<Column>
}
```

### Responsibilities

* Store predicate tree
* Store schema metadata
* Evaluate tuples against predicate

### Important Methods

```
new(predicate, schema)
evaluate_tuple(tuple_bytes)
```

---

## 2.6 TupleAccessor

Provides **safe access to fields inside a tuple**.

```rust
struct TupleAccessor<'a> {
    tuple_bytes: &'a [u8],
    tuple_length: u32,
    column_count: u16,
    null_bitmap: &'a [u8],
    offset_array_bytes: &'a [u8],
    field_data_start: usize
}
```

### Purpose

Parse tuple structure once and provide **O(1) column access**.

Tuple layout:

```
| Header | NULL Bitmap | Offset Array | Field Data |
```

Access process:

```
offset[i] → start
offset[i+1] → end
length = end - start
```

---

# 3. Core Methods

## 3.1 evaluate_tuple()

Entry point for tuple evaluation.

```rust
fn evaluate_tuple(&self, tuple_data: &[u8]) -> Result<bool, String>
```

### Steps

1. Create `TupleAccessor`
2. Validate tuple integrity
3. Skip logically deleted tuples
4. Evaluate predicate
5. Convert `TriValue` → boolean

```
True      → tuple included
False     → tuple excluded
Unknown   → tuple excluded
```

---

## 3.2 evaluate_predicate()

Recursive predicate evaluation.

```
fn evaluate_predicate(...)
```

Logic:

```
match predicate {
    comparison → compare values
    AND → short circuit if False
    OR → short circuit if True
}
```

This function evaluates the **predicate tree recursively**.

---

## 3.3 extract_column_value()

Extracts a column value from a tuple.

Steps:

1. Check NULL bitmap
2. Read offset array
3. Locate field in data region
4. Decode according to column type

Time complexity:

```
O(1)
```

Because of **direct offset lookup**.

---

## 3.4 compare_values()

Performs type-safe comparison.

Handles:

* INT
* FLOAT (epsilon comparison)
* DATE (lexicographic comparison)
* TEXT

Rules:

```
NULL comparison → UNKNOWN
type mismatch → error
```

---

## 3.5 Logical Operators

### apply_and()

SQL AND logic

```
False AND X → False
True AND True → True
otherwise → Unknown
```

### apply_or()

SQL OR logic

```
True OR X → True
False OR False → False
otherwise → Unknown
```

---

## 3.6 filter_tuples()

Main filtering function used by the execution pipeline.

```
fn filter_tuples(...)
```

### Inputs

* Catalog reference
* database name
* table name
* tuple iterator
* predicate
* output callback

### Process

```
for each tuple:
    evaluate predicate
    if TRUE:
        emit tuple
```

Returns:

```
number of matching tuples
```

---

# 4. Performance Characteristics

### Time Complexity

Initialization:

```
O(T × C)
```

T → predicate size
C → number of columns

Per Tuple:

```
O(T)
```

Total:

```
O(N × T)
```

N → number of tuples

---

### Key Optimizations

1. **Pre-resolved column indices**
2. **Offset array for O(1) attribute access**
3. **Streaming execution (constant memory)**
4. **Short-circuit logical evaluation**

---

# 5. Error Handling

The implementation validates:

* tuple length correctness
* offset bounds
* offset monotonicity
* column index validity
* type mismatches

Invalid tuples return errors instead of causing crashes.

---

# 6. Key Design Decisions

| Design Choice       | Reason                        |
| ------------------- | ----------------------------- |
| Predicate tree      | Flexible condition evaluation |
| TriValue logic      | Correct SQL NULL semantics    |
| TupleAccessor       | Safe tuple parsing            |
| Offset array access | O(1) attribute retrieval      |
| Streaming execution | Constant memory usage         |

---

# 7. File Summary

```
selection.rs
│
├── Predicate enum
├── ColumnReference struct
├── Constant enum
├── TriValue enum
│
├── SelectionExecutor
│   ├── new()
│   └── evaluate_tuple()
│
├── TupleAccessor
│   ├── new()
│   └── get_value()
│
└── Filtering Functions
    ├── evaluate_predicate
    ├── extract_column_value
    ├── compare_values
    ├── apply_and
    ├── apply_or
    └── filter_tuples
```

---

