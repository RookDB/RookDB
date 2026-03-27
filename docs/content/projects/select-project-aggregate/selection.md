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

Represents the **predicate expression tree** used for filtering. Each predicate is an expression tree whose leaf nodes are comparisons over `Expr` operands and whose internal nodes are logical operators. Evaluation produces three-valued logic results via recursive tree traversal.

```rust
pub enum Predicate {
    Compare(Box<Expr>, ComparisonOp, Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Not(Box<Predicate>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Between(Box<Expr>, Box<Expr>, Box<Expr>),  // rewritten at init
    In(Box<Expr>, Vec<Expr>),
    Like(Box<Expr>, String, Option<Regex>),
    Exists(Box<Predicate>),                    // logical wrapper only
}
```

### Variant Details

| Variant | Description |
|---|---|
| `Compare(lhs, op, rhs)` | General comparison: `lhs op rhs` using `ComparisonOp` |
| `IsNull(expr)` | True if expression evaluates to NULL |
| `IsNotNull(expr)` | True if expression is non-NULL |
| `Not(pred)` | Logical negation; propagates Unknown |
| `And(l, r)` | Short-circuits on False (left evaluated first) |
| `Or(l, r)` | Short-circuits on True (left evaluated first) |
| `Between(expr, low, high)` | Rewritten at init to `expr >= low AND expr <= high` |
| `In(expr, list)` | Membership test; Unknown if any NULL in list |
| `Like(expr, pattern, regex)` | Pattern match; regex compiled at init |
| `Exists(pred)` | Logical wrapper — not a subquery EXISTS |

### Purpose

Defines conditions equivalent to SQL `WHERE` clauses. For example:

```
id > 500 AND name LIKE 'A%'
```

is represented as:

```
And(
    Compare(Column("id"), GreaterThan, Constant(Int(500))),
    Like(Column("name"), "A%", Some(<compiled Regex>))
)
```

### Helper Constructors

```rust
Predicate::and(left, right)  -> Predicate::And(...)
Predicate::or(left, right)   -> Predicate::Or(...)
Predicate::not(inner)        -> Predicate::Not(...)
```

---

## 2.2 Expression System (Expr)

Expressions form the operands of predicates. They are evaluated recursively against each tuple.

```rust
pub enum Expr {
    Column(ColumnReference),
    Constant(Constant),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}
```

### Variant Details

| Variant | Description |
|---|---|
| `Column(ref)` | References a schema column by name; resolved to index at init |
| `Constant(c)` | Literal value (Int, Float, Date, Text, Null) |
| `Add(l, r)` | Arithmetic addition |
| `Sub(l, r)` | Arithmetic subtraction |
| `Mul(l, r)` | Arithmetic multiplication |
| `Div(l, r)` | Arithmetic division |

### Evaluation Rules

* Expressions are evaluated **recursively** against each tuple row
* If either operand is `NULL`, the result is `NULL` (NULL propagation)
* Division by zero returns `NULL` (does not panic)
* INT ÷ INT is promoted to FLOAT (`a as f64 / b as f64`)
* Mixed INT/FLOAT arithmetic promotes to FLOAT

---

## 2.3 ColumnReference

Identifies a column used in an expression.

```rust
pub struct ColumnReference {
    pub column_name: String,
    pub column_index: Option<usize>,
}
```

### Constructors

```rust
ColumnReference::new(name)                   // index unresolved (None)
ColumnReference::with_index(name, idx)       // pre-resolved
```

### Important Optimization

`column_index` is **resolved once during `SelectionExecutor::new()`**.

This avoids repeated schema lookups during tuple evaluation, making per-tuple access O(1).

---

## 2.4 Constant

Represents literal values used in predicate expressions.

```rust
pub enum Constant {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null,
}
```

### Supported Types

| Variant | Rust type | SQL type |
|---|---|---|
| `Int(i32)` | 4-byte signed integer | INT |
| `Float(f64)` | 8-byte IEEE 754 double | FLOAT |
| `Date(String)` | ISO-8601 string | DATE |
| `Text(String)` | UTF-8 string | TEXT / STRING |
| `Null` | absence of value | NULL |

---

## 2.5 ComparisonOp

Comparison operators used in `Predicate::Compare`.

```rust
pub enum ComparisonOp {
    Equals,
    LessThan,
    GreaterThan,
    LessOrEqual,
    GreaterOrEqual,
    NotEquals,
}
```

---

## 2.6 TriValue

Implements **SQL three-valued logic**.

```rust
pub enum TriValue {
    True,
    False,
    Unknown,
}
```

### Why Needed

SQL comparisons involving NULL produce **Unknown**, not True/False.

```
NULL = 5       → Unknown
NULL IS NULL   → True
```

Logical operators propagate this correctly:

| AND | True | False | Unknown |
|---|---|---|---|
| **True** | True | False | Unknown |
| **False** | False | False | False |
| **Unknown** | Unknown | False | Unknown |

| OR | True | False | Unknown |
|---|---|---|---|
| **True** | True | True | True |
| **False** | True | False | Unknown |
| **Unknown** | True | Unknown | Unknown |

Tuples where the predicate evaluates to **Unknown are excluded** (same as False).

---

## 2.7 DataType

Internal type enum used for type-checking at initialization (planning) time, and for fast deserialization at runtime.

```rust
pub enum DataType {
    Int,
    Float,
    Text,
    Date,
    Null,
}
```

Used by `infer_expr_type()` and `get_value_fast()`.

---

## 2.8 Value

Typed field value decoded from tuple storage.

```rust
pub enum Value {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null,
}
```

---

## 2.9 TupleAccessor

Provides **constant-time field access** into a raw tuple byte slice.

```rust
pub struct TupleAccessor<'a> {
    tuple: &'a [u8],
    tuple_length: u32,
    num_columns: usize,
    null_bitmap_start: usize,
    null_bitmap_len: usize,
    offset_array_start: usize,
    field_data_start: usize,
}
```

### Physical Tuple Layout

```
| Header (8 bytes) | NULL Bitmap | Offset Array | Field Data |
```

Header format:

```
[0–3]:   tuple_length (u32, little-endian)
[4]:     version (u8)
[5]:     flags (u8)
[6–7]:   column_count (u16, little-endian)
```

Offsets in the offset array are **relative to `field_data_start`**. A sentinel offset at index `num_columns` marks the end of the last field.

Access process:

```
relative_start = offset_array[i]
relative_end   = offset_array[i + 1]
field_bytes    = tuple[field_data_start + relative_start
                       ..
                       field_data_start + relative_end]
```

### Constructors

```rust
TupleAccessor::new(tuple, num_columns)           // full validation
TupleAccessor::new_unchecked(tuple, num_columns) // skip validation (hot path)
```

`new_unchecked()` is used by `SelectionExecutor::evaluate_tuple()` for maximum
throughput when tuples are pre-validated at ingestion time.

### Key Method: get_value_fast()

```rust
pub fn get_value_fast(&self, col_idx: usize, data_type: &DataType) -> Result<Value, TupleError>
```

Uses the `DataType` enum instead of string matching for fast deserialization. Avoids heap allocation for numeric types.

### Key Method: get_field_bytes()

```rust
pub fn get_field_bytes(&self, col_idx: usize) -> Result<&[u8], TupleError>
```

Returns a **zero-copy byte slice** into the tuple. No deserialization unless needed.

### Offset Validation

`validate_offsets()` is called inside `new()` and checks:

1. Each offset is ≥ the previous (monotonicity)
2. Each absolute offset is within `tuple_length` (bounds)

---

## 2.10 SelectionExecutor

Main execution component.

```rust
pub struct SelectionExecutor {
    predicate: Predicate,
    schema: Table,
    column_types: Vec<DataType>,
}
```

### Responsibilities

* Hold normalized, resolved predicate tree
* Store schema metadata
* Precompute `column_types` for fast per-column type dispatch
* Evaluate each tuple against the predicate

### Initialization: `new()`

```rust
pub fn new(mut predicate: Predicate, schema: Table) -> Result<Self, String>
```

Three steps at construction time:

1. **`normalize_predicate()`** — constant folding, BETWEEN rewrite, LIKE regex compile, column-left normalization
2. **`resolve_columns()`** — bind column names to schema indices; validate types
3. **`column_types` precomputation** — parse schema types once into `Vec<DataType>`

After `new()` returns, **no schema lookup happens at runtime**. Every per-tuple operation uses pre-resolved indices and pre-parsed types.

---

## 2.11 TupleError

Error variants for tuple parsing and field access.

```rust
pub enum TupleError {
    TupleTooShort,
    LengthMismatch,
    OffsetOutOfBounds,
    OffsetNotMonotonic,
    FieldRegionOutOfBounds,
    IncompleteOffsetArray,
    InvalidColumnIndex,
}
```

---

# 3. Predicate Normalization

`normalize_predicate()` and `normalize_expr()` are called once inside `SelectionExecutor::new()`. They transform the predicate tree at planning time to simplify and optimize runtime evaluation.

## 3.1 Constant Folding (`normalize_expr`)

All-constant arithmetic sub-expressions are evaluated at init time:

```
Add(Constant(Int(3)), Constant(Int(7)))  →  Constant(Int(10))
Mul(Constant(Float(2.0)), Constant(Float(3.0)))  →  Constant(Float(6.0))
```

NULL operands in arithmetic always fold to `None` (propagated as NULL):

```
Add(Constant(Null), Constant(Int(5)))  →  stays as-is (NULL propagation)
```

Division by zero folds to `NULL`:

```
Div(expr, Constant(Int(0)))   →  NULL
Div(expr, Constant(Float(0.0)))  →  NULL
```

INT ÷ INT is promoted to FLOAT during folding.

## 3.2 BETWEEN Rewrite

`Predicate::Between(expr, low, high)` is rewritten into an explicit AND of two comparisons using `>=`:

```
Between(expr, low, high)
  →  And(
       Compare(expr, GreaterOrEqual, low),
       Compare(expr, LessOrEqual, high)
     )
```

After this rewrite, `Between` is **never evaluated at runtime**. Reaching it is `unreachable!()`.

## 3.3 Comparison Normalization (Column-Left)

In `Compare(lhs, op, rhs)`, if `lhs` is a `Constant` and `rhs` is a `Column`, they are swapped:

```
Compare(Constant, op, Column)  →  Compare(Column, flipped_op, Constant)
```

Operator flip rules:

| Original | Flipped |
|---|---|
| LessThan | GreaterThan |
| GreaterThan | LessThan |
| LessOrEqual | GreaterOrEqual |
| GreaterOrEqual | LessOrEqual |
| Equals | Equals |
| NotEquals | NotEquals |

## 3.4 LIKE Pattern Compilation

LIKE patterns are compiled to `Regex` at init time inside `normalize_predicate()`:

| LIKE token | Regex equivalent |
|---|---|
| `%` | `.*` |
| `_` | `.` |
| literal char | `regex::escape(char)` |

The compiled `Regex` is stored inside `Predicate::Like(expr, pattern, Some(regex))`. At runtime, the pre-compiled regex is used directly — no pattern parsing per tuple.

If the pattern is invalid, it is flagged during `resolve_columns()` validation and `new()` returns `Err`.

---

# 4. Type System & Inference

## 4.1 DataType Enum

Used at both planning and runtime to avoid string comparisons:

```rust
pub enum DataType { Int, Float, Text, Date, Null }
```

## 4.2 `infer_expr_type()`

```rust
fn infer_expr_type(expr: &Expr, schema: &Table) -> Result<DataType, String>
```

Recursively infers the result type of an expression:

* `Column` → looks up schema type by resolved index
* `Constant` → maps to the corresponding `DataType`
* `Add / Sub / Mul / Div` → validates both operands are numeric; INT + FLOAT → FLOAT

## 4.3 Type Coercion Rules

| Left | Right | Result |
|---|---|---|
| INT | INT | INT (or FLOAT for Div) |
| FLOAT | FLOAT | FLOAT |
| INT | FLOAT | FLOAT |
| FLOAT | INT | FLOAT |
| NULL | any | NULL |
| TEXT | INT | parsed to INT (runtime) |
| TEXT | FLOAT | parsed to FLOAT (runtime) |

Type validation happens **at init time** in `resolve_columns()`. Incompatible type comparisons (e.g. TEXT vs DATE) return `Err` from `new()`.

## 4.4 NULL Propagation

* Any NULL operand in arithmetic → result is NULL (Value::Null)
* NULL in comparison → TriValue::Unknown
* `IsNull(NULL)` → TriValue::True
* `IsNotNull(NULL)` → TriValue::False
* `In(NULL, list)` → TriValue::Unknown
* `Like(NULL, ...)` → TriValue::Unknown

---

# 5. Core Methods

## 5.1 `evaluate_tuple()`

Entry point for tuple evaluation.

```rust
pub fn evaluate_tuple(&self, tuple: &[u8]) -> Result<TriValue, String>
```

### Steps

1. Create `TupleAccessor::new_unchecked()` (zero validation overhead)
2. Call `evaluate_predicate()` recursively
3. Return `TriValue`

```
TriValue::True    → tuple included
TriValue::False   → tuple excluded
TriValue::Unknown → tuple excluded
```

---

## 5.2 `evaluate_predicate()`

Recursive predicate evaluation with short-circuit logic.

```rust
fn evaluate_predicate(&self, predicate: &Predicate, accessor: &TupleAccessor) -> Result<TriValue, String>
```

Key short-circuit behaviour:

```
AND: if left == False  → skip right evaluation, return False
OR:  if left == True   → skip right evaluation, return True
```

---

## 5.3 `evaluate_expr()`

```rust
fn evaluate_expr(&self, expr: &Expr, accessor: &TupleAccessor) -> Result<Value, String>
```

* For `Column`: calls `accessor.get_value_fast(idx, &self.column_types[idx])`
* For `Constant`: converts directly (no allocation for numerics)
* For arithmetic: propagates NULL, performs type-promoted operation

---

## 5.4 `extract_column_value` (via `get_value_fast`)

Extracts a typed field value from a tuple column:

1. Check NULL bitmap → return `Value::Null` if set
2. Read `offset_array[col_idx]` and `offset_array[col_idx + 1]`
3. Slice field bytes: `tuple[field_data_start + start .. field_data_start + end]`
4. Deserialize using `DataType` enum dispatch

Time complexity: **O(1)** — direct offset lookup, no scanning.

---

## 5.5 Logical Operators

### `apply_and()`

```rust
pub fn apply_and(left: TriValue, right: TriValue) -> TriValue
```

```
False AND _ → False
_ AND False → False
True AND True → True
otherwise → Unknown
```

### `apply_or()`

```rust
pub fn apply_or(left: TriValue, right: TriValue) -> TriValue
```

```
True OR _ → True
_ OR True → True
False OR False → False
otherwise → Unknown
```

---

# 6. Public API Functions

All public filtering functions accept a `&SelectionExecutor` and operate over tuple byte slices.

## `filter_tuples`

```rust
pub fn filter_tuples(
    executor: &SelectionExecutor,
    tuples: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, String>
```

**Materializes** matching tuples into a `Vec`. Allocates output. Use when downstream needs random access to results.

---

## `filter_tuples_detailed`

```rust
pub fn filter_tuples_detailed(
    executor: &SelectionExecutor,
    tuples: Vec<Vec<u8>>,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<Vec<u8>>), String>
```

Returns three separate collections: `(matched, rejected, unknown)`. Intended for diagnostics and testing — reveals full TriValue categorization.

---

## `count_matching_tuples`

```rust
pub fn count_matching_tuples(
    executor: &SelectionExecutor,
    tuples: &[Vec<u8>],
) -> Result<usize, String>
```

Counts matching tuples **without materializing** them. Zero allocation beyond the counter. Useful for `COUNT(*)` queries.

---

## `filter_tuples_streaming`

```rust
pub fn filter_tuples_streaming(
    executor: &SelectionExecutor,
    tuple_iter: impl Iterator<Item = Result<Vec<u8>, String>>,
    mut output: impl FnMut(&[u8]),
) -> Result<usize, String>
```

Zero-buffering streaming model. Evaluates an iterator of tuples and pushes matches to an **output callback**. Memory usage is O(1) regardless of dataset size. Suitable for pipelining to downstream operators.

---

## `filter_iter`

```rust
pub fn filter_iter<'a>(
    executor: &'a SelectionExecutor,
    iter: impl Iterator<Item = Result<Vec<u8>, String>> + 'a,
) -> impl Iterator<Item = Result<Vec<u8>, String>> + 'a
```

**Iterator-based lazy evaluation**. Wraps an input iterator and yields only matching tuples. Suitable for Rust iterator chains. No buffering — tuples are evaluated on-demand.

---

# 7. Innovative Design Aspects

| Feature | Description |
|---|---|
| **O(1) offset-based column access** | The offset array makes field access index-direct, no scanning |
| **Predicate normalization at init** | BETWEEN rewrite, constant folding, LIKE compilation happen once |
| **Precompiled LIKE regex** | Regex stored in the predicate tree; used directly per tuple |
| **Constant folding** | All-constant arithmetic sub-expressions evaluated to literals at planning time |
| **Streaming execution (O(1) memory)** | `filter_tuples_streaming` and `filter_iter` hold at most one tuple in memory |
| **Separation of access vs computation** | `TupleAccessor` decodes storage; `SelectionExecutor` evaluates logic |
| **`new_unchecked` hot path** | Validation happens once at ingest; evaluation skips it entirely |
| **Column types precomputed** | `column_types: Vec<DataType>` eliminates schema string parsing per field |

---

# 8. Performance Characteristics

### Time Complexity

| Phase | Cost |
|---|---|
| Initialization | O(T × C): T = predicate nodes, C = schema columns |
| Per-tuple access | O(1): offset array lookup |
| Per-tuple evaluation | O(T): proportional to predicate tree depth |
| Total for N tuples | O(N × T) |

### Access-bound vs Compute-bound

* **Access-bound** workloads: small predicates, many columns — bottleneck is field decoding
* **Compute-bound** workloads: deep predicate trees, LIKE with complex regex — bottleneck is logic evaluation

### Key Optimizations

1. **Pre-resolved column indices** — no schema scan per tuple
2. **Offset array → O(1) attribute access** — no field scanning
3. **Streaming execution** — constant memory regardless of N
4. **Short-circuit AND/OR** — avoids evaluating second branch when result is known
5. **DataType enum dispatch** — avoids string comparisons in `get_value_fast()`
6. **Precompiled regex** — LIKE pattern compiled once, not per tuple
7. **`new_unchecked` constructor** — skips bounds checks in hot path

---

# 9. Error Handling

The implementation validates:

* Tuple length correctness (`TupleTooShort`, `LengthMismatch`)
* Offset array bounds (`OffsetOutOfBounds`, `IncompleteOffsetArray`)
* Offset monotonicity (`OffsetNotMonotonic`)
* Field region bounds (`FieldRegionOutOfBounds`)
* Column index validity (`InvalidColumnIndex`)
* Type compatibility at init time (returned as `Err(String)` from `new()`)
* LIKE regex compile validity (validated in `resolve_columns()`)

Invalid tuples return `Err` instead of causing panics. The only `unreachable!()` guards are for `Predicate::Between` and `Predicate::Between` after normalization — these can never be reached in correct usage.

---

# 10. Assumptions

This module assumes that input predicates represent a **parsed query AST**. No SQL parsing happens inside `selection.rs`.

* Input predicate is already in AST form (constructed by caller or parser layer)
* Column references contain correct names; resolution to indices happens in `new()`
* Schema passed to `new()` is valid and consistent with the tuples being evaluated
* Tuples are byte-encoded in the RookDB storage format (header + null bitmap + offset array + field data)
* Once `new()` succeeds, all column references in the predicate are resolved — runtime panics on unresolved columns are unreachable

---

# 11. Key Design Decisions

| Design Choice | Reason |
|---|---|
| `Predicate` over `Expr` leaf nodes | Decouples logical and arithmetic sub-languages |
| `TriValue` logic | Correct SQL NULL semantics throughout |
| `TupleAccessor` struct | Encapsulates layout parsing; reused across multiple field reads per tuple |
| `new_unchecked()` fast path | Moves validation cost to ingestion; eliminates it from the evaluation hot loop |
| `DataType` enum for dispatch | Avoids per-field string comparison; enables fast `match` dispatch |
| `column_types` precomputed | Single parse of schema at init; O(1) type lookup at runtime |
| Regex in predicate tree | LIKE pattern compiled once, stored inline with the predicate |
| Streaming APIs | Enable O(1)-memory pipelining without materializing full result sets |
| BETWEEN → AND rewrite | Eliminates special-casing of BETWEEN in the evaluation engine |
| Column-left normalization | Simplifies runtime comparison dispatch (column always as left operand) |

---

# 12. File Summary

```
selection.rs
│
├── Enums
│   ├── TriValue
│   ├── DataType
│   ├── Constant
│   ├── Expr
│   ├── Predicate
│   ├── ComparisonOp
│   ├── Value
│   └── TupleError
│
├── Structs
│   ├── ColumnReference
│   │   ├── new()
│   │   └── with_index()
│   └── TupleAccessor<'a>
│       ├── new()
│       ├── new_unchecked()
│       ├── validate_offsets()
│       ├── is_null()
│       ├── get_field_bytes()
│       ├── get_value()
│       └── get_value_fast()
│
├── Functions
│   ├── infer_expr_type()
│   ├── constant_to_value()
│   ├── compare_values()
│   ├── apply_and()
│   └── apply_or()
│
├── SelectionExecutor
│   ├── new()                   ← normalize + resolve + precompute types
│   ├── normalize_predicate()
│   ├── normalize_expr()
│   ├── resolve_columns()
│   ├── resolve_expr()
│   ├── evaluate_tuple()
│   ├── evaluate_predicate()
│   └── evaluate_expr()
│
└── Public API Functions
    ├── filter_tuples()
    ├── filter_tuples_detailed()
    ├── count_matching_tuples()
    ├── filter_tuples_streaming()
    └── filter_iter()
```

---
