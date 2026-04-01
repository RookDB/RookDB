---
title: Aggregation
sidebar_position: 3
---

# Aggregation

## Overview

RookDB relies on a Volcano-style iterator model utilizing a Hash-Based Aggregation strategy to support `GROUP BY` and complex `HAVING` expression tree evaluations. 

## Main API

The core aggregation loop relies on the following execution signature:

```rust
pub fn execute_aggregation(
    child: Box<dyn Executor>, 
    reqs: Vec<AggReq>, 
    group_by_cols: Vec<usize>, 
    having: Option<Expr>
) -> Option<Tuple>
```

- **Inputs**: 
  - `child`: Upstream Volcano iterator data source.
  - `reqs`: Array of aggregation function variants to compute (e.g., `SUM`, `AVG`).
  - `group_by_cols`: Target column indices defining the grouping key.
  - `having`: Optional recursive AST (`Expr`) used for mathematical/boolean filtering post-aggregation.
- **Output Format**: Iteratively yields `Option<Tuple>` representing completely aggregated, grouped, and filtered rows.

## Implementation & Justification

- **Hash-Based Grouping**: Utilizes `HashMap<Vec<Value>, AggregationState>` alongside Rust's Entry API (`entry().or_insert_with()`) to calculate mapped aggregates dynamically in a single O(N) pass, bypassing the need for pre-sorting.
- **HAVING Evaluation**: Implemented via a `Box<Expr>` recursive enumeration structure evaluating directly against finalized groups. Failed evaluates map to `Value::Boolean(false)` and are discarded silently.
- **Welford’s Algorithm**: Selected for `VARIANCE` and `STDDEV` computing logic to prevent catastrophic cancellation intrinsic to standard variance floating-point summations.
- **Ordered-Float**: Native `f64` primitives are wrapped inside `ordered-float` to strictly satisfy Rust's `Eq` and `Hash` constraints, permitting them safely in HashSets (`DISTINCT`) and HashMaps (`GROUP BY`).

## Case Handling

- **Nulls**: Operators (except `COUNT(*)`) bypass `Value::Null` during iteration accumulations. Math calculations resulting entirely from null input fields cleanly yield `Value::Null` outputs.
- **Overflow Limits**: Mathematical bounds rely upon intrinsic safe-math methods (e.g., `checked_add`), collapsing instantly to `Value::Null` indicators upon constraint violations rather than panicking. 
- **Type Mismatches**: Evaluation block handles distinct match arm logic, isolating mismatched numeric/string boundaries and falling back cleanly.

## Planned Testing Aspects

*(Note: These will be validated once the test and benchmark suites are written)*
- Evaluation accuracy spanning base aggregates (`MIN`, `MAX`, `SUM`, `AVG`) and bonus operations (`VARIANCE`, `STDDEV`, `BOOL_AND`, `BOOL_OR`).
- Structural isolation efficiency within composite multi-column `GROUP BY` Hash keys.
- Accurate AST recursion boolean results through chained `HAVING` filters.
- Graceful degradation through entirely empty input tables and heavy NULL data concentrations. 
- State isolation validity inside `DISTINCT` Set operations.

## Benchmark Results

- TBD