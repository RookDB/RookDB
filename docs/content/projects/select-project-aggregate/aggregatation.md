---
title: Aggregation
sidebar_position: 3
---

# Aggregation

## Overview

RookDB relies on a Volcano-style iterator model utilizing a Hash-Based Aggregation strategy to support `GROUP BY` and complex `HAVING` expression tree evaluations. 

**Model Reference & Alternatives:**
- The **Volcano Model** (Pipeline model) was chosen because it provides a clean, decoupled execution flow. Each operator implements a strict `next()` method, making it simple to chain nodes (e.g., `Scanner` -> `Filter` -> `Aggregator`) and keep memory footprints predictable compared to full materialization.

- **Database Reference:** Aggregation behavior, null-handling, and scalar semantics heavily reference the PostgreSQL Select/Aggregation Standard.

## Main API

The core aggregation loop relies on a single unifying function that takes the required input parameters and outputs the finalized tabular format. This decoupled design ensures the SQL parser frontend simply maps parsed statements to our Enums.

### Supported Aggregates (`AggFunc`)
To support dynamic selection and multiple aggregates per query, we use the `AggFunc` enum. RookDB actively lists and supports the following complete set:
```rust
pub enum AggFunc {
    CountStar, Count, Sum, Min, Max, Avg,
    CountDistinct, SumDistinct, Variance, StdDev,
    BoolAnd, BoolOr
}
```

The core aggregation loop relies on the following execution signature:

```rust
pub fn execute_aggregation(
    child: Box<dyn Executor>, 
    reqs: Vec<AggReq>, 
    group_by_cols: Vec<usize>, 
    having: Option<Expr>
) -> Vec<Tuple>
```

- **Inputs**: 
  - `child`: Upstream Volcano iterator data source.
  - `reqs`: Array of aggregation function variants to compute (e.g., `SUM`, `AVG`).
  - `group_by_cols`: Target column indices defining the grouping key.
  - `having`: Optional recursive AST (`Expr`) used for mathematical/boolean filtering post-aggregation.
- **Output Format**: Returns `Vec<Tuple>` representing completely aggregated, grouped, and filtered rows (forming the output table).

## Implementation & Justification

- **Hash-Based Grouping**: Utilizes `HashMap<Vec<Value>, AggregationState>` alongside Rust's Entry API (`entry().or_insert_with()`) to calculate mapped aggregates dynamically in a single O(N) pass, bypassing the need for pre-sorting.
- **HAVING Evaluation (Expression Tree)**: Implemented via a recursive `Box<Expr>` Abstract Syntax Tree (AST). Instead of relying on rigid, hardcoded column indices, this enables deep evaluations including:
  - **Arithmetic Operations:** `age + 1 > 18`
  - **Multiple Aggregates:** `HAVING COUNT(*) > 10 AND SUM(price) > 100`
  - **Column-to-Column Comparison:** `salary > bonus`
  - **Value to Constants:** `SUM(price) > 100`
Because expressions recursively resolve to primitive `Value` instances, the system effortlessly drops mathematically invalid or `false` groupings without failing.
- **Welford’s Algorithm**: Selected for `VARIANCE` and `STDDEV` computing logic to prevent catastrophic cancellation intrinsic to standard variance floating-point summations.
  - **Working Principle**: It calculates the variance in a single pass without keeping all elements in memory. It maintains a running `count`, `mean`, and sum of squared differences from the mean ($M_2$).
  - **Iteration Logic**: For each new value $x$:
    1. $count = count + 1$
    2. $delta = x - mean$
    3. $mean = mean + \frac{delta}{count}$
    4. $delta2 = x - mean$
    5. $M_2 = M_2 + (delta \times delta2)$
  - **Result Extraction**: The sample variance is computed as $\frac{M_2}{count - 1}$, and standard deviation is $\sqrt{\frac{M_2}{count - 1}}$.
- **Ordered-Float**: Native `f64` primitives are wrapped inside `ordered-float` to strictly satisfy Rust's `Eq` and `Hash` constraints, permitting them safely in HashSets (`DISTINCT`) and HashMaps (`GROUP BY`).

## Case Handling

- **Nulls**: Operators (except `COUNT(*)`) bypass `Value::Null` during iteration accumulations. Math calculations resulting entirely from null input fields cleanly yield `Value::Null` outputs.
- **Overflow Limits**: Mathematical bounds rely upon intrinsic safe-math methods (e.g., `checked_add`), collapsing instantly to `Value::Null` indicators upon constraint violations rather than panicking. 
- **Type Mismatches**: Evaluation block handles distinct match arm logic, isolating mismatched numeric/string boundaries and falling back cleanly.

## Testing & Edge Cases Tested

Robust automated integration tests (`benchmark_aggregation.rs`) have been explicitly written validating the following corner cases according to the rubric:
- **Expression Evaluation:** `HAVING` filters successfully evaluate expression-to-constant (`age + 1 > 18`) and column-to-column (`salary > bonus`) requirements.
- **Empty Table Handling:** Scalar queries over natively empty (`vec![]`) tables explicitly yield a single valid, zero-initialized array row (e.g., `COUNT`=0, `SUM`=Null) rather than no arrays, adhering strictly to the ISO SQL standard.
- **Overflow Limits:** Adding overwhelmingly colossal numbers forcefully evaluates `i32::MAX` overflows. Handled successfully using Rust's `checked_add`/`checked_mul` bounds checking to smoothly downgrade out-of-scale bounds into a safe `Value::Null` DB marker without triggering a Kernel panic framework crash.
- **Single/Multi-Column Group By:** Ensures complete grouping isolation for identically hashed keys.
- **DISTINCT Performance & Set Logic:** Accurate hash-bucket allocations verified securely tracking `HashSet` structures guaranteeing deduplication logic.

## BenchMarking Tracker
The formal benchmarking and testing structures actively measure and validate DB engine execution metrics for the following implemented milestones:
1. Predicate Filtering (HAVING clauses)
2. DISTINCT Performance (Hash-based Deduplication)
3. Expression Evaluation (AST Computation)
4. Empty Table Handling (SQL Scalar Compliance)