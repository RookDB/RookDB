---
title: Join Algorithms
sidebar_position: 2
---

# Join Algorithms in RookDB

The Join Engine in RookDB is a high-performance, modular system designed to efficiently combine relational data from multiple tables. It supports a comprehensive suite of join types, leveraging four distinct physical execution algorithms and an integrated Cost-Based Optimizer (CBO) to determine the optimal execution strategy automatically.

This is a developer guide to understanding the architecture, execution flow, and implementation details of RookDB's join system.

---

## 1. Supported Join Types

RookDB's join engine logically supports the following standard `JoinType`s:
- **Inner Join** (`Inner`)
- **Left Outer Join** (`LeftOuter`)
- **Right Outer Join** (`RightOuter`)
- **Full Outer Join** (`FullOuter`)
- **Cross Join** (`Cross`)
- **Semi Join** (`SemiJoin`)
- **Anti Join** (`AntiJoin`)
- **Natural Join** (`Natural`)

*Note: While the logical enums are defined for all, advanced join execution types (like Semi and Anti joins) are often processed as specialized variations of inner joins or heavily rely on query-planner rewrites/frontend evaluations before reaching the physical executor.*

---

## 2. Physical Join Algorithms

RookDB implements four physical join operators, located in `src/backend/join/`. All executors consume inputs via `TupleScanner`s and produce a stream of merged `Tuple`s.

### 2.1 Simple Nested Loop Join (NLJ)
- **Location:** `nlj.rs` (using `NLJMode::Simple`)
- **Mechanism:** For every tuple in the outer table, scans the entire inner table. 
- **Time Complexity:** $O(|Outer| \times |Inner|)$
- **Support:** All fundamental join types including `Cross`.
- **Use Case:** Fallback algorithm; highly inefficient for large datasets.

### 2.2 Block Nested Loop Join (BNLJ)
- **Location:** `nlj.rs` (using `NLJMode::Block`)
- **Mechanism:** Loads a "block" of outer tuples into memory before scanning the inner table, drastically reducing the number of disk I/O scans required for the inner table.
- **Time Complexity:** $O(|Outer| + \lceil\frac{|Outer|}{B}\rceil \times |Inner|)$
- **Support:** `Inner`, `LeftOuter`, `RightOuter`, `FullOuter`, `Cross`.
- **Use Case:** Best for non-equi joins (e.g., `<` or `>`) where sorting or hashing is impossible.

### 2.3 Sort-Merge Join (SMJ)
- **Location:** `smj.rs`
- **Mechanism:** Requires inputs to be sorted on the join key. If they aren't, SMJ automatically performs an external multi-way merge sort using `database/tmp/`. It then performs a linear scan-and-merge phase.
- **Time Complexity:** $O(|Outer| \log |Outer| + |Inner| \log |Inner|)$
- **Support:** Equi-joins (`Inner`, `LeftOuter`, `RightOuter`, `FullOuter`).
- **Use Case:** Excellent for very large datasets that exceed memory limits, or when datasets are already ordered.

### 2.4 Hash Join (HJ) Modes
- **Location:** `hj.rs` (using `HashJoinMode`)
- **Mechanism:** The hash join executor dynamically adapts to memory constraints using three distinct modes:
  - **In-Memory Hash Join:** Entirely buffers the build table into an in-memory hash map if it fits within the `memory_pages` limit. The probe table is then streamed against it.
  - **Grace Hash Join:** If the build table is too large, both tables are partitioned into $N$ buckets on disk based on the hash of the join key. Matching partitions are then loaded into memory and joined pair-wise.
  - **Hybrid Hash Join:** An optimized blend where Partition 0 of the build table is kept entirely in memory. During the probe table's partitioning phase, tuples belonging to Partition 0 are probed and emitted *immediately*, drastically reducing I/O write/read cycles.
- **Optimization:** Utilizes a **Bloom Filter** (`bloom_filter.rs`) built during the partition phase to drop non-matching probe tuples early, reducing I/O.
- **Time Complexity:** $O(|Outer| + |Inner|)$
- **Support:** Equi-joins (`Inner`, `LeftOuter`, `RightOuter`, `FullOuter`).
- **Use Case:** Highly efficient for equi-joins on large datasets, provided data distribution isn't heavily skewed.

### 2.5 Symmetric Hash Join (SHJ)
- **Location:** `shj.rs`
- **Mechanism:** A highly concurrent, pipelined execution model that maintains two separate in-memory hash tables. It reads tuples interchangeably from the Outer and Inner tables. Upon reading a tuple from one side, it immediately probes the opposite hash table; if a match is found, it emits the joined tuple instantly before inserting the read tuple into its own hash table.
- **Time Complexity:** $O(|Outer| + |Inner|)$
- **Support:** Equi-joins (`Inner`, `LeftOuter`, `RightOuter`, `FullOuter`).
- **Use Case:** The industry standard for streaming data and real-time query processing where returning early results (low latency) is prioritized over total throughput.

### 2.6 Direct Join
- **Location:** `direct.rs`
- **Mechanism:** A pure, unbuffered engine that blindly loads the entirety of the left and right datasets into memory vectors before performing nested loops. It operates with zero disk I/O buffer management overhead post-scan.
- **Time Complexity:** $O(|Outer| \times |Inner|)$
- **Support:** All join types.
- **Use Case:** Insanely fast for joining tiny dimension tables, but highly dangerous and prone to out-of-memory panics if datasets exceed available RAM.

---

## 3. The Execution Flow

When a join is requested (e.g., via the CLI in `src/frontend/join_cmd.rs`), the system follows this pipeline:

1. **Condition Parsing:** The requested table names, columns, and operator are mapped into a `JoinCondition` structure.
2. **Algorithm Selection (CBO):**
   - If the user selects "Auto", the request is passed to the **Cost-Based Optimizer** (`planner.rs` and `cost_model.rs`).
   - The CBO estimates the I/O and memory cost for BNLJ, SMJ, and Hash Join using table statistics (tuple count, page count, distinct values).
   - The CBO selects the algorithm with the lowest projected cost. It is smart enough to avoid selecting SMJ or HJ for non-equi joins.
3. **Execution Setup:** The chosen executor struct (e.g., `HashJoinExecutor`) is instantiated with the necessary parameters (conditions, memory limits, block sizes).
4. **Processing (`executor.execute()`):**
   - The executor requests `TupleScanner`s from the `storage_manager::heap`.
   - Operations like sorting or partitioning are performed using temporary files in `database/tmp/`.
   - Matching tuples are validated against the `evaluate_conditions` logic in `condition.rs`.
   - The resulting tuples are aggregated into a `JoinResult`.
5. **Output:** The `JoinResult` formats the output into an ASCII table (`result.rs`) and prints execution summaries (time taken, tuples produced).

---

## 4. Developer Guide

### Adding New Join Logic
All join algorithms should act as isolated modules that take schema metadata and returning merged tuples.
- **Predicates:** If you are adding complex predicate support (e.g., `A.x + B.y > 10`), update the evaluation engine in `src/backend/join/condition.rs`.
- **CBO Metrics:** To improve algorithm auto-selection, modify `src/backend/join/cost_model.rs`. The model relies heavily on heuristic math; adding real-time catalog histogram data here will vastly improve planner choices.

### Temporary File Management
Advanced joins (SMJ, HJ) write temporary data to disk. 
- Always ensure `database/tmp/` exists before launching these executors.
- Ensure temporary files are securely deleted immediately after execution completes to prevent disk-space leaks.

### Benchmarking and Testing
- **Tests:** The integration test suite is grouped into a single, high-performance integration crate at `tests/test_joins.rs`, which runs submodules located in `tests/join/` (e.g., `test_nlj.rs`, `test_smj.rs`, `test_hj.rs`, `test_shj.rs`, `test_direct.rs`). Because Cargo runs integration crates in parallel, this unified structure utilizes a shared global lock (`tests/join/common.rs`) to safely manage the global `catalog.json` state. Always run `cargo test --test test_joins` when making modifications.
