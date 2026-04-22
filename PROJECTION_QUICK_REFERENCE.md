# Projection Operator - Quick Reference Guide

##  Quick Start Examples

### 1. Simple Column Selection
```rust
// SQL: SELECT id, name FROM employees

let input = ProjectionInput {
    catalog: &catalog,
    db_name: "mydb",
    table_name: "employees",
    items: vec![
        ProjectionItem::Expr(Expr::Column(0), "id".to_string()),
        ProjectionItem::Expr(Expr::Column(1), "name".to_string()),
    ],
    predicate: None,
    distinct: false,
    cte_tables: HashMap::new(),
};

let result = ProjectionEngine::execute_simple(input)?;
result.data.print();
```

**Output**:
```
id | name
-----------
1  | Alice
2  | Bob
3  | Charlie
(3 rows)
```

---

### 2. Column Reordering
```rust
// SQL: SELECT salary, name, id FROM employees
// Original order: [id, name, salary]
// New order: [salary, name, id]

let reorder = ColumnReorderSpec::by_indices(vec![2, 1, 0]);

let result = ProjectionEngine::execute(input, Some(reorder), None)?;
// Columns now in order: [salary, name, id]
```

---

### 3. Filtering with WHERE
```rust
// SQL: SELECT name FROM employees WHERE salary > 50000

let where_pred = Expr::Gt(
    Box::new(Expr::Column(2)),  // salary
    Box::new(Expr::Const(Value::Int(50000)))
);

let input = ProjectionInput {
    predicate: Some(where_pred),
    items: vec![
        ProjectionItem::Expr(Expr::Column(1), "name".to_string())
    ],
    // ... other fields
};

let result = ProjectionEngine::execute_simple(input)?;
println!("High earners: {}", result.data.rows.len());
```

---

### 4. DISTINCT Deduplication
```rust
// SQL: SELECT DISTINCT department FROM employees

let input = ProjectionInput {
    distinct: true,
    items: vec![
        ProjectionItem::Expr(Expr::Column(3), "department".to_string())
    ],
    // ... other fields
};

let result = ProjectionEngine::execute_simple(input)?;
println!("Unique departments: {}", result.data.rows.len());
```

---

### 5. With Error Tracking
```rust
// Enable error collection during filtering

let filter = FilterConfig::new(Some(predicate))
    .with_error_tracking(10);  // Stop after 10 errors

let result = ProjectionEngine::execute(input, None, Some(filter))?;

// Check for errors
if !result.errors.is_empty() {
    println!("Errors encountered:");
    for (idx, error) in result.errors.iter().enumerate().take(5) {
        println!("  {}: {}", idx, error);
    }
}
```

---

### 6. Save to CSV File
```rust
// Export results to temporary CSV file

let result = ProjectionEngine::execute_simple(input)?;

let temp_path = save_projection_to_temp(&result, Some("/tmp"))?;
println!("Results exported to: {}", temp_path);

// File content:
// id,name,salary
// 1,Alice,75000
// 2,Bob,65000
// 3,Charlie,85000
```

---

##  Benchmarking Examples

### Basic Benchmark
```rust
use crate::backend::executor::projection_bench::*;

let config = BenchmarkConfig::new("projection_test", 5);
let report = ProjectionBenchmark::run(&config, input)?;

report.print();
// Iterations: 5
// Time (ms):
//   Min:    10.5
//   Max:    15.2
//   Avg:    12.8
//   Median: 12.1
//   StdDev: 1.8
```

---

### Variant Comparison (Ablation Study)
```rust
// Compare different projection strategies

let variants = vec![
    ("no_filter", input_no_filter),
    ("with_filter", input_with_filter),
    ("with_distinct", input_with_distinct),
];

let comparison = ProjectionBenchmark::compare_variants(variants)?;
comparison.print();

// Output shows speedup/slowdown for each variant
```

---

### Layer-by-Layer Profiling
```rust
let mut profiler = LayerProfiler::new();

profiler.record("load_rows", 10, 0, 1000);
profiler.record("filter_rows", 20, 1000, 800);
profiler.record("project_cols", 15, 800, 800);
profiler.record("apply_distinct", 5, 800, 750);

profiler.print();

// Shows timing and row reduction at each stage
```

---

## 🔧 Common Patterns

### Pattern 1: Complex WHERE Clause
```rust
// SELECT id, name FROM employees 
// WHERE salary > 50000 AND department = 'Engineering'

let where_expr = Expr::And(
    Box::new(
        Expr::Gt(
            Box::new(Expr::Column(2)),
            Box::new(Expr::Const(Value::Int(50000)))
        )
    ),
    Box::new(
        Expr::Eq(
            Box::new(Expr::Column(3)),
            Box::new(Expr::Const(Value::Text("Engineering".to_string())))
        )
    )
);

let input = ProjectionInput {
    predicate: Some(where_expr),
    // ...
};
```

---

### Pattern 2: Rename Columns During Reordering
```rust
// Reorder AND rename columns
let reorder = ColumnReorderSpec::by_indices_and_names(
    vec![2, 0, 1],  // [salary, id, name]
    vec![
        "annual_salary".to_string(),
        "employee_id".to_string(),
        "employee_name".to_string(),
    ]
);

let result = ProjectionEngine::execute(input, Some(reorder), None)?;
```

---

### Pattern 3: CTE Integration
```rust
// SELECT name, id FROM high_earners
// (where high_earners is a CTE)

let mut cte_tables = HashMap::new();
cte_tables.insert("high_earners".to_string(), cte_result);

let input = ProjectionInput {
    table_name: "high_earners",
    cte_tables,
    items: vec![
        ProjectionItem::Expr(Expr::Column(1), "name".to_string()),
        ProjectionItem::Expr(Expr::Column(0), "id".to_string()),
    ],
    // ...
};

let result = ProjectionEngine::execute_simple(input)?;
```

---

### Pattern 4: Large Data Set Handling
```rust
// For very large results, check memory usage
let result = ProjectionEngine::execute_simple(input)?;

println!("Memory used: {} bytes", result.metrics.memory_bytes);
println!("Rows: {}", result.metrics.rows_output);

// If memory is high, consider:
// 1. Using streaming iterator (stream_project)
// 2. Reducing column count
// 3. Applying stricter WHERE clause
```

---

## 📈 Metrics Interpretation

```rust
let result = ProjectionEngine::execute_simple(input)?;

// Understand the metrics
println!("Total rows processed: {}", result.metrics.rows_processed);
println!("Rows filtered out: {}", result.metrics.rows_filtered);
println!("Final result size: {}", result.metrics.rows_output);
println!("Execution time: {} ms", result.metrics.elapsed_ms);
println!("Pages from disk: {}", result.metrics.pages_read);
println!("Estimated memory: {} bytes", result.metrics.memory_bytes);

// Calculate filter selectivity
let selectivity = (result.metrics.rows_output as f64) 
    / (result.metrics.rows_processed as f64);
println!("Filter selectivity: {:.1}%", selectivity * 100.0);

// Calculate throughput
let throughput = result.metrics.throughput_rows_per_sec();
println!("Throughput: {:.0} rows/sec", throughput);
```

---

## ✅ Status Handling

```rust
let result = ProjectionEngine::execute(input, None, None)?;

match result.status {
    ProjectionStatus::Success => {
        println!("✓ All rows processed successfully");
    }
    ProjectionStatus::PartialSuccess { error_count, warning_count } => {
        println!("⚠ Processed with {} errors, {} warnings", 
                 error_count, warning_count);
        // Still has usable data in result.data
    }
    ProjectionStatus::Failed { reason } => {
        eprintln!("✗ Failed: {}", reason);
        // Data cannot be used
    }
}
```

---

## 🔍 Troubleshooting

### Issue: Column Index Out of Bounds
```rust
// ✗ Wrong:
let reorder = ColumnReorderSpec::by_indices(vec![0, 1, 5]);  // Only 3 columns!

// ✓ Correct:
let reorder = ColumnReorderSpec::by_indices(vec![0, 1, 2]);  // Matches actual count
```

### Issue: NULL Values in Expressions
```rust
// ✗ Problem: salary * 0.9 = NULL if salary is NULL
let expr = Expr::Mul(
    Box::new(Expr::Column(2)),
    Box::new(Expr::Const(Value::Float(0.9)))
);

// ✓ Solution: Use COALESCE
let expr = Expr::Mul(
    Box::new(Expr::Coalesce(vec![
        Expr::Column(2),
        Expr::Const(Value::Int(0))
    ])),
    Box::new(Expr::Const(Value::Float(0.9)))
);
```

### Issue: Memory Usage Too High
```rust
// Check metrics
if result.metrics.memory_bytes > 100_000_000 {  // 100MB
    eprintln!("Warning: High memory usage");
    
    // Solutions:
    // 1. Select fewer columns
    // 2. Add more restrictive WHERE clause
    // 3. Use streaming operator instead
}
```

---

## 📋 Checklists

### Before Running Projection
- [ ] Verify database exists in catalog
- [ ] Verify table exists in catalog
- [ ] Verify column indices < table.columns.len()
- [ ] Verify predicates are valid expressions
- [ ] Check available memory for result size

### After Running Projection
- [ ] Check result.status for errors
- [ ] Verify output column count matches expectation
- [ ] Validate sample data accuracy
- [ ] Check metrics.rows_output > 0
- [ ] Monitor metrics for performance issues

### For Benchmarking
- [ ] Run at least 3 iterations (warmup + 2 real)
- [ ] Compare similar variants fairly
- [ ] Record hardware specs for reproducibility
- [ ] Note data size and characteristics
- [ ] Allow system to be otherwise idle

---

## 🚀 Performance Tips

### Tip 1: Early Filtering
```rust
// ✓ Faster: Filter before projection
let input = ProjectionInput {
    items: vec![ProjectionItem::Expr(Expr::Column(0), "id".to_string())],
    predicate: Some(expensive_filter),  // Applied early
    // ...
};
```

### Tip 2: Fewer Columns
```rust
// ✓ Better: Select only needed columns
let items = vec![
    ProjectionItem::Expr(Expr::Column(0), "id".to_string()),
    ProjectionItem::Expr(Expr::Column(2), "salary".to_string()),
];
// ✗ Avoid: SELECT * if unnecessary
let items = vec![ProjectionItem::Star];
```

### Tip 3: Skip DISTINCT if Possible
```rust
// ✓ Faster: No DISTINCT
distinct: false,

// ✗ Slower: Uses extra O(n) space
distinct: true,
```

### Tip 4: Use Streaming for Large Results
```rust
// For results > available memory
let stream = stream_project(input)?;
// Processes rows one at a time
```

---

## 🎓 Learning Path

1. **Start Here**: Simple column selection (Example 1)
2. **Add Complexity**: Filtering with WHERE (Example 3)
3. **Optimize**: Column reordering (Example 2)
4. **Measure**: Benchmarking (Examples 5-7)
5. **Integrate**: CTEs and set operations (Pattern 3)
6. **Master**: Layer profiling and ablation studies

---

## 📚 Related Functions

In the base projection module (projection.rs):
```rust
pub fn project(input: ProjectionInput) → io::Result<ResultTable>
pub fn select(catalog, db, table, predicate) → io::Result<ResultTable>
pub fn apply_distinct(rows) → Vec<Row>
pub fn filter_rows(rows, predicate) → io::Result<Vec<Row>>
pub fn eval_projection_list(rows, items, schema) → io::Result<(Vec<OutputColumn>, Vec<Row>)>
```

In the enhanced module (projection_enhanced.rs):
```rust
pub fn execute(...) → io::Result<ProjectionResult>
pub fn execute_simple(input) → io::Result<ProjectionResult>
pub fn save_projection_to_temp(result, temp_dir) → io::Result<String>
```

In the benchmarking module (projection_bench.rs):
```rust
pub fn run(config, input) → io::Result<BenchmarkReport>
pub fn compare_variants(variants) → io::Result<ComparisonReport>
pub struct LayerProfiler
```

---

## 💡 Tips & Tricks

**Tip 1**: Always check `result.status` for errors
```rust
match result.status {
    ProjectionStatus::Success => { /* use result */ },
    _ => { /* handle error */ }
}
```

**Tip 2**: Use `.print_detailed()` for debugging
```rust
result.print_detailed();  // Shows everything
```

**Tip 3**: Compare variants for optimization decisions
```rust
let comparison = ProjectionBenchmark::compare_variants(variants)?;
// Identify the fastest variant
```

**Tip 4**: Profile by layer to find bottlenecks
```rust
let mut profiler = LayerProfiler::new();
// ... record each stage
profiler.print();  // Shows which stage is slowest
```

---

## 🔗 Quick Links in Documentation

- [Full Documentation](../projection.md)
- [Architecture Overview](../projection.md#architecture)
- [Feature Details](../projection.md#core-features)
- [Usage Examples](../projection.md#usage-examples)
- [Optimization Guide](../projection.md#performance-optimization-guidelines)
- [Troubleshooting](../projection.md#common-issues-and-troubleshooting)

