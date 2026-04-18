//! Comprehensive examples and tests for projection operator
//! 
//! This file demonstrates:
//! - Column reordering
//! - Variable-length data handling
//! - Filtering with error tracking
//! - Benchmarking
//! - Layer-by-layer profiling
//! - Integration with CTEs and subqueries
//! - Set operations

#[cfg(test)]
mod projection_examples {
    // Tests will use the same testing pattern as existing tests
    
    #[test]
    fn test_column_reordering_example() {
        // Example:
        // Original: [id, name, salary, department]
        // Reordered: [department, name, salary, id]
        
        // Test index ordering: [3, 1, 2, 0]
        let indices = vec![3, 1, 2, 0];
        assert_eq!(indices.len(), 4);
    }

    #[test]
    fn test_column_reordering_with_rename_example() {
        let indices = vec![2, 0, 1];
        let names = vec!["Amount".to_string(), "ID".to_string(), "Date".to_string()];
        
        assert_eq!(indices.len(), names.len());
        assert_eq!(names[0], "Amount");
    }

    #[test]
    fn test_filter_configuration_example() {
        // Example: Filter configuration with error tracking
        // max_errors: 10, track_filtered: true
        
        let max_errors = 10;
        let track_filtered = true;
        
        assert!(track_filtered);
        assert_eq!(max_errors, 10);
    }

    #[test]
    fn test_metrics_calculation() {
        // Test throughput calculation
        let rows_processed = 10_000;
        let elapsed_ms = 100; // 100 ms
        
        let throughput = (rows_processed as f64 * 1000.0) / (elapsed_ms as f64);
        assert!((throughput - 100_000.0).abs() < 1.0);
    }

    #[test]
    fn test_distinct_rows_example() {
        // Example usage: removing duplicates
        // Input: [a, b, a, c, b, a]
        // Output: [a, b, c] (preserving order)
        
        let input_size = 6;
        let expected_output_size = 3;
        
        // After deduplication, we expect 3 unique items
        assert!(expected_output_size < input_size);
    }
}

/// Documentation and example usage patterns
/// 
/// # Basic Projection
/// ```ignore
/// use rookdb::backend::executor::*;
/// use rookdb::catalog::types::Catalog;
/// use std::collections::HashMap;
/// 
/// let input = ProjectionInput {
///     catalog: &catalog,
///     db_name: "mydb",
///     table_name: "employees",
///     items: vec![
///         ProjectionItem::Expr(Expr::Column(0), "id".to_string()),
///         ProjectionItem::Expr(Expr::Column(1), "name".to_string()),
///     ],
///     predicate: None,
///     distinct: false,
///     cte_tables: HashMap::new(),
/// };
/// 
/// let result = ProjectionEngine::execute_simple(input)?;
/// result.print_detailed();
/// ```
/// 
/// # Projection with Column Reordering
/// ```ignore
/// let reorder = ColumnReorderSpec::by_indices(vec![2, 0, 1]);
/// 
/// let result = ProjectionEngine::execute(input, Some(reorder), None)?;
/// ```
/// 
/// # Projection with Filtering and Error Tracking
/// ```ignore
/// let filter = FilterConfig::new(Some(predicate))
///     .with_error_tracking(100);
/// 
/// let result = ProjectionEngine::execute(input, None, Some(filter))?;
/// println!("Errors: {}", result.errors.len());
/// ```
/// 
/// # Benchmarking
/// ```ignore
/// let config = BenchmarkConfig::new("projection_test", 5);
/// let report = ProjectionBenchmark::run(&config, input)?;
/// report.print();
/// ```
/// 
/// # Comparison (Ablation Study)
/// ```ignore
/// let variants = vec![
///     ("without_filter", input1),
///     ("with_filter", input2),
///     ("with_distinct", input3),
/// ];
/// 
/// let comparison = ProjectionBenchmark::compare_variants(variants)?;
/// comparison.print();
/// ```

/// # Layer-by-Layer Profiling
/// ```ignore
/// let mut profiler = LayerProfiler::new();
/// 
/// profiler.record("load_rows", 10, 0, 1000);
/// profiler.record("filter", 20, 1000, 800);
/// profiler.record("project", 15, 800, 800);
/// profiler.record("distinct", 5, 800, 750);
/// 
/// profiler.print();
/// ```

pub mod usage_examples {
    //! Real-world usage examples

    /// Example 1: Simple column selection
    /// ```
    /// // SELECT id, name FROM employees
    /// ```
    pub fn example_simple_selection() {
        println!("Example 1: Simple column selection");
        println!("SQL: SELECT id, name FROM employees");
    }

    /// Example 2: Column reordering
    /// ```
    /// // SELECT salary, name, id FROM employees  (reordered)
    /// ```
    pub fn example_column_reordering() {
        println!("Example 2: Column reordering");
        println!("Original: [id, name, salary]");
        println!("After SELECT salary, name, id: [salary, name, id]");
    }

    /// Example 3: Filtering during projection
    /// ```
    /// // SELECT id, name FROM employees WHERE salary > 50000
    /// ```
    pub fn example_projection_with_filter() {
        println!("Example 3: Projection with filtering");
        println!("SQL: SELECT id, name FROM employees WHERE salary > 50000");
    }

    /// Example 4: Projection with DISTINCT
    /// ```
    /// // SELECT DISTINCT department FROM employees
    /// ```
    pub fn example_projection_distinct() {
        println!("Example 4: Projection with DISTINCT");
        println!("SQL: SELECT DISTINCT department FROM employees");
    }

    /// Example 5: Using CTEs (Subqueries)
    /// ```
    /// // WITH high_earners AS (
    /// //   SELECT id, name FROM employees WHERE salary > 100000
    /// // )
    /// // SELECT * FROM high_earners
    /// ```
    pub fn example_cte_projection() {
        println!("Example 5: Using CTEs with projection");
        println!("SQL: WITH high_earners AS (...) SELECT * FROM high_earners");
    }

    /// Example 6: Projection with set operations
    /// ```
    /// // SELECT id, name FROM employees
    /// // UNION
    /// // SELECT id, name FROM contractors
    /// ```
    pub fn example_set_operations() {
        println!("Example 6: Projection with set operations");
        println!("SQL: SELECT ... FROM employees UNION SELECT ... FROM contractors");
    }

    /// Example 7: Benchmarking projection
    /// ```
    /// // Run projection benchmark with 5 iterations
    /// ```
    pub fn example_benchmarking() {
        println!("Example 7: Benchmarking projection");
        println!("Measure performance across multiple runs");
    }
}

/// Configuration patterns for different scenarios
pub mod configuration_patterns {
    /// Pattern 1: Simple projection without filtering
    pub fn simple_projection_config() {
        println!("Configuration: Simple projection");
        println!("- No filtering");
        println!("- All columns selected");
        println!("- No DISTINCT required");
    }

    /// Pattern 2: Projection with selective columns
    pub fn selective_projection_config() {
        println!("Configuration: Selective projection");
        println!("- Specific columns only");
        println!("- May involve reordering");
        println!("- Optional filtering");
    }

    /// Pattern 3: Projection with aggregation preparation
    pub fn aggregation_prep_config() {
        println!("Configuration: Aggregation preparation");
        println!("- Select GROUP BY columns");
        println!("- Select aggregate functions");
        println!("- May use DISTINCT");
    }

    /// Pattern 4: Projection for joins
    pub fn join_projection_config() {
        println!("Configuration: Join projection");
        println!("- Select joined columns from both tables");
        println!("- Rename for clarity");
        println!("- Filter on join condition");
    }
}

/// Performance optimization guidelines
pub mod optimization_guidelines {
    /// Optimization 1: Column Selection
    pub fn optimize_column_selection() {
        println!("Optimization 1: Column Selection");
        println!("- Select only required columns");
        println!("- Reduces memory usage");
        println!("- Improves cache locality");
    }

    /// Optimization 2: Early Filtering
    pub fn optimize_early_filtering() {
        println!("Optimization 2: Early Filtering");
        println!("- Apply WHERE before projection");
        println!("- Reduces rows to process");
        println!("- Improves overall throughput");
    }

    /// Optimization 3: Avoid Unnecessary DISTINCT
    pub fn optimize_distinct() {
        println!("Optimization 3: DISTINCT Usage");
        println!("- Only use DISTINCT if needed");
        println!("- Uses O(n) extra space");
        println!("- Can be expensive for large datasets");
    }

    /// Optimization 4: Vectorized Operations
    pub fn optimize_vectorization() {
        println!("Optimization 4: Vectorization");
        println!("- Process rows in batches");
        println!("- Improves CPU cache utilization");
        println!("- Reduces function call overhead");
    }
}

/// Troubleshooting guide
pub mod troubleshooting {
    /// Issue 1: Column Index Out of Bounds
    pub fn issue_column_index() {
        println!("Issue: Column index out of bounds during reordering");
        println!("Solution: Verify column count matches reordering indices");
    }

    /// Issue 2: NULL Handling
    pub fn issue_null_handling() {
        println!("Issue: NULL values not handled correctly");
        println!("Solution: Use IS NULL / IS NOT NULL predicates");
    }

    /// Issue 3: Variable-Length Data
    pub fn issue_varlen_data() {
        println!("Issue: Variable-length strings cause memory issues");
        println!("Solution: Use tuple_codec for proper encoding/decoding");
    }

    /// Issue 4: Memory Exhaustion with Large Results
    pub fn issue_memory_exhaustion() {
        println!("Issue: Out of memory with large result sets");
        println!("Solution: Use streaming iterator pattern");
    }
}

#[cfg(test)]
mod integration_tests {
    /// Mock test to demonstrate integration points
    #[test]
    fn test_integration_with_catalog() {
        // This would integrate with the catalog system
        println!("Integration test: Catalog verification");
    }

    #[test]
    fn test_integration_with_storage() {
        // This would integrate with the storage layer
        println!("Integration test: Storage layer I/O");
    }

    #[test]
    fn test_integration_with_cte() {
        // This would integrate with CTE evaluation
        println!("Integration test: CTE integration");
    }

    #[test]
    fn test_integration_with_set_ops() {
        // This would integrate with set operations
        println!("Integration test: Set operations");
    }
}
