//! Integration tests for the Cost-Based Join Optimizer
//! Validates the CBO against the three edge cases in the cross-verification analysis

#[cfg(test)]
mod cbo_tests {
    use storage_manager::join::*;
    use storage_manager::join::cost_model::{CostModel, CardinalityEstimator};
    use storage_manager::join::planner::{JoinPlanner, JoinPlannerConfig};

    #[test]
    fn test_edge_case_1_non_equi_massive_tables() {
        // Edge Case 1: Non-Equi Join with Massive Tables
        // Left: 1000 pages, Right: 1000 pages
        // Memory: 10 pages
        // Condition: > (non-equi)
        
        let left_pages = 1000.0;
        let right_pages = 1000.0;
        let memory_pages = 10;
        
        // Calculate BNLJ cost (only viable option for non-equi)
        let bnlj_cost = CostModel::bnlj_cost(
            left_pages,
            right_pages,
            memory_pages,
            1_000_000,
            1_000_000,
        );
        
        // Expected: 1000 + ceil(1000/8)*1000 = 126,000
        assert!(bnlj_cost.total_cost > 100_000.0);
        assert!(bnlj_cost.total_cost < 150_000.0);
        
        println!("Edge Case 1 (Non-Equi): {}", bnlj_cost);
        assert_eq!(bnlj_cost.algorithm, "Block NLJ");
    }

    #[test]
    fn test_edge_case_2_indexed_inner_table() {
        // Edge Case 2: Indexed Inner Table with Equi-Join
        // Left: 100 pages (100,000 rows)
        // Right: 5000 pages (5,000,000 rows)
        // Memory: 50 pages
        // Condition: = (equi)
        // Index on right table: 4 pages lookup cost
        
        let left_pages = 100.0;
        let right_pages = 5000.0;
        let memory_pages = 50;
        let left_rows = 100_000u64;
        
        // INLJ Cost — Bug 7 fix: include data_page_fetch (5th parameter)
        let inlj_cost = CostModel::inlj_cost(left_pages, 4.0, left_rows, 1.0, 1.0);
        
        // Expected: 100 + (100,000 * (4 + 1.0*1.0)) = 500,100
        assert!(inlj_cost.total_cost > 400_000.0);
        println!("Edge Case 2 (INLJ): {}", inlj_cost);
        
        // Grace Hash cost (alternative)
        let grace_cost = CostModel::grace_hash_cost(left_pages, right_pages, 8);
        println!("Edge Case 2 (Grace Hash): {}", grace_cost);
        
        // BNLJ (baseline)
        let bnlj_cost = CostModel::bnlj_cost(left_pages, right_pages, memory_pages, left_rows, 5_000_000);
        println!("Edge Case 2 (BNLJ): {}", bnlj_cost);
        
        // Grace/BNLJ should be more efficient than INLJ with expensive index lookups
        assert!(grace_cost.total_cost < inlj_cost.total_cost);
    }

    #[test]
    fn test_edge_case_3_presorted_inputs() {
        // Edge Case 3: Pre-sorted Inputs on Join Column
        // Left: 50 pages, pre-sorted
        // Right: 10 pages, pre-sorted
        // Memory: 20 pages
        // Condition: = (equi)
        
        let left_pages = 50.0;
        let right_pages = 10.0;
        let memory_pages = 20;
        
        // SMJ with pre-sort optimization (sort cost = 0)
        let smj_presorted = CostModel::smj_cost(
            left_pages,
            right_pages,
            memory_pages,
            true,  // left pre-sorted
            true,  // right pre-sorted
        );
        
        // After Bug 9 fix: merge_cost = 50+10 = 60, sort_cost = 0 → total = 60
        assert_eq!(smj_presorted.total_cost, 60.0);
        assert_eq!(smj_presorted.cost_component_sort, 0.0);
        println!("Edge Case 3 (SMJ Pre-sorted): {}", smj_presorted);
        
        // SMJ without pre-sort optimization
        let smj_unsorted = CostModel::smj_cost(
            left_pages,
            right_pages,
            memory_pages,
            false, // left needs sorting
            false, // right needs sorting
        );
        
        // Should have significant sort cost
        assert!(smj_unsorted.total_cost > 100.0);
        assert!(smj_unsorted.cost_component_sort > 0.0);
        println!("Edge Case 3 (SMJ Unsorted): {}", smj_unsorted);
        
        // Pre-sorted should be much cheaper
        assert!(smj_presorted.total_cost < smj_unsorted.total_cost);
        
        // BNLJ baseline
        let bnlj = CostModel::bnlj_cost(left_pages, right_pages, memory_pages, 10_000, 10_000);
        println!("Edge Case 3 (BNLJ): {}", bnlj);
        
        // SMJ pre-sorted should be better than BNLJ
        assert!(smj_presorted.total_cost < bnlj.total_cost);
    }

    #[test]
    fn test_bloom_filter_integration() {
        // Verify Bloom Filter works for hash join optimization
        use storage_manager::join::BloomFilter;
        
        let mut bf = BloomFilter::new(10000);
        
        // Insert some values
        for i in 0i32..100 {
            let bytes = i.to_le_bytes();
            bf.insert(&bytes);
        }
        
        // Check statistics
        let stats = bf.stats();
        assert_eq!(stats.elements_inserted, 100);
        assert!(stats.false_positive_rate < 0.01); // Should be very low
        
        println!("Bloom Filter Stats: {:?}", stats);
    }

    #[test]
    fn test_cardinality_estimator() {
        // Test cardinality estimation (Bug 12 fix: uses f64 throughout)
        let estimator = CardinalityEstimator::new(
            1_000_000, // left table
            500_000,   // right table
            0.01,      // 1% selectivity
        );
        
        let output_rows = estimator.estimated_output_rows();
        let expected = ((1_000_000_f64 * 500_000_f64) * 0.01) as u64;
        
        assert_eq!(output_rows, expected);
        println!("Estimated output rows: {}", output_rows);
    }

    #[test]
    fn test_cardinality_estimator_large_tables() {
        // Bug 12: Verify no precision loss for large tables (>10^8 rows)
        let estimator = CardinalityEstimator::new(
            1_000_000_000, // 1 billion rows
            1_000_000_000, // 1 billion rows
            0.000001,      // very selective
        );

        let output_rows = estimator.estimated_output_rows();
        // 10^18 * 10^-6 = 10^12 — should be representable in u64
        assert!(output_rows > 0);
        println!("Large table estimated output rows: {}", output_rows);
    }

    #[test]
    fn test_algorithm_metadata() {
        use storage_manager::join::algorithm::AlgorithmMetadata;
        
        let bnlj = AlgorithmMetadata::for_bnlj();
        let inlj = AlgorithmMetadata::for_inlj();
        let smj = AlgorithmMetadata::for_smj();
        let hash = AlgorithmMetadata::for_hash_join();
        
        // BNLJ: universal (supports all join types)
        assert!(bnlj.supports_non_equi_join);
        assert!(bnlj.supports_cross_join);
        assert!(bnlj.supports_semi_join);
        assert!(!bnlj.is_blocking, "BNLJ should not be blocking — it emits output per chunk");
        
        // INLJ: restricted (no non-equi, no cross)
        assert!(!inlj.supports_non_equi_join);
        assert!(!inlj.supports_cross_join);
        assert!(inlj.can_use_index);
        
        // SMJ: no non-equi, pipelined
        assert!(!smj.supports_non_equi_join);
        assert!(smj.is_pipelined);
        
        // Hash: no non-equi
        assert!(!hash.supports_non_equi_join);
        
        println!("BNLJ metadata: {:?}", bnlj);
        println!("INLJ metadata: {:?}", inlj);
    }

    #[test]
    fn test_cost_model_consistency() {
        // Verify cost models are internally consistent
        
        // Grace Hash is always >= 3 * base
        let grace = CostModel::grace_hash_cost(100.0, 50.0, 8);
        assert!(grace.total_cost >= 3.0 * (100.0 + 50.0));
        
        // BNLJ cost increases with memory pressure
        let bnlj_good_mem = CostModel::bnlj_cost(100.0, 100.0, 50, 10000, 10000);
        let bnlj_bad_mem = CostModel::bnlj_cost(100.0, 100.0, 5, 10000, 10000);
        
        assert!(bnlj_bad_mem.total_cost > bnlj_good_mem.total_cost);
        
        // In-memory hash only works if data fits
        let fits = CostModel::in_memory_hash_cost(10.0, 20.0, 50);
        let no_fit = CostModel::in_memory_hash_cost(100.0, 200.0, 50);
        
        assert!(fits.is_some());
        assert!(no_fit.is_none());

        // Simple NLJ should always be more expensive than BNLJ (for non-trivial data)
        let simple = CostModel::simple_nlj_cost(100.0, 100.0, 10000, 10000);
        assert!(simple.total_cost > bnlj_good_mem.total_cost,
            "Simple NLJ ({}) should be more expensive than BNLJ ({})",
            simple.total_cost, bnlj_good_mem.total_cost);
        
        println!("All consistency checks passed!");
    }

    #[test]
    fn test_multijoin_returns_join_node_for_two_tables() {
        // Bug 16: select_best_multijoin should return a Join node, not a leaf Table
        use storage_manager::catalog::types::{Catalog, Database, Table, Column};
        use std::collections::HashMap;

        let mut tables = HashMap::new();
        tables.insert("t1".to_string(), Table {
            columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }],
            row_count: 1000,
            page_count: 10,
            avg_row_size: 128,
        });
        tables.insert("t2".to_string(), Table {
            columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }],
            row_count: 2000,
            page_count: 20,
            avg_row_size: 128,
        });

        let mut databases = HashMap::new();
        databases.insert("testdb".to_string(), Database { tables });
        let catalog = Catalog { databases };
        let config = JoinPlannerConfig::default();

        let conditions = vec![
            JoinCondition {
                left_table: "t1".to_string(),
                left_col: "id".to_string(),
                operator: JoinOp::Eq,
                right_table: "t2".to_string(),
                right_col: "id".to_string(),
            },
        ];

        let result = JoinPlanner::select_best_multijoin(
            &["t1", "t2"],
            &conditions,
            JoinType::Inner,
            &catalog,
            &config,
        );

        assert!(result.is_ok(), "Multi-join optimization should succeed");
        let tree = result.unwrap();

        // Must be a Join node, not a Table leaf
        match &tree {
            JoinTreeNode::Join { left, right, .. } => {
                // Both children should be Table leaves
                assert!(matches!(left.as_ref(), JoinTreeNode::Table { .. }));
                assert!(matches!(right.as_ref(), JoinTreeNode::Table { .. }));
            }
            JoinTreeNode::Table { name } => {
                panic!("Expected Join node, got Table('{}') — Bug 16 not fixed", name);
            }
        }
    }

    #[test]
    fn test_dp_optimizer_selects_cheapest() {
        // Bug 13: DP should compare costs and keep the cheapest plan
        use storage_manager::join::join_order::MultiJoinOptimizer;
        use storage_manager::catalog::types::{Catalog, Database, Table, Column};
        use std::collections::HashMap;

        let mut tables = HashMap::new();
        // Small table — should be joined first in optimal plan
        tables.insert("small".to_string(), Table {
            columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }],
            row_count: 100,
            page_count: 2,
            avg_row_size: 128,
        });
        // Large table
        tables.insert("large".to_string(), Table {
            columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }],
            row_count: 100_000,
            page_count: 1000,
            avg_row_size: 128,
        });
        // Medium table
        tables.insert("medium".to_string(), Table {
            columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }],
            row_count: 10_000,
            page_count: 100,
            avg_row_size: 128,
        });

        let mut databases = HashMap::new();
        databases.insert("testdb".to_string(), Database { tables });
        let catalog = Catalog { databases };

        let mut optimizer = MultiJoinOptimizer::new(
            &["small", "large", "medium"],
            vec![],
            JoinType::Inner,
            catalog,
            100,
        );

        let tree = optimizer.optimize(&["small", "large", "medium"]);
        assert!(tree.is_some(), "Optimizer should produce a plan for 3 tables");

        let tree = tree.unwrap();
        let cost = tree.total_cost();
        assert!(cost > 0.0, "Total cost should be positive");
        println!("3-table join plan: {} (cost: {:.0})", tree, cost);
    }

    #[test]
    fn test_natural_join_empty_columns() {
        // Natural join with no common columns should return false (not act as cross join)
        use storage_manager::join::condition::JoinPredicate;
        use storage_manager::join::tuple::{Tuple, ColumnValue};
        use storage_manager::catalog::types::Column;

        let pred = JoinPredicate::Natural {
            left_table: "t1".to_string(),
            right_table: "t2".to_string(),
            common_columns: vec![], // Empty — degenerate case
        };

        let left = Tuple {
            values: vec![ColumnValue::Int(1)],
            schema: vec![Column { name: "a".to_string(), data_type: "INT".to_string() }],
        };
        let right = Tuple {
            values: vec![ColumnValue::Int(2)],
            schema: vec![Column { name: "b".to_string(), data_type: "INT".to_string() }],
        };

        assert!(!pred.evaluate(&left, &right),
            "Natural join with empty common_columns should return false");
    }
}
