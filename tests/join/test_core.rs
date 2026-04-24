//! Comprehensive tests for the join module — moved from inline #[cfg(test)] blocks.

use storage_manager::join::{JoinType, JoinAlgorithmType, ColumnValue, Tuple};
use storage_manager::join::condition::{JoinCondition, JoinOp, JoinPredicate, evaluate_conditions};
use storage_manager::join::cost_model::{CostModel, CardinalityEstimator, SkewEstimate};
use storage_manager::join::planner::JoinPlannerConfig;
use storage_manager::join::join_order::{RelationSet, JoinTreeNode};
use storage_manager::join::bloom_filter::BloomFilter;
use storage_manager::catalog::types::Column;

// ══════════════════════════════════════════════════════════════════════
// Condition tests (moved from condition.rs)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn condition_equality_check() {
    let c = JoinCondition {
        left_table: "t1".into(), left_col: "id".into(),
        operator: JoinOp::Eq,
        right_table: "t2".into(), right_col: "id".into(),
    };
    assert!(c.is_equality());
}

#[test]
fn condition_non_equality_check() {
    let c = JoinCondition {
        left_table: "t1".into(), left_col: "a".into(),
        operator: JoinOp::Gt,
        right_table: "t2".into(), right_col: "b".into(),
    };
    assert!(!c.is_equality());
}

#[test]
fn condition_and_predicate_has_equi() {
    let p = JoinPredicate::And(vec![
        JoinCondition { left_table: "t1".into(), left_col: "a".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "a".into() },
        JoinCondition { left_table: "t1".into(), left_col: "b".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "b".into() },
    ]);
    assert!(p.has_equi_condition());
}

#[test]
fn condition_antijoin_null_returns_false() {
    let pred = JoinPredicate::AntiJoinExpr { left_col: "id".into(), right_subquery_values: vec!["1".into(), "2".into()] };
    let left = Tuple { values: vec![ColumnValue::Null], schema: vec![] };
    let right = Tuple { values: vec![], schema: vec![] };
    assert!(!pred.evaluate(&left, &right));
}

#[test]
fn condition_antijoin_missing_col_returns_false() {
    let pred = JoinPredicate::AntiJoinExpr { left_col: "missing".into(), right_subquery_values: vec!["1".into()] };
    let left = Tuple { values: vec![], schema: vec![] };
    let right = Tuple { values: vec![], schema: vec![] };
    assert!(!pred.evaluate(&left, &right));
}

#[test]
fn condition_antijoin_value_not_in_set() {
    let pred = JoinPredicate::AntiJoinExpr { left_col: "id".into(), right_subquery_values: vec!["1".into(), "2".into()] };
    let left = Tuple {
        values: vec![ColumnValue::Int(5)],
        schema: vec![Column { name: "id".into(), data_type: "INT".into() }],
    };
    let right = Tuple { values: vec![], schema: vec![] };
    assert!(pred.evaluate(&left, &right));
}

#[test]
fn condition_disjunctive_detection() {
    let c1 = JoinCondition { left_table: "t1".into(), left_col: "id".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "id".into() };
    let c2 = JoinCondition { left_table: "t1".into(), left_col: "name".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "name".into() };
    assert!(!JoinPredicate::Simple(c1.clone()).is_disjunctive());
    assert!(!JoinPredicate::And(vec![c1.clone(), c2.clone()]).is_disjunctive());
    assert!(JoinPredicate::Or(vec![c1, c2]).is_disjunctive());
}

#[test]
fn condition_or_predicate_is_disjunctive() {
    let pred = JoinPredicate::Or(vec![
        JoinCondition { left_table: "t1".into(), left_col: "id".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "id".into() },
        JoinCondition { left_table: "t1".into(), left_col: "name".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "name".into() },
    ]);
    assert!(pred.is_disjunctive());
}

#[test]
fn condition_semijoin_matches() {
    let pred = JoinPredicate::SemiJoinExpr { left_col: "id".into(), right_subquery_values: vec!["5".into()] };
    let left = Tuple {
        values: vec![ColumnValue::Int(5)],
        schema: vec![Column { name: "id".into(), data_type: "INT".into() }],
    };
    let right = Tuple { values: vec![], schema: vec![] };
    assert!(pred.evaluate(&left, &right));
}

#[test]
fn condition_semijoin_no_match() {
    let pred = JoinPredicate::SemiJoinExpr { left_col: "id".into(), right_subquery_values: vec!["10".into()] };
    let left = Tuple {
        values: vec![ColumnValue::Int(5)],
        schema: vec![Column { name: "id".into(), data_type: "INT".into() }],
    };
    let right = Tuple { values: vec![], schema: vec![] };
    assert!(!pred.evaluate(&left, &right));
}

#[test]
fn condition_evaluate_conditions_all_must_pass() {
    let schema = vec![
        Column { name: "a".into(), data_type: "INT".into() },
        Column { name: "b".into(), data_type: "INT".into() },
    ];
    let left = Tuple { values: vec![ColumnValue::Int(1), ColumnValue::Int(2)], schema: schema.clone() };
    let right = Tuple { values: vec![ColumnValue::Int(1), ColumnValue::Int(3)], schema: schema.clone() };
    let conds = vec![
        JoinCondition { left_table: "t1".into(), left_col: "a".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "a".into() },
        JoinCondition { left_table: "t1".into(), left_col: "b".into(), operator: JoinOp::Eq, right_table: "t2".into(), right_col: "b".into() },
    ];
    // a matches but b doesn't → AND fails
    assert!(!evaluate_conditions(&conds, &left, &right));
}

#[test]
fn condition_natural_join_predicate() {
    let schema = vec![Column { name: "id".into(), data_type: "INT".into() }];
    let left = Tuple { values: vec![ColumnValue::Int(1)], schema: schema.clone() };
    let right = Tuple { values: vec![ColumnValue::Int(1)], schema: schema.clone() };
    let pred = JoinPredicate::Natural { left_table: "t1".into(), right_table: "t2".into(), common_columns: vec!["id".into()] };
    assert!(pred.evaluate(&left, &right));
}

// ══════════════════════════════════════════════════════════════════════
// Bloom filter tests (moved from bloom_filter.rs)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn bloom_basic_insert_and_lookup() {
    let mut bf = BloomFilter::new(10_000);
    bf.insert(b"hello");
    bf.insert(b"world");
    assert!(bf.might_contain(b"hello"));
    assert!(bf.might_contain(b"world"));
}

#[test]
fn bloom_no_false_negatives() {
    let mut bf = BloomFilter::new(10_000);
    for i in 0..100u32 { bf.insert(&i.to_le_bytes()); }
    for i in 0..100u32 { assert!(bf.might_contain(&i.to_le_bytes()), "false negative for {}", i); }
}

#[test]
fn bloom_deterministic_hashing() {
    let d = b"test_data";
    assert_eq!(BloomFilter::fnv1a_hash(d, 0), BloomFilter::fnv1a_hash(d, 0));
    assert_ne!(BloomFilter::fnv1a_hash(d, 1), BloomFilter::fnv1a_hash(d, 2));
}

#[test]
fn bloom_custom_hash_functions() {
    let mut bf = BloomFilter::with_hash_functions(10_000, 5);
    assert_eq!(bf.stats().hash_functions, 5);
    bf.insert(b"test");
    assert!(bf.might_contain(b"test"));
}

#[test]
fn bloom_saturation_detection() {
    let cap = 1_000usize;
    let mut bf = BloomFilter::new(cap);
    assert!(!bf.is_saturated());
    let threshold = ((cap as f64) * (0.693 / 14.0)) as u32 + 1;
    for i in 0..threshold { bf.insert(&i.to_le_bytes()); }
    assert!(bf.is_saturated());
    assert!(bf.saturation_percent() >= 0.9);
}

#[test]
fn bloom_fpr_increases_with_load() {
    let mut lo = BloomFilter::new(10_000);
    let mut hi = BloomFilter::new(1_000);
    for i in 0..10u32 { lo.insert(&i.to_le_bytes()); }
    for i in 0..500u32 { hi.insert(&i.to_le_bytes()); }
    assert!(hi.false_positive_rate() > lo.false_positive_rate() * 5.0);
}

#[test]
fn bloom_clear_resets() {
    let mut bf = BloomFilter::new(1_000);
    bf.insert(b"data");
    assert!(bf.might_contain(b"data"));
    bf.clear();
    assert!(!bf.might_contain(b"data"));
    assert_eq!(bf.stats().elements_inserted, 0);
}

// ══════════════════════════════════════════════════════════════════════
// Cost model tests (moved from cost_model.rs)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cost_bnlj_basic() {
    let cost = CostModel::bnlj_cost(100.0, 50.0, 20, 10_000, 5_000);
    assert!(cost.total_cost > 100.0);
    assert!(!cost.can_pipeline);
}

#[test]
fn cost_smj_presort_saves() {
    let unsorted = CostModel::smj_cost(100.0, 50.0, 20, false, false);
    let sorted = CostModel::smj_cost(100.0, 50.0, 20, true, true);
    assert!(sorted.total_cost < unsorted.total_cost);
    assert_eq!(sorted.cost_component_sort, 0.0);
}

#[test]
fn cost_in_memory_hash_feasibility() {
    assert!(CostModel::in_memory_hash_cost(10.0, 5.0, 20).is_some());
    assert!(CostModel::in_memory_hash_cost(100.0, 150.0, 20).is_none());
}

#[test]
fn cost_simple_nlj() {
    let cost = CostModel::simple_nlj_cost(10.0, 20.0, 100, 200);
    assert_eq!(cost.total_cost, 10.0 + 100.0 * 20.0);
}

#[test]
fn cost_grace_hash() {
    let cost = CostModel::grace_hash_cost(100.0, 200.0, 8);
    assert_eq!(cost.total_cost, 3.0 * 300.0);
}

#[test]
fn cost_hybrid_hash() {
    let cost = CostModel::hybrid_hash_cost(100.0, 200.0, 50, 8);
    let expected = 2.1 * 300.0;
    assert!((cost.total_cost - expected).abs() < 0.01);
}

#[test]
fn cost_symmetric_hash() {
    let cost = CostModel::symmetric_hash_cost(100.0, 200.0, 50, 1000, 2000);
    assert_eq!(cost.total_cost, 300.0);
    assert!(cost.can_pipeline);
}

#[test]
fn cost_broadcast_hash() {
    let cost = CostModel::broadcast_hash_cost(10.0, 200.0, 4);
    assert_eq!(cost.total_cost, 10.0 * 4.0 + 200.0);
}

#[test]
fn cost_inlj() {
    let cost = CostModel::inlj_cost(50.0, 4.0, 5000, 0.5, 1.0);
    // 50 + 5000*(4 + 0.5*1) = 50 + 22500 = 22550
    assert_eq!(cost.total_cost, 22550.0);
}

#[test]
fn cost_estimate_pages() {
    let pages = CostModel::estimate_pages(1000, 100, 4096);
    // 1000*100 = 100_000 bytes, /4096 = 24.41 → 25
    assert_eq!(pages, 25.0);
}

#[test]
fn cost_cardinality_estimator() {
    let est = CardinalityEstimator::new(1000, 500, 0.1);
    assert_eq!(est.estimated_output_rows(), 50_000);
}

#[test]
fn cost_cardinality_clamp() {
    let est = CardinalityEstimator::new(100, 100, 2.0); // selectivity clamped to 1.0
    assert_eq!(est.estimated_output_rows(), 10_000);
}

// ══════════════════════════════════════════════════════════════════════
// Planner tests (moved from planner.rs)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn planner_config_defaults() {
    let c = JoinPlannerConfig::default();
    assert_eq!(c.available_memory_pages, 100);
    assert!(c.enable_hash_join);
    assert!(c.enable_sort_merge);
    assert!(c.enable_index_scan);
    assert_eq!(c.num_hash_partitions, 8);
}

#[test]
fn planner_equi_condition_via_is_equality() {
    let cond = JoinCondition {
        left_table: "t1".into(), left_col: "id".into(),
        operator: JoinOp::Eq,
        right_table: "t2".into(), right_col: "id".into(),
    };
    assert!(cond.is_equality());
}

#[test]
fn planner_non_equi_condition() {
    let cond = JoinCondition {
        left_table: "t1".into(), left_col: "x".into(),
        operator: JoinOp::Lt,
        right_table: "t2".into(), right_col: "y".into(),
    };
    assert!(!cond.is_equality());
}

// ══════════════════════════════════════════════════════════════════════
// Join order / DP tests (moved from join_order.rs)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn join_order_relation_set_ops() {
    let a = RelationSet::new(&["t1", "t2"]);
    let b = RelationSet::single("t3");
    let u = a.union(&b);
    assert_eq!(u.size(), 3);
    assert!(u.contains("t1"));
    assert!(u.contains("t3"));
    assert!(a.is_disjoint(&b));
}

#[test]
fn join_order_relation_set_minus() {
    let a = RelationSet::new(&["t1", "t2", "t3"]);
    let b = RelationSet::new(&["t2"]);
    let diff = a.minus(&b);
    assert_eq!(diff.size(), 2);
    assert!(diff.contains("t1"));
    assert!(diff.contains("t3"));
    assert!(!diff.contains("t2"));
}

#[test]
fn join_order_tree_cost() {
    let tree = JoinTreeNode::Join {
        left: Box::new(JoinTreeNode::Table { name: "t1".into() }),
        right: Box::new(JoinTreeNode::Table { name: "t2".into() }),
        algorithm: JoinAlgorithmType::BlockNLJ,
        cost: 100.0,
    };
    assert_eq!(tree.total_cost(), 100.0);
}

#[test]
fn join_order_nested_tree_cost() {
    let tree = JoinTreeNode::Join {
        left: Box::new(JoinTreeNode::Join {
            left: Box::new(JoinTreeNode::Table { name: "t1".into() }),
            right: Box::new(JoinTreeNode::Table { name: "t2".into() }),
            algorithm: JoinAlgorithmType::BlockNLJ, cost: 50.0,
        }),
        right: Box::new(JoinTreeNode::Table { name: "t3".into() }),
        algorithm: JoinAlgorithmType::SortMergeJoin, cost: 30.0,
    };
    assert_eq!(tree.total_cost(), 80.0);
}

#[test]
fn join_order_tree_relations() {
    let tree = JoinTreeNode::Join {
        left: Box::new(JoinTreeNode::Table { name: "t1".into() }),
        right: Box::new(JoinTreeNode::Table { name: "t2".into() }),
        algorithm: JoinAlgorithmType::BlockNLJ, cost: 10.0,
    };
    let rels = tree.relations();
    assert!(rels.contains("t1"));
    assert!(rels.contains("t2"));
    assert_eq!(rels.size(), 2);
}

// ══════════════════════════════════════════════════════════════════════
// Skew estimate tests
// ══════════════════════════════════════════════════════════════════════

#[test]
fn skew_uniform_distribution() {
    let s = SkewEstimate::estimate_skew(1000, 10, 100);
    assert!(!s.is_skewed);
    assert!(s.fallback_algorithm.is_none());
}

#[test]
fn skew_heavy_distribution() {
    let s = SkewEstimate::estimate_skew(1000, 10, 300);
    assert!(s.is_skewed);
    assert!(s.fallback_algorithm.is_some());
}

// ══════════════════════════════════════════════════════════════════════
// Algorithm metadata tests
// ══════════════════════════════════════════════════════════════════════

#[test]
fn algorithm_bnlj_supports_all() {
    let meta = storage_manager::join::algorithm::AlgorithmMetadata::for_bnlj();
    assert!(meta.supports_equi_join);
    assert!(meta.supports_non_equi_join);
    assert!(meta.supports_cross_join);
    assert!(meta.can_execute(JoinType::Inner, true, false));
    assert!(meta.can_execute(JoinType::Cross, false, false));
    assert!(meta.can_execute(JoinType::FullOuter, false, true));
}

#[test]
fn algorithm_smj_no_cross() {
    let meta = storage_manager::join::algorithm::AlgorithmMetadata::for_smj();
    assert!(!meta.supports_cross_join);
    assert!(!meta.can_execute(JoinType::Cross, false, false));
    assert!(meta.can_execute(JoinType::Inner, true, false));
}

#[test]
fn algorithm_hash_no_non_equi() {
    let meta = storage_manager::join::algorithm::AlgorithmMetadata::for_hash_join();
    assert!(!meta.supports_non_equi_join);
    assert!(!meta.can_execute(JoinType::Inner, false, true));
}
