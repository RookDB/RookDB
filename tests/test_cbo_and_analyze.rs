//! Integration tests for Critique 5: CBO (Cost-Based Optimizer) & ANALYZE.
//!
//! Verifies:
//! - ANALYZE TABLE collects HyperLogLog + histograms and persists them into sys_statistics.
//! - CREATE INDEX collects statistics during heap scan.
//! - CBO CostModel correctly selects IndexScan for selective lookups and SeqScan for unselective queries.
//! - HashJoin sizing swaps build and probe when right child is smaller, preserving row layout.

mod common;

use storage_manager::backend::executor::analyze::analyze_table;
use storage_manager::backend::executor::create_index::create_index;
use storage_manager::backend::executor::physical::planner::cost::CostModel;
use storage_manager::backend::executor::physical::planner::PhysicalPlanner;
use storage_manager::backend::system_table::load_column_statistics;
use storage_manager::catalog::{
    create_database, create_table, load_catalog, save_catalog, Catalog, Column,
};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

fn setup_cbo_table(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "employees",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(50)),
            column("dept", DataType::Varchar(50)),
            column("salary", DataType::Int),
        ],
    );
    save_catalog(&catalog).unwrap();

    // Insert 20 rows with varying values
    for i in 1..=20 {
        let dept = match i % 4 {
            0 => "Engineering",
            1 => "Marketing",
            2 => "Sales",
            _ => "Finance",
        };
        let salary = (40000 + i * 2500).to_string();
        let id = i.to_string();
        let name = format!("Emp{}", i);
        insert_single_tuple(&catalog, db, "employees", &[&id, &name, dept, &salary]).unwrap();
    }
    catalog
}

#[test]
fn test_analyze_table_persists_statistics() {
    let _ws = common::TestWorkspace::new("cbo", "analyze_persists");
    let db = "test_cbo_analyze_db";
    let catalog = setup_cbo_table(db);

    // Run ANALYZE TABLE
    let stats_res = analyze_table(&catalog, db, "employees").expect("ANALYZE should succeed");
    assert_eq!(stats_res.columns_analyzed, 4, "Should analyze all 4 columns");

    // Check persistence in sys_statistics
    let stats = load_column_statistics(db, "employees").expect("Should load column statistics");
    assert_eq!(stats.len(), 4, "Should have 4 column stat rows in sys_statistics");

    // Verify 'id' column statistics
    let id_stat = stats.iter().find(|s| s.column_name.eq_ignore_ascii_case("id")).unwrap();
    assert_eq!(id_stat.total_count, 20);
    assert_eq!(id_stat.null_count, 0);
    assert!(id_stat.distinct_count >= 15, "HLL distinct estimate for 20 unique values should be close to 20, got {}", id_stat.distinct_count);
    assert!(!id_stat.histogram.buckets.is_empty(), "Histogram buckets should be created");

    // Verify 'salary' column statistics
    let salary_stat = stats.iter().find(|s| s.column_name.eq_ignore_ascii_case("salary")).unwrap();
    assert_eq!(salary_stat.total_count, 20);
    assert!(!salary_stat.histogram.buckets.is_empty());
}

#[test]
fn test_create_index_collects_column_statistics() {
    let _ws = common::TestWorkspace::new("cbo", "create_idx_stats");
    let db = "test_cbo_create_idx_stats_db";
    let catalog = setup_cbo_table(db);

    // CREATE INDEX on 'salary'
    let cols = vec!["salary".to_string()];
    create_index(&catalog, db, "employees", "idx_emp_salary", &cols).expect("CREATE INDEX should succeed");

    // Verify statistics were collected and persisted for 'salary'
    let stats = load_column_statistics(db, "employees").expect("Should load column statistics");
    let salary_stat = stats.iter().find(|s| s.column_name.eq_ignore_ascii_case("salary"));
    assert!(salary_stat.is_some(), "CREATE INDEX should collect and persist column statistics");
    let s = salary_stat.unwrap();
    assert_eq!(s.total_count, 20);
}

#[test]
fn test_cbo_cost_model_decisions() {
    // Unclustered IndexScan vs SeqScan
    let pages = 100u32;
    let tuples = 10_000u64;

    let c_seq = CostModel::cost_seq_scan(pages, tuples);

    // 1. Point lookup (0.01% selectivity)
    let c_point = CostModel::cost_index_scan(pages, tuples, 0.0001);
    assert!(c_point < c_seq, "Point lookup must be cheaper via index: point={}, seq={}", c_point, c_seq);

    // 2. Highly selective range (0.1% selectivity)
    let c_sel_range = CostModel::cost_index_scan(pages, tuples, 0.001);
    assert!(c_sel_range < c_seq, "Selective range must be cheaper via index: range={}, seq={}", c_sel_range, c_seq);

    // 3. Unselective range (40% selectivity)
    let c_unsel = CostModel::cost_index_scan(pages, tuples, 0.40);
    assert!(c_seq < c_unsel, "SeqScan must be cheaper than IndexScan for 40% selectivity: seq={}, idx={}", c_seq, c_unsel);
}

#[test]
fn test_hash_join_sizing_and_swap_execution() {
    use storage_manager::backend::executor::physical::operators::{
        HashJoinOperator, PhysicalOperator,
    };
    use storage_manager::backend::executor::physical::tuple::{ColumnInfo, Tuple};
    use storage_manager::backend::executor::physical::expr::Expr;
    use storage_manager::types::DataValue;

    // Build mock child: left has 2 rows [id, val], right has 1 row [id, name]
    struct MockScan {
        tuples: Vec<Tuple>,
        schema: Vec<ColumnInfo>,
        pos: usize,
    }
    impl PhysicalOperator for MockScan {
        fn next(&mut self) -> storage_manager::backend::error::RookResult<Option<Tuple>> {
            if self.pos < self.tuples.len() {
                let t = self.tuples[self.pos].clone();
                self.pos += 1;
                Ok(Some(t))
            } else {
                Ok(None)
            }
        }
        fn schema(&self) -> &[ColumnInfo] { &self.schema }
        fn name(&self) -> &'static str { "MockScan" }
        fn estimate_cardinality(&self) -> usize { self.tuples.len() }
    }

    let left_schema = vec![
        ColumnInfo { name: "l_id".into(), data_type: DataType::Int, table: Some("left".into()) },
        ColumnInfo { name: "l_val".into(), data_type: DataType::Varchar(20), table: Some("left".into()) },
    ];
    let right_schema = vec![
        ColumnInfo { name: "r_id".into(), data_type: DataType::Int, table: Some("right".into()) },
        ColumnInfo { name: "r_val".into(), data_type: DataType::Varchar(20), table: Some("right".into()) },
    ];

    let left_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Varchar("Left1".into()))]),
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Varchar("Left2".into()))]),
    ];
    let right_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Varchar("Right1".into()))]),
    ];

    let left_op = Box::new(MockScan { tuples: left_tuples, schema: left_schema.clone(), pos: 0 });
    let right_op = Box::new(MockScan { tuples: right_tuples, schema: right_schema.clone(), pos: 0 });

    // Since right child has 1 row and left child has 2 rows:
    // Sizing chooses to build on right (smaller) and probe with left (larger), swapped = true!
    let build_keys = vec![Expr::Column { table: Some("right".into()), column: "r_id".into() }];
    let probe_keys = vec![Expr::Column { table: Some("left".into()), column: "l_id".into() }];

    let mut hj = HashJoinOperator::new_with_swap(
        right_op,
        left_op,
        build_keys,
        probe_keys,
        None,
        true, // swapped
    );

    // Verify output schema preserves [left_columns..., right_columns...]
    let schema = hj.schema();
    assert_eq!(schema.len(), 4);
    assert_eq!(schema[0].name, "l_id");
    assert_eq!(schema[1].name, "l_val");
    assert_eq!(schema[2].name, "r_id");
    assert_eq!(schema[3].name, "r_val");

    // Verify next() produces joined row with correct left+right layout
    let result = hj.next().unwrap().expect("Should produce 1 matched row");
    assert_eq!(result.values.len(), 4);
    assert_eq!(result.values[0], Some(DataValue::Int(1)));
    assert_eq!(result.values[1], Some(DataValue::Varchar("Left1".into())));
    assert_eq!(result.values[2], Some(DataValue::Int(1)));
    assert_eq!(result.values[3], Some(DataValue::Varchar("Right1".into())));

    assert!(hj.next().unwrap().is_none(), "No further rows should match");
}

#[test]
fn test_cbo_planner_selects_index_scan_for_point_lookup() {
    let _ws = common::TestWorkspace::new("cbo", "planner_choice");
    let db = "test_cbo_planner_db";
    let catalog = setup_cbo_table(db);

    // Create index on salary
    let cols = vec!["salary".to_string()];
    create_index(&catalog, db, "employees", "idx_emp_salary", &cols).unwrap();

    let planner = PhysicalPlanner::new(catalog.clone(), db.to_string());
    let ts = rook_ast::logical::LogicalTableScan {
        table: "employees".to_string(),
        alias: None,
        schema: rook_ast::logical::ColumnSchema::empty(),
        system_table_name: None,
    };

    // 1. Point lookup: WHERE salary = 42500 -> should choose IndexScanOperator
    let pred_point = rook_ast::PredicateNode::Compare {
        left: Box::new(rook_ast::ExprNode::Column("salary".to_string())),
        op: rook_ast::ComparisonOp::Eq,
        right: Box::new(rook_ast::ExprNode::Constant(rook_ast::ConstantValue::Int(42500))),
    };
    let planned_op = planner.try_plan_index_scan_with_predicate(&ts, &pred_point).unwrap();
    assert!(planned_op.is_some(), "CBO should choose IndexScan for selective point lookup");
    let op = planned_op.unwrap();
    assert_eq!(op.name(), "IndexScan(Point)");

    // 2. Unselective range lookup on table where selectivity is high (> 30%):
    // e.g. WHERE salary >= 40000 (all 20 rows match, 100% selectivity)
    let pred_unsel = rook_ast::PredicateNode::Compare {
        left: Box::new(rook_ast::ExprNode::Column("salary".to_string())),
        op: rook_ast::ComparisonOp::Ge,
        right: Box::new(rook_ast::ExprNode::Constant(rook_ast::ConstantValue::Int(40000))),
    };
    let planned_op2 = planner.try_plan_index_scan_with_predicate(&ts, &pred_unsel).unwrap();
    assert!(planned_op2.is_none(), "CBO should reject IndexScan for unselective range predicate (falling back to SeqScan)");
}
