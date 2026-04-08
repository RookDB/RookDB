use storage_manager::executor::AggFunc;
use storage_manager::executor::AggReq;
use storage_manager::executor::expr::{BinaryOperator, ComparisonOperator, Expr};
use storage_manager::executor::hash_aggregator::execute_aggregation;
use storage_manager::executor::iterator::Executor;
use storage_manager::executor::tuple::Tuple;
use storage_manager::executor::value::Value;

pub struct MockScanner {
    pub tuples: std::vec::IntoIter<Tuple>,
}

impl Executor for MockScanner {
    fn next(&mut self) -> Option<Tuple> {
        self.tuples.next()
    }
}

#[test]
fn test_basic_numerics() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(10)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(20)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(30)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(40)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(50)], is_null_bitmap: vec![] },
    ];
    let child_node = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::Sum, col_index: Some(0) },
        AggReq { agg_type: AggFunc::Avg, col_index: Some(0) },
        AggReq { agg_type: AggFunc::Min, col_index: Some(0) },
        AggReq { agg_type: AggFunc::Max, col_index: Some(0) },
    ];
    let result = execute_aggregation(child_node, reqs, vec![], None);
    
    assert_eq!(result[0].values[0], Value::Int(150));      // SUM
    assert_eq!(result[0].values[1], Value::Int(30));   // AVG
    assert_eq!(result[0].values[2], Value::Int(10));       // MIN
    assert_eq!(result[0].values[3], Value::Int(50));       // MAX
}

#[test]
fn test_duplicates() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(10)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(10)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(20)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(20)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(30)], is_null_bitmap: vec![] },
    ];
    
    let child_node = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::SumDistinct, col_index: Some(0) },
        AggReq { agg_type: AggFunc::CountDistinct, col_index: Some(0) },
    ];
    let result = execute_aggregation(child_node, reqs, vec![], None);
    
    assert_eq!(result[0].values[0], Value::Int(60)); // Sum Distinct: 10 + 20 + 30 = 60
    assert_eq!(result[0].values[1], Value::Int(3));  // Count Distinct: 3 expected
}

#[test]
fn test_null_handling() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(10)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Null], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(20)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Null], is_null_bitmap: vec![] },
    ];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::Count, col_index: Some(0) },
        AggReq { agg_type: AggFunc::CountStar, col_index: Some(0) },
        AggReq { agg_type: AggFunc::Sum, col_index: Some(0) },
    ];
    let res = execute_aggregation(child, reqs, vec![], None);
    
    assert_eq!(res[0].values[0], Value::Int(2));   // COUNT (ignores NULL)
    assert_eq!(res[0].values[1], Value::Int(4));   // COUNT(*) (includes NULL)
    assert_eq!(res[0].values[2], Value::Int(30));  // SUM
}

#[test]
fn test_booleans() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Boolean(true)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Null], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Boolean(false)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Boolean(true)], is_null_bitmap: vec![] },
    ];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::BoolAnd, col_index: Some(0) },
        AggReq { agg_type: AggFunc::BoolOr, col_index: Some(0) },
    ];
    let res = execute_aggregation(child, reqs, vec![], None);
    
    assert_eq!(res[0].values[0], Value::Boolean(false)); // AND = false
    assert_eq!(res[0].values[1], Value::Boolean(true));  // OR = true
}

#[test]
fn test_empty_table() {
    let dummy_data: Vec<Tuple> = vec![];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::Sum, col_index: Some(0) },
        AggReq { agg_type: AggFunc::CountStar, col_index: Some(0) },
    ];
    let res = execute_aggregation(child, reqs, vec![], None);
    
    assert_eq!(res[0].values[0], Value::Null);   // Empty SUM is NULL
    assert_eq!(res[0].values[1], Value::Int(0)); // Empty COUNT(*) is 0
}

#[test]
fn test_single_row_variance() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(10)], is_null_bitmap: vec![] },
    ];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![
        AggReq { agg_type: AggFunc::Variance, col_index: Some(0) },
        AggReq { agg_type: AggFunc::StdDev, col_index: Some(0) },
    ];
    let res = execute_aggregation(child, reqs, vec![], None);
    
    assert_eq!(res[0].values[0], Value::Null); // Cannot compute variance on 1 row
    assert_eq!(res[0].values[1], Value::Null);
}

#[test]
fn test_group_by_single_column() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Int(100)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Int(200)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("IT".to_string()), Value::Int(300)], is_null_bitmap: vec![] },
    ];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![AggReq { agg_type: AggFunc::Sum, col_index: Some(1) }];
    let res = execute_aggregation(child, reqs, vec![0], None);
    
    assert_eq!(res.len(), 2);
    let hr_res = res.iter().find(|t| t.values[0] == Value::Text("HR".to_string())).unwrap();
    let it_res = res.iter().find(|t| t.values[0] == Value::Text("IT".to_string())).unwrap();
    
    assert_eq!(hr_res.values[1], Value::Int(300));
    assert_eq!(it_res.values[1], Value::Int(300));
}

#[test]
fn test_group_by_multiple_columns() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Manager".to_string()), Value::Int(100)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(200)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(210)], is_null_bitmap: vec![] },
    ];
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![AggReq { agg_type: AggFunc::Sum, col_index: Some(2) }];
    let res = execute_aggregation(child, reqs, vec![0, 1], None);
    
    assert_eq!(res.len(), 2);
    let hr_staff = res.iter().find(|t| t.values[1] == Value::Text("Staff".to_string())).unwrap();
    let hr_manager = res.iter().find(|t| t.values[1] == Value::Text("Manager".to_string())).unwrap();
    
    assert_eq!(hr_staff.values[2], Value::Int(410));
    assert_eq!(hr_manager.values[2], Value::Int(100));
}

#[test]
fn test_having_clause_filter() {
    let dummy_data = vec![
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Manager".to_string()), Value::Int(100)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(200)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(210)], is_null_bitmap: vec![] },
    ];
    
    let having_expr = Expr::Comparison {
        left: Box::new(Expr::ColumnRef(2)), 
        op: ComparisonOperator::Gt,
        right: Box::new(Expr::Constant(Value::Int(150))),
    };
    
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let reqs = vec![AggReq { agg_type: AggFunc::Sum, col_index: Some(2) }];
    let res = execute_aggregation(child, reqs, vec![0, 1], Some(having_expr));
    
    assert_eq!(res.len(), 1); // Only HR-Staff should remain (sum > 150)
    assert_eq!(res[0].values[1], Value::Text("Staff".to_string()));
    assert_eq!(res[0].values[2], Value::Int(410));
}

#[test]
fn test_having_column_to_column() {
    // Scenario: salary > bonus
    // Input: [Dept, Salary, Bonus]
    // Group by Dept (col 0). Aggregate: SUM(Salary) col 1, SUM(Bonus) col 2.
    // Output tuple will be [Dept, SUM(Salary), SUM(Bonus)]
    let dummy_data = vec![
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Int(100), Value::Int(150)], is_null_bitmap: vec![] }, // HR: Sal 100, Bonus 150 (Sal < Bonus) -> Drops
        Tuple { values: vec![Value::Text("IT".to_string()), Value::Int(300), Value::Int(200)], is_null_bitmap: vec![] }, // IT: Sal 300, Bonus 200 (Sal > Bonus) -> Keeps
    ];
    let reqs = vec![
        AggReq { agg_type: AggFunc::Sum, col_index: Some(1) },
        AggReq { agg_type: AggFunc::Sum, col_index: Some(2) },
    ];
    
    // expression-to-expression comparison
    let expr = Expr::Comparison {
        left: Box::new(Expr::ColumnRef(1)), // SUM(Salary) mapped to index 1 of output
        op: ComparisonOperator::Gt,
        right: Box::new(Expr::ColumnRef(2)), // SUM(Bonus) mapped to index 2 of output
    };
    
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let res = execute_aggregation(child, reqs, vec![0], Some(expr));
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].values[0], Value::Text("IT".to_string()));
    assert_eq!(res[0].values[1], Value::Int(300));
}

#[test]
fn test_having_expr_to_constant() {
    // Scenario: age + 1 > 18
    // Grouping by Age (col 0).
    // Output tuple: [Age, COUNT(*)]
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(17), Value::Text("A".to_string())], is_null_bitmap: vec![] }, // 17+1 = 18 > 18 (False) -> Drops
        Tuple { values: vec![Value::Int(20), Value::Text("B".to_string())], is_null_bitmap: vec![] }, // 20+1 = 21 > 18 (True) -> Keeps
    ];
    let reqs = vec![AggReq { agg_type: AggFunc::CountStar, col_index: None }];
    
    // Arithmetic operations within HAVING 
    let expr = Expr::Comparison {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef(0)), // Age mapped to index 0 of output 
            op: BinaryOperator::Add,
            right: Box::new(Expr::Constant(Value::Int(1))),
        }),
        op: ComparisonOperator::Gt,
        right: Box::new(Expr::Constant(Value::Int(18))),
    };
    
    let child = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    let res = execute_aggregation(child, reqs, vec![0], Some(expr));
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].values[0], Value::Int(20)); // Only age 20 remains
    assert_eq!(res[0].values[1], Value::Int(1));  // Count is 1
}

#[test]
fn test_aggregation_overflow_handling() {
    // Scenario: Adding so many numbers result OVERFLOW
    let dummy_data = vec![
        Tuple { values: vec![Value::Int(i32::MAX)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(1)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Int(1)], is_null_bitmap: vec![] },
    ];
    let child_node = Box::new(MockScanner { tuples: dummy_data.into_iter() });
    
    // Checking SUM which invokes `checked_add` internally to safely return Null
    let reqs = vec![AggReq { agg_type: AggFunc::Sum, col_index: Some(0) }];
    let result = execute_aggregation(child_node, reqs, vec![], None);
    
    assert_eq!(result[0].values[0], Value::Null); // SUM overflows, returns Null as safely handled in backend
}