use super::super::expr::{ComparisonOp, Expr, Predicate};
use super::super::tuple::{ColumnInfo, Tuple};
use super::*;
use crate::backend::error::RookResult;

use crate::types::datatype::DataType;
use crate::types::value::DataValue;

/// A mock operator that yields a fixed set of tuples.
struct MockOperator {
    tuples: Vec<Tuple>,
    pos: usize,
    schema: Vec<ColumnInfo>,
}

impl MockOperator {
    fn new(tuples: Vec<Tuple>, schema: Vec<ColumnInfo>) -> Self {
        Self {
            tuples,
            pos: 0,
            schema,
        }
    }
}

impl PhysicalOperator for MockOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.pos < self.tuples.len() {
            let t = self.tuples[self.pos].clone();
            self.pos += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.pos = 0;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Mock"
    }
}

fn int_schema() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            merged: false,
            name: "a".into(),
            data_type: DataType::Int,
            table: None,
        },
        ColumnInfo {
            merged: false,
            name: "b".into(),
            data_type: DataType::Int,
            table: None,
        },
    ]
}

fn int_tuples(data: Vec<(Option<i32>, Option<i32>)>) -> (Vec<Tuple>, Vec<ColumnInfo>) {
    let schema = int_schema();
    let tuples = data
        .into_iter()
        .map(|(a, b)| Tuple::new(vec![a.map(DataValue::Int), b.map(DataValue::Int)]))
        .collect();
    (tuples, schema)
}

#[test]
fn test_mock_operator() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(2)), (Some(3), Some(4))]);
    let mut op = MockOperator::new(tuples, schema);
    assert!(op.next().unwrap().is_some());
    assert!(op.next().unwrap().is_some());
    assert!(op.next().unwrap().is_none());
}

#[test]
fn test_filter_operator() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(3), Some(30)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut filter = FilterOperator::new(
        Box::new(child),
        Predicate::Compare(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            ComparisonOp::GreaterThan,
            Expr::Constant(DataValue::Int(1)),
        ),
    );

    let t1 = filter.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    let t2 = filter.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(3)));
    assert!(filter.next().unwrap().is_none());
}

#[test]
fn test_limit_operator() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(3), Some(30)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut limit = LimitOperator::new(Box::new(child), 2, 0);

    assert!(limit.next().unwrap().is_some());
    assert!(limit.next().unwrap().is_some());
    assert!(limit.next().unwrap().is_none());
}

#[test]
fn test_limit_with_offset() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(3), Some(30)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut limit = LimitOperator::new(Box::new(child), 2, 1);

    let t1 = limit.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    let t2 = limit.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(3)));
    assert!(limit.next().unwrap().is_none());
}

/// A child that fails if it is read: an operator that needs none of its
/// rows must not pull any.
struct UnreadOperator(Vec<ColumnInfo>);

impl PhysicalOperator for UnreadOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        panic!("the child was read");
    }
    fn schema(&self) -> &[ColumnInfo] {
        &self.0
    }
    fn reset(&mut self) -> RookResult<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "Unread"
    }
}

#[test]
fn test_limit_zero_with_offset_reads_nothing() {
    let mut limit = LimitOperator::new(Box::new(UnreadOperator(int_schema())), 0, 1000);
    assert!(limit.next().unwrap().is_none());
}

#[test]
fn test_limit_zero() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(10)), (Some(2), Some(20))]);
    let child = MockOperator::new(tuples, schema);
    let mut limit = LimitOperator::new(Box::new(child), 0, 0);

    assert!(limit.next().unwrap().is_none());
}

#[test]
fn test_projection_operator() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
    let child = MockOperator::new(tuples, schema);
    let mut proj =
        ProjectionOperator::from_indices(Box::new(child), &[1], &["b".to_string()]).unwrap();

    let t = proj.next().unwrap().unwrap();
    assert_eq!(t.values.len(), 1);
    assert_eq!(t.values[0], Some(DataValue::Int(10)));
    assert_eq!(proj.schema()[0].name, "b");
    assert!(proj.next().unwrap().is_none());
}

#[test]
fn test_star_projection() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
    let child = MockOperator::new(tuples, schema);
    let mut proj = ProjectionOperator::star(Box::new(child));

    let t = proj.next().unwrap().unwrap();
    assert_eq!(t.values.len(), 2);
    assert!(proj.next().unwrap().is_none());
}

#[test]
fn test_distinct_operator() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(1), Some(10)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut distinct = DistinctOperator::new(Box::new(child));

    let t1 = distinct.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(1)));
    let t2 = distinct.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    assert!(distinct.next().unwrap().is_none());
}

#[test]
fn test_distinct_operator_reset() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(1), Some(10)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut distinct = DistinctOperator::new(Box::new(child));

    // First pass: should get 2 unique tuples
    let t1 = distinct.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(1)));
    let t2 = distinct.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    assert!(distinct.next().unwrap().is_none());

    // Reset: should re-scan child and produce the same result
    distinct.reset().unwrap();

    let r1 = distinct.next().unwrap().unwrap();
    assert_eq!(r1.values[0], Some(DataValue::Int(1)));
    let r2 = distinct.next().unwrap().unwrap();
    assert_eq!(r2.values[0], Some(DataValue::Int(2)));
    assert!(distinct.next().unwrap().is_none());
}

#[test]
fn test_sort_operator() {
    let (tuples, schema) = int_tuples(vec![
        (Some(3), Some(30)),
        (Some(1), Some(10)),
        (Some(2), Some(20)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut sort = SortOperator::new(Box::new(child), vec![(0, false)]);

    let t1 = sort.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(1)));
    let t2 = sort.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    let t3 = sort.next().unwrap().unwrap();
    assert_eq!(t3.values[0], Some(DataValue::Int(3)));
    assert!(sort.next().unwrap().is_none());
}

#[test]
fn test_sort_descending() {
    let (tuples, schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(3), Some(30)),
        (Some(2), Some(20)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut sort = SortOperator::new(Box::new(child), vec![(0, true)]);

    assert_eq!(
        sort.next().unwrap().unwrap().values[0],
        Some(DataValue::Int(3))
    );
    assert_eq!(
        sort.next().unwrap().unwrap().values[0],
        Some(DataValue::Int(2))
    );
    assert_eq!(
        sort.next().unwrap().unwrap().values[0],
        Some(DataValue::Int(1))
    );
    assert!(sort.next().unwrap().is_none());
}

fn name_schema() -> Vec<ColumnInfo> {
    vec![
        ColumnInfo {
            merged: false,
            name: "name".into(),
            data_type: DataType::Varchar(100),
            table: None,
        },
        ColumnInfo {
            merged: false,
            name: "age".into(),
            data_type: DataType::Int,
            table: None,
        },
        ColumnInfo {
            merged: false,
            name: "salary".into(),
            data_type: DataType::DoublePrecision,
            table: None,
        },
    ]
}

fn employee_tuples(data: Vec<(&str, Option<i32>, Option<f64>)>) -> (Vec<Tuple>, Vec<ColumnInfo>) {
    let schema = name_schema();
    let tuples = data
        .into_iter()
        .map(|(name, age, salary)| {
            Tuple::new(vec![
                Some(DataValue::Varchar(name.to_string())),
                age.map(DataValue::Int),
                salary.map(|v| DataValue::DoublePrecision(crate::types::value::OrderedF64(v))),
            ])
        })
        .collect();
    (tuples, schema)
}

#[test]
fn test_aggregate_count_star() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", Some(25), Some(60000.0)),
        ("Charlie", Some(35), Some(70000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: None,
            output_name: "cnt".to_string(),
            output_type: DataType::BigInt,
            distinct: false,
        }],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::BigInt(3)));
    assert!(agg.next().unwrap().is_none());
}

#[test]
fn test_aggregate_groups_in_first_seen_order_with_having_and_reset() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(1.0)),
        ("Bob", Some(25), Some(2.0)),
        ("Carol", None, Some(3.0)),
        ("Dan", Some(30), Some(4.0)),
        ("Eve", Some(25), Some(5.0)),
        ("Fay", Some(40), Some(6.0)),
        ("Gus", Some(30), Some(7.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![Expr::Column {
            table: None,
            column: "age".into(),
        }],
        vec!["age".to_string()],
        vec![DataType::Int],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: None,
            output_name: "cnt".to_string(),
            output_type: DataType::BigInt,
            distinct: false,
        }],
        // HAVING cnt <> 2 drops the age-25 group from the middle.
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "cnt".into(),
            },
            ComparisonOp::NotEquals,
            Expr::Constant(DataValue::BigInt(2)),
        )),
    );
    let expected = vec![
        vec![Some(DataValue::Int(30)), Some(DataValue::BigInt(3))],
        vec![None, Some(DataValue::BigInt(1))],
        vec![Some(DataValue::Int(40)), Some(DataValue::BigInt(1))],
    ];
    for _ in 0..2 {
        let mut got = Vec::new();
        while let Some(t) = agg.next().unwrap() {
            got.push(t.values);
        }
        assert_eq!(got, expected);
        assert!(agg.next().unwrap().is_none());
        agg.reset().unwrap();
    }
}

#[test]
fn test_aggregate_count_column() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", Some(25), Some(60000.0)),
        ("Charlie", Some(35), Some(70000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: Some(Expr::Column {
                table: None,
                column: "age".into(),
            }),
            output_name: "cnt".to_string(),
            distinct: false,
            output_type: DataType::BigInt,
        }],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::BigInt(3)));
}

#[test]
fn test_aggregate_count_with_nulls() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", None, Some(60000.0)),
        ("Charlie", Some(35), Some(70000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: Some(Expr::Column {
                table: None,
                column: "age".into(),
            }),
            distinct: false,
            output_name: "cnt".to_string(),
            output_type: DataType::BigInt,
        }],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::BigInt(2)));
}

#[test]
fn test_aggregate_group_by() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", Some(25), Some(60000.0)),
        ("Alice", Some(32), Some(55000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![Expr::Column {
            table: None,
            column: "name".into(),
        }],
        vec!["name".to_string()],
        vec![DataType::Varchar(100)],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            distinct: false,
            input: None,
            output_name: "cnt".to_string(),
            output_type: DataType::BigInt,
        }],
        None,
    );

    let t1 = agg.next().unwrap().unwrap();
    let t2 = agg.next().unwrap().unwrap();
    assert!(agg.next().unwrap().is_none());

    let names: Vec<String> = [t1.clone(), t2.clone()]
        .iter()
        .map(|t| match &t.values[0] {
            Some(DataValue::Varchar(s)) => s.clone(),
            _ => panic!("Expected Varchar"),
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
}

#[test]
fn test_aggregate_sum_avg() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(100.0)),
        ("Bob", Some(25), Some(200.0)),
        ("Charlie", Some(35), Some(300.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![
            AggregateInfo {
                function: AggregateFunction::Sum,
                input: Some(Expr::Column {
                    table: None,
                    column: "salary".into(),
                }),
                output_name: "total".to_string(),
                output_type: DataType::DoublePrecision,
                distinct: false,
            },
            AggregateInfo {
                function: AggregateFunction::Avg,
                input: Some(Expr::Column {
                    table: None,
                    column: "salary".into(),
                }),
                output_name: "avg".to_string(),
                output_type: DataType::DoublePrecision,
                distinct: false,
            },
        ],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert!(agg.next().unwrap().is_none());

    match &result.values[0] {
        Some(DataValue::DoublePrecision(v)) => assert!((v.0 - 600.0).abs() < 0.001),
        _ => panic!("Expected DoublePrecision"),
    }
    match &result.values[1] {
        Some(DataValue::DoublePrecision(v)) => assert!((v.0 - 200.0).abs() < 0.001),
        _ => panic!("Expected DoublePrecision"),
    }
}

#[test]
fn test_aggregate_min_max() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", Some(25), Some(60000.0)),
        ("Charlie", Some(35), Some(70000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![
            AggregateInfo {
                function: AggregateFunction::Min,
                input: Some(Expr::Column {
                    table: None,
                    column: "age".into(),
                }),
                output_name: "min_age".to_string(),
                distinct: false,
                output_type: DataType::Int,
            },
            AggregateInfo {
                function: AggregateFunction::Max,
                input: Some(Expr::Column {
                    table: None,
                    column: "age".into(),
                }),
                output_name: "max_age".to_string(),
                output_type: DataType::Int,
                distinct: false,
            },
        ],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::Int(25)));
    assert_eq!(result.values[1], Some(DataValue::Int(35)));
}

#[test]
fn test_aggregate_sum_null() {
    let (tuples, schema) =
        employee_tuples(vec![("Alice", Some(30), None), ("Bob", Some(25), None)]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Sum,
            input: Some(Expr::Column {
                table: None,
                column: "salary".into(),
            }),
            output_name: "total".to_string(),
            output_type: DataType::DoublePrecision,
            distinct: false,
        }],
        None,
    );

    let result = agg.next().unwrap().unwrap();
    assert_eq!(result.values[0], None);
}

#[test]
fn test_aggregate_reset() {
    let (tuples, schema) = employee_tuples(vec![
        ("Alice", Some(30), Some(50000.0)),
        ("Bob", Some(25), Some(60000.0)),
    ]);
    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: None,
            output_name: "cnt".to_string(),
            output_type: DataType::BigInt,
            distinct: false,
        }],
        None,
    );

    let r1 = agg.next().unwrap().unwrap();
    assert_eq!(r1.values[0], Some(DataValue::BigInt(2)));
    assert!(agg.next().unwrap().is_none());

    agg.reset().unwrap();
    let r2 = agg.next().unwrap().unwrap();
    assert_eq!(r2.values[0], Some(DataValue::BigInt(2)));
    assert!(agg.next().unwrap().is_none());
}

fn single_col_schema(name: &str) -> Vec<ColumnInfo> {
    vec![ColumnInfo {
        merged: false,
        name: name.into(),
        data_type: DataType::Int,
        table: None,
    }]
}

fn single_col_tuples(data: Vec<Option<i32>>, name: &str) -> (Vec<Tuple>, Vec<ColumnInfo>) {
    let schema = single_col_schema(name);
    let tuples = data
        .into_iter()
        .map(|v| Tuple::new(vec![v.map(DataValue::Int)]))
        .collect();
    (tuples, schema)
}

#[test]
fn test_nl_join_inner() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2), Some(3)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3), Some(4)], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Inner,
    );

    let t1 = join.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    assert_eq!(t1.values[1], Some(DataValue::Int(2)));

    let t2 = join.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(3)));
    assert_eq!(t2.values[1], Some(DataValue::Int(3)));

    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_nl_join_left_outer() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3)], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Left,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    let matched = results.iter().find(|t| t.values[1].is_some()).unwrap();
    assert_eq!(matched.values[0], Some(DataValue::Int(2)));
    assert_eq!(matched.values[1], Some(DataValue::Int(2)));

    let unmatched = results.iter().find(|t| t.values[1].is_none()).unwrap();
    assert_eq!(unmatched.values[0], Some(DataValue::Int(1)));
}

#[test]
fn test_nl_join_right_outer() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3)], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Right,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);

    let matched = results.iter().find(|t| t.values[0].is_some()).unwrap();
    assert_eq!(matched.values[0], Some(DataValue::Int(2)));
    assert_eq!(matched.values[1], Some(DataValue::Int(2)));

    let unmatched = results.iter().find(|t| t.values[0].is_none()).unwrap();
    assert_eq!(unmatched.values[1], Some(DataValue::Int(3)));
}

#[test]
fn test_nl_join_full_outer() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3)], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Full,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_nl_cross_join() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(10), Some(20)], "val");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join =
        NestedLoopJoinOperator::new(Box::new(left), Box::new(right), None, JoinType::Cross);

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn test_nl_join_empty() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Inner,
    );

    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_nl_join_no_matches() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(3), Some(4)], "rid");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "rid".into(),
            },
        )),
        JoinType::Inner,
    );

    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_nl_join_reset() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "left_id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2)], "right_id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left),
        Box::new(right),
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "left_id".into(),
            },
            ComparisonOp::Equals,
            Expr::Column {
                table: None,
                column: "right_id".into(),
            },
        )),
        JoinType::Inner,
    );

    let t1 = join.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    assert!(join.next().unwrap().is_none());

    join.reset().unwrap();
    let t2 = join.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_hash_join_basic() {
    let (b_tuples, b_schema) = single_col_tuples(vec![Some(1), Some(2), Some(3)], "id");
    let (p_tuples, p_schema) = single_col_tuples(vec![Some(2), Some(3), Some(4)], "id");

    let build = MockOperator::new(b_tuples, b_schema);
    let probe = MockOperator::new(p_tuples, p_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        None,
    );

    let t1 = join.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    assert_eq!(t1.values[1], Some(DataValue::Int(2)));

    let t2 = join.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(3)));
    assert_eq!(t2.values[1], Some(DataValue::Int(3)));

    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_hash_join_with_nulls() {
    let (b_tuples, b_schema) = single_col_tuples(vec![Some(1), None, Some(3)], "id");
    let (p_tuples, p_schema) = single_col_tuples(vec![Some(1), Some(2), Some(3)], "id");

    let build = MockOperator::new(b_tuples, b_schema);
    let probe = MockOperator::new(p_tuples, p_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        None,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2); // NULL keys skipped
}

#[test]
fn test_hash_join_reset() {
    let (b_tuples, b_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (p_tuples, p_schema) = single_col_tuples(vec![Some(2)], "id");

    let build = MockOperator::new(b_tuples, b_schema);
    let probe = MockOperator::new(p_tuples, p_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        None,
    );

    let t1 = join.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2)));
    assert!(join.next().unwrap().is_none());

    join.reset().unwrap();
    let t2 = join.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_hash_join_table_qualified() {
    // Both sides have a column named "id", differentiated by table qualifier.
    // Tests that table-qualified Expr::Column refs work correctly as join keys.
    let build_schema = vec![
        ColumnInfo {
            merged: false,
            name: "id".into(),
            data_type: DataType::Int,
            table: Some("t1".into()),
        },
        ColumnInfo {
            merged: false,
            name: "name".into(),
            data_type: DataType::Varchar(20),
            table: Some("t1".into()),
        },
    ];
    let probe_schema = vec![
        ColumnInfo {
            merged: false,
            name: "id".into(),
            data_type: DataType::Int,
            table: Some("t2".into()),
        },
        ColumnInfo {
            merged: false,
            name: "val".into(),
            data_type: DataType::Varchar(20),
            table: Some("t2".into()),
        },
    ];

    let build_tuples = vec![
        Tuple::new(vec![
            Some(DataValue::Int(1)),
            Some(DataValue::Varchar("Alice".into())),
        ]),
        Tuple::new(vec![
            Some(DataValue::Int(2)),
            Some(DataValue::Varchar("Bob".into())),
        ]),
        Tuple::new(vec![
            Some(DataValue::Int(3)),
            Some(DataValue::Varchar("Charlie".into())),
        ]),
    ];
    let probe_tuples = vec![
        Tuple::new(vec![
            Some(DataValue::Int(2)),
            Some(DataValue::Varchar("x".into())),
        ]),
        Tuple::new(vec![
            Some(DataValue::Int(3)),
            Some(DataValue::Varchar("y".into())),
        ]),
        Tuple::new(vec![
            Some(DataValue::Int(4)),
            Some(DataValue::Varchar("z".into())),
        ]),
    ];

    let build = MockOperator::new(build_tuples, build_schema);
    let probe = MockOperator::new(probe_tuples, probe_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        // Table-qualified: t1.id = t2.id
        vec![Expr::Column {
            table: Some("t1".into()),
            column: "id".into(),
        }],
        vec![Expr::Column {
            table: Some("t2".into()),
            column: "id".into(),
        }],
        None,
    );

    // Should match t1.id=2 with t2.id=2, and t1.id=3 with t2.id=3
    let t1 = join.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(2))); // t1.id
    assert_eq!(t1.values[1], Some(DataValue::Varchar("Bob".into()))); // t1.name
    assert_eq!(t1.values[2], Some(DataValue::Int(2))); // t2.id
    assert_eq!(t1.values[3], Some(DataValue::Varchar("x".into()))); // t2.val

    let t2 = join.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(3))); // t1.id
    assert_eq!(t2.values[1], Some(DataValue::Varchar("Charlie".into()))); // t1.name
    assert_eq!(t2.values[2], Some(DataValue::Int(3))); // t2.id
    assert_eq!(t2.values[3], Some(DataValue::Varchar("y".into()))); // t2.val

    assert!(join.next().unwrap().is_none());
}

#[test]
fn test_hash_join_multi_column() {
    // Multi-column join key: t1.a = t2.a AND t1.b = t2.b
    let (b_tuples, b_schema) = int_tuples(vec![
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (Some(3), Some(30)),
        (Some(2), Some(99)), // same a=2 but different b
    ]);
    let (p_tuples, p_schema) = int_tuples(vec![
        (Some(2), Some(20)), // matches build (2,20)
        (Some(2), Some(99)), // matches build (2,99)
        (Some(3), Some(30)), // matches build (3,30)
        (Some(3), Some(31)), // no match (b=31 != 30)
        (Some(4), Some(40)), // no match (a=4 not in build)
    ]);

    let build = MockOperator::new(b_tuples, b_schema);
    let probe = MockOperator::new(p_tuples, p_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        ],
        vec![
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        ],
        None,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 3);

    // Verify (2,20) matched (2,20)
    assert!(results.iter().any(|t| {
        t.values[0] == Some(DataValue::Int(2))
            && t.values[1] == Some(DataValue::Int(20))
            && t.values[2] == Some(DataValue::Int(2))
            && t.values[3] == Some(DataValue::Int(20))
    }));

    // Verify (2,99) matched (2,99)
    assert!(results.iter().any(|t| {
        t.values[0] == Some(DataValue::Int(2))
            && t.values[1] == Some(DataValue::Int(99))
            && t.values[2] == Some(DataValue::Int(2))
            && t.values[3] == Some(DataValue::Int(99))
    }));

    // Verify (3,30) matched (3,30)
    assert!(results.iter().any(|t| {
        t.values[0] == Some(DataValue::Int(3))
            && t.values[1] == Some(DataValue::Int(30))
            && t.values[2] == Some(DataValue::Int(3))
            && t.values[3] == Some(DataValue::Int(30))
    }));
}

#[test]
fn test_hash_join_table_qualified_multi_column() {
    // Multi-column join key with table-qualified refs where both sides
    // have columns with the same names ("id", "code").
    let build_schema = vec![
        ColumnInfo {
            merged: false,
            name: "id".into(),
            data_type: DataType::Int,
            table: Some("t1".into()),
        },
        ColumnInfo {
            merged: false,
            name: "code".into(),
            data_type: DataType::Int,
            table: Some("t1".into()),
        },
    ];
    let probe_schema = vec![
        ColumnInfo {
            merged: false,
            name: "id".into(),
            data_type: DataType::Int,
            table: Some("t2".into()),
        },
        ColumnInfo {
            merged: false,
            name: "code".into(),
            data_type: DataType::Int,
            table: Some("t2".into()),
        },
    ];

    let build_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Int(100))]),
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(200))]),
        Tuple::new(vec![Some(DataValue::Int(3)), Some(DataValue::Int(300))]),
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(999))]), // same id, different code
    ];
    let probe_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(200))]), // matches build (2,200)
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(300))]), // build has (2,999) but code=300 != 999, and (2,200) but code=200 != 300
        Tuple::new(vec![Some(DataValue::Int(3)), Some(DataValue::Int(300))]), // matches build (3,300)
    ];

    let build = MockOperator::new(build_tuples, build_schema);
    let probe = MockOperator::new(probe_tuples, probe_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        vec![
            Expr::Column {
                table: Some("t1".into()),
                column: "id".into(),
            },
            Expr::Column {
                table: Some("t1".into()),
                column: "code".into(),
            },
        ],
        vec![
            Expr::Column {
                table: Some("t2".into()),
                column: "id".into(),
            },
            Expr::Column {
                table: Some("t2".into()),
                column: "code".into(),
            },
        ],
        None,
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Expected matches:
    // (2,200)probe → (2,200)build = MATCH
    // (2,300)probe → no match (build has (2,200) and (2,999), neither has code=300) = NO MATCH
    // (3,300)probe → (3,300)build = MATCH
    assert_eq!(results.len(), 2);

    // Verify output_schema has table qualifiers preserved in output
    assert_eq!(join.schema()[0].table, Some("t1".into()));
    assert_eq!(join.schema()[2].table, Some("t2".into()));
}

#[test]
fn test_hash_join_remaining_predicate() {
    // Hash join on "department_id" with a remaining non-equality predicate
    // on a uniquely-named column ("salary" > "min_salary").
    let build_schema = vec![
        ColumnInfo {
            merged: false,
            name: "dept_id".into(),
            data_type: DataType::Int,
            table: None,
        },
        ColumnInfo {
            merged: false,
            name: "salary".into(),
            data_type: DataType::Int,
            table: None,
        },
    ];
    let probe_schema = vec![
        ColumnInfo {
            merged: false,
            name: "dept_id".into(),
            data_type: DataType::Int,
            table: None,
        },
        ColumnInfo {
            merged: false,
            name: "min_salary".into(),
            data_type: DataType::Int,
            table: None,
        },
    ];

    // Build: employees (dept_id, salary)
    // Probe: department minimum salary thresholds (dept_id, min_salary)
    let build_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Int(50000))]),
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Int(70000))]),
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(30000))]),
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(60000))]),
    ];
    let probe_tuples = vec![
        Tuple::new(vec![Some(DataValue::Int(1)), Some(DataValue::Int(55000))]), // dept 1 min salary
        Tuple::new(vec![Some(DataValue::Int(2)), Some(DataValue::Int(50000))]), // dept 2 min salary
    ];

    let build = MockOperator::new(build_tuples, build_schema);
    let probe = MockOperator::new(probe_tuples, probe_schema);

    let mut join = HashJoinOperator::new(
        Box::new(build),
        Box::new(probe),
        // Hash join on dept_id
        vec![Expr::Column {
            table: None,
            column: "dept_id".into(),
        }],
        vec![Expr::Column {
            table: None,
            column: "dept_id".into(),
        }],
        // Remaining predicate: salary >= min_salary (employee's salary meets the department minimum)
        Some(Predicate::Compare(
            Expr::Column {
                table: None,
                column: "salary".into(),
            },
            ComparisonOp::GreaterOrEqual,
            Expr::Column {
                table: None,
                column: "min_salary".into(),
            },
        )),
    );

    let results: Vec<Tuple> = std::iter::from_fn(|| join.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Expected matches:
    // Dept 1 probe (55000): build (50000) → 50000 >= 55000? NO; (70000) → 70000 >= 55000? YES
    // Dept 2 probe (50000): build (30000) → 30000 >= 50000? NO; (60000) → 60000 >= 50000? YES
    assert_eq!(results.len(), 2);

    // Check actual values: first should be (1, 70000, 1, 55000) or (1, 55000, 1, 70000)
    // Order depends on hash table insertion order (dept 1 first in build)
    let dept1_result = results
        .iter()
        .find(|t| t.values[0] == Some(DataValue::Int(1)))
        .unwrap();
    assert_eq!(dept1_result.values[1], Some(DataValue::Int(70000))); // salary >= 55000
    assert_eq!(dept1_result.values[3], Some(DataValue::Int(55000))); // min_salary

    let dept2_result = results
        .iter()
        .find(|t| t.values[0] == Some(DataValue::Int(2)))
        .unwrap();
    assert_eq!(dept2_result.values[1], Some(DataValue::Int(60000))); // salary >= 50000
    assert_eq!(dept2_result.values[3], Some(DataValue::Int(50000))); // min_salary
}

#[test]
fn test_set_op_union_all() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(3), Some(4)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Union, true);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn test_set_op_union_distinct() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(1), Some(2)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Union, false);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_set_op_intersect_all() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(1), Some(2), Some(3)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(1), Some(2), Some(2)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Intersect, true);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_set_op_intersect_distinct() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2), Some(3)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2), Some(3), Some(4)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Intersect, false);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_set_op_except_all() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(1), Some(2), Some(3)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(1), Some(2)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Except, true);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_set_op_except_distinct() {
    let (l_tuples, l_schema) = single_col_tuples(vec![Some(1), Some(2), Some(3)], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![Some(2)], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Except, false);

    let results: Vec<Tuple> = std::iter::from_fn(|| op.next().transpose())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_set_op_empty_input() {
    let (l_tuples, l_schema) = single_col_tuples(vec![], "id");
    let (r_tuples, r_schema) = single_col_tuples(vec![], "id");

    let left = MockOperator::new(l_tuples, l_schema);
    let right = MockOperator::new(r_tuples, r_schema);

    let mut op = SetOpOperator::new(Box::new(left), Box::new(right), SetOpType::Union, true);

    assert!(op.next().unwrap().is_none());
}

#[test]
fn test_subquery_exec_scalar() {
    let (tuples, schema) = int_tuples(vec![(Some(42), Some(10))]);
    let child = MockOperator::new(tuples, schema);
    let mut sub = SubqueryExecOperator::new(Box::new(child), SubqueryType::Scalar, None);

    let result = sub.next().unwrap().unwrap();
    assert_eq!(result.values.len(), 2);
    assert_eq!(result.values[0], Some(DataValue::Int(42)));
    assert!(sub.next().unwrap().is_none());
}

#[test]
fn test_subquery_exec_exists_true() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(2))]);
    let child = MockOperator::new(tuples, schema);
    let mut sub = SubqueryExecOperator::new(Box::new(child), SubqueryType::Exists, None);

    let result = sub.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::Bool(true)));
    assert!(sub.next().unwrap().is_none());
}

#[test]
fn test_subquery_exec_exists_false() {
    let (tuples, schema) = int_tuples(vec![]);
    let child = MockOperator::new(tuples, schema);
    let mut sub = SubqueryExecOperator::new(Box::new(child), SubqueryType::Exists, None);

    let result = sub.next().unwrap().unwrap();
    assert_eq!(result.values[0], Some(DataValue::Bool(false)));
    assert!(sub.next().unwrap().is_none());
}

#[test]
fn test_single_row_operator() {
    let mut op = SingleRowOperator::new();
    let t1 = op.next().unwrap().unwrap();
    assert_eq!(t1.values.len(), 0);
    assert!(op.next().unwrap().is_none());

    op.reset().unwrap();
    let t2 = op.next().unwrap().unwrap();
    assert_eq!(t2.values.len(), 0);
}

#[test]
fn test_null_operator() {
    let schema = vec![ColumnInfo {
        merged: false,
        name: "x".into(),
        data_type: DataType::Int,
        table: None,
    }];
    let mut op = NullOperator::new(schema);
    assert!(op.next().unwrap().is_none());
}

#[test]
fn test_cte_scan_operator() {
    let (tuples, schema) = int_tuples(vec![(Some(1), Some(10)), (Some(2), Some(20))]);
    let mut op = CteScanOperator::new(tuples, schema);

    assert_eq!(op.estimate_cardinality(), 2);
    let t1 = op.next().unwrap().unwrap();
    assert_eq!(t1.values[0], Some(DataValue::Int(1)));
    let t2 = op.next().unwrap().unwrap();
    assert_eq!(t2.values[0], Some(DataValue::Int(2)));
    assert!(op.next().unwrap().is_none());

    op.reset().unwrap();
    let t3 = op.next().unwrap().unwrap();
    assert_eq!(t3.values[0], Some(DataValue::Int(1)));
}

#[test]
fn test_batch_filter_and_projection() {
    // Generate 2500 tuples to verify multi-batch execution (1024 + 1024 + 452)
    let schema = int_schema();
    let tuples: Vec<Tuple> = (0..2500)
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i)), Some(DataValue::Int(i * 10))]))
        .collect();

    let child = MockOperator::new(tuples, schema);
    // Filter even values of `a`: 1250 matching rows
    let filter = FilterOperator::new(
        Box::new(child),
        Predicate::Compare(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            ComparisonOp::GreaterThan,
            Expr::Constant(DataValue::Int(1000)), // 1499 rows
        ),
    );
    let mut proj = ProjectionOperator::new(
        Box::new(filter),
        vec![(
            Expr::Column {
                table: None,
                column: "b".into(),
            },
            "b".into(),
            DataType::Int,
        )],
    );

    let mut batch = Vec::new();
    let count1 = proj.next_batch(&mut batch).unwrap();
    assert_eq!(count1, 1024);
    assert_eq!(batch.len(), 1024);
    assert_eq!(batch[0].values[0], Some(DataValue::Int(1001 * 10)));

    batch.clear();
    let count2 = proj.next_batch(&mut batch).unwrap();
    assert_eq!(count2, 475);
    assert_eq!(batch.len(), 475);
    assert_eq!(batch[474].values[0], Some(DataValue::Int(2499 * 10)));

    batch.clear();
    let count3 = proj.next_batch(&mut batch).unwrap();
    assert_eq!(count3, 0);
}

#[test]
fn test_batch_count_star_acceleration() {
    let schema = int_schema();
    let tuples: Vec<Tuple> = (0..3500)
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i)), Some(DataValue::Int(i))]))
        .collect();

    let child = MockOperator::new(tuples, schema);
    let mut agg = AggregateOperator::new(
        Box::new(child),
        vec![],
        vec![],
        vec![],
        vec![AggregateInfo {
            function: AggregateFunction::Count,
            input: None,
            output_name: "cnt".into(),
            output_type: DataType::BigInt,
            distinct: false,
        }],
        None,
    );

    let mut batch = Vec::new();
    let count = agg.next_batch(&mut batch).unwrap();
    assert_eq!(count, 1);
    assert_eq!(batch[0].values[0], Some(DataValue::BigInt(3500)));
}

#[test]
fn test_batch_hash_join() {
    let schema1 = vec![ColumnInfo {
        merged: false,
        name: "id".into(),
        data_type: DataType::Int,
        table: None,
    }];
    let schema2 = vec![ColumnInfo {
        merged: false,
        name: "fk".into(),
        data_type: DataType::Int,
        table: None,
    }];

    let tuples1: Vec<Tuple> = (0..1500)
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i))]))
        .collect();
    let tuples2: Vec<Tuple> = (0..1500)
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i))]))
        .collect();

    let op1 = MockOperator::new(tuples1, schema1);
    let op2 = MockOperator::new(tuples2, schema2);

    let mut hj = HashJoinOperator::new(
        Box::new(op1),
        Box::new(op2),
        vec![Expr::Column {
            table: None,
            column: "id".into(),
        }],
        vec![Expr::Column {
            table: None,
            column: "fk".into(),
        }],
        None,
    );

    let mut batch = Vec::new();
    let count1 = hj.next_batch(&mut batch).unwrap();
    assert_eq!(count1, 1024);

    batch.clear();
    let count2 = hj.next_batch(&mut batch).unwrap();
    assert_eq!(count2, 476);

    batch.clear();
    let count3 = hj.next_batch(&mut batch).unwrap();
    assert_eq!(count3, 0);
}

#[test]
fn test_batch_sort_operator() {
    let schema = vec![ColumnInfo {
        merged: false,
        name: "val".into(),
        data_type: DataType::Int,
        table: None,
    }];
    let tuples: Vec<Tuple> = (0..2000)
        .rev()
        .map(|i| Tuple::new(vec![Some(DataValue::Int(i))]))
        .collect();

    let child = MockOperator::new(tuples, schema);
    let mut sort = SortOperator::new(Box::new(child), vec![(0, false)]);

    let mut batch = Vec::new();
    let count1 = sort.next_batch(&mut batch).unwrap();
    assert_eq!(count1, 1024);
    assert_eq!(batch[0].values[0], Some(DataValue::Int(0)));
    assert_eq!(batch[1023].values[0], Some(DataValue::Int(1023)));

    batch.clear();
    let count2 = sort.next_batch(&mut batch).unwrap();
    assert_eq!(count2, 976);
    assert_eq!(batch[975].values[0], Some(DataValue::Int(1999)));

    batch.clear();
    let count3 = sort.next_batch(&mut batch).unwrap();
    assert_eq!(count3, 0);
}
