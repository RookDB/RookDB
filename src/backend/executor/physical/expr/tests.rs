//! Tests for expression and predicate evaluation.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::super::operators::PhysicalOperator;
    use super::super::super::tuple::{ColumnInfo, Tuple};
    use super::super::{BooleanTest, ComparisonOp, Expr, Predicate, evaluate_predicate};
    use crate::backend::error::RookResult;

    use crate::types::datatype::DataType;
    use crate::types::value::DataValue;
    use std::cell::RefCell;
    use std::rc::Rc;

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
        vec![ColumnInfo {
            merged: false,
            name: "val".into(),
            data_type: DataType::Int,
            table: None,
        }]
    }

    fn int_tuples(data: Vec<Option<i32>>) -> (Vec<Tuple>, Vec<ColumnInfo>) {
        let schema = int_schema();
        let tuples = data
            .into_iter()
            .map(|v| Tuple::new(vec![v.map(DataValue::Int)]))
            .collect();
        (tuples, schema)
    }

    fn outer_schema() -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Int,
            table: None,
        }]
    }

    /// `val <> 0 AND 10 / val >= 1` (or the OR form) on `val`.
    fn guarded_division(and: bool) -> Expr {
        let col = || {
            Box::new(Expr::Column {
                table: None,
                column: "val".into(),
            })
        };
        let int = |v| Box::new(Expr::Constant(DataValue::Int(v)));
        let compare = |l: Box<Expr>, op, r: Box<Expr>| Predicate::Compare(*l, op, *r);
        let quotient = compare(
            Box::new(Expr::Div(int(10), col())),
            ComparisonOp::GreaterOrEqual,
            int(1),
        );
        Expr::Predicate(Box::new(if and {
            Predicate::and(compare(col(), ComparisonOp::NotEquals, int(0)), quotient)
        } else {
            Predicate::or(compare(col(), ComparisonOp::Equals, int(0)), quotient)
        }))
    }

    #[test]
    fn test_and_or_short_circuit_in_expressions() {
        let schema = int_schema();
        let zero = Tuple::new(vec![Some(DataValue::Int(0))]);
        assert_eq!(
            guarded_division(true).evaluate(&zero, &schema).unwrap(),
            Some(DataValue::Bool(false))
        );
        assert_eq!(
            guarded_division(false).evaluate(&zero, &schema).unwrap(),
            Some(DataValue::Bool(true))
        );
        // The right side still runs when the left does not decide.
        let five = Tuple::new(vec![Some(DataValue::Int(5))]);
        assert_eq!(
            guarded_division(true).evaluate(&five, &schema).unwrap(),
            Some(DataValue::Bool(true))
        );
    }

    #[test]
    fn test_correlated_exists_true() {
        // Inner plan produces tuples → EXISTS should return true
        let (tuples, schema) = int_tuples(vec![Some(42)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let outer_tuple = Tuple::new(vec![Some(DataValue::Int(1))]);
        let o_schema = outer_schema();

        let pred = Predicate::CorrelatedExists {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(
            result,
            Some(true),
            "EXISTS should be true when inner plan has tuples"
        );

        // Verify the param was set from the outer tuple
        assert_eq!(*param.borrow(), Some(DataValue::Int(1)));
    }

    #[test]
    fn test_correlated_exists_false() {
        // Inner plan is empty → EXISTS should return false
        let (tuples, schema) = int_tuples(vec![]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let outer_tuple = Tuple::new(vec![Some(DataValue::Int(99))]);
        let o_schema = outer_schema();

        let pred = Predicate::CorrelatedExists {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(
            result,
            Some(false),
            "EXISTS should be false when inner plan is empty"
        );
    }

    #[test]
    fn test_correlated_exists_sets_param_and_resets() {
        // Verify that calling evaluate twice resets and re-executes the inner plan
        let (tuples, schema) = int_tuples(vec![Some(42)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let pred = Predicate::CorrelatedExists {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
        };

        let o_schema = outer_schema();

        // First evaluation
        let outer1 = Tuple::new(vec![Some(DataValue::Int(10))]);
        let r1 = evaluate_predicate(&pred, &outer1, &o_schema).unwrap();
        assert_eq!(r1, Some(true));
        assert_eq!(*param.borrow(), Some(DataValue::Int(10)));

        // Second evaluation with different outer value
        let outer2 = Tuple::new(vec![Some(DataValue::Int(20))]);
        let r2 = evaluate_predicate(&pred, &outer2, &o_schema).unwrap();
        assert_eq!(r2, Some(true));
        // Param should now be set to the new outer value (from reset + re-execution)
        assert_eq!(*param.borrow(), Some(DataValue::Int(20)));
    }

    #[test]
    fn test_correlated_in_subquery_match() {
        // Inner plan has a matching value → IN should return true
        let (tuples, schema) = int_tuples(vec![Some(42), Some(7)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let outer_tuple = Tuple::new(vec![Some(DataValue::Int(7))]);
        let o_schema = outer_schema();

        // lhs_expr is the outer column (index 0)
        let pred = Predicate::CorrelatedInSubquery {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
            lhs_expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            negated: false,
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(
            result,
            Some(true),
            "IN should be true when inner plan has a matching value"
        );
    }

    #[test]
    fn test_correlated_in_subquery_no_match() {
        // Inner plan has no matching value → IN should return false
        let (tuples, schema) = int_tuples(vec![Some(42), Some(7)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let outer_tuple = Tuple::new(vec![Some(DataValue::Int(99))]);
        let o_schema = outer_schema();

        let pred = Predicate::CorrelatedInSubquery {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
            lhs_expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            negated: false,
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(
            result,
            Some(false),
            "IN should be false when inner plan has no matching value"
        );
    }

    #[test]
    fn test_correlated_in_subquery_null_lhs() {
        // LHS is NULL → IN should return UNKNOWN (None)
        let (tuples, schema) = int_tuples(vec![Some(42)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        // Outer tuple has NULL value
        let outer_tuple = Tuple::new(vec![None]);
        let o_schema = outer_schema();

        let pred = Predicate::CorrelatedInSubquery {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
            lhs_expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            negated: false,
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(result, None, "NULL IN (...) should return UNKNOWN (None)");
    }

    #[test]
    fn test_correlated_in_subquery_negated() {
        // NOT IN: inner plan has no matching value → !false = true
        let (tuples, schema) = int_tuples(vec![Some(42)]);
        let child = MockOperator::new(tuples, schema);
        let filter_op: Box<dyn PhysicalOperator> = Box::new(
            crate::backend::executor::physical::operators::FilterOperator::new(
                Box::new(child),
                Predicate::AlwaysTrue,
            ),
        );

        let param: Rc<RefCell<Option<DataValue>>> = Rc::new(RefCell::new(None));
        let inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>> = Rc::new(RefCell::new(filter_op));

        let outer_tuple = Tuple::new(vec![Some(DataValue::Int(99))]);
        let o_schema = outer_schema();

        // NOT IN with no match → true
        let pred = Predicate::CorrelatedInSubquery {
            inner_plan: inner_plan.clone(),
            params: vec![param.clone()],
            outer_col_indices: vec![0],
            lhs_expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            negated: true,
        };

        let result = evaluate_predicate(&pred, &outer_tuple, &o_schema).unwrap();
        assert_eq!(
            result,
            Some(true),
            "NOT IN should be true when there's no matching value"
        );

        // NOT IN with match → false
        let outer_tuple2 = Tuple::new(vec![Some(DataValue::Int(42))]);
        let result2 = evaluate_predicate(&pred, &outer_tuple2, &o_schema).unwrap();
        assert_eq!(
            result2,
            Some(false),
            "NOT IN should be false when there IS a matching value"
        );
    }

    // ── IsDistinctFrom tests ────────────────────────────────────────────────

    #[test]
    fn test_is_distinct_from_both_null() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "a".into(),
            data_type: DataType::Int,
            table: None,
        }];
        let tuple = Tuple::new(vec![None]);
        let pred = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "a".into(),
            },
        );
        let result = evaluate_predicate(&pred, &tuple, &schema).unwrap();
        assert_eq!(result, Some(false), "NULL IS DISTINCT FROM NULL → false");
    }

    #[test]
    fn test_is_distinct_from_null_and_value() {
        let schema = vec![
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
        ];
        // a = NULL, b = 5
        let tuple = Tuple::new(vec![None, Some(DataValue::Int(5))]);
        let pred = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        );
        let result = evaluate_predicate(&pred, &tuple, &schema).unwrap();
        assert_eq!(result, Some(true), "NULL IS DISTINCT FROM 5 → true");

        // Reverse order
        let pred2 = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "b".into(),
            },
            Expr::Column {
                table: None,
                column: "a".into(),
            },
        );
        let result2 = evaluate_predicate(&pred2, &tuple, &schema).unwrap();
        assert_eq!(result2, Some(true), "5 IS DISTINCT FROM NULL → true");
    }

    #[test]
    fn test_is_distinct_from_equal_values() {
        let schema = vec![
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
        ];
        // a = 5, b = 5
        let tuple = Tuple::new(vec![Some(DataValue::Int(5)), Some(DataValue::Int(5))]);
        let pred = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        );
        let result = evaluate_predicate(&pred, &tuple, &schema).unwrap();
        assert_eq!(result, Some(false), "5 IS DISTINCT FROM 5 → false");
    }

    #[test]
    fn test_is_distinct_from_different_values() {
        let schema = vec![
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
        ];
        // a = 5, b = 7
        let tuple = Tuple::new(vec![Some(DataValue::Int(5)), Some(DataValue::Int(7))]);
        let pred = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        );
        let result = evaluate_predicate(&pred, &tuple, &schema).unwrap();
        assert_eq!(result, Some(true), "5 IS DISTINCT FROM 7 → true");
    }

    #[test]
    fn test_is_distinct_from_text_values() {
        let schema = vec![
            ColumnInfo {
                merged: false,
                name: "a".into(),
                data_type: DataType::Varchar(50),
                table: None,
            },
            ColumnInfo {
                merged: false,
                name: "b".into(),
                data_type: DataType::Varchar(50),
                table: None,
            },
        ];
        // a = 'hello', b = 'world'
        let tuple = Tuple::new(vec![
            Some(DataValue::Varchar("hello".into())),
            Some(DataValue::Varchar("world".into())),
        ]);
        let pred = Predicate::IsDistinctFrom(
            Expr::Column {
                table: None,
                column: "a".into(),
            },
            Expr::Column {
                table: None,
                column: "b".into(),
            },
        );
        let result = evaluate_predicate(&pred, &tuple, &schema).unwrap();
        assert_eq!(
            result,
            Some(true),
            "'hello' IS DISTINCT FROM 'world' → true"
        );

        // Same values
        let tuple2 = Tuple::new(vec![
            Some(DataValue::Varchar("hello".into())),
            Some(DataValue::Varchar("hello".into())),
        ]);
        let result2 = evaluate_predicate(&pred, &tuple2, &schema).unwrap();
        assert_eq!(
            result2,
            Some(false),
            "'hello' IS DISTINCT FROM 'hello' → false"
        );
    }

    // ── IsBoolean tests ─────────────────────────────────────────────────────

    #[test]
    fn test_is_true_on_true() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![Some(DataValue::Bool(true))]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::True,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );
    }

    #[test]
    fn test_is_true_on_false() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![Some(DataValue::Bool(false))]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::True,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_true_on_null() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![None]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::True,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_false_on_false() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![Some(DataValue::Bool(false))]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::False,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );
    }

    #[test]
    fn test_is_false_on_true() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![Some(DataValue::Bool(true))]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::False,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_unknown_on_null() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![None]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::Unknown,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );
    }

    #[test]
    fn test_is_unknown_on_known_value() {
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let tuple = Tuple::new(vec![Some(DataValue::Bool(true))]);
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::Unknown,
            negated: false,
        };
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_not_true_on_false() {
        // IS NOT TRUE is true when value is false OR null
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::True,
            negated: true,
        };
        let tuple = Tuple::new(vec![Some(DataValue::Bool(false))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );

        // Not true on null → also true (null is not true)
        let tuple2 = Tuple::new(vec![None]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple2, &schema).unwrap(),
            Some(true)
        );

        // Not true on true → false
        let tuple3 = Tuple::new(vec![Some(DataValue::Bool(true))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple3, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_not_false_on_true() {
        // IS NOT FALSE is true when value is true OR null
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::False,
            negated: true,
        };
        let tuple = Tuple::new(vec![Some(DataValue::Bool(true))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );

        // Not false on null → also true
        let tuple2 = Tuple::new(vec![None]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple2, &schema).unwrap(),
            Some(true)
        );

        // Not false on false → false
        let tuple3 = Tuple::new(vec![Some(DataValue::Bool(false))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple3, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_is_not_unknown() {
        // IS NOT UNKNOWN is true when value is known (true or false)
        let schema = vec![ColumnInfo {
            merged: false,
            name: "x".into(),
            data_type: DataType::Bool,
            table: None,
        }];
        let pred = Predicate::IsBoolean {
            expr: Expr::Column {
                table: None,
                column: "x".into(),
            },
            test: BooleanTest::Unknown,
            negated: true,
        };
        let tuple = Tuple::new(vec![Some(DataValue::Bool(true))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple, &schema).unwrap(),
            Some(true)
        );

        let tuple2 = Tuple::new(vec![Some(DataValue::Bool(false))]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple2, &schema).unwrap(),
            Some(true)
        );

        let tuple3 = Tuple::new(vec![None]);
        assert_eq!(
            evaluate_predicate(&pred, &tuple3, &schema).unwrap(),
            Some(false)
        );
    }

    #[test]
    fn test_new_scalar_functions() {
        let schema = vec![];
        let tuple = Tuple::new(vec![]);

        // 1. Test CONCAT
        let concat_expr = Expr::Function {
            name: "CONCAT".to_string(),
            args: vec![
                Expr::Constant(DataValue::Varchar("Hello ".to_string())),
                Expr::Constant(DataValue::Int(42)),
                Expr::Null,
                Expr::Constant(DataValue::Varchar(" World".to_string())),
            ],
        };
        assert_eq!(
            concat_expr.evaluate(&tuple, &schema).unwrap(),
            Some(DataValue::Varchar("Hello 42 World".to_string()))
        );

        // 2. Test MOD
        let mod_expr = Expr::Function {
            name: "MOD".to_string(),
            args: vec![
                Expr::Constant(DataValue::Int(10)),
                Expr::Constant(DataValue::Int(3)),
            ],
        };
        assert_eq!(
            mod_expr.evaluate(&tuple, &schema).unwrap(),
            Some(DataValue::Int(1))
        );

        // Test MOD with Real and DoublePrecision
        let mod_double_expr = Expr::Function {
            name: "MOD".to_string(),
            args: vec![
                Expr::Constant(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    10.5,
                ))),
                Expr::Constant(DataValue::Int(3)),
            ],
        };
        assert_eq!(
            mod_double_expr.evaluate(&tuple, &schema).unwrap(),
            Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                1.5
            )))
        );

        // Test MOD by zero
        let mod_zero_expr = Expr::Function {
            name: "MOD".to_string(),
            args: vec![
                Expr::Constant(DataValue::Int(10)),
                Expr::Constant(DataValue::Int(0)),
            ],
        };
        assert!(mod_zero_expr.evaluate(&tuple, &schema).is_err());

        // Test MOD with Null
        let mod_null_expr = Expr::Function {
            name: "MOD".to_string(),
            args: vec![Expr::Constant(DataValue::Int(10)), Expr::Null],
        };
        assert_eq!(mod_null_expr.evaluate(&tuple, &schema).unwrap(), None);

        // 3. Test POWER
        let power_expr = Expr::Function {
            name: "POWER".to_string(),
            args: vec![
                Expr::Constant(DataValue::Int(2)),
                Expr::Constant(DataValue::Int(3)),
            ],
        };
        assert_eq!(
            power_expr.evaluate(&tuple, &schema).unwrap(),
            Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                8.0
            )))
        );

        // Test POWER with Null
        let power_null_expr = Expr::Function {
            name: "POWER".to_string(),
            args: vec![Expr::Null, Expr::Constant(DataValue::Int(3))],
        };
        assert_eq!(power_null_expr.evaluate(&tuple, &schema).unwrap(), None);

        // 4. Test SQRT
        let sqrt_expr = Expr::Function {
            name: "SQRT".to_string(),
            args: vec![Expr::Constant(DataValue::Int(9))],
        };
        assert_eq!(
            sqrt_expr.evaluate(&tuple, &schema).unwrap(),
            Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                3.0
            )))
        );

        // Test SQRT with negative
        let sqrt_neg_expr = Expr::Function {
            name: "SQRT".to_string(),
            args: vec![Expr::Constant(DataValue::Int(-9))],
        };
        assert!(sqrt_neg_expr.evaluate(&tuple, &schema).is_err());

        // Test SQRT with Null
        let sqrt_null_expr = Expr::Function {
            name: "SQRT".to_string(),
            args: vec![Expr::Null],
        };
        assert_eq!(sqrt_null_expr.evaluate(&tuple, &schema).unwrap(), None);
    }
    fn col(table: &str, name: &str, merged: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int,
            table: Some(table.to_string()),
            merged,
        }
    }

    #[test]
    fn resolve_column_rejects_a_bare_name_two_columns_share() {
        use super::super::resolve_column;
        use crate::backend::error::RookError;
        let schema = vec![
            col("l", "id", false),
            col("r", "id", false),
            col("r", "x", false),
        ];
        assert!(matches!(
            resolve_column(&schema, None, "ID"),
            Err(RookError::AmbiguousColumn(_))
        ));
        assert_eq!(resolve_column(&schema, Some("r"), "id").unwrap(), Some(1));
        assert_eq!(resolve_column(&schema, None, "x").unwrap(), Some(2));
        assert_eq!(resolve_column(&schema, None, "nope").unwrap(), None);
    }

    #[test]
    fn merged_copy_does_not_make_a_bare_name_ambiguous() {
        use super::super::{binding_names, resolve_column};
        let schema = vec![col("l", "k", false), col("r", "k", true)];
        assert_eq!(resolve_column(&schema, None, "k").unwrap(), Some(0));
        // The marked copy is still reachable by its table.
        assert_eq!(resolve_column(&schema, Some("r"), "k").unwrap(), Some(1));
        assert_eq!(binding_names(&schema), ["k", ""]);
        // A RIGHT join keeps the right copy.
        let schema = vec![col("l", "k", true), col("r", "k", false)];
        assert_eq!(resolve_column(&schema, None, "k").unwrap(), Some(1));
    }
}
