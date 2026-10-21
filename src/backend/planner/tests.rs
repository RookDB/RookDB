//! Tests for the logical planner — plan structure, error handling, wildcard expansion,
//! semantic analysis, column schema, and INFORMATION_SCHEMA routing.

use crate::catalog::{Catalog, Column, Constraints, Database, Table};
use crate::types::DataType;

use super::helpers::*;
use super::plan_select;

use rook_ast::logical::*;
use rook_ast::*;
use std::collections::HashMap;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_test_catalog() -> Catalog {
    let mut catalog = Catalog::new(crate::backend::engine::Engine::current_dir());
    let users_table = Table {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                constraints: Constraints::default(),
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Varchar(100),
                nullable: true,
                constraints: Constraints::default(),
            },
            Column {
                name: "age".to_string(),
                data_type: DataType::Int,
                nullable: true,
                constraints: Constraints::default(),
            },
            Column {
                name: "email".to_string(),
                data_type: DataType::Varchar(255),
                nullable: true,
                constraints: Constraints::default(),
            },
        ],
    };
    let orders_table = Table {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                constraints: Constraints::default(),
            },
            Column {
                name: "user_id".to_string(),
                data_type: DataType::Int,
                nullable: true,
                constraints: Constraints::default(),
            },
            Column {
                name: "amount".to_string(),
                data_type: DataType::DoublePrecision,
                nullable: true,
                constraints: Constraints::default(),
            },
        ],
    };
    let mut tables = HashMap::new();
    tables.insert("users".to_string(), users_table);
    tables.insert("orders".to_string(), orders_table);
    catalog.databases.insert(
        "test_db".to_string(),
        Database {
            tables,
            views: HashMap::new(),
        },
    );
    catalog
}

fn constant_int(value: i64) -> ExprNode {
    ExprNode::Constant(ConstantValue::Int(value))
}
fn column(name: &str) -> ExprNode {
    ExprNode::Column(name.to_string())
}

fn eq_pred(left: ExprNode, right: ExprNode) -> PredicateNode {
    PredicateNode::Compare {
        left: Box::new(left),
        op: ComparisonOp::Eq,
        right: Box::new(right),
    }
}
fn gt_pred(left: ExprNode, right: ExprNode) -> PredicateNode {
    PredicateNode::Compare {
        left: Box::new(left),
        op: ComparisonOp::Gt,
        right: Box::new(right),
    }
}

fn plan_labels(select: &SelectPlan, catalog: &Catalog, db: &str) -> Vec<String> {
    let plan = plan_select(select, catalog, db).expect("planning failed");
    let mut labels = Vec::new();
    collect_labels_recursive(&plan, &mut labels);
    labels
}

// ── Basic plan structure tests ────────────────────────────────────────────────

#[test]
fn test_simple_select_star() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    assert_eq!(collect_labels(&plan), vec!["Project", "TableScan"]);
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 4);
            assert_eq!(p.expressions[0].name, "id");
            assert_eq!(p.expressions[3].name, "email");
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.table, "users");
                    assert_eq!(t.schema.columns.len(), 4);
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_select_with_where() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: Some(gt_pred(column("age"), constant_int(18))),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Project", "Filter", "TableScan"]
    );
}

#[test]
fn test_select_with_order_by() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: column("name"),
            ascending: true,
        }],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Sort", "Project", "TableScan"]
    );
}

#[test]
fn test_select_with_limit() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: Some(LimitClause {
            limit: 10,
            offset: None,
        }),
        distinct: false,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Limit", "Project", "TableScan"]
    );
    let plan = plan_select(&select, &catalog, "test_db").expect("planning failed");
    match plan {
        LogicalPlan::Limit(l) => {
            assert_eq!(l.limit, 10);
            assert_eq!(l.offset, 0);
        }
        _ => panic!("Expected Limit root"),
    }
}

#[test]
fn test_select_with_distinct() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::UnnamedExpr(column("name"))],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: true,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Distinct", "Project", "TableScan"]
    );
}

#[test]
fn test_select_with_group_by() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::UnnamedExpr(column("name"))],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![column("name")],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Project", "Aggregate", "TableScan"]
    );
}

#[test]
fn test_select_with_where_and_order_by() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: Some(gt_pred(column("age"), constant_int(21))),
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: column("name"),
            ascending: true,
        }],
        limit: Some(LimitClause {
            limit: 5,
            offset: None,
        }),
        distinct: false,
        ctes: vec![],
    };
    assert_eq!(
        plan_labels(&select, &catalog, "test_db"),
        vec!["Limit", "Sort", "Project", "Filter", "TableScan"]
    );
}

#[test]
fn test_select_with_join() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![JoinClause {
            relation: TableRef {
                name: "orders".to_string(),
                alias: None,
            },
            join_type: JoinType::Inner,
            condition: Some(eq_pred(
                ExprNode::Compound(vec!["users".to_string(), "id".to_string()]),
                ExprNode::Compound(vec!["orders".to_string(), "user_id".to_string()]),
            )),
            using: vec![],
        }],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    assert_eq!(
        collect_labels(&plan),
        vec!["Project", "Join", "TableScan", "TableScan"]
    );
    match plan {
        LogicalPlan::Project(_) => {}
        _ => panic!("Expected Project root"),
    }
}

// ── Error handling tests ──────────────────────────────────────────────────────

#[test]
fn test_table_not_found() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "nonexistent".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let result = plan_select(&select, &catalog, "test_db");
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("not found"));
}

#[test]
fn test_database_not_found() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let result = plan_select(&select, &catalog, "does_not_exist");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .message
            .contains("Database 'does_not_exist' not found")
    );
}

#[test]
fn test_empty_from_clause() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::UnnamedExpr(ExprNode::Constant(
            ConstantValue::Int(1),
        ))],
        from: vec![],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let result = plan_select(&select, &catalog, "test_db");
    assert!(result.is_ok());
    let plan = result.unwrap();
    assert_eq!(collect_labels(&plan), vec!["Project", "TableScan"]);
}

// ── Wildcard expansion tests ──────────────────────────────────────────────────

#[test]
fn test_wildcard_expansion() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 4);
            assert_eq!(p.expressions[0].name, "id");
            assert_eq!(p.expressions[3].name, "email");
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_explicit_columns() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![
            SelectExpr::UnnamedExpr(column("name")),
            SelectExpr::UnnamedExpr(column("age")),
        ],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 2);
            assert_eq!(p.expressions[0].name, "name");
            assert_eq!(p.expressions[1].name, "age");
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_column_alias() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::ExprWithAlias {
            expr: column("name"),
            alias: "user_name".to_string(),
        }],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 1);
            assert_eq!(p.expressions[0].name, "user_name");
        }
        _ => panic!("Expected Project root"),
    }
}

// ── ColumnSchema tests ────────────────────────────────────────────────────────

#[test]
fn test_column_schema_find() {
    let schema = ColumnSchema {
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "VARCHAR(100)".to_string(),
                nullable: true,
            },
        ],
    };
    assert_eq!(schema.columns.len(), 2);
    assert!(schema.contains("id"));
    assert!(schema.contains("name"));
    assert!(schema.find("id").is_some());
    assert!(schema.find("unknown").is_none());
    assert_eq!(schema.index_of("id"), Some(0));
    assert_eq!(schema.index_of("name"), Some(1));
}

// ── INFORMATION_SCHEMA tests ──────────────────────────────────────────────────

#[test]
fn test_info_schema_tables_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.tables".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("info_schema plan should succeed");
    assert_eq!(collect_labels(&plan), vec!["Project", "TableScan"]);
    match plan {
        LogicalPlan::Project(p) => {
            let col_names: Vec<&str> = p.expressions.iter().map(|e| e.name.as_str()).collect();
            assert_eq!(
                col_names,
                vec!["table_catalog", "table_schema", "table_name", "table_type"]
            );
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.table, "tables");
                    assert_eq!(t.system_table_name, Some("tables".to_string()));
                    assert_eq!(t.schema.columns.len(), 4);
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_info_schema_columns_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.columns".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    assert_eq!(collect_labels(&plan), vec!["Project", "TableScan"]);
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 8);
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.system_table_name, Some("columns".to_string()));
                    assert_eq!(t.schema.columns[0].name, "table_catalog");
                    assert_eq!(t.schema.columns[3].name, "column_name");
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_info_schema_schemata_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.schemata".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 2);
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.system_table_name, Some("databases".to_string()));
                    assert_eq!(t.schema.columns[0].name, "catalog_name");
                    assert_eq!(t.schema.columns[1].name, "schema_name");
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_info_schema_indexes_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.indexes".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 6);
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.system_table_name, Some("indexes".to_string()))
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_info_schema_table_constraints_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.table_constraints".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 6);
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.system_table_name, Some("constraints".to_string()))
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_info_schema_views_plan() {
    let catalog = make_test_catalog();
    let select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "information_schema.views".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let plan = plan_select(&select, &catalog, "test_db").expect("plan should succeed");
    match plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 4);
            match &*p.child {
                LogicalPlan::TableScan(t) => {
                    assert_eq!(t.system_table_name, Some("views".to_string()));
                    assert_eq!(t.schema.columns[2].name, "view_name");
                    assert_eq!(t.schema.columns[3].name, "view_definition");
                }
                _ => panic!("Expected TableScan child"),
            }
        }
        _ => panic!("Expected Project root"),
    }
}

#[test]
fn test_catalog_columns_to_schema() {
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
            constraints: Constraints::default(),
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::Varchar(100),
            nullable: true,
            constraints: Constraints::default(),
        },
    ];
    let schema = crate::planner::semantic::catalog_columns_to_schema(&columns);
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].data_type, "INT");
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[1].data_type, "VARCHAR(100)");
}

// ── Stats activation test ───────────────────────────────────────────────────

#[test]
fn test_plan_query_with_stats_fallback() {
    // Verify that plan_query gracefully handles the case when no stats
    // are available (no .dat files). It should fall back to Optimizer::new()
    // and produce a valid plan.
    use super::plan_query;

    let catalog = make_test_catalog();
    let select_plan = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let query = QueryPlan::Select(select_plan);
    // This should not panic or error, even though there are no .dat files
    let result = plan_query(&query, &catalog, "test_db");
    assert!(
        result.is_ok(),
        "plan_query should succeed without .dat files: {:?}",
        result
    );
    let plan = result.unwrap();
    assert_eq!(collect_labels(&plan), vec!["Project", "TableScan"]);
}

// ── Recursive aggregate extraction tests ──────────────────────────────────────

#[test]
fn test_recursive_aggregate_extraction_simple() {
    use crate::planner::helpers::extract_aggregates;
    // Simple top-level aggregate: SUM(price)
    let proj = vec![SelectExpr::UnnamedExpr(ExprNode::Function {
        name: "SUM".to_string(),
        args: vec![FunctionArg::Expr(Box::new(column("price")))],
        distinct: false,
    })];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].function, rook_ast::logical::AggregateFunction::Sum);
}

#[test]
fn test_recursive_aggregate_extraction_nested() {
    use crate::planner::helpers::extract_aggregates;
    // Nested aggregate: SUM(price)+1
    let proj = vec![SelectExpr::UnnamedExpr(ExprNode::Binary {
        left: Box::new(ExprNode::Function {
            name: "SUM".to_string(),
            args: vec![FunctionArg::Expr(Box::new(column("price")))],
            distinct: false,
        }),
        op: ArithOp::Add,
        right: Box::new(constant_int(1)),
    })];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 1, "SUM nested in Binary should be extracted");
    assert_eq!(aggs[0].function, rook_ast::logical::AggregateFunction::Sum);
}

#[test]
fn test_recursive_aggregate_extraction_multiple() {
    use crate::planner::helpers::extract_aggregates;
    // Multiple aggregates: SUM(price)+AVG(qty)
    let proj = vec![SelectExpr::UnnamedExpr(ExprNode::Binary {
        left: Box::new(ExprNode::Function {
            name: "SUM".to_string(),
            args: vec![FunctionArg::Expr(Box::new(column("price")))],
            distinct: false,
        }),
        op: ArithOp::Add,
        right: Box::new(ExprNode::Function {
            name: "AVG".to_string(),
            args: vec![FunctionArg::Expr(Box::new(column("qty")))],
            distinct: false,
        }),
    })];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 2, "Both SUM and AVG should be extracted");
    assert!(
        aggs.iter()
            .any(|a| a.function == rook_ast::logical::AggregateFunction::Sum)
    );
    assert!(
        aggs.iter()
            .any(|a| a.function == rook_ast::logical::AggregateFunction::Avg)
    );
}

#[test]
fn test_recursive_aggregate_extraction_skips_non_aggregates() {
    use crate::planner::helpers::extract_aggregates;
    // Non-aggregate function: UPPER(name)
    let proj = vec![SelectExpr::UnnamedExpr(ExprNode::Function {
        name: "UPPER".to_string(),
        args: vec![FunctionArg::Expr(Box::new(column("name")))],
        distinct: false,
    })];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 0, "UPPER is not an aggregate");
}

#[test]
fn test_recursive_aggregate_extraction_with_alias() {
    use crate::planner::helpers::extract_aggregates;
    // Aggregate with alias: MIN(age) AS min_age
    let proj = vec![SelectExpr::ExprWithAlias {
        expr: ExprNode::Function {
            name: "MIN".to_string(),
            args: vec![FunctionArg::Expr(Box::new(column("age")))],
            distinct: false,
        },
        alias: "min_age".to_string(),
    }];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].alias, Some("min_age".to_string()));
}

#[test]
fn test_recursive_aggregate_extraction_aggregate_in_cast() {
    use crate::planner::helpers::extract_aggregates;
    // Aggregate inside CAST: CAST(SUM(price) AS DOUBLE)
    let proj = vec![SelectExpr::UnnamedExpr(ExprNode::Cast {
        expr: Box::new(ExprNode::Function {
            name: "SUM".to_string(),
            args: vec![FunctionArg::Expr(Box::new(column("price")))],
            distinct: false,
        }),
        data_type: "DOUBLE".to_string(),
    })];
    let aggs = extract_aggregates(&proj);
    assert_eq!(aggs.len(), 1, "SUM inside CAST should be extracted");
    assert_eq!(aggs[0].function, rook_ast::logical::AggregateFunction::Sum);
}

// ── plan_insert() and QueryPlan::Insert dispatch tests ───────────────────────

#[test]
fn test_plan_insert_basic() {
    use super::plan_insert;

    let catalog = make_test_catalog();
    let source_select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let insert = InsertPlan {
        table: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        values: vec![],
        source_select: Some(Box::new(source_select)),
    };

    let plan = plan_insert(&insert, &catalog, "test_db").expect("plan_insert should succeed");

    // Should produce LogicalPlan::Insert with the correct structure
    match plan {
        LogicalPlan::Insert(inp) => {
            assert_eq!(inp.table, "users");
            assert_eq!(inp.columns, vec!["id", "name"]);
            // Child should be the planned SELECT: Project -> TableScan
            assert_eq!(collect_labels(&inp.child), vec!["Project", "TableScan"]);
        }
        other => panic!("Expected LogicalPlan::Insert, got {:?}", other.label()),
    }
}

#[test]
fn test_plan_insert_no_source() {
    use super::plan_insert;

    let catalog = make_test_catalog();
    let insert = InsertPlan {
        table: "users".to_string(),
        columns: vec![],
        values: vec![],
        source_select: None,
    };

    let result = plan_insert(&insert, &catalog, "test_db");
    assert!(
        result.is_err(),
        "plan_insert should fail with neither VALUES nor SELECT"
    );
    let err = result.unwrap_err();
    assert!(
        err.message.contains("VALUES clause or a SELECT source"),
        "Expected error about missing VALUES/SELECT source, got: {}",
        err.message
    );
}

#[test]
fn test_plan_insert_with_explicit_columns() {
    use super::plan_insert;

    let catalog = make_test_catalog();
    let source_select = SelectPlan {
        projections: vec![
            SelectExpr::UnnamedExpr(column("id")),
            SelectExpr::UnnamedExpr(column("name")),
        ],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let insert = InsertPlan {
        table: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        values: vec![],
        source_select: Some(Box::new(source_select)),
    };

    let plan = plan_insert(&insert, &catalog, "test_db").expect("plan_insert should succeed");

    match plan {
        LogicalPlan::Insert(inp) => {
            assert_eq!(inp.table, "users");
            assert_eq!(inp.columns, vec!["id", "name"]);
            // Verify the child SELECT produces 2 columns (id, name)
            match &*inp.child {
                LogicalPlan::Project(p) => {
                    assert_eq!(p.expressions.len(), 2);
                    assert_eq!(p.expressions[0].name, "id");
                    assert_eq!(p.expressions[1].name, "name");
                }
                other => panic!("Expected Project child, got {:?}", other.label()),
            }
        }
        other => panic!("Expected LogicalPlan::Insert, got {:?}", other.label()),
    }
}

#[test]
fn test_plan_insert_preserves_select_structure() {
    use super::plan_insert;

    let catalog = make_test_catalog();
    let source_select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: Some(gt_pred(column("age"), constant_int(21))),
        group_by: vec![],
        having: None,
        order_by: vec![OrderByExpr {
            expr: column("name"),
            ascending: true,
        }],
        limit: Some(LimitClause {
            limit: 10,
            offset: None,
        }),
        distinct: false,
        ctes: vec![],
    };
    let insert = InsertPlan {
        table: "users".to_string(),
        columns: vec![],
        values: vec![],
        source_select: Some(Box::new(source_select)),
    };

    let plan = plan_insert(&insert, &catalog, "test_db").expect("plan_insert should succeed");

    // The SELECT structure (Limit -> Sort -> Project -> Filter -> TableScan)
    // should be preserved inside the Insert operator
    match plan {
        LogicalPlan::Insert(inp) => {
            assert_eq!(
                collect_labels(&inp.child),
                vec!["Limit", "Sort", "Project", "Filter", "TableScan"],
                "SELECT plan structure should be preserved inside Insert"
            );
        }
        other => panic!("Expected LogicalPlan::Insert, got {:?}", other.label()),
    }
}

#[test]
fn test_plan_query_insert_dispatch() {
    use super::plan_query;

    let catalog = make_test_catalog();
    let source_select = SelectPlan {
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: "users".to_string(),
            alias: None,
        }],
        joins: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        distinct: false,
        ctes: vec![],
    };
    let query = QueryPlan::Insert(InsertPlan {
        table: "users".to_string(),
        columns: vec![],
        values: vec![],
        source_select: Some(Box::new(source_select)),
    });

    let plan = plan_query(&query, &catalog, "test_db")
        .expect("plan_query should dispatch Insert correctly");

    // plan_query for Insert with source_select should route to plan_insert
    match plan {
        LogicalPlan::Insert(inp) => {
            assert_eq!(inp.table, "users");
            assert_eq!(collect_labels(&inp.child), vec!["Project", "TableScan"]);
        }
        other => panic!("Expected LogicalPlan::Insert, got {:?}", other.label()),
    }
}

#[test]
fn test_plan_query_insert_values_plans_successfully() {
    use super::plan_query;

    let catalog = make_test_catalog();
    let query = QueryPlan::Insert(InsertPlan {
        table: "users".to_string(),
        columns: vec![],
        values: vec![vec![
            constant_int(1),
            ExprNode::Constant(ConstantValue::Text("a".to_string())),
            constant_int(30),
            ExprNode::Constant(ConstantValue::Text("x".to_string())),
        ]],
        source_select: None,
    });

    // Retirement step 5: INSERT ... VALUES is planned like any other
    // statement (rows carried as constants for the insert operator).
    let logical = plan_query(&query, &catalog, "test_db")
        .expect("INSERT ... VALUES must plan through the pipeline");
    match &logical {
        LogicalPlan::Insert(inp) => {
            assert_eq!(inp.values_rows.len(), 1, "one constant row expected");
            assert_eq!(inp.values_rows[0].len(), 4, "full table arity preserved");
        }
        other => panic!("Expected LogicalPlan::Insert, got {:?}", other.label()),
    }
}

#[test]
fn test_plan_query_ddl_rejected() {
    use super::plan_query;

    let catalog = make_test_catalog();
    // Verify that DDL statements (CreateTable, DropTable, etc.) are rejected
    // with the appropriate error message
    let ddl_queries: Vec<QueryPlan> = vec![
        QueryPlan::CreateTable(CreateTablePlan {
            table: "t".to_string(),
            columns: vec![],
            if_not_exists: false,
            constraints: vec![],
            ignored_indexes: vec![],
        }),
        QueryPlan::ShowTables,
        QueryPlan::UseDatabase("test_db".to_string()),
    ];

    for query in &ddl_queries {
        let result = plan_query(query, &catalog, "test_db");
        assert!(
            result.is_err(),
            "DDL query {:?} should be rejected",
            query.statement_type()
        );
        let err = result.unwrap_err();
        assert!(
            err.message.contains("be handled by the executor")
                || err.message.contains("not yet supported"),
            "DDL error should mention executor: {}",
            err.message
        );
    }
}

#[test]
fn test_plan_query_set_operation_dispatch() {
    use super::plan_query;

    let catalog = make_test_catalog();
    let query = QueryPlan::SetOperation(SetOperationPlan {
        left: SelectPlan {
            projections: vec![SelectExpr::Wildcard],
            from: vec![TableRef {
                name: "users".to_string(),
                alias: None,
            }],
            joins: vec![],
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            distinct: false,
            ctes: vec![],
        },
        right: SelectPlan {
            projections: vec![SelectExpr::Wildcard],
            from: vec![TableRef {
                name: "orders".to_string(),
                alias: None,
            }],
            joins: vec![],
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            distinct: false,
            ctes: vec![],
        },
        op: "UNION".to_string(),
        all: true,
        ctes: vec![],
        order_by: vec![],
        limit: None,
    });

    let plan = plan_query(&query, &catalog, "test_db")
        .expect("plan_query should dispatch SetOperation correctly");

    match plan {
        LogicalPlan::SetOp(s) => {
            assert!(s.all, "Expected UNION ALL");
        }
        other => panic!("Expected LogicalPlan::SetOp, got {:?}", other.label()),
    }
}
