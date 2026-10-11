//! Tests for the rule-based optimizer.
//!
//! Tests cover constant folding, predicate pushdown, projection pruning,
//! limit pushdown, join ordering, and the full optimizer pipeline.

use crate::catalog::{Catalog, Column, Constraints, Database, Table};
use crate::types::DataType;

use super::super::collect_labels;
use super::Optimizer;
use super::helpers::*;

use rook_ast::logical::*;
use rook_ast::*;
use std::collections::HashMap;

// ── Test helpers ──────────────────────────────────────────────────────────────

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

fn column(name: &str) -> ExprNode {
    ExprNode::Column(name.to_string())
}
fn constant_int(value: i64) -> ExprNode {
    ExprNode::Constant(ConstantValue::Int(value))
}
fn constant_float(value: f64) -> ExprNode {
    ExprNode::Constant(ConstantValue::Float(value))
}

fn gt_pred(left: ExprNode, right: ExprNode) -> PredicateNode {
    PredicateNode::Compare {
        left: Box::new(left),
        op: ComparisonOp::Gt,
        right: Box::new(right),
    }
}
fn eq_pred(left: ExprNode, right: ExprNode) -> PredicateNode {
    PredicateNode::Compare {
        left: Box::new(left),
        op: ComparisonOp::Eq,
        right: Box::new(right),
    }
}

fn make_table_scan(table: &str, columns: &[&str]) -> LogicalPlan {
    LogicalPlan::TableScan(LogicalTableScan {
        table: table.to_string(),
        alias: None,
        schema: ColumnSchema {
            columns: columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.to_string(),
                    data_type: "INT".to_string(),
                    nullable: true,
                })
                .collect(),
        },
        system_table_name: None,
    })
}

// ─── Constant Folding Tests ───────────────────────────────────────────────────

#[test]
fn test_fold_simple_arithmetic() {
    let expr = ExprNode::Binary {
        left: Box::new(constant_int(2)),
        op: ArithOp::Add,
        right: Box::new(constant_int(3)),
    };
    assert_eq!(fold_expr(&expr), ExprNode::Constant(ConstantValue::Int(5)));
}

#[test]
fn test_fold_complex_expression() {
    let inner = ExprNode::Binary {
        left: Box::new(constant_int(2)),
        op: ArithOp::Add,
        right: Box::new(constant_int(3)),
    };
    let expr = ExprNode::Binary {
        left: Box::new(inner),
        op: ArithOp::Mul,
        right: Box::new(constant_int(4)),
    };
    assert_eq!(fold_expr(&expr), ExprNode::Constant(ConstantValue::Int(20)));
}

#[test]
fn test_fold_float_arithmetic() {
    let expr = ExprNode::Binary {
        left: Box::new(constant_float(1.5)),
        op: ArithOp::Add,
        right: Box::new(constant_float(2.5)),
    };
    assert_eq!(
        fold_expr(&expr),
        ExprNode::Constant(ConstantValue::Float(4.0))
    );
}

#[test]
fn test_fold_mixed_types() {
    let expr = ExprNode::Binary {
        left: Box::new(constant_int(3)),
        op: ArithOp::Add,
        right: Box::new(constant_float(2.5)),
    };
    assert_eq!(
        fold_expr(&expr),
        ExprNode::Constant(ConstantValue::Float(5.5))
    );
}

#[test]
fn test_fold_null_propagation() {
    let expr = ExprNode::Binary {
        left: Box::new(constant_int(5)),
        op: ArithOp::Add,
        right: Box::new(ExprNode::Constant(ConstantValue::Null)),
    };
    assert_eq!(fold_expr(&expr), ExprNode::Constant(ConstantValue::Null));
}

#[test]
fn test_fold_division_by_zero() {
    let expr = ExprNode::Binary {
        left: Box::new(constant_int(5)),
        op: ArithOp::Div,
        right: Box::new(constant_int(0)),
    };
    assert_eq!(fold_expr(&expr), ExprNode::Constant(ConstantValue::Null));
}

#[test]
fn test_fold_identity_ops() {
    let expr = ExprNode::Binary {
        left: Box::new(column("age")),
        op: ArithOp::Add,
        right: Box::new(constant_int(0)),
    };
    assert_eq!(fold_expr(&expr), column("age"));

    let expr2 = ExprNode::Binary {
        left: Box::new(column("age")),
        op: ArithOp::Mul,
        right: Box::new(constant_int(1)),
    };
    assert_eq!(fold_expr(&expr2), column("age"));
}

#[test]
fn test_fold_column_expression_unchanged() {
    let expr = ExprNode::Binary {
        left: Box::new(column("age")),
        op: ArithOp::Sub,
        right: Box::new(constant_int(5)),
    };
    match fold_expr(&expr) {
        ExprNode::Binary {
            ref left,
            op: _,
            ref right,
        } => {
            assert_eq!(*left.as_ref(), column("age"));
            assert_eq!(*right.as_ref(), constant_int(5));
        }
        _ => panic!("Expected Binary expression"),
    }
}

// ─── Predicate Pushdown Tests ─────────────────────────────────────────────────

#[test]
fn test_push_filter_through_sort() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(column("age"), constant_int(18)),
        child: Box::new(LogicalPlan::Sort(LogicalSort {
            order_by: vec![OrderByExpr {
                expr: column("name"),
                ascending: true,
            }],
            child: Box::new(make_table_scan("users", &["id", "name", "age"])),
            limit: None,
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Sort", "Filter", "TableScan"]
    );
}

#[test]
fn test_push_filter_through_distinct() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(column("age"), constant_int(18)),
        child: Box::new(LogicalPlan::Distinct(LogicalDistinct {
            child: Box::new(make_table_scan("users", &["id", "name", "age"])),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Distinct", "Filter", "TableScan"]
    );
}

#[test]
fn test_merge_consecutive_filters() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(column("age"), constant_int(18)),
        child: Box::new(LogicalPlan::Filter(LogicalFilter {
            predicate: eq_pred(column("name"), constant_int(1)),
            child: Box::new(make_table_scan("users", &["id", "name", "age"])),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);
    assert_eq!(collect_labels(&optimized), vec!["Filter", "TableScan"]);
}

#[test]
fn test_push_filter_through_project_passthrough() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(column("age"), constant_int(18)),
        child: Box::new(LogicalPlan::Project(LogicalProject {
            expressions: vec![
                NamedExpr {
                    name: "id".to_string(),
                    expr: column("id"),
                },
                NamedExpr {
                    name: "name".to_string(),
                    expr: column("name"),
                },
                NamedExpr {
                    name: "age".to_string(),
                    expr: column("age"),
                },
            ],
            child: Box::new(make_table_scan("users", &["id", "name", "age"])),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Project", "Filter", "TableScan"]
    );
}

#[test]
fn test_cannot_push_filter_through_limit() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(column("age"), constant_int(18)),
        child: Box::new(LogicalPlan::Limit(LogicalLimit {
            limit: 10,
            offset: 0,
            child: Box::new(make_table_scan("users", &["id", "name", "age"])),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Filter", "Limit", "TableScan"]
    );
}

// ─── Limit Pushdown Tests ─────────────────────────────────────────────────────

#[test]
fn test_limit_pushdown_through_sort() {
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 10,
        offset: 0,
        child: Box::new(LogicalPlan::Sort(LogicalSort {
            order_by: vec![OrderByExpr {
                expr: column("name"),
                ascending: true,
            }],
            child: Box::new(make_table_scan("users", &["id", "name"])),
            limit: None,
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.limit_pushdown(plan);
    assert_eq!(collect_labels(&optimized), vec!["Sort", "TableScan"]);
    if let LogicalPlan::Sort(s) = &optimized {
        assert_eq!(s.limit, Some(10));
    } else {
        panic!("Expected Sort node");
    }
}

#[test]
fn test_limit_with_offset_not_pushed() {
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 10,
        offset: 5,
        child: Box::new(LogicalPlan::Sort(LogicalSort {
            order_by: vec![OrderByExpr {
                expr: column("name"),
                ascending: true,
            }],
            child: Box::new(make_table_scan("users", &["id", "name"])),
            limit: None,
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.limit_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Limit", "Sort", "TableScan"]
    );
}

#[test]
fn test_limit_pushdown_with_project() {
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 5,
        offset: 0,
        child: Box::new(LogicalPlan::Project(LogicalProject {
            expressions: vec![NamedExpr {
                name: "age".to_string(),
                expr: column("age"),
            }],
            child: Box::new(LogicalPlan::Sort(LogicalSort {
                order_by: vec![OrderByExpr {
                    expr: column("name"),
                    ascending: true,
                }],
                child: Box::new(make_table_scan("users", &["id", "name", "age"])),
                limit: None,
            })),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.limit_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Project", "Sort", "TableScan"]
    );
    if let LogicalPlan::Project(p) = &optimized {
        if let LogicalPlan::Sort(s) = &*p.child {
            assert_eq!(s.limit, Some(5));
        } else {
            panic!("Expected Sort under Project");
        }
    } else {
        panic!("Expected Project root");
    }
}

// ─── Full Optimizer Pipeline Tests ───────────────────────────────────────────

#[test]
fn test_full_optimizer_pipeline() {
    let catalog = make_test_catalog();
    let table_scan = LogicalPlan::TableScan(LogicalTableScan {
        table: "users".to_string(),
        alias: None,
        schema: ColumnSchema {
            columns: catalog
                .databases
                .get("test_db")
                .unwrap()
                .tables
                .get("users")
                .unwrap()
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.to_string(),
                    nullable: c.nullable,
                })
                .collect(),
        },
        system_table_name: None,
    });
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 10,
        offset: 0,
        child: Box::new(LogicalPlan::Sort(LogicalSort {
            order_by: vec![OrderByExpr {
                expr: column("name"),
                ascending: true,
            }],
            child: Box::new(LogicalPlan::Project(LogicalProject {
                expressions: vec![NamedExpr {
                    name: "name".to_string(),
                    expr: column("name"),
                }],
                child: Box::new(LogicalPlan::Filter(LogicalFilter {
                    predicate: gt_pred(column("age"), constant_int(18)),
                    child: Box::new(table_scan),
                })),
            })),
            limit: None,
        })),
    });
    assert_eq!(
        collect_labels(&plan),
        vec!["Limit", "Sort", "Project", "Filter", "TableScan"]
    );

    let optimizer = Optimizer::new();
    let optimized = optimizer.optimize(plan);
    // The sort-hoisting pass rewrites Sort(Project(Filter(scan))) into
    // Project(Sort(Filter(scan))) so ORDER BY can use columns that the
    // SELECT list drops; limit_pushdown then folds LIMIT 10 into the Sort.
    assert_eq!(
        collect_labels(&optimized),
        vec!["Project", "Sort", "Filter", "TableScan"]
    );

    if let LogicalPlan::Project(p) = &optimized {
        if let LogicalPlan::Sort(s) = &*p.child {
            assert_eq!(s.limit, Some(10));
        } else {
            panic!("Expected Sort under Project");
        }
    } else {
        panic!("Expected Project root");
    }
}

#[test]
fn test_constant_folding_in_predicate() {
    let folded_pred = fold_predicate(&gt_pred(column("age"), {
        ExprNode::Binary {
            left: Box::new(constant_int(5)),
            op: ArithOp::Add,
            right: Box::new(constant_int(3)),
        }
    }));
    if let PredicateNode::Compare { left, op: _, right } = &folded_pred {
        assert_eq!(*left.as_ref(), column("age"));
        assert_eq!(*right.as_ref(), constant_int(8));
    } else {
        panic!("Expected Compare predicate");
    }
}

#[test]
fn test_constant_folding_entire_plan() {
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: gt_pred(
            column("age"),
            ExprNode::Binary {
                left: Box::new(constant_int(5)),
                op: ArithOp::Add,
                right: Box::new(constant_int(3)),
            },
        ),
        child: Box::new(make_table_scan("users", &["id", "name", "age"])),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.constant_folding(plan);
    if let LogicalPlan::Filter(f) = &optimized {
        match &f.predicate {
            PredicateNode::Compare { right, .. } => assert_eq!(*right.as_ref(), constant_int(8)),
            _ => panic!("Expected Compare predicate"),
        }
    } else {
        panic!("Expected Filter node");
    }
}

// ─── Projection Pruning Tests ─────────────────────────────────────────────────

#[test]
fn test_projection_pruning_single_column() {
    let plan = LogicalPlan::Project(LogicalProject {
        expressions: vec![NamedExpr {
            name: "name".to_string(),
            expr: column("name"),
        }],
        child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.projection_pruning(plan);
    if let LogicalPlan::Project(p) = &optimized {
        if let LogicalPlan::TableScan(t) = &*p.child {
            let col_names: Vec<&str> = t.schema.columns.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(col_names, vec!["name"]);
        } else {
            panic!("Expected TableScan under Project");
        }
    } else {
        panic!("Expected Project root");
    }
}

// ─── Projection Pruning with Insert Tests ─────────────────────────────────

#[test]
fn test_projection_pruning_through_insert_single_column() {
    // INSERT INTO t (name) SELECT name FROM users;
    // Only 'name' column is referenced → other columns should be pruned
    let plan = LogicalPlan::Insert(LogicalInsert {
        table: "target".to_string(),
        columns: vec!["name".to_string()],
        child: Box::new(LogicalPlan::Project(LogicalProject {
            expressions: vec![NamedExpr {
                name: "name".to_string(),
                expr: column("name"),
            }],
            child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
        })),
        values_rows: Vec::new(),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.projection_pruning(plan);

    // Structure should be preserved: Insert -> Project -> TableScan
    assert_eq!(
        collect_labels(&optimized),
        vec!["Insert", "Project", "TableScan"]
    );

    // TableScan should only have 'name' column
    if let LogicalPlan::Insert(inp) = &optimized {
        if let LogicalPlan::Project(p) = &*inp.child {
            if let LogicalPlan::TableScan(t) = &*p.child {
                let col_names: Vec<&str> =
                    t.schema.columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(
                    col_names,
                    vec!["name"],
                    "Unreferenced columns should be pruned through Insert"
                );
            } else {
                panic!("Expected TableScan under Project");
            }
        } else {
            panic!("Expected Project under Insert");
        }
    } else {
        panic!("Expected Insert root");
    }
}

#[test]
fn test_projection_pruning_through_insert_all_columns() {
    // INSERT INTO t SELECT * FROM users (all 4 columns)
    // All columns are referenced → none should be pruned
    let plan = LogicalPlan::Insert(LogicalInsert {
        table: "target".to_string(),
        columns: vec![],
        child: Box::new(LogicalPlan::Project(LogicalProject {
            expressions: vec![
                NamedExpr {
                    name: "id".to_string(),
                    expr: column("id"),
                },
                NamedExpr {
                    name: "name".to_string(),
                    expr: column("name"),
                },
                NamedExpr {
                    name: "age".to_string(),
                    expr: column("age"),
                },
                NamedExpr {
                    name: "email".to_string(),
                    expr: column("email"),
                },
            ],
            child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
        })),
        values_rows: Vec::new(),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.projection_pruning(plan);

    assert_eq!(
        collect_labels(&optimized),
        vec!["Insert", "Project", "TableScan"]
    );

    // All 4 columns referenced in Project → none pruned from TableScan
    if let LogicalPlan::Insert(inp) = &optimized {
        if let LogicalPlan::Project(p) = &*inp.child {
            if let LogicalPlan::TableScan(t) = &*p.child {
                assert_eq!(
                    t.schema.columns.len(),
                    4,
                    "All 4 columns should be retained"
                );
                let col_names: Vec<&str> =
                    t.schema.columns.iter().map(|c| c.name.as_str()).collect();
                assert!(col_names.contains(&"id"));
                assert!(col_names.contains(&"name"));
                assert!(col_names.contains(&"age"));
                assert!(col_names.contains(&"email"));
            } else {
                panic!("Expected TableScan under Project");
            }
        } else {
            panic!("Expected Project under Insert");
        }
    } else {
        panic!("Expected Insert root");
    }
}

#[test]
fn test_projection_pruning_through_insert_with_filter() {
    // INSERT INTO t SELECT name FROM users WHERE age > 18
    // Project references 'name', filter references 'age' → both columns needed
    let plan = LogicalPlan::Insert(LogicalInsert {
        table: "target".to_string(),
        columns: vec!["name".to_string()],
        child: Box::new(LogicalPlan::Project(LogicalProject {
            expressions: vec![NamedExpr {
                name: "name".to_string(),
                expr: column("name"),
            }],
            child: Box::new(LogicalPlan::Filter(LogicalFilter {
                predicate: gt_pred(column("age"), constant_int(18)),
                child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
            })),
        })),
        values_rows: Vec::new(),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.projection_pruning(plan);

    assert_eq!(
        collect_labels(&optimized),
        vec!["Insert", "Project", "Filter", "TableScan"]
    );

    // TableScan should have 'name' (from Project) and 'age' (from Filter)
    if let LogicalPlan::Insert(inp) = &optimized {
        if let LogicalPlan::Project(p) = &*inp.child {
            if let LogicalPlan::Filter(f) = &*p.child {
                if let LogicalPlan::TableScan(t) = &*f.child {
                    let col_names: Vec<&str> =
                        t.schema.columns.iter().map(|c| c.name.as_str()).collect();
                    assert_eq!(col_names.len(), 2, "Should keep name+age, prune id+email");
                    assert!(col_names.contains(&"name"));
                    assert!(col_names.contains(&"age"));
                } else {
                    panic!("Expected TableScan under Filter");
                }
            } else {
                panic!("Expected Filter under Project");
            }
        } else {
            panic!("Expected Project under Insert");
        }
    } else {
        panic!("Expected Insert root");
    }
}

#[test]
fn test_optimizer_pipeline_preserves_insert() {
    // Run the full optimizer pipeline — Insert should be preserved
    // and all optimisation passes should handle it correctly
    let plan = LogicalPlan::Insert(LogicalInsert {
        table: "target".to_string(),
        columns: vec!["name".to_string()],
        child: Box::new(LogicalPlan::Limit(LogicalLimit {
            limit: 10,
            offset: 0,
            child: Box::new(LogicalPlan::Sort(LogicalSort {
                order_by: vec![OrderByExpr {
                    expr: column("name"),
                    ascending: true,
                }],
                child: Box::new(LogicalPlan::Project(LogicalProject {
                    expressions: vec![NamedExpr {
                        name: "name".to_string(),
                        expr: column("name"),
                    }],
                    child: Box::new(LogicalPlan::Filter(LogicalFilter {
                        predicate: gt_pred(column("age"), constant_int(18)),
                        child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
                    })),
                })),
                limit: None,
            })),
        })),
        values_rows: Vec::new(),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.optimize(plan);

    // Insert should still be at root
    assert!(
        matches!(&optimized, LogicalPlan::Insert(_)),
        "Insert should remain at root after optimization"
    );

    // Structure should be: Insert -> Sort (limit pushed) -> Project -> Filter -> TableScan
    // (Limit is pushed into Sort, TableScan columns pruned to name+age)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Insert", "Sort", "Project", "Filter", "TableScan"],
        "Optimizer should preserve Insert at root and apply all passes to child"
    );

    // Verify Sort has the pushed-down limit
    if let LogicalPlan::Insert(inp) = &optimized {
        if let LogicalPlan::Sort(s) = &*inp.child {
            assert_eq!(s.limit, Some(10), "Limit should be pushed into Sort");
        } else {
            panic!("Expected Sort under Insert");
        }
    }
}

// ─── Join Ordering Tests ──────────────────────────────────────────────────────

#[test]
fn test_join_ordering_smaller_on_left() {
    let mut stats = HashMap::new();
    let large = crate::statistics::TableStatistics {
        total_tuple_count: 1000,
        total_pages: 10,
        data_pages: 9,
        file_size_bytes: 81920,
        total_tuple_bytes: 50000,
        total_slot_bytes: 4000,
        total_header_bytes: 9 * 128,
        total_free_bytes: 30000,
        pages_with_tuples: 9,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    let small = crate::statistics::TableStatistics {
        total_tuple_count: 100,
        total_pages: 2,
        data_pages: 1,
        file_size_bytes: 16384,
        total_tuple_bytes: 5000,
        total_slot_bytes: 400,
        total_header_bytes: 128,
        total_free_bytes: 10000,
        pages_with_tuples: 1,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    stats.insert("users".to_string(), large);
    stats.insert("orders".to_string(), small);

    let plan = LogicalPlan::Join(LogicalJoin {
        left: Box::new(make_table_scan("users", &["id"])),
        right: Box::new(make_table_scan("orders", &["id"])),
        join_type: rook_ast::JoinType::Inner,
        condition: Some(eq_pred(column("id"), column("id"))),
    });
    let optimizer = Optimizer::with_statistics(stats);
    let optimized = optimizer.join_ordering(plan);
    if let LogicalPlan::Join(j) = &optimized {
        if let LogicalPlan::TableScan(left_scan) = &*j.left {
            assert_eq!(
                left_scan.table, "orders",
                "Smaller table should be on the left"
            );
        } else {
            panic!("Expected TableScan on left side");
        }
        if let LogicalPlan::TableScan(right_scan) = &*j.right {
            assert_eq!(
                right_scan.table, "users",
                "Larger table should be on the right"
            );
        } else {
            panic!("Expected TableScan on right side");
        }
    } else {
        panic!("Expected Join root");
    }
}

#[test]
fn test_join_ordering_preserves_left_join() {
    // LEFT JOIN should NOT be reordered even when right side is smaller.
    // Swapping would change LEFT JOIN semantics (preserve-left becomes preserve-right).
    let mut stats = HashMap::new();
    let large = crate::statistics::TableStatistics {
        total_tuple_count: 1000,
        total_pages: 10,
        data_pages: 9,
        file_size_bytes: 81920,
        total_tuple_bytes: 50000,
        total_slot_bytes: 4000,
        total_header_bytes: 9 * 128,
        total_free_bytes: 30000,
        pages_with_tuples: 9,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    let small = crate::statistics::TableStatistics {
        total_tuple_count: 100,
        total_pages: 2,
        data_pages: 1,
        file_size_bytes: 16384,
        total_tuple_bytes: 5000,
        total_slot_bytes: 400,
        total_header_bytes: 128,
        total_free_bytes: 10000,
        pages_with_tuples: 1,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    // users is LARGE (should stay on left for LEFT JOIN), orders is SMALL
    stats.insert("users".to_string(), large);
    stats.insert("orders".to_string(), small);

    // LEFT JOIN with large left (users) and small right (orders) — should NOT swap
    let plan = LogicalPlan::Join(LogicalJoin {
        left: Box::new(make_table_scan("users", &["id"])),
        right: Box::new(make_table_scan("orders", &["id"])),
        join_type: rook_ast::JoinType::Left,
        condition: Some(eq_pred(column("id"), column("id"))),
    });
    let optimizer = Optimizer::with_statistics(stats.clone());
    let optimized = optimizer.join_ordering(plan);
    if let LogicalPlan::Join(j) = &optimized {
        if let LogicalPlan::TableScan(left_scan) = &*j.left {
            assert_eq!(
                left_scan.table, "users",
                "LEFT JOIN should keep large table on left (preserve-left semantics)"
            );
        } else {
            panic!("Expected TableScan on left side");
        }
    } else {
        panic!("Expected Join root");
    }
}

#[test]
fn test_join_ordering_preserves_right_join() {
    // RIGHT JOIN should NOT be reordered even when right side is smaller.
    // Swapping would change RIGHT JOIN semantics (preserve-right becomes preserve-left).
    let mut stats = HashMap::new();
    let large = crate::statistics::TableStatistics {
        total_tuple_count: 1000,
        total_pages: 10,
        data_pages: 9,
        file_size_bytes: 81920,
        total_tuple_bytes: 50000,
        total_slot_bytes: 4000,
        total_header_bytes: 9 * 128,
        total_free_bytes: 30000,
        pages_with_tuples: 9,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    let small = crate::statistics::TableStatistics {
        total_tuple_count: 100,
        total_pages: 2,
        data_pages: 1,
        file_size_bytes: 16384,
        total_tuple_bytes: 5000,
        total_slot_bytes: 400,
        total_header_bytes: 128,
        total_free_bytes: 10000,
        pages_with_tuples: 1,
        min_page_free_bytes: 100,
        max_page_free_bytes: 5000,
        min_tuple_bytes: 20,
        max_tuple_bytes: 100,
        page_breakdown: vec![],
    };
    // orders is SMALL (on right), users is LARGE (on left) — for RIGHT JOIN, the
    // right side (orders, small) must be preserved, NOT swapped to the left.
    stats.insert("users".to_string(), large);
    stats.insert("orders".to_string(), small);

    let plan = LogicalPlan::Join(LogicalJoin {
        left: Box::new(make_table_scan("users", &["id"])),
        right: Box::new(make_table_scan("orders", &["id"])),
        join_type: rook_ast::JoinType::Right,
        condition: Some(eq_pred(column("id"), column("id"))),
    });
    let optimizer = Optimizer::with_statistics(stats);
    let optimized = optimizer.join_ordering(plan);
    if let LogicalPlan::Join(j) = &optimized {
        if let LogicalPlan::TableScan(right_scan) = &*j.right {
            assert_eq!(
                right_scan.table, "orders",
                "RIGHT JOIN should keep small table on right (preserve-right semantics)"
            );
        } else {
            panic!("Expected TableScan on right side");
        }
    } else {
        panic!("Expected Join root");
    }
}

#[test]
fn test_join_ordering_reorders_inner_join() {
    // INNER JOIN is commutative — should be reordered when right is smaller.
    let mut stats = HashMap::new();
    stats.insert(
        "users".to_string(),
        crate::statistics::TableStatistics {
            total_tuple_count: 1000,
            total_pages: 10,
            data_pages: 9,
            file_size_bytes: 81920,
            total_tuple_bytes: 50000,
            total_slot_bytes: 4000,
            total_header_bytes: 9 * 128,
            total_free_bytes: 30000,
            pages_with_tuples: 9,
            min_page_free_bytes: 100,
            max_page_free_bytes: 5000,
            min_tuple_bytes: 20,
            max_tuple_bytes: 100,
            page_breakdown: vec![],
        },
    );
    stats.insert(
        "orders".to_string(),
        crate::statistics::TableStatistics {
            total_tuple_count: 100,
            total_pages: 2,
            data_pages: 1,
            file_size_bytes: 16384,
            total_tuple_bytes: 5000,
            total_slot_bytes: 400,
            total_header_bytes: 128,
            total_free_bytes: 10000,
            pages_with_tuples: 1,
            min_page_free_bytes: 100,
            max_page_free_bytes: 5000,
            min_tuple_bytes: 20,
            max_tuple_bytes: 100,
            page_breakdown: vec![],
        },
    );

    // INNER JOIN: large (users) left, small (orders) right — should swap
    let plan = LogicalPlan::Join(LogicalJoin {
        left: Box::new(make_table_scan("users", &["id"])),
        right: Box::new(make_table_scan("orders", &["id"])),
        join_type: rook_ast::JoinType::Inner,
        condition: Some(eq_pred(column("id"), column("id"))),
    });
    let optimizer = Optimizer::with_statistics(stats);
    let optimized = optimizer.join_ordering(plan);
    if let LogicalPlan::Join(j) = &optimized {
        if let LogicalPlan::TableScan(left_scan) = &*j.left {
            assert_eq!(
                left_scan.table, "orders",
                "INNER JOIN should move smaller table (orders) to left"
            );
        } else {
            panic!("Expected TableScan on left side");
        }
    } else {
        panic!("Expected Join root");
    }
}

// ─── Predicate Pushdown Into Join Conditions (M3) ──────────────────────────

#[test]
fn test_push_filter_with_both_side_predicate_merged_into_inner_join_condition() {
    // Filter: users.age > orders.amount
    // This predicate references columns from BOTH sides of the INNER JOIN.
    // It should be merged INTO the join condition, not kept as a separate Filter.
    // One comparison that reads a column from each side.
    let pred = gt_pred(column("age"), column("amount"));

    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: pred,
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("users", &["id", "name", "age"])),
            right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
            join_type: JoinType::Inner,
            condition: Some(eq_pred(column("user_id"), column("id"))),
        })),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);

    // After pushdown, the predicate should be merged into the join condition
    // so the structure should be: Join (not Filter(Join)).
    // collect_labels includes children: Join -> TableScan(left), TableScan(right)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Join", "TableScan", "TableScan"],
        "Cross-side predicate with INNER join should be merged into join condition"
    );

    // Verify the join condition has been extended with the new predicate
    if let LogicalPlan::Join(j) = &optimized {
        assert!(j.condition.is_some(), "Join should have a condition");
        // The condition should be an AND of the original + new predicate
        match &j.condition.as_ref().unwrap() {
            PredicateNode::BinaryOp {
                op: BinaryOp::And, ..
            } => {
                // Condition was merged — correct
            }
            other => panic!("Expected merged AND condition, got {:?}", other),
        }
    } else {
        panic!("Expected Join node");
    }
}

#[test]
fn test_push_cross_side_predicate_stays_above_left_join() {
    // Filter on both sides of a LEFT JOIN should stay above the join.
    // LEFT JOIN is not commutative — merging the filter into the condition
    // would change semantics (the filter would affect which rows are preserved).
    // One comparison that reads a column from each side.
    let pred = gt_pred(column("age"), column("amount"));

    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: pred,
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("users", &["id", "name", "age"])),
            right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
            join_type: JoinType::Left,
            condition: Some(eq_pred(column("user_id"), column("id"))),
        })),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);

    // For LEFT JOIN, the cross-side predicate should stay above as Filter.
    // collect_labels includes children: Filter -> Join -> TableScan(left), TableScan(right)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Filter", "Join", "TableScan", "TableScan"],
        "Cross-side predicate with LEFT JOIN should stay above as Filter"
    );
}

#[test]
fn test_push_cross_side_predicate_stays_above_right_join() {
    // One comparison that reads a column from each side.
    let pred = gt_pred(column("age"), column("amount"));

    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: pred,
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("users", &["id", "name", "age"])),
            right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
            join_type: JoinType::Right,
            condition: Some(eq_pred(column("user_id"), column("id"))),
        })),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);

    // collect_labels includes children: Filter -> Join -> TableScan(left), TableScan(right)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Filter", "Join", "TableScan", "TableScan"],
        "Cross-side predicate with RIGHT JOIN should stay above as Filter"
    );
}

#[test]
fn test_push_conjuncts_are_placed_independently() {
    // users.age > 18 AND orders.amount > 100.0: each conjunct reads one side,
    // so each is pushed to its own input instead of travelling as one unit.
    let pred = PredicateNode::BinaryOp {
        left: Box::new(gt_pred(column("age"), constant_int(18))),
        op: BinaryOp::And,
        right: Box::new(gt_pred(column("amount"), constant_float(100.0))),
    };
    let join = |join_type| {
        LogicalPlan::Filter(LogicalFilter {
            predicate: pred.clone(),
            child: Box::new(LogicalPlan::Join(LogicalJoin {
                left: Box::new(make_table_scan("users", &["id", "name", "age"])),
                right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
                join_type,
                condition: Some(eq_pred(column("user_id"), column("id"))),
            })),
        })
    };
    let optimizer = Optimizer::new();

    assert_eq!(
        collect_labels(&optimizer.predicate_pushdown(join(JoinType::Inner))),
        vec!["Join", "Filter", "TableScan", "Filter", "TableScan"],
    );
    // LEFT JOIN: only the preserved (left) side may be filtered below the join.
    assert_eq!(
        collect_labels(&optimizer.predicate_pushdown(join(JoinType::Left))),
        vec!["Filter", "Join", "Filter", "TableScan", "TableScan"],
    );
}

#[test]
fn test_push_qualified_predicate_is_not_single_sided() {
    // a.k = r.k: the bare name `k` exists on both sides, and the qualifiers
    // say it reads one column from each. It must not be pushed to one input.
    let qualified = |t: &str, c: &str| ExprNode::Compound(vec![t.to_string(), c.to_string()]);
    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: PredicateNode::BinaryOp {
            left: Box::new(eq_pred(qualified("a", "k"), qualified("r", "k"))),
            op: BinaryOp::And,
            right: Box::new(gt_pred(qualified("a", "id"), constant_int(2))),
        },
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("a", &["id", "x", "k"])),
            right: Box::new(make_table_scan("r", &["k", "v"])),
            join_type: JoinType::Cross,
            condition: None,
        })),
    });

    let optimized = Optimizer::new().predicate_pushdown(plan);
    assert_eq!(
        collect_labels(&optimized),
        vec!["Join", "Filter", "TableScan", "TableScan"],
    );
    match &optimized {
        LogicalPlan::Join(j) => assert!(j.condition.is_some(), "a.k = r.k joins the two sides"),
        _ => panic!("Expected Join node"),
    }
}

#[test]
fn test_push_single_side_predicate_pushed_through_join() {
    // Filter only on left side (users.age > 18) of an INNER JOIN.
    // Should be pushed to the left child (existing behavior).
    let pred = gt_pred(column("age"), constant_int(18));

    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: pred,
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("users", &["id", "name", "age"])),
            right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
            join_type: JoinType::Inner,
            condition: Some(eq_pred(column("user_id"), column("id"))),
        })),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);

    // Single-side predicate pushed to left child → Join(Filter(users), orders)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Join", "Filter", "TableScan", "TableScan"],
        "Single-side predicate should be pushed through join to correct side"
    );
}

#[test]
fn test_push_cross_side_predicate_merged_into_cross_join() {
    // CROSS JOIN with cross-side predicate referencing columns from BOTH sides
    // (age is left-only, amount is right-only) — should merge into condition
    // One comparison that reads a column from each side.
    let pred = gt_pred(column("age"), column("amount"));

    let plan = LogicalPlan::Filter(LogicalFilter {
        predicate: pred,
        child: Box::new(LogicalPlan::Join(LogicalJoin {
            left: Box::new(make_table_scan("users", &["id", "name", "age"])),
            right: Box::new(make_table_scan("orders", &["id", "user_id", "amount"])),
            join_type: JoinType::Cross,
            condition: None,
        })),
    });

    let optimizer = Optimizer::new();
    let optimized = optimizer.predicate_pushdown(plan);

    // Cross join with no existing condition — the predicate becomes the condition.
    // collect_labels includes children: Join -> TableScan(left), TableScan(right)
    let labels = collect_labels(&optimized);
    assert_eq!(
        labels,
        vec!["Join", "TableScan", "TableScan"],
        "Cross-side predicate with CROSS JOIN should be merged into condition"
    );

    if let LogicalPlan::Join(j) = &optimized {
        assert!(
            j.condition.is_some(),
            "CROSS JOIN should have a condition after merge"
        );
        assert_eq!(j.join_type, JoinType::Cross);
    } else {
        panic!("Expected Join node");
    }
}

// ─── Edge Case Tests ──────────────────────────────────────────────────────────

#[test]
fn test_optimize_simple_select_star() {
    let plan = LogicalPlan::Project(LogicalProject {
        expressions: vec![
            NamedExpr {
                name: "id".to_string(),
                expr: column("id"),
            },
            NamedExpr {
                name: "name".to_string(),
                expr: column("name"),
            },
            NamedExpr {
                name: "age".to_string(),
                expr: column("age"),
            },
            NamedExpr {
                name: "email".to_string(),
                expr: column("email"),
            },
        ],
        child: Box::new(make_table_scan("users", &["id", "name", "age", "email"])),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.optimize(plan);
    assert_eq!(collect_labels(&optimized), vec!["Project", "TableScan"]);
}

#[test]
fn test_optimize_identity_no_changes() {
    let plan = make_table_scan("users", &["id", "name"]);
    let optimizer = Optimizer::new();
    let optimized = optimizer.optimize(plan);
    assert_eq!(collect_labels(&optimized), vec!["TableScan"]);
}

#[test]
fn test_multiple_optimization_passes() {
    let table_scan = make_table_scan("users", &["id", "name", "age", "email"]);
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 10,
        offset: 0,
        child: Box::new(LogicalPlan::Sort(LogicalSort {
            order_by: vec![OrderByExpr {
                expr: column("name"),
                ascending: true,
            }],
            child: Box::new(LogicalPlan::Project(LogicalProject {
                expressions: vec![NamedExpr {
                    name: "name".to_string(),
                    expr: column("name"),
                }],
                child: Box::new(LogicalPlan::Filter(LogicalFilter {
                    predicate: gt_pred(column("age"), constant_int(18)),
                    child: Box::new(table_scan),
                })),
            })),
            limit: None,
        })),
    });
    let optimizer = Optimizer::new();
    let once = optimizer.optimize(plan);
    let labels_once = collect_labels(&once);
    let twice = optimizer.optimize(once);
    let labels_twice = collect_labels(&twice);
    assert_eq!(labels_once, labels_twice, "Optimizer should be idempotent");
}

#[test]
fn test_distinct_prevents_limit_pushdown_to_sort() {
    let table_scan = make_table_scan("items", &["id", "category"]);
    let plan = LogicalPlan::Limit(LogicalLimit {
        limit: 5,
        offset: 2,
        child: Box::new(LogicalPlan::Distinct(LogicalDistinct {
            child: Box::new(LogicalPlan::Sort(LogicalSort {
                order_by: vec![OrderByExpr {
                    expr: column("category"),
                    ascending: true,
                }],
                child: Box::new(table_scan),
                limit: None,
            })),
        })),
    });
    let optimizer = Optimizer::new();
    let optimized = optimizer.optimize(plan);

    // Verify the sort beneath Distinct did NOT have a limit pushed into it
    match optimized {
        LogicalPlan::Limit(l) => {
            assert_eq!(l.limit, 5);
            assert_eq!(l.offset, 2);
            match *l.child {
                LogicalPlan::Distinct(d) => match *d.child {
                    LogicalPlan::Sort(s) => {
                        assert_eq!(
                            s.limit, None,
                            "Sort under Distinct must NOT have top-k limit injected"
                        );
                    }
                    other => panic!("expected Sort under Distinct, got {:?}", other),
                },
                other => panic!("expected Distinct under Limit, got {:?}", other),
            }
        }
        other => panic!("expected Limit at top, got {:?}", other),
    }
}
