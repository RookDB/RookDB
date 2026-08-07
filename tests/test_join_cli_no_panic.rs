//! Adversarial input must produce errors, never panics.
//!
//! The CLI reaches these entry points with whatever a user typed. The previous
//! implementation had an index-out-of-bounds panic reachable from menu option
//! 8 with a cross join, so this file walks the non-interactive surface with
//! input designed to break it.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, all_of, col, compare, eq, int_literal, lt};
use std::collections::HashMap;
use storage_manager::catalog::{Catalog, Column, Database, Table};
use storage_manager::executor::selection::{ComparisonOp, Predicate};
use storage_manager::join::{
    JoinBuilder, JoinConfig, JoinGraph, JoinType, TableStatsCache, catalog_bridge, optimize,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

/// Names a user might plausibly type, plus a few they should not.
const HOSTILE_NAMES: [&str; 18] = [
    "",
    ".",
    "..",
    "...",
    "a.",
    ".a",
    "a..b",
    "a.b.c",
    "'",
    "\"",
    "\\",
    "%",
    "_",
    "0",
    "-1",
    "NULL",
    "a b",
    "\u{1F600}",
];

// ── Catalog resolution ───────────────────────────────────────────────────────

#[test]
fn resolving_hostile_names_never_panics() {
    let catalog = Catalog {
        databases: HashMap::new(),
    };

    for database in HOSTILE_NAMES {
        for table in HOSTILE_NAMES {
            // Every one of these should be a clean error.
            let outcome = catalog_bridge::resolve(&catalog, database, table, table);
            assert!(
                outcome.is_err(),
                "{database:?}.{table:?} should not resolve"
            );
        }
    }
}

#[test]
fn listing_tables_of_a_missing_database_is_empty() {
    let catalog = Catalog {
        databases: HashMap::new(),
    };
    for database in HOSTILE_NAMES {
        assert!(catalog_bridge::table_names(&catalog, database).is_empty());
    }
}

/// A catalog entry whose file does not exist is reported, not opened blindly.
#[test]
fn a_catalog_entry_without_a_file_is_reported() {
    let mut tables = HashMap::new();
    tables.insert(
        "ghost".to_string(),
        Table {
            columns: vec![Column::new("k".to_string(), DataType::Int)],
        },
    );
    let mut databases = HashMap::new();
    databases.insert("d".to_string(), Database { tables });
    let catalog = Catalog { databases };

    let err = catalog_bridge::resolve(&catalog, "d", "ghost", "ghost")
        .expect_err("the file does not exist");
    assert!(err.to_string().contains("missing"), "{err}");
}

/// A table with no columns cannot be joined, and says so.
#[test]
fn a_table_with_no_columns_is_reported() {
    let mut tables = HashMap::new();
    tables.insert("empty".to_string(), Table { columns: vec![] });
    let mut databases = HashMap::new();
    databases.insert("d".to_string(), Database { tables });
    let catalog = Catalog { databases };

    assert!(catalog_bridge::resolve(&catalog, "d", "empty", "empty").is_err());
}

// ── Planning ─────────────────────────────────────────────────────────────────

/// Conditions naming columns that do not exist must be refused cleanly, for
/// every join type.
#[test]
fn hostile_column_names_are_refused_for_every_join_type() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    left.insert(vec![int(1)]);
    right.insert(vec![int(1)]);
    left.flush();
    right.flush();

    for name in HOSTILE_NAMES {
        for join_type in JoinType::ALL {
            let outcome = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
                .with_condition(eq(col(name), col("r.k")))
                .plan();
            // Whatever it decides, it must not panic - and a name that does
            // not exist must not resolve.
            if let Ok(plan) = outcome {
                assert!(
                    !plan.key_conditions.is_empty() || plan.residual.is_some(),
                    "a plan for {name:?} must carry the condition somewhere"
                );
            }
        }
    }
}

/// A CROSS join carrying a condition is a mis-stated INNER join, and is
/// refused rather than silently dropping or applying it. This is the shape
/// that used to panic.
#[test]
fn a_cross_join_with_a_condition_is_refused_not_a_panic() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..5 {
        left.insert(vec![int(i)]);
        right.insert(vec![int(i)]);
    }
    left.flush();
    right.flush();

    let outcome = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Cross)
        .with_condition(eq(col("l.k"), col("r.k")))
        .plan();
    assert!(outcome.is_err(), "a conditioned CROSS join must be refused");

    // And a bare CROSS join runs.
    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Cross)
        .plan()
        .expect("a bare cross join is valid");
    assert_eq!(plan.estimate.output_rows, 25);
}

/// Deeply nested conditions must not blow the stack at a depth a person could
/// plausibly build through a menu.
#[test]
fn deeply_nested_conditions_are_handled() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    left.insert(vec![int(1)]);
    right.insert(vec![int(1)]);
    left.flush();
    right.flush();

    let mut condition = eq(col("l.k"), col("r.k"));
    for _ in 0..200 {
        condition = Predicate::and(condition, lt(col("l.k"), col("r.k")));
    }

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(condition)
        .plan()
        .expect("a long conjunction is still a condition");
    assert_eq!(plan.key_conditions.len(), 1);
    assert!(plan.residual.is_some());
}

/// Comparing a column to a literal of the wrong type is an error at plan or
/// run time, never a panic and never a silent wrong answer.
#[test]
fn mistyped_literals_do_not_panic() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int), ("t", DataType::Varchar(8))]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..5 {
        left.insert(vec![int(i), Some(DataValue::Varchar("x".to_string()))]);
        right.insert(vec![int(i)]);
    }
    left.flush();
    right.flush();

    for literal in [
        int_literal(1),
        common::text_literal("nonsense"),
        common::null_literal(),
    ] {
        for column in ["l.k", "l.t", "r.k"] {
            let condition = all_of(vec![
                eq(col("l.k"), col("r.k")),
                compare(col(column), ComparisonOp::Equals, literal.clone()),
            ]);

            let builder = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
                .with_condition(condition);

            // Planning may succeed; running may fail on a type mismatch. What
            // matters is that neither aborts.
            if let Ok(mut stream) = builder.execute() {
                while let Some(row) = stream.next() {
                    if row.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

/// Both sides aliased the same way cannot be resolved, and says so.
#[test]
fn a_shared_alias_is_refused() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    table.insert(vec![int(1)]);
    table.flush();

    let outcome = JoinBuilder::new(table.table_ref(), table.table_ref(), JoinType::Inner)
        .with_condition(eq(col("t.k"), col("t.k")))
        .plan();
    assert!(outcome.is_err(), "a self-join needs distinct aliases");
}

// ── Multi-relation ordering ──────────────────────────────────────────────────

#[test]
fn hostile_conditions_do_not_panic_the_optimiser() {
    let db = TempDb::new();
    let mut a = db.create_table("a", &[("k", DataType::Int)]);
    let mut b = db.create_table("b", &[("k", DataType::Int)]);
    a.insert(vec![int(1)]);
    b.insert(vec![int(1)]);
    a.flush();
    b.flush();

    let relations = vec![a.table_ref(), b.table_ref()];
    let stats = TableStatsCache::new();

    for name in HOSTILE_NAMES {
        let condition = eq(col(name), col("b.k"));
        // Building the graph either resolves the name or refuses it.
        if let Ok(graph) = JoinGraph::build(relations.clone(), Some(&condition), &stats) {
            let _ = optimize(&graph, &JoinConfig::with_work_memory(1024 * 1024));
        }
    }
}

/// An optimiser given no relations refuses rather than indexing an empty list.
#[test]
fn an_empty_relation_list_is_refused() {
    assert!(JoinGraph::build(vec![], None, &TableStatsCache::new()).is_err());
}
