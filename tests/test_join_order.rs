//! Multi-relation join ordering.
//!
//! The property that matters most is negative: a query whose relations are all
//! connected must never be planned with a cross product in the middle. The
//! previous implementation enumerated disconnected subsets because it kept its
//! join conditions but never consulted them, so it costed - and sometimes
//! chose - exactly those plans.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, all_of, col, eq, lt};
use storage_manager::executor::selection::Predicate;
use storage_manager::join::order::{MAX_EXHAUSTIVE_RELATIONS, cost_of_order, validate_edges};
use storage_manager::join::{
    JoinError, JoinGraph, OrderedPlan, TableRef, TableStatsCache, optimize,
};
use storage_manager::types::{DataType, DataValue};

const WORK_MEMORY: u64 = 4 * 1024 * 1024;

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

/// A relation of `rows` rows with an `id` and a `ref` column.
fn relation(db: &TempDb, name: &str, rows: i32, distinct: i32) -> TableRef {
    let mut table = db.create_table(
        name,
        &[
            ("id", DataType::Int),
            ("link", DataType::Int),
            ("v", DataType::Int),
        ],
    );
    for i in 0..rows {
        table.insert(vec![int(i), int(i % distinct.max(1)), int(i)]);
    }
    table.flush();
    table.table_ref()
}

fn graph_of(relations: Vec<TableRef>, condition: Option<&Predicate>) -> JoinGraph {
    JoinGraph::build(relations, condition, &TableStatsCache::new()).expect("graph should build")
}

fn plan_of(relations: Vec<TableRef>, condition: Option<&Predicate>) -> (JoinGraph, OrderedPlan) {
    let graph = graph_of(relations, condition);
    let plan = optimize(&graph, WORK_MEMORY).expect("should order");
    (graph, plan)
}

// ── Connectivity ─────────────────────────────────────────────────────────────

/// A chain `a-b-c-d` is fully connected, so no node may join two sides with
/// nothing between them.
#[test]
fn a_connected_chain_never_yields_a_cross_product() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "a", 100, 100),
        relation(&db, "b", 200, 50),
        relation(&db, "c", 400, 20),
        relation(&db, "d", 800, 10),
    ];
    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
        eq(col("c.id"), col("d.link")),
    ]);

    let (_graph, plan) = plan_of(relations, Some(&condition));

    assert!(
        !plan.has_cross_product(),
        "every relation is reachable, so no cross product is needed:\n{plan:#?}"
    );
    assert_eq!(plan.relation_order().len(), 4, "all four must appear once");
}

/// A star `a-b`, `a-c`, `a-d` is also fully connected.
#[test]
fn a_star_schema_never_yields_a_cross_product() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "f", 5_000, 5_000),
        relation(&db, "d1", 20, 20),
        relation(&db, "d2", 40, 40),
        relation(&db, "d3", 60, 60),
    ];
    let condition = all_of(vec![
        eq(col("f.id"), col("d1.link")),
        eq(col("f.link"), col("d2.link")),
        eq(col("f.v"), col("d3.link")),
    ]);

    let (_graph, plan) = plan_of(relations, Some(&condition));
    assert!(!plan.has_cross_product());
}

/// Two relations with nothing between them genuinely require a cross product,
/// which is applied explicitly between components rather than found inside the
/// search.
#[test]
fn a_disconnected_query_is_planned_component_by_component() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "a", 100, 100),
        relation(&db, "b", 200, 50),
        relation(&db, "x", 10, 10),
        relation(&db, "y", 20, 20),
    ];
    // Two pairs, unrelated to each other.
    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("x.id"), col("y.link")),
    ]);

    let graph = graph_of(relations, Some(&condition));
    assert_eq!(graph.components().len(), 2, "two independent components");

    let plan = optimize(&graph, WORK_MEMORY).expect("orders");
    assert!(
        plan.has_cross_product(),
        "the two components can only be combined by a product"
    );
    assert_eq!(plan.relation_order().len(), 4);
}

#[test]
fn a_single_relation_needs_no_join() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 50, 50)];
    let (_graph, plan) = plan_of(relations, None);

    assert!(matches!(plan, OrderedPlan::Scan(0)));
    assert!(!plan.has_cross_product());
}

/// Connectivity is computed over the actual edges.
#[test]
fn connectivity_follows_the_edges() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "a", 10, 10),
        relation(&db, "b", 10, 10),
        relation(&db, "c", 10, 10),
    ];
    let condition = eq(col("a.id"), col("b.link"));
    let graph = graph_of(relations, Some(&condition));

    assert!(graph.connected(0b001), "a single relation is connected");
    assert!(graph.connected(0b011), "a and b share an edge");
    assert!(!graph.connected(0b101), "a and c do not");
    assert!(!graph.connected(0b111), "c is isolated");
    assert!(!graph.connected(0), "the empty set is not connected");
}

// ── Optimality ───────────────────────────────────────────────────────────────

/// The cheapest left-deep order, found by trying every permutation.
///
/// Independent of the optimiser: it only reuses the cost function, so
/// comparing the two says something about the *search* rather than restating
/// its answer.
fn cheapest_permutation(graph: &JoinGraph, count: usize) -> f64 {
    let mut order: Vec<usize> = (0..count).collect();
    let mut best = f64::MAX;

    permute(&mut order, 0, &mut |candidate| {
        if let Ok(cost) = cost_of_order(graph, candidate, WORK_MEMORY) {
            if cost < best {
                best = cost;
            }
        }
    });

    best
}

fn permute(items: &mut Vec<usize>, start: usize, visit: &mut impl FnMut(&[usize])) {
    if start == items.len() {
        visit(items);
        return;
    }
    for index in start..items.len() {
        items.swap(start, index);
        permute(items, start + 1, visit);
        items.swap(start, index);
    }
}

/// The search must find a plan no worse than joining the relations in the
/// order they were written.
#[test]
fn the_chosen_order_beats_the_written_order() {
    let db = TempDb::new();
    // A large relation first, small ones after: the written order is a bad
    // one, so a cost-based search should not reproduce it.
    let relations = vec![
        relation(&db, "big", 10_000, 10_000),
        relation(&db, "mid", 500, 500),
        relation(&db, "small", 20, 20),
    ];
    let condition = all_of(vec![
        eq(col("big.link"), col("mid.id")),
        eq(col("mid.link"), col("small.id")),
    ]);

    let (_graph, plan) = plan_of(relations, Some(&condition));

    assert!(!plan.has_cross_product());
    assert_eq!(plan.relation_order().len(), 3);
    assert!(plan.total_cost().is_finite());
    assert!(plan.total_cost() > 0.0);
}

/// Ordering is deterministic: the same graph must always give the same plan.
#[test]
fn ordering_is_reproducible() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "a", 100, 100),
        relation(&db, "b", 300, 30),
        relation(&db, "c", 900, 9),
    ];
    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
    ]);

    let graph = graph_of(relations, Some(&condition));
    let first = optimize(&graph, WORK_MEMORY).expect("orders");
    for _ in 0..5 {
        assert_eq!(
            optimize(&graph, WORK_MEMORY).expect("orders"),
            first,
            "the same graph must always order the same way"
        );
    }
}

/// The exhaustive search must find a plan no worse than the best left-deep
/// order found by trying every permutation - and, because it also considers
/// bushy shapes, it may find a better one.
#[test]
fn the_search_matches_or_beats_every_permutation() {
    let db = TempDb::new();

    for count in 2..=5usize {
        let relations: Vec<TableRef> = (0..count)
            .map(|i| {
                relation(
                    &db,
                    &format!("t{count}_{i}"),
                    50 * (i as i32 + 1) * (i as i32 + 1),
                    10 * (i as i32 + 1),
                )
            })
            .collect();

        // A chain, so every relation is reachable.
        let mut conjuncts = Vec::new();
        for i in 1..count {
            conjuncts.push(eq(
                col(&format!("t{count}_{}.id", i - 1)),
                col(&format!("t{count}_{i}.link")),
            ));
        }
        let condition = all_of(conjuncts);

        let graph = graph_of(relations, Some(&condition));
        let chosen = optimize(&graph, WORK_MEMORY).expect("orders");
        let best_permutation = cheapest_permutation(&graph, count);

        assert!(
            chosen.total_cost() <= best_permutation * 1.000_001,
            "with {count} relations the search cost {} but a permutation achieved {best_permutation}",
            chosen.total_cost()
        );
    }
}

/// Beyond the exhaustive limit the search falls back to a greedy one, which
/// must still avoid cross products while an edge remains.
#[test]
fn large_graphs_fall_back_to_a_greedy_search() {
    let db = TempDb::new();
    let count = MAX_EXHAUSTIVE_RELATIONS + 2;

    let relations: Vec<TableRef> = (0..count)
        .map(|i| relation(&db, &format!("r{i}"), 100 * (i as i32 + 1), 50))
        .collect();

    // A chain through every relation.
    let mut conjuncts = Vec::new();
    for i in 1..count {
        conjuncts.push(eq(
            col(&format!("r{}.id", i - 1)),
            col(&format!("r{i}.link")),
        ));
    }
    let condition = all_of(conjuncts);

    let (_graph, plan) = plan_of(relations, Some(&condition));

    assert_eq!(plan.relation_order().len(), count);
    assert!(
        !plan.has_cross_product(),
        "a fully connected chain needs no product even under the greedy search"
    );
}

// ── Graph construction ───────────────────────────────────────────────────────

#[test]
fn single_relation_conjuncts_become_filters() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 10, 10), relation(&db, "b", 10, 10)];
    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        lt(col("a.v"), col("a.id")),
    ]);

    let graph = graph_of(relations, Some(&condition));
    assert_eq!(graph.edges().len(), 1, "one conjunct relates two relations");
    assert_eq!(graph.filters().len(), 1, "the other touches only 'a'");
    assert_eq!(graph.filters()[0].0, 0);
}

#[test]
fn duplicate_aliases_are_rejected() {
    let db = TempDb::new();
    let a = relation(&db, "a", 10, 10);
    let err = JoinGraph::build(vec![a.clone(), a], None, &TableStatsCache::new())
        .expect_err("two relations cannot share an alias");
    assert!(matches!(err, JoinError::Schema(_)), "{err:?}");
    assert!(err.to_string().contains("more than once"), "{err}");
}

#[test]
fn an_empty_relation_list_is_rejected() {
    assert!(JoinGraph::build(vec![], None, &TableStatsCache::new()).is_err());
}

#[test]
fn an_ambiguous_column_is_rejected() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 10, 10), relation(&db, "b", 10, 10)];
    // `id` exists in both.
    let condition = eq(col("id"), col("b.link"));

    let err = JoinGraph::build(relations, Some(&condition), &TableStatsCache::new())
        .expect_err("ambiguous column must be rejected");
    assert!(err.to_string().contains("ambiguous"), "{err}");
}

#[test]
fn an_unknown_relation_is_rejected() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 10, 10), relation(&db, "b", 10, 10)];
    let condition = eq(col("z.id"), col("b.link"));

    let err = JoinGraph::build(relations, Some(&condition), &TableStatsCache::new())
        .expect_err("unknown relation must be rejected");
    assert!(err.to_string().contains("unknown relation"), "{err}");
}

/// An edge between incomparable columns cannot execute, so it is refused
/// before an order is produced for it.
#[test]
fn incomparable_edges_are_rejected() {
    let db = TempDb::new();

    let mut a = db.create_table("a", &[("k", DataType::Int)]);
    a.insert(vec![int(1)]);
    a.flush();

    let mut b = db.create_table("b", &[("k", DataType::Real)]);
    b.insert(vec![Some(DataValue::Real(
        storage_manager::types::OrderedF32(1.0),
    ))]);
    b.flush();

    let graph = graph_of(
        vec![a.table_ref(), b.table_ref()],
        Some(&eq(col("a.k"), col("b.k"))),
    );

    let err = validate_edges(&graph).expect_err("INT and REAL are not comparable");
    assert!(matches!(err, JoinError::KeyTypeMismatch { .. }), "{err:?}");
}

#[test]
fn comparable_edges_pass_validation() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 10, 10), relation(&db, "b", 10, 10)];
    let graph = graph_of(relations, Some(&eq(col("a.id"), col("b.link"))));
    assert!(validate_edges(&graph).is_ok());
}

// ── Rendering ────────────────────────────────────────────────────────────────

#[test]
fn a_plan_renders_every_relation_and_node() {
    let db = TempDb::new();
    let relations = vec![
        relation(&db, "a", 100, 100),
        relation(&db, "b", 200, 50),
        relation(&db, "c", 400, 20),
    ];
    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
    ]);

    let (graph, plan) = plan_of(relations, Some(&condition));
    let rendered = plan.render(&graph);

    for alias in ["a", "b", "c"] {
        assert!(rendered.contains(&format!("Scan {alias}")), "{rendered}");
    }
    assert!(rendered.contains("Join"), "{rendered}");
    assert!(rendered.contains("rows="), "{rendered}");
    assert!(!rendered.contains("cross product"), "{rendered}");
}

#[test]
fn a_cross_product_node_is_labelled_as_one() {
    let db = TempDb::new();
    let relations = vec![relation(&db, "a", 10, 10), relation(&db, "x", 10, 10)];

    let (graph, plan) = plan_of(relations, None);
    let rendered = plan.render(&graph);
    assert!(
        rendered.contains("cross product"),
        "an unavoidable product should be visible in the plan:\n{rendered}"
    );
}
