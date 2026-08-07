//! Executing a chosen multi-relation join order.
//!
//! Each case is checked against an independent reference that joins the
//! relations naively, in the order they were written, with no ordering, no
//! partitioning and no materialised intermediates. Agreeing with it means the
//! chosen order and the machinery that runs it did not change the answer.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, all_of, assert_rows_eq, col, eq, lt};
use storage_manager::executor::selection::Predicate;
use storage_manager::join::{
    JoinConfig, JoinGraph, RowCodec, TableRef, TableStatsCache, execute_ordered, optimize,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

/// Run a chosen order and decode every row.
fn run(
    relations: Vec<TableRef>,
    condition: Option<&Predicate>,
    work_memory: u64,
    scratch: &TempDb,
) -> Vec<Vec<Option<DataValue>>> {
    let graph = JoinGraph::build(relations, condition, &TableStatsCache::new())
        .expect("graph should build");
    let config = JoinConfig::with_work_memory(work_memory).spill_root(scratch.path());
    let plan = optimize(&graph, &config).expect("should order");
    let mut stream = execute_ordered(&graph, &plan, &config).expect("should execute");

    let codec = RowCodec::new(stream.schema().types.clone());
    let mut rows = Vec::new();
    while let Some(row) = stream.next() {
        rows.push(codec.decode(&row.expect("should run")).expect("decode"));
    }
    rows
}

/// The output column order of a chosen plan, as qualified names.
fn column_names(
    relations: Vec<TableRef>,
    condition: Option<&Predicate>,
    scratch: &TempDb,
) -> Vec<String> {
    let graph = JoinGraph::build(relations, condition, &TableStatsCache::new()).expect("graph");
    let config = JoinConfig::with_work_memory(4 * 1024 * 1024).spill_root(scratch.path());
    let plan = optimize(&graph, &config).expect("order");
    let stream = execute_ordered(&graph, &plan, &config).expect("execute");
    stream
        .schema()
        .columns
        .iter()
        .map(|column| column.qualified_name.clone())
        .collect()
}

/// Join every relation in the written order with a naive nested loop, applying
/// each conjunct as soon as its relations are all present.
///
/// Shares nothing with the optimiser or the operators except the evaluator for
/// individual comparisons, so agreeing with it says the ordering and execution
/// machinery preserved the answer.
fn reference(
    tables: &[&common::TableHandle],
    condition: Option<&Predicate>,
) -> Vec<Vec<Option<DataValue>>> {
    let relations: Vec<TableRef> = tables.iter().map(|t| t.table_ref()).collect();
    let graph = JoinGraph::build(relations, condition, &TableStatsCache::new()).expect("graph");

    // Column offset of each relation in the concatenated row.
    let mut offsets = Vec::new();
    let mut running = 0usize;
    for table in tables {
        offsets.push(running);
        running += table.table_ref().columns.len();
    }

    // Start with the first relation and add one at a time.
    let mut current: Vec<Vec<Option<DataValue>>> = tables[0].rows().iter().cloned().collect();
    let mut present = 1u64;

    for (index, table) in tables.iter().enumerate().skip(1) {
        let mut next = Vec::new();
        for left in &current {
            for right in table.rows() {
                let mut combined = left.clone();
                combined.extend(right.iter().cloned());
                next.push(combined);
            }
        }
        present |= 1u64 << index;

        // Apply every conjunct whose relations are now all present.
        current = next
            .into_iter()
            .filter(|row| {
                graph.conjuncts().iter().all(|conjunct| {
                    if conjunct.mask & !present != 0 {
                        return true;
                    }
                    evaluate(&graph, &conjunct.predicate, row, &offsets)
                })
            })
            .collect();
    }

    // A conjunct over a single relation still has to hold.
    current
        .into_iter()
        .filter(|row| {
            graph
                .conjuncts()
                .iter()
                .all(|conjunct| evaluate(&graph, &conjunct.predicate, row, &offsets))
        })
        .collect()
}

/// Evaluate one comparison directly against a concatenated row.
fn evaluate(
    graph: &JoinGraph,
    predicate: &Predicate,
    row: &[Option<DataValue>],
    offsets: &[usize],
) -> bool {
    use storage_manager::executor::selection::{ComparisonOp, Expr};
    use storage_manager::types::comparison::compare_nullable;

    let value = |expr: &Expr| -> Option<Option<DataValue>> {
        match expr {
            Expr::Column(reference) => {
                let (relation, column) = graph.resolve(&reference.column_name).ok()?;
                row.get(offsets[relation] + column).cloned()
            }
            Expr::Constant(constant) => {
                Some(storage_manager::executor::selection::constant_to_data_value(constant))
            }
            _ => None,
        }
    };

    let Predicate::Compare(left, op, right) = predicate else {
        return true;
    };
    let (Some(left), Some(right)) = (value(left), value(right)) else {
        return true;
    };

    match compare_nullable(left.as_ref(), right.as_ref()) {
        Ok(Some(ordering)) => match op {
            ComparisonOp::Equals => ordering.is_eq(),
            ComparisonOp::NotEquals => !ordering.is_eq(),
            ComparisonOp::LessThan => ordering.is_lt(),
            ComparisonOp::LessOrEqual => ordering.is_le(),
            ComparisonOp::GreaterThan => ordering.is_gt(),
            ComparisonOp::GreaterOrEqual => ordering.is_ge(),
        },
        // NULL or incomparable: never a match.
        _ => false,
    }
}

/// `a → b → c → d`, with a NULL key and an unmatched row in each.
fn chain(db: &TempDb) -> Vec<common::TableHandle> {
    let mut a = db.create_table("a", &[("id", DataType::Int), ("v", DataType::Int)]);
    a.insert_all(vec![
        vec![int(1), int(10)],
        vec![int(2), int(20)],
        vec![int(3), int(30)],
        vec![None, int(40)],
    ]);
    a.flush();

    let mut b = db.create_table("b", &[("link", DataType::Int), ("id", DataType::Int)]);
    b.insert_all(vec![
        vec![int(1), int(100)],
        vec![int(1), int(101)],
        vec![int(2), int(102)],
        vec![int(9), int(103)],
    ]);
    b.flush();

    let mut c = db.create_table("c", &[("link", DataType::Int), ("id", DataType::Int)]);
    c.insert_all(vec![
        vec![int(100), int(1000)],
        vec![int(102), int(1001)],
        vec![int(999), int(1002)],
    ]);
    c.flush();

    let mut d = db.create_table(
        "d",
        &[("link", DataType::Int), ("label", DataType::Varchar(8))],
    );
    d.insert_all(vec![
        vec![int(1000), text("x")],
        vec![int(1001), text("y")],
        vec![None, text("z")],
    ]);
    d.flush();

    vec![a, b, c, d]
}

fn chain_condition() -> Predicate {
    all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
        eq(col("c.id"), col("d.link")),
    ])
}

// ── Correctness ──────────────────────────────────────────────────────────────

#[test]
fn a_four_relation_chain_matches_the_reference() {
    let db = TempDb::new();
    let tables = chain(&db);
    let condition = chain_condition();

    let expected = reference(
        &[&tables[0], &tables[1], &tables[2], &tables[3]],
        Some(&condition),
    );
    let actual = run(
        tables.iter().map(|t| t.table_ref()).collect(),
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );

    assert!(!expected.is_empty(), "the fixture should produce rows");
    assert_rows_eq(&actual, &expected, "four-relation chain");
}

/// A star: one relation joined to three others.
#[test]
fn a_star_schema_matches_the_reference() {
    let db = TempDb::new();

    let mut f = db.create_table(
        "f",
        &[
            ("k1", DataType::Int),
            ("k2", DataType::Int),
            ("k3", DataType::Int),
        ],
    );
    for i in 0..40 {
        f.insert(vec![int(i % 5), int(i % 7), int(i % 3)]);
    }
    f.flush();

    let mut d1 = db.create_table("d1", &[("id", DataType::Int), ("n", DataType::Varchar(8))]);
    let mut d2 = db.create_table("d2", &[("id", DataType::Int), ("n", DataType::Varchar(8))]);
    let mut d3 = db.create_table("d3", &[("id", DataType::Int), ("n", DataType::Varchar(8))]);
    for i in 0..5 {
        d1.insert(vec![int(i), text(&format!("a{i}"))]);
    }
    for i in 0..7 {
        d2.insert(vec![int(i), text(&format!("b{i}"))]);
    }
    for i in 0..3 {
        d3.insert(vec![int(i), text(&format!("c{i}"))]);
    }
    d1.flush();
    d2.flush();
    d3.flush();

    let condition = all_of(vec![
        eq(col("f.k1"), col("d1.id")),
        eq(col("f.k2"), col("d2.id")),
        eq(col("f.k3"), col("d3.id")),
    ]);

    let expected = reference(&[&f, &d1, &d2, &d3], Some(&condition));
    let actual = run(
        vec![
            f.table_ref(),
            d1.table_ref(),
            d2.table_ref(),
            d3.table_ref(),
        ],
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );

    assert_eq!(
        actual.len(),
        40,
        "every fact row matches one of each dimension"
    );
    assert_rows_eq(&actual, &expected, "star schema");
}

/// A conjunct mentioning three relations can only be evaluated once all three
/// are present, so it must be deferred rather than forced onto an edge.
#[test]
fn a_three_relation_conjunct_is_applied_once_all_three_are_present() {
    let db = TempDb::new();
    let tables = chain(&db);

    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
        // Spans a, b and c at once.
        lt(col("a.v"), col("c.id")),
    ]);

    let expected = reference(&[&tables[0], &tables[1], &tables[2]], Some(&condition));
    let actual = run(
        vec![
            tables[0].table_ref(),
            tables[1].table_ref(),
            tables[2].table_ref(),
        ],
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );

    assert_rows_eq(&actual, &expected, "three-relation conjunct");
}

/// A single-relation conjunct is still applied.
#[test]
fn single_relation_filters_are_applied() {
    let db = TempDb::new();
    let tables = chain(&db);

    let with_filter = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("a.id"), col("a.id")),
        lt(col("a.v"), col("a.id")),
    ]);

    let actual = run(
        vec![tables[0].table_ref(), tables[1].table_ref()],
        Some(&with_filter),
        4 * 1024 * 1024,
        &db,
    );
    assert!(
        actual.is_empty(),
        "no row has v < id, so the filter must eliminate everything: {actual:#?}"
    );
}

/// NULL keys never match, however many relations deep.
#[test]
fn null_keys_never_match_across_a_chain() {
    let db = TempDb::new();
    let tables = chain(&db);
    let condition = chain_condition();

    let actual = run(
        tables.iter().map(|t| t.table_ref()).collect(),
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );

    for row in &actual {
        // a.id, b.link, b.id, c.link, c.id, d.link
        for index in [0, 2, 3, 4, 5, 6] {
            assert!(
                row[index].is_some(),
                "column {index} of a matched row must be non-NULL: {row:?}"
            );
        }
    }
}

/// Two components with nothing between them produce their cross product.
#[test]
fn a_disconnected_query_produces_the_product() {
    let db = TempDb::new();

    let mut a = db.create_table("a", &[("id", DataType::Int)]);
    let mut b = db.create_table("b", &[("link", DataType::Int)]);
    let mut x = db.create_table("x", &[("id", DataType::Int)]);

    a.insert_all(vec![vec![int(1)], vec![int(2)]]);
    b.insert_all(vec![vec![int(1)], vec![int(2)], vec![int(3)]]);
    x.insert_all(vec![vec![int(7)], vec![int(8)]]);
    a.flush();
    b.flush();
    x.flush();

    let condition = eq(col("a.id"), col("b.link"));
    let actual = run(
        vec![a.table_ref(), b.table_ref(), x.table_ref()],
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );

    // a joins b on two rows, times two unrelated x rows.
    assert_eq!(actual.len(), 4);
}

// ── Shape and resources ──────────────────────────────────────────────────────

/// Output columns keep the names they had at the leaves, in relation order.
#[test]
fn output_columns_keep_their_leaf_names() {
    let db = TempDb::new();
    let tables = chain(&db);
    let condition = chain_condition();

    let names = column_names(
        tables.iter().map(|t| t.table_ref()).collect(),
        Some(&condition),
        &db,
    );

    for expected in [
        "a.id", "a.v", "b.link", "b.id", "c.link", "c.id", "d.link", "d.label",
    ] {
        assert!(
            names.contains(&expected.to_string()),
            "{expected} missing from {names:?}"
        );
    }
    assert_eq!(names.len(), 8);
}

/// A budget far below the intermediates forces them to spill, and must not
/// change the answer.
#[test]
fn spilling_intermediates_does_not_change_the_result() {
    let db = TempDb::new();

    let mut a = db.create_table(
        "a",
        &[("id", DataType::Int), ("pad", DataType::Varchar(32))],
    );
    let mut b = db.create_table("b", &[("link", DataType::Int), ("id", DataType::Int)]);
    let mut c = db.create_table(
        "c",
        &[("link", DataType::Int), ("pad", DataType::Varchar(32))],
    );

    for i in 0..150 {
        a.insert(vec![int(i % 30), text(&format!("a{i}-padding-here"))]);
        b.insert(vec![int(i % 30), int(i % 25)]);
        c.insert(vec![int(i % 25), text(&format!("c{i}-padding-here"))]);
    }
    a.flush();
    b.flush();
    c.flush();

    let condition = all_of(vec![
        eq(col("a.id"), col("b.link")),
        eq(col("b.id"), col("c.link")),
    ]);
    let relations = vec![a.table_ref(), b.table_ref(), c.table_ref()];

    let roomy = run(relations.clone(), Some(&condition), 8 * 1024 * 1024, &db);
    let cramped = run(relations, Some(&condition), 8 * 1024, &db);

    assert!(!roomy.is_empty());
    assert_rows_eq(&cramped, &roomy, "spilling intermediates");
}

/// Every spill file is removed once the join finishes.
#[test]
fn intermediates_leave_no_files_behind() {
    let db = TempDb::new();
    let spill_root = db.path().join("spill");
    let tables = chain(&db);
    let condition = chain_condition();

    let graph = JoinGraph::build(
        tables.iter().map(|t| t.table_ref()).collect(),
        Some(&condition),
        &TableStatsCache::new(),
    )
    .expect("graph");
    let config = JoinConfig::with_work_memory(8 * 1024).spill_root(&spill_root);
    let plan = optimize(&graph, &config).expect("order");

    {
        let mut stream = execute_ordered(&graph, &plan, &config).expect("execute");
        while let Some(row) = stream.next() {
            row.expect("runs");
        }
    }

    let leftovers: Vec<_> = std::fs::read_dir(&spill_root)
        .map(|entries| entries.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(leftovers.is_empty(), "spill files remain: {leftovers:?}");
}

/// Two relations go through the same path as many.
#[test]
fn a_two_relation_join_still_works() {
    let db = TempDb::new();
    let tables = chain(&db);
    let condition = eq(col("a.id"), col("b.link"));

    let expected = reference(&[&tables[0], &tables[1]], Some(&condition));
    let actual = run(
        vec![tables[0].table_ref(), tables[1].table_ref()],
        Some(&condition),
        4 * 1024 * 1024,
        &db,
    );
    assert_rows_eq(&actual, &expected, "two relations");
}

/// A single relation is just a scan.
#[test]
fn a_single_relation_is_scanned() {
    let db = TempDb::new();
    let tables = chain(&db);

    let actual = run(vec![tables[0].table_ref()], None, 4 * 1024 * 1024, &db);
    assert_eq!(actual.len(), 4);
    assert_eq!(actual[0].len(), 2);
}

/// A single relation with a filter on it: there is no join node to evaluate
/// the condition at, so it has to be applied after the plan.
#[test]
fn a_single_relation_with_a_filter_is_filtered() {
    let db = TempDb::new();
    let tables = chain(&db);

    let actual = run(
        vec![tables[0].table_ref()],
        Some(&lt(col("a.id"), col("a.v"))),
        4 * 1024 * 1024,
        &db,
    );

    // Every non-NULL row has id < v; the NULL-id row cannot compare.
    assert_eq!(actual.len(), 3, "got {actual:#?}");
    for row in &actual {
        assert!(row[0].is_some());
    }
}
