//! The nested-loop join streams its outer side: it must return the rows a
//! brute-force reference join does for every join type, with NULL keys,
//! duplicates, empty inputs and non-equality conditions, read the outer side
//! only as far as its consumer asks, and agree with the hash join on the same
//! data.

mod common;

use std::cell::Cell;
use std::rc::Rc;

use rook_ast::logical::LogicalPlan;
use storage_manager::backend::executor::physical::operators::{
    JoinType, NestedLoopJoinOperator, PhysicalOperator,
};
use storage_manager::backend::executor::physical::planner::PhysicalPlanner;
use storage_manager::backend::executor::physical::{
    ColumnInfo, ComparisonOp, Expr, Predicate, Tuple,
};
use storage_manager::backend::session::Session;
use storage_manager::types::{DataType, DataValue};

type Row = Vec<Option<DataValue>>;
/// An input row: `(id, k)`.
type Side = (i32, Option<i32>);

/// Hands out fixed rows one at a time and counts how many it handed out.
struct CountingSource {
    rows: Vec<Tuple>,
    schema: Vec<ColumnInfo>,
    pos: usize,
    pulled: Rc<Cell<usize>>,
}

impl CountingSource {
    fn new(table: &str, rows: &[Side]) -> (Self, Rc<Cell<usize>>) {
        let pulled = Rc::new(Cell::new(0));
        let schema = ["id", "k"]
            .iter()
            .map(|name| ColumnInfo {
                name: name.to_string(),
                data_type: DataType::Int,
                table: Some(table.to_string()),
            })
            .collect();
        let rows = rows
            .iter()
            .map(|&(id, k)| Tuple::new(vec![Some(DataValue::Int(id)), k.map(DataValue::Int)]))
            .collect();
        let source = Self {
            rows,
            schema,
            pos: 0,
            pulled: Rc::clone(&pulled),
        };
        (source, pulled)
    }
}

impl PhysicalOperator for CountingSource {
    fn next(&mut self) -> storage_manager::backend::error::RookResult<Option<Tuple>> {
        let row = self.rows.get(self.pos).cloned();
        if row.is_some() {
            self.pos += 1;
            self.pulled.set(self.pulled.get() + 1);
        }
        Ok(row)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> storage_manager::backend::error::RookResult<()> {
        self.pos = 0;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "CountingSource"
    }
}

fn col(table: &str, column: &str) -> Expr {
    Expr::Column {
        table: Some(table.to_string()),
        column: column.to_string(),
    }
}

/// `l.k <op> r.k`.
fn k_compare(op: ComparisonOp) -> Predicate {
    Predicate::Compare(col("l", "k"), op, col("r", "k"))
}

/// A join condition and the same condition evaluated in plain Rust
/// (`None` = UNKNOWN, so the pair does not match).
struct Condition {
    name: &'static str,
    predicate: Option<Predicate>,
    reference: fn(&Side, &Side) -> bool,
}

fn conditions() -> Vec<Condition> {
    fn both(l: Option<i32>, r: Option<i32>, f: fn(i32, i32) -> bool) -> bool {
        matches!((l, r), (Some(a), Some(b)) if f(a, b))
    }
    vec![
        Condition {
            name: "l.k < r.k",
            predicate: Some(k_compare(ComparisonOp::LessThan)),
            reference: |l, r| both(l.1, r.1, |a, b| a < b),
        },
        Condition {
            name: "l.k = r.k",
            predicate: Some(k_compare(ComparisonOp::Equals)),
            reference: |l, r| both(l.1, r.1, |a, b| a == b),
        },
        Condition {
            name: "l.k = r.k AND l.id <> r.id",
            predicate: Some(Predicate::and(
                k_compare(ComparisonOp::Equals),
                Predicate::Compare(col("l", "id"), ComparisonOp::NotEquals, col("r", "id")),
            )),
            reference: |l, r| both(l.1, r.1, |a, b| a == b) && l.0 != r.0,
        },
        Condition {
            name: "no condition",
            predicate: None,
            reference: |_, _| true,
        },
    ]
}

/// Brute-force join: every pair, then the unmatched rows of preserved sides.
fn reference_join(
    left: &[Side],
    right: &[Side],
    join_type: JoinType,
    matches: fn(&Side, &Side) -> bool,
) -> Vec<Row> {
    let row = |r: Option<&Side>| match r {
        Some(&(id, k)) => vec![Some(DataValue::Int(id)), k.map(DataValue::Int)],
        None => vec![None, None],
    };
    let matches = |l: &Side, r: &Side| join_type == JoinType::Cross || matches(l, r);
    let mut out = Vec::new();
    for l in left {
        for r in right {
            if matches(l, r) {
                out.push([row(Some(l)), row(Some(r))].concat());
            }
        }
    }
    if matches!(join_type, JoinType::Left | JoinType::Full) {
        for l in left {
            if !right.iter().any(|r| matches(l, r)) {
                out.push([row(Some(l)), row(None)].concat());
            }
        }
    }
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for r in right {
            if !left.iter().any(|l| matches(l, r)) {
                out.push([row(None), row(Some(r))].concat());
            }
        }
    }
    out
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_cached_key(|row| format!("{:?}", row));
    rows
}

fn nested_loop(
    left: &[Side],
    right: &[Side],
    predicate: Option<Predicate>,
    join_type: JoinType,
) -> (NestedLoopJoinOperator, Rc<Cell<usize>>, Rc<Cell<usize>>) {
    let (l, l_pulled) = CountingSource::new("l", left);
    let (r, r_pulled) = CountingSource::new("r", right);
    let op = NestedLoopJoinOperator::new(Box::new(l), Box::new(r), predicate, join_type);
    (op, l_pulled, r_pulled)
}

fn drain(op: &mut dyn PhysicalOperator) -> Vec<Row> {
    let mut out = Vec::new();
    while let Some(t) = op.next().unwrap() {
        out.push(t.values);
    }
    out
}

/// Duplicate keys, NULL keys, keys only one side has.
const LEFT: &[Side] = &[
    (1, Some(1)),
    (2, Some(2)),
    (3, Some(2)),
    (4, None),
    (5, Some(7)),
    (6, Some(3)),
    (7, None),
];
const RIGHT: &[Side] = &[
    (2, Some(2)),
    (3, Some(2)),
    (10, Some(3)),
    (11, None),
    (12, Some(9)),
    (13, Some(0)),
];

const JOIN_TYPES: &[JoinType] = &[
    JoinType::Inner,
    JoinType::Left,
    JoinType::Right,
    JoinType::Full,
    JoinType::Cross,
];

#[test]
fn nested_loop_matches_reference_for_every_join_type() {
    let inputs: &[(&str, &[Side], &[Side])] = &[
        ("both sides", LEFT, RIGHT),
        ("empty left", &[], RIGHT),
        ("empty right", LEFT, &[]),
        ("both empty", &[], &[]),
        ("all-NULL keys", &[(1, None), (2, None)], &[(3, None)]),
    ];
    for &(input, left, right) in inputs {
        for condition in conditions() {
            for &join_type in JOIN_TYPES {
                let (mut op, _, _) =
                    nested_loop(left, right, condition.predicate.clone(), join_type);
                let got = sorted(drain(&mut op));
                let expected = sorted(reference_join(left, right, join_type, condition.reference));
                assert_eq!(
                    got, expected,
                    "{:?} join on {} ({})",
                    join_type, condition.name, input
                );
                // Exhausted stays exhausted.
                assert!(op.next().unwrap().is_none());
            }
        }
    }
}

#[test]
fn nested_loop_reset_runs_the_join_again() {
    for &join_type in JOIN_TYPES {
        let (mut op, _, _) = nested_loop(
            LEFT,
            RIGHT,
            Some(k_compare(ComparisonOp::LessThan)),
            join_type,
        );
        let first = drain(&mut op);
        op.reset().unwrap();
        assert_eq!(drain(&mut op), first, "{:?}", join_type);
    }
}

#[test]
fn nested_loop_next_batch_returns_every_row() {
    // 40 x 40 cross join: more rows than one batch.
    let side: Vec<Side> = (0..40).map(|i| (i, Some(i))).collect();
    let (mut op, _, _) = nested_loop(&side, &side, None, JoinType::Cross);
    let mut batch = Vec::new();
    let mut total = 0;
    while op.next_batch(&mut batch).unwrap() > 0 {
        total += batch.len();
    }
    assert_eq!(total, 1600);
    assert!(batch.is_empty());
}

#[test]
fn nested_loop_does_not_read_the_outer_side_ahead() {
    let outer: Vec<Side> = (0..1000).map(|i| (i, Some(i))).collect();
    let inner: &[Side] = &[(0, Some(500)), (1, Some(501)), (2, Some(502))];

    // A cross join's first row needs one outer row.
    let (mut op, outer_pulled, inner_pulled) = nested_loop(&outer, inner, None, JoinType::Cross);
    assert!(op.next().unwrap().is_some());
    assert_eq!(outer_pulled.get(), 1);
    assert_eq!(inner_pulled.get(), 3, "the inner side is read once, whole");
    // The next two rows pair the same outer row with the other inner rows.
    op.next().unwrap();
    op.next().unwrap();
    assert_eq!(outer_pulled.get(), 1);
    op.next().unwrap();
    assert_eq!(outer_pulled.get(), 2);

    // `l.k < r.k`: outer rows 0.. all match, so ten rows need four outer rows.
    for join_type in [JoinType::Inner, JoinType::Left, JoinType::Full] {
        let (mut op, outer_pulled, _) = nested_loop(
            &outer,
            inner,
            Some(k_compare(ComparisonOp::LessThan)),
            join_type,
        );
        for _ in 0..10 {
            assert!(op.next().unwrap().is_some());
        }
        assert_eq!(outer_pulled.get(), 4, "{:?}", join_type);
    }

    // A LEFT join emits an unmatched outer row as soon as its pairings end,
    // not after the whole outer side.
    let (mut op, outer_pulled, _) = nested_loop(
        &outer,
        inner,
        Some(k_compare(ComparisonOp::GreaterThan)),
        JoinType::Left,
    );
    let first = op.next().unwrap().unwrap();
    assert_eq!(first.values[2], None);
    assert_eq!(outer_pulled.get(), 1);

    // Nothing to pair with and nothing to preserve: the outer side is not read.
    for join_type in [JoinType::Inner, JoinType::Right, JoinType::Cross] {
        let (mut op, outer_pulled, _) = nested_loop(&outer, &[], None, join_type);
        assert!(op.next().unwrap().is_none());
        assert_eq!(outer_pulled.get(), 0, "{:?}", join_type);
    }
}

// ── SQL level ─────────────────────────────────────────────────────────────────

fn rows(s: &mut Session, sql: &str) -> Vec<Row> {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e)))
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(_, rows)| rows.into_iter().map(|t| t.values).collect())
        .unwrap_or_default()
}

fn setup(tag: &str, db: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("nlj", tag);
    let mut s = Session::new(common::load_catalog());
    rows(&mut s, &format!("CREATE DATABASE {}", db));
    rows(&mut s, &format!("USE {}", db));
    for (table, distinct) in [("a", 13), ("b", 9)] {
        rows(
            &mut s,
            &format!("CREATE TABLE {} (id INT, k INT, name VARCHAR(20))", table),
        );
        let values: Vec<String> = (0..120)
            .map(|i| {
                let k = if i % 7 == 0 {
                    "NULL".to_string()
                } else {
                    (i % distinct).to_string()
                };
                format!("({}, {}, '{}_{}')", i, k, table, i)
            })
            .collect();
        rows(
            &mut s,
            &format!("INSERT INTO {} VALUES {}", table, values.join(", ")),
        );
    }
    rows(
        &mut s,
        "CREATE TABLE empty_t (id INT, k INT, name VARCHAR(20))",
    );
    (ws, s)
}

/// Name of the operator the planner picks for the query's join.
fn join_operator(db: &str, sql: &str) -> &'static str {
    let catalog = common::load_catalog();
    let parsed = rook_parser::parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&parsed, &catalog, db).unwrap();
    // Cut the plan off at the join, keeping any CTE (derived table) above it.
    fn cut(plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Cte(c) => {
                let mut c = c.clone();
                c.outer = Box::new(cut(&c.outer));
                LogicalPlan::Cte(c)
            }
            LogicalPlan::Project(p) => cut(&p.child),
            LogicalPlan::Limit(l) => cut(&l.child),
            LogicalPlan::Join(_) => plan.clone(),
            other => panic!("no join in {:?}", other),
        }
    }
    let planner = PhysicalPlanner::new(catalog, db.to_string());
    planner.plan(&cut(&logical)).unwrap().name()
}

#[test]
fn nested_loop_agrees_with_hash_join() {
    let (_ws, mut s) = setup("agree", "nlj_agree");
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        for (left, right) in [("a", "b"), ("a", "empty_t"), ("empty_t", "b")] {
            // `<=` and `>=` together mean `=`, but give no key to hash on.
            let nested = format!(
                "SELECT * FROM {left} {join} {right} ON {left}.k <= {right}.k AND {left}.k >= {right}.k"
            );
            let hashed = format!("SELECT * FROM {left} {join} {right} ON {left}.k = {right}.k");
            assert_eq!(join_operator("nlj_agree", &nested), "NestedLoopJoin");
            assert_eq!(join_operator("nlj_agree", &hashed), "HashJoin");
            assert_eq!(
                sorted(rows(&mut s, &nested)),
                sorted(rows(&mut s, &hashed)),
                "{}",
                nested
            );
        }
    }
}

#[test]
fn nested_loop_with_limit_returns_the_first_rows() {
    let (_ws, mut s) = setup("limit", "nlj_limit");
    for sql in [
        "SELECT a.id, b.id FROM a JOIN b ON a.k < b.k LIMIT 10",
        "SELECT a.id, b.id FROM a LEFT JOIN b ON a.k < b.k LIMIT 10",
        "SELECT a.id, b.id FROM a CROSS JOIN b LIMIT 10",
    ] {
        let limited = rows(&mut s, sql);
        let all = rows(&mut s, sql.trim_end_matches(" LIMIT 10"));
        assert_eq!(limited.len(), 10, "{}", sql);
        assert_eq!(limited[..], all[..10], "{}", sql);
    }
}
