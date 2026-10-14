//! `EXPLAIN`: the physical plan of a statement as data (S10).
//!
//! The plan comes back from `Session::execute` as a tree of operators with
//! the decisions made for them, so nothing has to be read out of the log.

mod common;

use storage_manager::backend::session::Session;
use storage_manager::executor::physical::PlanDescription;
use storage_manager::types::DataValue;

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
    common::init_catalog();
    Session::new(common::load_catalog())
}

fn exec(session: &mut Session, sql: &str) {
    let plan = rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    session
        .execute(plan)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn explain(session: &mut Session, sql: &str) -> PlanDescription {
    let plan = rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let result = session
        .execute(plan)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
    let description = result.plan.expect("EXPLAIN returns a plan");
    // The rows are the same plan as text.
    let (schema, rows) = result.rows.expect("EXPLAIN returns rows");
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "plan");
    let text: Vec<String> = rows
        .iter()
        .map(|t| match &t.values[0] {
            Some(DataValue::Varchar(s)) => s.clone(),
            other => panic!("plan line is not text: {:?}", other),
        })
        .collect();
    assert_eq!(text, description.lines());
    description
}

fn count(session: &mut Session, table: &str) -> usize {
    let plan = rook_parser::parse_sql(&format!("SELECT * FROM {}", table)).unwrap();
    session.execute(plan).unwrap().rows.unwrap().1.len()
}

/// `emp` (primary key, secondary index on dept), `dept` and `loc` (no index).
fn setup(prefix: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("explain", prefix);
    let mut s = session();
    exec(&mut s, "CREATE DATABASE app");
    exec(&mut s, "USE app");
    exec(
        &mut s,
        "CREATE TABLE emp (id INT PRIMARY KEY, name VARCHAR(20), dept INT, sal INT)",
    );
    exec(&mut s, "CREATE TABLE dept (dno INT, dname VARCHAR(20))");
    let rows: Vec<String> = (1..=200)
        .map(|i| format!("({}, 'e{}', {}, {})", i, i, i % 10, 1000 + i))
        .collect();
    exec(
        &mut s,
        &format!("INSERT INTO emp VALUES {}", rows.join(", ")),
    );
    let rows: Vec<String> = (0..10).map(|i| format!("({}, 'd{}')", i, i)).collect();
    exec(
        &mut s,
        &format!("INSERT INTO dept VALUES {}", rows.join(", ")),
    );
    exec(&mut s, "CREATE TABLE loc (dno INT, city VARCHAR(20))");
    let rows: Vec<String> = (0..10).map(|i| format!("({}, 'c{}')", i, i)).collect();
    exec(
        &mut s,
        &format!("INSERT INTO loc VALUES {}", rows.join(", ")),
    );
    exec(&mut s, "CREATE INDEX emp_dept_idx ON emp (dept)");
    (ws, s)
}

#[test]
fn scan_reports_the_index_and_lookup_mode() {
    let (_ws, mut s) = setup("scan");

    let plan = explain(&mut s, "EXPLAIN SELECT * FROM emp WHERE id = 42");
    let scan = plan.root.find("IndexScan").expect("primary key lookup");
    assert_eq!(scan.detail("table"), Some("emp"));
    assert_eq!(scan.detail("mode"), Some("Point"));
    assert_eq!(scan.detail("key"), Some("42"));
    assert_eq!(scan.detail("key_columns"), Some("id"));

    let plan = explain(&mut s, "EXPLAIN SELECT name FROM emp WHERE dept = 3");
    let scan = plan.root.find("IndexScan").expect("secondary index lookup");
    assert_eq!(scan.detail("index"), Some("emp_dept_idx"));
    assert_eq!(scan.detail("mode"), Some("Point"));

    let plan = explain(&mut s, "EXPLAIN SELECT * FROM emp WHERE id BETWEEN 5 AND 9");
    let scan = plan.root.find("IndexScan").expect("range scan");
    assert_eq!(scan.detail("mode"), Some("Range"));
    assert_eq!(scan.detail("range"), Some("[5, 9]"));

    // No index on dept.dname: a sequential scan under a filter, decoding
    // only the column the query reads.
    let plan = explain(&mut s, "EXPLAIN SELECT dname FROM dept WHERE dname = 'd1'");
    assert!(!plan.root.contains("IndexScan"));
    let filter = plan.root.find("Filter").expect("filter over the scan");
    let scan = filter.find("SeqScan").expect("sequential scan");
    assert_eq!(scan.detail("table"), Some("dept"));
    assert_eq!(scan.detail("columns_decoded"), Some("1 of 2"));
}

#[test]
fn join_reports_the_algorithm() {
    let (_ws, mut s) = setup("join");

    // Neither side has an index on dno: hash join.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT d.dname, l.city FROM dept d JOIN loc l ON d.dno = l.dno",
    );
    let join = plan.root.find("HashJoin").expect("hash join");
    assert_eq!(join.detail("equi_keys"), Some("1"));
    assert_eq!(join.detail("residual"), Some("no"));
    assert_eq!(join.children.len(), 2);
    let scan = &join.children[0];
    assert_eq!(scan.operator, "SeqScan");
    assert_eq!(scan.detail("table"), Some("dept"));
    assert_eq!(scan.detail("alias"), Some("d"));

    // Every dept finds 20 emp rows: reading all 200 through the index costs
    // more than one scan of emp, so the join hashes.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT d.dname, e.name FROM dept d JOIN emp e ON d.dno = e.dept",
    );
    assert!(plan.root.contains("HashJoin"), "{:?}", plan.root);
    assert!(
        plan.trace
            .notes
            .iter()
            .any(|n| n.starts_with("HashJoin chosen over the index")),
        "{:?}",
        plan.trace.notes
    );

    // One outer row and an index on the inner join key: index nested loop.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT d.dname, e.name FROM dept d JOIN emp e ON d.dno = e.dept WHERE d.dname = 'd3'",
    );
    let join = plan
        .root
        .find("IndexNestedLoopJoin")
        .expect("index nested loop join");
    assert_eq!(join.detail("inner_table"), Some("emp"));
    assert_eq!(join.detail("index"), Some("emp_dept_idx"));
    assert_eq!(join.detail("outer_key"), Some("dno"));
    assert!(
        plan.trace
            .notes
            .iter()
            .any(|n| n.starts_with("IndexNestedLoopJoin chosen")),
        "{:?}",
        plan.trace.notes
    );

    // No equality between the sides: nested loop.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT e.name FROM emp e JOIN dept d ON e.dept < d.dno",
    );
    let join = plan.root.find("NestedLoopJoin").expect("nested loop join");
    assert_eq!(join.detail("type"), Some("Inner"));

    // An outer equi-join hashes the right side.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.dno",
    );
    let join = plan.root.find("HashJoin").expect("hash join");
    assert_eq!(join.detail("type"), Some("Left"));
}

#[test]
fn sort_reports_top_n_and_elimination() {
    let (_ws, mut s) = setup("sort");

    let plan = explain(
        &mut s,
        "EXPLAIN SELECT * FROM dept ORDER BY dname DESC LIMIT 3",
    );
    let sort = plan.root.find("Sort").expect("sort");
    assert_eq!(sort.detail("keys"), Some("dname DESC"));
    assert_eq!(sort.detail("top_n"), Some("3"));
    assert_eq!(plan.root.find("Limit").unwrap().detail("limit"), Some("3"));

    let plan = explain(&mut s, "EXPLAIN SELECT * FROM dept ORDER BY dname");
    let sort = plan.root.find("Sort").expect("sort");
    assert_eq!(sort.detail("top_n"), None);
    assert!(sort.detail("memory_budget").is_some());
    // Not run: nothing to say about how it was sorted.
    assert_eq!(sort.detail("sorted"), None);

    // An index range scan already yields rows in key order.
    let plan = explain(
        &mut s,
        "EXPLAIN SELECT * FROM emp WHERE id BETWEEN 5 AND 50 ORDER BY id",
    );
    assert!(!plan.root.contains("Sort"));
    assert!(
        plan.trace
            .notes
            .iter()
            .any(|n| n.starts_with("Sort eliminated") && n.contains("id ASC")),
        "{:?}",
        plan.trace.notes
    );
}

#[test]
fn explain_analyze_reports_a_sort_that_went_to_disk() {
    let (ws, mut s) = setup("analyze");

    let plan = explain(&mut s, "EXPLAIN ANALYZE SELECT * FROM emp ORDER BY sal");
    assert_eq!(
        plan.root.find("Sort").unwrap().detail("sorted"),
        Some("in memory")
    );
    assert!(
        plan.trace
            .notes
            .contains(&"Executed: 200 row(s) returned".to_string()),
        "{:?}",
        plan.trace.notes
    );

    ws.engine().set_sort_memory_budget(1024);
    let plan = explain(&mut s, "EXPLAIN ANALYZE SELECT * FROM emp ORDER BY sal");
    assert_eq!(
        plan.root.find("Sort").unwrap().detail("sorted"),
        Some("on disk")
    );

    let plan = explain(
        &mut s,
        "EXPLAIN ANALYZE SELECT d.dname FROM dept d JOIN loc l ON d.dno = l.dno",
    );
    assert_eq!(
        plan.root.find("HashJoin").unwrap().detail("build"),
        Some("in memory")
    );
}

#[test]
fn explain_does_not_run_a_statement_that_writes() {
    let (_ws, mut s) = setup("writes");

    let plan = explain(
        &mut s,
        "EXPLAIN INSERT INTO dept VALUES (10, 'x'), (11, 'y')",
    );
    assert_eq!(plan.root.operator, "Insert");
    assert_eq!(plan.root.detail("table"), Some("dept"));
    assert_eq!(plan.root.find("Values").unwrap().detail("rows"), Some("2"));
    assert_eq!(count(&mut s, "dept"), 10);

    let plan = explain(&mut s, "EXPLAIN UPDATE emp SET sal = sal + 1 WHERE id = 7");
    assert_eq!(plan.root.operator, "Update");
    assert_eq!(plan.root.detail("table"), Some("emp"));
    assert_eq!(plan.root.detail("assignments"), Some("1"));
    assert_eq!(
        plan.root.find("IndexScan").unwrap().detail("mode"),
        Some("Point")
    );

    let plan = explain(&mut s, "EXPLAIN DELETE FROM dept WHERE dno > 4");
    assert_eq!(plan.root.operator, "Delete");
    assert!(plan.root.contains("SeqScan"));
    assert_eq!(count(&mut s, "dept"), 10);

    // ANALYZE would run it.
    for sql in [
        "EXPLAIN ANALYZE INSERT INTO dept VALUES (10, 'x')",
        "EXPLAIN ANALYZE DELETE FROM dept",
        "EXPLAIN ANALYZE UPDATE emp SET sal = 0",
    ] {
        let err = s
            .execute(rook_parser::parse_sql(sql).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("supports only queries"), "{}: {}", sql, err);
    }
    assert_eq!(count(&mut s, "dept"), 10);

    let err = s
        .execute(rook_parser::parse_sql("EXPLAIN CREATE TABLE z (a INT)").unwrap())
        .unwrap_err()
        .to_string();
    assert!(err.contains("EXPLAIN supports"), "{}", err);
}

#[test]
fn cte_and_subquery_decisions_are_part_of_the_plan() {
    let (_ws, mut s) = setup("cte");

    let plan = explain(
        &mut s,
        "EXPLAIN WITH big AS (SELECT * FROM emp WHERE sal > 1150) SELECT name FROM big",
    );
    let scan = plan.root.find("CteScan").expect("scan of the CTE");
    assert_eq!(scan.detail("rows"), Some("50"));
    assert_eq!(plan.trace.ctes.len(), 1);
    assert_eq!(plan.trace.ctes[0].0, "big");
    assert!(plan.trace.ctes[0].1.contains("SeqScan") || plan.trace.ctes[0].1.contains("IndexScan"));
    assert!(plan.lines().iter().any(|l| l == "CTE big"));

    let plan = explain(
        &mut s,
        "EXPLAIN SELECT name FROM emp WHERE dept IN (SELECT dno FROM dept WHERE dno < 2)",
    );
    assert!(
        plan.trace
            .notes
            .iter()
            .any(|n| n == "IN subquery evaluated once while planning: 2 value(s)"),
        "{:?}",
        plan.trace.notes
    );
}
