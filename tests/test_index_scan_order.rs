//! An index scan fetches each batch of matches page by page (P7) but must
//! still return rows in index order, which ORDER BY elimination relies on,
//! and return exactly the rows a full scan finds.

mod common;

use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    common::init_catalog();
    Session::new(common::load_catalog())
}

fn run(s: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    s.execute(plan).map_err(|e| e.to_string())
}

fn ok(s: &mut Session, sql: &str) {
    run(s, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn ids(s: &mut Session, sql: &str) -> Vec<i32> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter()
        .map(|t| match t.values[0] {
            Some(DataValue::Int(v)) => v,
            ref other => panic!("not an INT: {:?}", other),
        })
        .collect()
}

#[test]
fn index_scans_return_index_order_across_pages() {
    let _ws = common::TestWorkspace::new("index_scan_order", "pages");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(
        &mut s,
        "CREATE TABLE p (id INT PRIMARY KEY, k INT, pad VARCHAR(100))",
    );
    ok(&mut s, "CREATE INDEX p_k ON p (k)");
    // Ids arrive shuffled, so index order and heap order disagree, over
    // many pages (about 60 rows per page) and more than one fetch batch.
    let n = 2500;
    let order: Vec<i32> = (0..n).map(|i| (i * 1237) % n).collect();
    for chunk in order.chunks(400) {
        let values: Vec<String> = chunk
            .iter()
            .map(|id| format!("({}, {}, '{}')", id, id % 7, "x".repeat(100)))
            .collect();
        ok(
            &mut s,
            &format!("INSERT INTO p VALUES {}", values.join(", ")),
        );
    }
    ok(&mut s, "DELETE FROM p WHERE id % 10 = 3");

    let want: Vec<i32> = (100..=2400).filter(|i| i % 10 != 3).collect();
    assert_eq!(
        ids(
            &mut s,
            "SELECT id FROM p WHERE id BETWEEN 100 AND 2400 ORDER BY id"
        ),
        want
    );
    // Non-unique key: the same rows as a scan that cannot use the index.
    let mut by_index = ids(&mut s, "SELECT id FROM p WHERE k = 5");
    let mut by_scan = ids(&mut s, "SELECT id FROM p WHERE k + 0 = 5");
    by_index.sort();
    by_scan.sort();
    assert_eq!(
        by_index.len(),
        (0..n).filter(|i| i % 7 == 5 && i % 10 != 3).count()
    );
    assert_eq!(by_index, by_scan);
}
