//! ORDER BY ... LIMIT keeps only the needed rows while sorting. Its result
//! must equal the same slice of the full sort, ties included.

mod common;

use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(_, rows)| rows.into_iter().map(|t| t.values).collect())
        .unwrap_or_default()
}

#[test]
fn limited_sort_matches_the_full_sort() {
    let _ws = common::TestWorkspace::new("topn", "slices");
    let mut s = Session::new(common::load_catalog());
    rows(&mut s, "CREATE DATABASE tn");
    rows(&mut s, "USE tn");
    rows(&mut s, "CREATE TABLE t (id INT, k INT)");
    // 5,000 rows, 37 distinct keys plus NULLs: many ties per key.
    for start in (0..5000).step_by(500) {
        let values: Vec<String> = (start..start + 500)
            .map(|i| {
                if i % 101 == 0 {
                    format!("({}, NULL)", i)
                } else {
                    format!("({}, {})", i, (i * 7) % 37)
                }
            })
            .collect();
        rows(
            &mut s,
            &format!("INSERT INTO t VALUES {}", values.join(", ")),
        );
    }

    for order in ["k", "k DESC", "k, id DESC"] {
        let full = rows(&mut s, &format!("SELECT id, k FROM t ORDER BY {}", order));
        assert_eq!(full.len(), 5000);
        for limit in [0usize, 1, 5, 100, 1500, 5000, 6000] {
            let top = rows(
                &mut s,
                &format!("SELECT id, k FROM t ORDER BY {} LIMIT {}", order, limit),
            );
            assert_eq!(
                top,
                full[..limit.min(5000)],
                "ORDER BY {} LIMIT {}",
                order,
                limit
            );
        }
        let paged = rows(
            &mut s,
            &format!(
                "SELECT id, k FROM t ORDER BY {} LIMIT 50 OFFSET 1200",
                order
            ),
        );
        assert_eq!(
            paged,
            full[1200..1250],
            "ORDER BY {} LIMIT 50 OFFSET 1200",
            order
        );
        let tail = rows(
            &mut s,
            &format!("SELECT id, k FROM t ORDER BY {} OFFSET 4990", order),
        );
        assert_eq!(tail, full[4990..], "ORDER BY {} OFFSET 4990", order);
    }
}
