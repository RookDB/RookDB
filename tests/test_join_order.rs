//! Join ordering changes no result. Every query here runs on two identical
//! databases, one joining in the order the query is written and one letting
//! the optimizer reorder inner joins, and must give the same columns and,
//! up to the order of rows the query leaves unordered, the same rows.

use std::path::{Path, PathBuf};

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rookdb_joinorder_{}_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempRoot(dir)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn session(root: &Path, keep_join_order: bool) -> Session {
    let engine = Engine::open(root);
    engine.set_keep_join_order(keep_join_order);
    init_catalog_in(&engine).expect("init catalog");
    Session::new(load_catalog_in(&engine))
}

/// Column names, then rows as text (sorted unless `ordered`), or the error.
fn answer(s: &mut Session, sql: &str, ordered: bool) -> Result<Vec<String>, String> {
    let plan = rook_parser::parse_sql(sql)?;
    let result = s.execute(plan).map_err(|e| e.to_string())?;
    let mut lines = Vec::new();
    if let Some((schema, rows)) = result.rows {
        lines.push(
            schema
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut rows: Vec<String> = rows.iter().map(|t| format!("{:?}", t.values)).collect();
        if !ordered {
            rows.sort();
        }
        lines.extend(rows);
    }
    lines.extend(result.messages);
    Ok(lines)
}

fn values(rows: impl Iterator<Item = String>) -> String {
    rows.collect::<Vec<_>>().join(", ")
}

fn null_or(i: usize, every: usize, v: usize) -> String {
    if i % every == every - 1 {
        "NULL".to_string()
    } else {
        v.to_string()
    }
}

fn setup() -> Vec<String> {
    vec![
        "CREATE DATABASE jo".to_string(),
        "USE jo".to_string(),
        "CREATE TABLE big (id INT, mid_id INT, v INT, name VARCHAR(10))".to_string(),
        "CREATE TABLE mid (id INT, small_id INT, w INT, name VARCHAR(10))".to_string(),
        "CREATE TABLE small (id INT, x INT, name VARCHAR(10))".to_string(),
        "CREATE TABLE other (k INT, id INT, name VARCHAR(10))".to_string(),
        format!(
            "INSERT INTO big VALUES {}",
            values((0..400).map(|i| format!(
                "({}, {}, {}, 'b{}')",
                i,
                null_or(i, 37, i % 50),
                i % 7,
                i % 13
            )))
        ),
        format!(
            "INSERT INTO mid VALUES {}",
            values((0..50).map(|i| format!(
                "({}, {}, {}, 'm{}')",
                i,
                null_or(i, 11, i % 8),
                i % 10,
                i % 6
            )))
        ),
        format!(
            "INSERT INTO small VALUES {}",
            values((0..8).map(|i| format!("({}, {}, 's{}')", i, i % 3, i % 4)))
        ),
        format!(
            "INSERT INTO other VALUES {}",
            values((0..20).map(|i| format!("({}, {}, 'm{}')", i, i * 3, i % 6)))
        ),
    ]
}

/// (query, whether its ORDER BY fixes the order of every row)
const QUERIES: &[(&str, bool)] = &[
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id WHERE small.x = 1",
        false,
    ),
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id WHERE small.x = 1 ORDER BY big.id",
        true,
    ),
    (
        "SELECT big.id, mid.name, small.name FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id",
        false,
    ),
    (
        "SELECT v, w, x FROM big JOIN mid ON mid_id = mid.id JOIN small ON small_id = small.id",
        false,
    ),
    (
        "SELECT mid.name FROM big JOIN mid ON big.mid_id = mid.id",
        false,
    ),
    (
        "SELECT small.id, small.name FROM small JOIN mid ON small.id = mid.small_id JOIN big ON big.mid_id = mid.id",
        false,
    ),
    (
        "SELECT big.id, other.k FROM big JOIN mid ON big.mid_id = mid.id \
         LEFT JOIN other ON other.id = mid.id JOIN small ON small.id = mid.small_id",
        false,
    ),
    (
        "SELECT * FROM small LEFT JOIN mid ON mid.small_id = small.id \
         JOIN big ON big.mid_id = mid.id",
        false,
    ),
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id RIGHT JOIN small ON small.id = mid.small_id",
        false,
    ),
    ("SELECT * FROM mid NATURAL JOIN other", false),
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id NATURAL JOIN small",
        false,
    ),
    (
        "SELECT * FROM mid JOIN small USING (id) JOIN big ON big.mid_id = mid.id",
        false,
    ),
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id JOIN other USING (name)",
        false,
    ),
    (
        "SELECT small.x, COUNT(*), SUM(big.v), MIN(mid.name) FROM big \
         JOIN mid ON big.mid_id = mid.id JOIN small ON mid.small_id = small.id \
         GROUP BY small.x ORDER BY small.x",
        true,
    ),
    (
        "SELECT mid.name, COUNT(*) FROM big, mid, small WHERE big.mid_id = mid.id \
         AND mid.small_id = small.id AND small.x < 2 GROUP BY mid.name ORDER BY mid.name",
        true,
    ),
    (
        "SELECT small.id, other.k FROM small CROSS JOIN other WHERE other.k < 3",
        false,
    ),
    (
        "SELECT big.id, mid.id FROM big JOIN mid ON big.mid_id = mid.id ORDER BY mid.id DESC, big.id",
        true,
    ),
    (
        "SELECT * FROM big JOIN mid ON big.mid_id = mid.id JOIN small ON mid.small_id = small.id \
         ORDER BY small.id, big.id LIMIT 7",
        true,
    ),
    (
        "SELECT DISTINCT small.name FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id",
        false,
    ),
    (
        "SELECT big.id, small.id FROM big JOIN small ON big.v < small.x \
         JOIN mid ON mid.id = big.mid_id WHERE big.id < 40",
        false,
    ),
    (
        "SELECT b.id, m.id, s.id FROM big b JOIN mid m ON b.mid_id = m.id \
         JOIN small s ON m.small_id = s.id WHERE s.x = 2",
        false,
    ),
    (
        "SELECT * FROM big b JOIN mid m ON b.mid_id = m.id JOIN small s ON m.small_id = s.id \
         WHERE s.x = 2 ORDER BY b.id",
        true,
    ),
    (
        "SELECT a.id, b.id FROM mid a JOIN mid b ON a.small_id = b.small_id \
         JOIN small ON small.id = a.small_id WHERE small.x = 0",
        false,
    ),
    (
        "SELECT big.id FROM big JOIN mid ON big.mid_id = mid.id \
         WHERE mid.small_id IN (SELECT id FROM small WHERE x = 1)",
        false,
    ),
    (
        "SELECT big.id, other.k FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id JOIN other ON other.id = small.id",
        false,
    ),
    (
        "SELECT big.*, small.name FROM big JOIN mid ON big.mid_id = mid.id \
         JOIN small ON mid.small_id = small.id",
        false,
    ),
    (
        "SELECT mid.id, COUNT(*) FROM big JOIN mid ON big.mid_id = mid.id \
         GROUP BY mid.id HAVING COUNT(*) > 7 ORDER BY mid.id",
        true,
    ),
    (
        "SELECT t.id, small.name FROM (SELECT id, small_id FROM mid) t \
         JOIN small ON t.small_id = small.id JOIN big ON big.mid_id = t.id",
        false,
    ),
    (
        "SELECT big.id FROM big JOIN mid ON big.mid_id = mid.id AND mid.w > 5 \
         JOIN small ON small.id = mid.small_id AND small.x = 1",
        false,
    ),
    (
        "SELECT big.id, mid.w FROM big JOIN mid ON big.mid_id = mid.id \
         WHERE big.v + mid.w > 12 OR mid.name = 'm1'",
        false,
    ),
    (
        "SELECT COUNT(*) FROM big JOIN mid ON big.mid_id = mid.id JOIN small ON mid.small_id = small.id",
        true,
    ),
    (
        "SELECT big.name, COUNT(*) FROM small JOIN mid ON mid.small_id = small.id \
         JOIN big ON big.mid_id = mid.id GROUP BY big.name ORDER BY 2 DESC, 1",
        true,
    ),
    (
        "WITH s AS (SELECT id FROM small WHERE x = 1) SELECT big.id FROM big \
         JOIN mid ON big.mid_id = mid.id JOIN s ON s.id = mid.small_id",
        false,
    ),
    (
        "SELECT big.id FROM big JOIN mid ON big.mid_id = mid.id \
         UNION SELECT small.id FROM small JOIN mid ON mid.small_id = small.id",
        false,
    ),
];

#[test]
fn join_ordering_changes_no_result() {
    let kept = TempRoot::new("kept");
    let ordered = TempRoot::new("ordered");
    let mut as_written = session(&kept.0, true);
    let mut reordered = session(&ordered.0, false);
    for sql in setup() {
        answer(&mut as_written, &sql, true).unwrap_or_else(|e| panic!("{}: {}", sql, e));
        answer(&mut reordered, &sql, true).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }

    // The optimizer does reorder these queries' joins.
    let plan = |s: &Session, sql: &str| {
        let query = rook_parser::parse_sql(sql).unwrap();
        storage_manager::planner::plan_query(&query, &s.catalog, "jo")
            .map(|p| format!("{:?}", p))
            .ok()
    };
    let reordered_queries: Vec<&str> = QUERIES
        .iter()
        .map(|(sql, _)| *sql)
        .filter(|sql| plan(&as_written, sql) != plan(&reordered, sql))
        .collect();
    assert!(reordered_queries.contains(&QUERIES[0].0));
    assert!(
        reordered_queries.len() >= QUERIES.len() / 2,
        "only {} reordered",
        reordered_queries.len()
    );

    for (sql, is_ordered) in QUERIES {
        let expected = answer(&mut as_written, sql, *is_ordered);
        assert!(expected.is_ok(), "{}: {:?}", sql, expected);
        assert_eq!(
            answer(&mut reordered, sql, *is_ordered),
            expected,
            "{}",
            sql
        );
    }
}
