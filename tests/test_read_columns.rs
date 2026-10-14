//! Scans decode only the columns a query reads. Every query here runs on two
//! identical databases, one decoding every column and one decoding only what
//! the read-column analysis asks for, and must give the same answer.

use std::path::{Path, PathBuf};

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rookdb_readcols_{}_{}", std::process::id(), tag));
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

fn session(root: &Path, decode_all: bool) -> Session {
    let engine = Engine::open(root);
    engine.set_decode_all_columns(decode_all);
    init_catalog_in(&engine).expect("init catalog");
    Session::new(load_catalog_in(&engine))
}

/// Rows as text, or the error text.
fn answer(s: &mut Session, sql: &str) -> Result<Vec<String>, String> {
    let plan = rook_parser::parse_sql(sql)?;
    let result = s.execute(plan).map_err(|e| e.to_string())?;
    let mut lines: Vec<String> = result
        .rows
        .map(|(_, rows)| rows.iter().map(|t| format!("{:?}", t.values)).collect())
        .unwrap_or_default();
    lines.extend(result.messages);
    Ok(lines)
}

const SETUP: &[&str] = &[
    "CREATE DATABASE rc",
    "USE rc",
    "CREATE TABLE dept (id INT PRIMARY KEY, dname VARCHAR(20))",
    "CREATE TABLE emp (id INT, name VARCHAR(20), dept INT, salary INT, mgr INT)",
    "CREATE TABLE tgt (id INT, name VARCHAR(20), dept INT, salary INT, mgr INT)",
    "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops'), (3, 'empty')",
    "INSERT INTO emp VALUES (1, 'ann', 1, 90, NULL), (2, 'bob', 1, 40, 1), \
     (3, 'cyd', 2, 70, 1), (4, 'dee', 2, NULL, 3), (5, 'eve', NULL, 15, 3), \
     (6, 'fay', 1, 55, 2), (7, 'gus', 2, 70, NULL)",
    "CREATE VIEW v AS SELECT name, dept FROM emp",
];

const QUERIES: &[&str] = &[
    "SELECT * FROM emp ORDER BY id",
    "SELECT name FROM emp ORDER BY id",
    "SELECT salary, name FROM emp ORDER BY id",
    "SELECT COUNT(*) FROM emp",
    "SELECT COUNT(mgr), SUM(salary), MIN(name), MAX(salary) FROM emp",
    "SELECT COUNT(DISTINCT dept) FROM emp",
    "SELECT dept, COUNT(*), SUM(salary) FROM emp GROUP BY dept ORDER BY dept",
    "SELECT dept, SUM(salary) FROM emp GROUP BY dept HAVING SUM(salary) > 100 ORDER BY dept",
    "SELECT DISTINCT dept FROM emp ORDER BY dept",
    "SELECT DISTINCT * FROM emp ORDER BY id",
    "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.id ORDER BY e.name",
    "SELECT * FROM emp e JOIN dept d ON e.dept = d.id ORDER BY e.id",
    "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.id ORDER BY e.id",
    "SELECT * FROM emp NATURAL JOIN dept ORDER BY id",
    "SELECT name, dname FROM emp NATURAL JOIN dept ORDER BY name",
    "SELECT name FROM emp JOIN dept USING (id) ORDER BY name",
    "SELECT a.name, b.name FROM emp a JOIN emp b ON a.mgr = b.id ORDER BY a.id",
    "SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp) ORDER BY name",
    "SELECT name FROM emp WHERE dept IN (SELECT id FROM dept WHERE dname LIKE 'e%') ORDER BY name",
    "SELECT name FROM emp e WHERE EXISTS (SELECT 1 FROM dept d WHERE d.id = e.dept) ORDER BY name",
    "SELECT name, (SELECT dname FROM dept d WHERE d.id = e.dept) FROM emp e ORDER BY id",
    "WITH x AS (SELECT dept, salary FROM emp) SELECT dept, MAX(salary) FROM x GROUP BY dept ORDER BY dept",
    "SELECT dept FROM emp UNION SELECT id FROM dept ORDER BY 1",
    "SELECT * FROM emp UNION SELECT * FROM emp ORDER BY 1",
    "SELECT id, salary * 2 + 1 AS s2 FROM emp ORDER BY s2 DESC, id",
    "SELECT UPPER(name) || '-' || CAST(dept AS VARCHAR(5)) FROM emp ORDER BY 1",
    "SELECT CASE WHEN salary > 50 THEN name ELSE 'low' END FROM emp ORDER BY id",
    "SELECT name FROM emp ORDER BY 1 LIMIT 3",
    "SELECT id FROM emp ORDER BY UPPER(name) DESC, salary",
    "SELECT name FROM emp ORDER BY dept * -1, mgr, id LIMIT 4",
    "SELECT name FROM emp ORDER BY salary DESC, id LIMIT 2 OFFSET 1",
    "SELECT * FROM (SELECT name, salary FROM emp WHERE salary > 10) t ORDER BY name",
    "SELECT t.n FROM (SELECT name AS n FROM emp) t ORDER BY t.n",
    "SELECT id FROM emp WHERE mgr IS NULL ORDER BY id",
    "SELECT id FROM emp WHERE name BETWEEN 'b' AND 'f' ORDER BY id",
    "SELECT id FROM emp WHERE NOT (dept = 1 OR salary < 20) ORDER BY id",
    "SELECT * FROM v ORDER BY name",
    "SELECT name FROM v WHERE dept = 2 ORDER BY name",
    "INSERT INTO tgt SELECT * FROM emp WHERE dept = 1",
    "INSERT INTO tgt (id, name) SELECT id, name FROM emp WHERE dept = 2",
    "SELECT * FROM tgt ORDER BY id, name",
    "UPDATE emp SET salary = salary + 1 WHERE dept = 1",
    "DELETE FROM emp WHERE mgr IS NULL",
    "SELECT * FROM emp ORDER BY id",
];

#[test]
fn reading_fewer_columns_changes_no_result() {
    let all = TempRoot::new("all");
    let some = TempRoot::new("some");
    let mut full = session(&all.0, true);
    let mut pruned = session(&some.0, false);
    for sql in SETUP {
        answer(&mut full, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
        answer(&mut pruned, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    for sql in QUERIES {
        assert_eq!(answer(&mut pruned, sql), answer(&mut full, sql), "{}", sql);
    }
}
