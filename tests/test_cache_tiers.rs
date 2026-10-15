//! Every cache tier can be switched off without changing a result. The same
//! script runs on two identical databases, one with every cache and one with
//! none, and must give the same answers; the lookup counters must show the
//! uncached engine never hit.

use std::path::{Path, PathBuf};

use storage_manager::backend::cache::{CacheSite, CacheTier};
use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rookdb_cachetiers_{}_{}", std::process::id(), tag));
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

fn session(root: &Path, cached: bool) -> Session {
    let engine = Engine::open(root);
    // Paths is process-wide; switching it would reach the cached engine too.
    for tier in [CacheTier::Plans, CacheTier::Meta, CacheTier::Handles] {
        engine.set_cache_enabled(tier, cached);
    }
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

const SCRIPT: &[&str] = &[
    "CREATE DATABASE ct",
    "USE ct",
    "CREATE TABLE dept (id INT PRIMARY KEY, dname VARCHAR(20) UNIQUE)",
    "CREATE TABLE emp (id INT PRIMARY KEY, dept INT REFERENCES dept(id), \
     salary INT CHECK (salary >= 0))",
    "CREATE INDEX emp_salary ON emp(salary)",
    "INSERT INTO dept VALUES (1, 'eng'), (2, 'ops'), (3, 'empty')",
    "INSERT INTO emp VALUES (1, 1, 90), (2, 1, 40), (3, 2, 70), (4, 2, 10), (5, 1, 55)",
    "INSERT INTO emp VALUES (6, 9, 10)",
    "INSERT INTO emp VALUES (7, 1, -5)",
    "INSERT INTO emp VALUES (1, 2, 30)",
    "SELECT * FROM emp ORDER BY id",
    "SELECT id FROM emp WHERE salary = 70",
    "SELECT d.dname, COUNT(*) FROM emp e JOIN dept d ON e.dept = d.id GROUP BY d.dname \
     ORDER BY d.dname",
    "UPDATE emp SET salary = salary + 5 WHERE dept = 1",
    "SELECT id, salary FROM emp WHERE salary > 50 ORDER BY id",
    "DELETE FROM dept WHERE id = 2",
    "DELETE FROM emp WHERE dept = 2",
    "DELETE FROM dept WHERE id = 2",
    "ALTER TABLE emp ADD COLUMN bonus INT",
    "INSERT INTO emp VALUES (8, 3, 20, 1)",
    "VACUUM emp",
    "SELECT * FROM emp ORDER BY id",
    "ANALYZE emp",
    "SELECT * FROM emp e JOIN dept d ON e.dept = d.id WHERE e.salary < 60 ORDER BY e.id",
];

#[test]
fn uncached_engine_gives_the_same_answers() {
    let cached_root = TempRoot::new("on");
    let uncached_root = TempRoot::new("off");
    let mut cached = session(&cached_root.0, true);
    let mut uncached = session(&uncached_root.0, false);

    for sql in SCRIPT {
        assert_eq!(
            answer(&mut cached, sql),
            answer(&mut uncached, sql),
            "answers differ for: {}",
            sql
        );
    }

    let stats = uncached.catalog.engine().cache_stats();
    for site in CacheSite::ALL {
        if site.tier() != CacheTier::Paths {
            assert_eq!(stats.hits(site), 0, "{} hit with its tier off", site.name());
        }
    }
    let stats = cached.catalog.engine().cache_stats();
    assert!(stats.hits(CacheSite::TableMeta) > 0);
    assert!(stats.hits(CacheSite::Heap) > 0);
}

#[test]
fn switching_a_tier_back_on_caches_again() {
    let root = TempRoot::new("toggle");
    let mut s = session(&root.0, false);
    for sql in [
        "CREATE DATABASE tg",
        "USE tg",
        "CREATE TABLE t (id INT PRIMARY KEY)",
    ] {
        answer(&mut s, sql).unwrap();
    }
    let engine = std::sync::Arc::clone(s.catalog.engine());
    engine.set_cache_enabled(CacheTier::Meta, true);
    let before = engine.cache_stats();
    answer(&mut s, "INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let lookups = engine.cache_stats().since(&before);
    assert!(lookups.hits(CacheSite::TableMeta) > 0, "{}", lookups);
}

#[test]
fn tier_lists_parse() {
    assert_eq!(
        CacheTier::parse_list("all").unwrap(),
        CacheTier::ALL.to_vec()
    );
    assert_eq!(
        CacheTier::parse_list("meta, Handles").unwrap(),
        vec![CacheTier::Meta, CacheTier::Handles]
    );
    assert!(CacheTier::parse_list("meta,bogus").is_err());
}
