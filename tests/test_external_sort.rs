//! ORDER BY sorts on disk when its input outgrows the engine's sort memory
//! budget. Every query here runs on two identical databases, one sorting in
//! memory and one with a budget of a few rows and a merge fan-in of 3, so
//! its sorts write hundreds of runs and merge them in several passes. Both
//! must give the same rows in the same order.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rookdb_extsort_sql_{}_{}", std::process::id(), tag));
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

fn session(root: &Path) -> (Arc<Engine>, Session) {
    let engine = Engine::open(root);
    init_catalog_in(&engine);
    let session = Session::new(load_catalog_in(&engine));
    (engine, session)
}

/// Rows as text, in the order the statement returned them.
fn answer(s: &mut Session, sql: &str) -> Vec<String> {
    let plan = rook_parser::parse_sql(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    let result = s.execute(plan).unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(_, rows)| rows.iter().map(|t| format!("{:?}", t.values)).collect())
        .unwrap_or_default()
}

/// Run files currently in the engine's temp area, or `None` if it has never
/// sorted on disk.
fn run_files(engine: &Engine) -> Option<usize> {
    std::fs::read_dir(engine.paths().temp_dir())
        .ok()
        .map(|d| d.count())
}

const ROWS: usize = 3000;

/// `CREATE`s and `INSERT`s for a table with few distinct values per column
/// (so sorts have ties), NULLs in every sort column, text and numbers.
fn setup() -> Vec<String> {
    let mut sql = vec![
        "CREATE DATABASE es".to_string(),
        "USE es".to_string(),
        "CREATE TABLE t (id INT, grp INT, name VARCHAR(20), amount BIGINT, \
         price DOUBLE PRECISION, day DATE)"
            .to_string(),
        "CREATE TABLE g (grp INT, label VARCHAR(10))".to_string(),
        "INSERT INTO g VALUES (0, 'zero'), (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')"
            .to_string(),
    ];
    let mut seed = 0x2545_f491u64;
    let mut next = |n: u64| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) % n
    };
    let names = ["ann", "Bob", "cyd", "", "dee dee", "évé", "zed", "ANN"];
    let mut values = Vec::new();
    for id in 0..ROWS {
        let grp = match next(8) {
            7 => "NULL".to_string(),
            g => (g % 5).to_string(),
        };
        let name = match next(10) {
            9 => "NULL".to_string(),
            n => format!("'{}{}'", names[n as usize % names.len()], next(3)),
        };
        let amount = match next(12) {
            11 => "NULL".to_string(),
            _ => (next(40) as i64 * 1_000_000_007 - 20_000_000_000).to_string(),
        };
        let price = match next(9) {
            8 => "NULL".to_string(),
            _ => format!("{}.{}", next(30) as i64 - 15, next(4) * 25),
        };
        let day = match next(11) {
            10 => "NULL".to_string(),
            _ => format!("'2024-{:02}-{:02}'", next(12) + 1, next(28) + 1),
        };
        values.push(format!(
            "({}, {}, {}, {}, {}, {})",
            id, grp, name, amount, price, day
        ));
        if values.len() == 250 || id + 1 == ROWS {
            sql.push(format!("INSERT INTO t VALUES {}", values.join(", ")));
            values.clear();
        }
    }
    sql
}

/// Queries whose ORDER BY sorts on disk under the tiny budget. Most leave
/// ties, which must come out in the same order both ways.
const SPILLING: &[&str] = &[
    "SELECT * FROM t ORDER BY id DESC",
    "SELECT * FROM t ORDER BY grp",
    "SELECT * FROM t ORDER BY grp DESC",
    "SELECT * FROM t ORDER BY name",
    "SELECT * FROM t ORDER BY name DESC, amount",
    "SELECT * FROM t ORDER BY grp, name DESC, amount",
    "SELECT * FROM t ORDER BY amount DESC, grp DESC",
    "SELECT * FROM t ORDER BY price, day DESC",
    "SELECT * FROM t ORDER BY day, price DESC, name",
    "SELECT * FROM t ORDER BY grp DESC, name, amount DESC, price, day DESC, id DESC",
    "SELECT name, id FROM t ORDER BY name, grp",
    "SELECT id, amount FROM t WHERE grp IS NOT NULL ORDER BY amount",
    "SELECT id, amount / 7 + grp AS x FROM t ORDER BY x DESC",
    "SELECT id, name, grp FROM t ORDER BY UPPER(name), grp DESC",
    "SELECT t.id, g.label FROM t JOIN g ON t.grp = g.grp ORDER BY g.label DESC, t.name",
    "SELECT grp, name, COUNT(*), SUM(amount) FROM t GROUP BY grp, name ORDER BY name DESC, grp",
    "SELECT * FROM (SELECT id, name, price FROM t WHERE price > 0) s ORDER BY price DESC, name",
    "SELECT grp FROM t UNION ALL SELECT grp FROM g ORDER BY 1 DESC",
];

/// Queries with a limit: they keep only the rows they need and never spill.
const LIMITED: &[&str] = &[
    "SELECT * FROM t ORDER BY grp LIMIT 7",
    "SELECT * FROM t ORDER BY name DESC, amount LIMIT 40 OFFSET 15",
    "SELECT id, amount / 7 + grp AS x FROM t ORDER BY x DESC LIMIT 5",
];

#[test]
fn sort_on_disk_matches_sort_in_memory() {
    let (mem_root, disk_root) = (TempRoot::new("mem"), TempRoot::new("disk"));
    let (mem_engine, mut mem) = session(&mem_root.0);
    let (disk_engine, mut disk) = session(&disk_root.0);
    for sql in setup() {
        answer(&mut mem, &sql);
        answer(&mut disk, &sql);
    }
    // A handful of rows per run, so 3,000 rows make hundreds of runs, merged
    // three at a time in several passes.
    disk_engine.set_sort_memory_budget(2048);
    disk_engine.set_sort_merge_fan_in(3);

    for sql in LIMITED {
        let expected = answer(&mut mem, sql);
        assert!(!expected.is_empty(), "{}", sql);
        assert_eq!(answer(&mut disk, sql), expected, "{}", sql);
        assert_eq!(run_files(&disk_engine), None, "{} must not spill", sql);
    }

    for sql in SPILLING {
        let expected = answer(&mut mem, sql);
        assert!(expected.len() > 100, "{}: {} rows", sql, expected.len());
        let got = answer(&mut disk, sql);
        assert_eq!(got.len(), expected.len(), "{}", sql);
        if let Some(i) = (0..got.len()).find(|&i| got[i] != expected[i]) {
            panic!(
                "{}: row {} is {} on disk, {} in memory",
                sql, i, got[i], expected[i]
            );
        }
        assert_eq!(
            run_files(&disk_engine),
            Some(0),
            "{} must sort on disk and remove its run files",
            sql
        );
        std::fs::remove_dir(disk_engine.paths().temp_dir()).unwrap();
    }
    assert_eq!(
        run_files(&mem_engine),
        None,
        "the default budget holds this"
    );
}

/// A sort abandoned part-way, here by a LIMIT above it reading only the
/// first rows of the merge, leaves no run files behind.
#[test]
fn abandoned_sort_removes_its_run_files() {
    let root = TempRoot::new("abandoned");
    let (engine, mut s) = session(&root.0);
    for sql in setup() {
        answer(&mut s, &sql);
    }
    engine.set_sort_memory_budget(2048);
    engine.set_sort_merge_fan_in(3);

    let rows = answer(
        &mut s,
        "SELECT * FROM (SELECT id, name FROM t ORDER BY name, id) s LIMIT 3",
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(run_files(&engine), Some(0));
}
