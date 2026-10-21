//! Differential test against SQLite: random queries over the same data must
//! return the same rows in RookDB and in SQLite.
//!
//! The generator keeps to SQL on which the two agree: integer arithmetic
//! without division, byte-ordered strings, case-sensitive LIKE, and ORDER BY
//! only with every NULL coalesced away (SQLite sorts NULL first, RookDB
//! last). Results are compared as sorted multisets unless the query has
//! ORDER BY + LIMIT, where order matters.
//!
//! `ROOK_FUZZ_SEED` picks the seed (default fixed, so CI is repeatable) and
//! `ROOK_FUZZ_QUERIES` the number of queries. A failure prints the seed and
//! the first `ROOK_FUZZ_SHOW` (default 10) differing queries with both
//! results.

mod common;

use std::fmt::Write as _;

use rusqlite::Connection;
use rusqlite::types::ValueRef;
use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;

const ROWS: usize = 40;
const STRINGS: &[&str] = &["x", "y", "ab", "abc", "Z", "b_c", ""];
const PATTERNS: &[&str] = &["a%", "%b", "_b%", "%", "x", "%c%", "b\\_c"];

/// xorshift64*: deterministic across platforms and needs no crate.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as usize) as i64
    }
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// The tables a query reads: `x` is t1(a, b, c); `y`, when joined, is
/// t2(a, d, e).
struct Scope {
    ints: Vec<&'static str>,
    strs: Vec<&'static str>,
}

struct Gen<'a> {
    rng: &'a mut Rng,
}

impl Gen<'_> {
    fn int_lit(&mut self) -> String {
        self.rng.range(-5, 5).to_string()
    }

    fn str_lit(&mut self) -> String {
        format!("'{}'", self.rng.pick(STRINGS))
    }

    fn int_expr(&mut self, s: &Scope, depth: u32) -> String {
        let leaf = depth == 0 || self.rng.chance(45);
        if leaf {
            return if self.rng.chance(75) {
                s.ints[self.rng.below(s.ints.len())].to_string()
            } else {
                self.int_lit()
            };
        }
        match self.rng.below(6) {
            0 => format!(
                "({} + {})",
                self.int_expr(s, depth - 1),
                self.int_expr(s, depth - 1)
            ),
            1 => format!(
                "({} - {})",
                self.int_expr(s, depth - 1),
                self.int_expr(s, depth - 1)
            ),
            2 => format!(
                "({} * {})",
                self.int_expr(s, depth - 1),
                self.int_expr(s, depth - 1)
            ),
            3 => format!("ABS({})", self.int_expr(s, depth - 1)),
            4 => format!(
                "COALESCE({}, {})",
                self.int_expr(s, depth - 1),
                self.int_lit()
            ),
            _ => {
                let else_part = if self.rng.chance(70) {
                    format!(" ELSE {}", self.int_expr(s, depth - 1))
                } else {
                    String::new()
                };
                format!(
                    "CASE WHEN {} THEN {}{} END",
                    self.pred(s, depth - 1),
                    self.int_expr(s, depth - 1),
                    else_part
                )
            }
        }
    }

    fn str_expr(&mut self, s: &Scope) -> String {
        if self.rng.chance(80) {
            s.strs[self.rng.below(s.strs.len())].to_string()
        } else {
            self.str_lit()
        }
    }

    fn cmp(&mut self) -> &'static str {
        self.rng.pick(&["=", "<>", "<", "<=", ">", ">="])
    }

    fn pred(&mut self, s: &Scope, depth: u32) -> String {
        let leaf = depth == 0 || self.rng.chance(40);
        if leaf {
            return match self.rng.below(8) {
                0 | 1 => {
                    let op = self.cmp();
                    format!("{} {} {}", self.int_expr(s, 1), op, self.int_expr(s, 1))
                }
                2 => {
                    let op = self.cmp();
                    format!("{} {} {}", self.str_expr(s), op, self.str_expr(s))
                }
                3 => {
                    let not = if self.rng.chance(50) { "NOT " } else { "" };
                    if self.rng.chance(50) {
                        format!("{} IS {}NULL", self.int_expr(s, 1), not)
                    } else {
                        format!("{} IS {}NULL", self.str_expr(s), not)
                    }
                }
                4 => {
                    let not = if self.rng.chance(30) { "NOT " } else { "" };
                    format!(
                        "{} {}BETWEEN {} AND {}",
                        self.int_expr(s, 1),
                        not,
                        self.int_lit(),
                        self.int_lit()
                    )
                }
                5 => {
                    let not = if self.rng.chance(30) { "NOT " } else { "" };
                    let n = 1 + self.rng.below(4);
                    let items: Vec<String> = (0..n).map(|_| self.int_lit()).collect();
                    format!("{} {}IN ({})", self.int_expr(s, 1), not, items.join(", "))
                }
                6 => {
                    let not = if self.rng.chance(30) { "NOT " } else { "" };
                    let pattern = self.rng.pick(PATTERNS);
                    let escape = if pattern.contains('\\') {
                        " ESCAPE '\\'"
                    } else {
                        ""
                    };
                    format!(
                        "{} {}LIKE '{}'{}",
                        self.rng.pick(&s.strs),
                        not,
                        pattern,
                        escape
                    )
                }
                _ => self.subquery_pred(s),
            };
        }
        match self.rng.below(3) {
            0 => format!("NOT ({})", self.pred(s, depth - 1)),
            1 => format!(
                "({} AND {})",
                self.pred(s, depth - 1),
                self.pred(s, depth - 1)
            ),
            _ => format!(
                "({} OR {})",
                self.pred(s, depth - 1),
                self.pred(s, depth - 1)
            ),
        }
    }

    /// EXISTS / IN / scalar subqueries over t2, correlated to `x` half the time.
    fn subquery_pred(&mut self, s: &Scope) -> String {
        let inner = Scope {
            ints: vec!["z.a", "z.d"],
            strs: vec!["z.e"],
        };
        let mut filter = self.pred(&inner, 0);
        if self.rng.chance(50) {
            filter = format!("{} AND z.a = x.a", filter);
        }
        match self.rng.below(3) {
            0 => {
                let not = if self.rng.chance(40) { "NOT " } else { "" };
                format!("{}EXISTS (SELECT 1 FROM t2 z WHERE {})", not, filter)
            }
            1 => {
                let not = if self.rng.chance(40) { "NOT " } else { "" };
                format!(
                    "{} {}IN (SELECT z.d FROM t2 z WHERE {})",
                    self.rng.pick(&s.ints),
                    not,
                    filter
                )
            }
            _ => {
                let agg = self
                    .rng
                    .pick(&["COUNT(*)", "MIN(z.d)", "MAX(z.d)", "SUM(z.d)"]);
                let op = self.cmp();
                format!(
                    "{} {} (SELECT {} FROM t2 z WHERE {})",
                    self.rng.pick(&s.ints),
                    op,
                    agg,
                    filter
                )
            }
        }
    }

    fn from(&mut self) -> (String, Scope) {
        match self.rng.below(4) {
            0 | 1 => (
                "t1 x".to_string(),
                Scope {
                    ints: vec!["x.a", "x.b"],
                    strs: vec!["x.c"],
                },
            ),
            n => {
                let kind = if n == 2 { "JOIN" } else { "LEFT JOIN" };
                let on = if self.rng.chance(70) {
                    "x.a = y.a".to_string()
                } else {
                    "x.a = y.a AND x.b < y.d".to_string()
                };
                (
                    format!("t1 x {} t2 y ON {}", kind, on),
                    Scope {
                        ints: vec!["x.a", "x.b", "y.a", "y.d"],
                        strs: vec!["x.c", "y.e"],
                    },
                )
            }
        }
    }

    fn where_clause(&mut self, s: &Scope) -> String {
        if self.rng.chance(75) {
            format!(" WHERE {}", self.pred(s, 2))
        } else {
            String::new()
        }
    }

    fn projection(&mut self, s: &Scope) -> Vec<String> {
        let n = 1 + self.rng.below(3);
        (0..n)
            .map(|_| {
                if self.rng.chance(70) {
                    self.int_expr(s, 2)
                } else {
                    self.str_expr(s)
                }
            })
            .collect()
    }

    /// One query, and whether its row order is part of the result.
    fn query(&mut self) -> (String, bool) {
        let (from, s) = self.from();
        match self.rng.below(10) {
            // SELECT [DISTINCT] ... [WHERE]
            0..=3 => {
                let distinct = if self.rng.chance(25) { "DISTINCT " } else { "" };
                let mut cols = self.projection(&s);
                // A predicate as a value: TRUE / FALSE / NULL (1 / 0 in SQLite).
                if self.rng.chance(30) {
                    cols.push(self.pred(&s, 1));
                }
                let w = self.where_clause(&s);
                (
                    format!("SELECT {}{} FROM {}{}", distinct, cols.join(", "), from, w),
                    false,
                )
            }
            // GROUP BY with aggregates and HAVING
            4..=6 => {
                let key = if self.rng.chance(70) {
                    self.int_expr(&s, 1)
                } else {
                    s.strs[self.rng.below(s.strs.len())].to_string()
                };
                let arg = self.int_expr(&s, 1);
                let agg = self
                    .rng
                    .pick(&["COUNT(*)", "COUNT({})", "SUM({})", "MIN({})", "MAX({})"])
                    .replace("{}", &arg);
                let w = self.where_clause(&s);
                let grouped = self.rng.chance(85);
                let (key_col, group_by) = if grouped {
                    (format!("{}, ", key), format!(" GROUP BY {}", key))
                } else {
                    (String::new(), String::new())
                };
                let having = if grouped && self.rng.chance(40) {
                    format!(" HAVING COUNT(*) > {}", self.rng.below(3))
                } else {
                    String::new()
                };
                (
                    format!(
                        "SELECT {}{}, COUNT(*) FROM {}{}{}{}",
                        key_col, agg, from, w, group_by, having
                    ),
                    false,
                )
            }
            // Set operations
            7 => {
                let op = self
                    .rng
                    .pick(&["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]);
                let left_scope = Scope {
                    ints: vec!["x.a", "x.b"],
                    strs: vec!["x.c"],
                };
                let right_scope = Scope {
                    ints: vec!["z.a", "z.d"],
                    strs: vec!["z.e"],
                };
                let l = self.int_expr(&left_scope, 1);
                let lw = self.pred(&left_scope, 1);
                let r = self.int_expr(&right_scope, 1);
                let rw = self.pred(&right_scope, 0);
                (
                    format!(
                        "SELECT {} FROM t1 x WHERE {} {} SELECT {} FROM t2 z WHERE {}",
                        l, lw, op, r, rw
                    ),
                    false,
                )
            }
            // ORDER BY every output (NULLs coalesced) + LIMIT/OFFSET
            _ => {
                let cols = self.projection(&s);
                let w = self.where_clause(&s);
                let keys: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let is_str = s.strs.contains(&c.as_str()) || c.starts_with('\'');
                        let dir = if self.rng.chance(50) { " DESC" } else { "" };
                        if is_str {
                            format!("COALESCE({}, '~'){}", c, dir)
                        } else {
                            format!("COALESCE({}, 99999){}", c, dir)
                        }
                    })
                    .collect();
                let selected: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        if s.strs.contains(&c.as_str()) || c.starts_with('\'') {
                            format!("COALESCE({}, '~')", c)
                        } else {
                            format!("COALESCE({}, 99999)", c)
                        }
                    })
                    .collect();
                let offset = if self.rng.chance(30) {
                    format!(" OFFSET {}", self.rng.below(5))
                } else {
                    String::new()
                };
                (
                    format!(
                        "SELECT {} FROM {}{} ORDER BY {} LIMIT {}{}",
                        selected.join(", "),
                        from,
                        w,
                        keys.join(", "),
                        1 + self.rng.below(10),
                        offset
                    ),
                    true,
                )
            }
        }
    }
}

/// A value as both engines should print it: integral numbers without a
/// fraction, other numbers to 6 places, strings bare.
fn normalise_number(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{:.6}", v)
    }
}

fn rook_cell(v: &Option<DataValue>) -> String {
    match v {
        None => "NULL".to_string(),
        Some(DataValue::SmallInt(n)) => n.to_string(),
        Some(DataValue::Int(n)) => n.to_string(),
        Some(DataValue::BigInt(n)) => n.to_string(),
        Some(DataValue::Real(f)) => normalise_number(f.0 as f64),
        Some(DataValue::DoublePrecision(f)) => normalise_number(f.0),
        Some(n @ DataValue::Numeric(_)) => {
            normalise_number(n.to_string().parse::<f64>().expect("numeric text"))
        }
        Some(DataValue::Bool(b)) => (*b as i64).to_string(),
        Some(DataValue::Char(s)) => s.trim_end_matches(' ').to_string(),
        Some(DataValue::Varchar(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

fn sqlite_cell(v: ValueRef<'_>) -> String {
    match v {
        ValueRef::Null => "NULL".to_string(),
        ValueRef::Integer(n) => n.to_string(),
        ValueRef::Real(f) => normalise_number(f),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
        ValueRef::Blob(b) => format!("{:?}", b),
    }
}

fn run_rook(s: &mut Session, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let plan = rook_parser::parse_sql(sql).map_err(|e| format!("parse: {}", e))?;
    let result = s.execute(plan).map_err(|e| e.to_string())?;
    let (_, rows) = result.rows.ok_or("no rows")?;
    Ok(rows
        .iter()
        .map(|t| t.values.iter().map(rook_cell).collect())
        .collect())
}

fn run_sqlite(c: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = c.prepare(sql).map_err(|e| e.to_string())?;
    let n = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            (0..n)
                .map(|i| row.get_ref(i).map(sqlite_cell))
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
}

fn exec_both(s: &mut Session, c: &Connection, sql: &str) {
    s.execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    c.execute_batch(sql)
        .unwrap_or_else(|e| panic!("sqlite {}: {}", sql, e));
}

fn load(rng: &mut Rng, s: &mut Session, c: &Connection) {
    exec_both(s, c, "CREATE TABLE t1 (a INT, b INT, c VARCHAR(8))");
    exec_both(s, c, "CREATE TABLE t2 (a INT, d INT, e VARCHAR(8))");
    for table in ["t1", "t2"] {
        let mut values = Vec::with_capacity(ROWS);
        for _ in 0..ROWS {
            let mut row = Vec::with_capacity(3);
            row.push(if rng.chance(10) {
                "NULL".to_string()
            } else {
                rng.range(0, 9).to_string()
            });
            row.push(if rng.chance(15) {
                "NULL".to_string()
            } else {
                rng.range(-20, 20).to_string()
            });
            row.push(if rng.chance(15) {
                "NULL".to_string()
            } else {
                format!("'{}'", rng.pick(STRINGS))
            });
            values.push(format!("({})", row.join(", ")));
        }
        exec_both(
            s,
            c,
            &format!("INSERT INTO {} VALUES {}", table, values.join(", ")),
        );
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn random_queries_match_sqlite() {
    let seed: u64 = env_or("ROOK_FUZZ_SEED", 0x5EED_2026);
    let queries: usize = env_or("ROOK_FUZZ_QUERIES", 400);
    let shown: usize = env_or("ROOK_FUZZ_SHOW", 10);

    let _ws = common::TestWorkspace::new("differential", "sqlite");
    let mut session = Session::new(common::load_catalog());
    for sql in ["CREATE DATABASE fuzz_db", "USE fuzz_db"] {
        session
            .execute(rook_parser::parse_sql(sql).unwrap())
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    let sqlite = Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch("PRAGMA case_sensitive_like = ON")
        .unwrap();

    let mut rng = Rng(seed | 1);
    load(&mut rng, &mut session, &sqlite);

    let mut failures = String::new();
    let mut failed = 0;
    for i in 0..queries {
        let (sql, ordered) = Gen { rng: &mut rng }.query();
        let expected = run_sqlite(&sqlite, &sql);
        let actual = run_rook(&mut session, &sql);
        let same = match (&expected, &actual) {
            (Ok(e), Ok(a)) if ordered => e == a,
            (Ok(e), Ok(a)) => {
                let (mut e, mut a) = (e.clone(), a.clone());
                e.sort();
                a.sort();
                e == a
            }
            // A query SQLite rejects tells us nothing about RookDB.
            (Err(_), _) => true,
            (Ok(_), Err(_)) => false,
        };
        if !same {
            failed += 1;
            if failed <= shown {
                let _ = writeln!(
                    failures,
                    "#{}: {}\n  sqlite: {:?}\n  rookdb: {:?}\n",
                    i, sql, expected, actual
                );
            }
        }
    }
    assert!(
        failed == 0,
        "{} of {} queries differ from SQLite (ROOK_FUZZ_SEED={}):\n{}",
        failed,
        queries,
        seed,
        failures
    );
}
