//! Column binding: ambiguous names are rejected, quoted names are kept (D2, D10).

mod common;

use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
    common::init_catalog();
    Session::new(common::load_catalog())
}

fn run(session: &mut Session, sql: &str) -> Result<Vec<Vec<Option<DataValue>>>, String> {
    let plan = rook_parser::parse_sql(sql).map_err(|e| e.to_string())?;
    let result = session.execute(plan).map_err(|e| e.to_string())?;
    Ok(result
        .rows
        .map(|(_, rows)| rows.into_iter().map(|t| t.values).collect())
        .unwrap_or_default())
}

fn ok(session: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    run(session, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
}

fn ints(rows: Vec<Vec<Option<DataValue>>>) -> Vec<Vec<i32>> {
    rows.into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| match v {
                    Some(DataValue::Int(i)) => i,
                    other => panic!("not an INT: {:?}", other),
                })
                .collect()
        })
        .collect()
}

fn setup(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("binding", tag);
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    (ws, s)
}

#[test]
fn quoted_column_name_is_usable_everywhere() {
    let (_ws, mut s) = setup("quoted");
    ok(
        &mut s,
        r#"CREATE TABLE Mixed ("my col" INT, "Total" INT, plain INT)"#,
    );
    ok(
        &mut s,
        r#"INSERT INTO Mixed ("my col", "Total", plain) VALUES (3, 30, 1)"#,
    );
    ok(&mut s, r#"INSERT INTO Mixed VALUES (1, 10, 2), (2, 20, 3)"#);
    assert_eq!(
        ints(ok(
            &mut s,
            r#"SELECT "my col" FROM Mixed ORDER BY "my col""#
        )),
        [[1], [2], [3]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            r#"SELECT "Total" FROM Mixed WHERE "my col" > 1 ORDER BY "Total" DESC"#
        )),
        [[30], [20]]
    );
    ok(
        &mut s,
        r#"UPDATE Mixed SET "my col" = 9 WHERE "Total" = 10"#,
    );
    assert_eq!(
        ints(ok(&mut s, r#"SELECT "my col" FROM Mixed WHERE plain = 2"#)),
        [[9]]
    );
    // Unquoted spelling of an unquoted column is case-insensitive.
    assert_eq!(
        ints(ok(&mut s, "SELECT PLAIN FROM Mixed WHERE plain = 1")),
        [[1]]
    );
}

#[test]
fn quoted_column_name_survives_a_reload() {
    let (_ws, mut s) = setup("reload");
    ok(&mut s, r#"CREATE TABLE q ("my col" INT, "Total" INT)"#);
    ok(&mut s, "INSERT INTO q VALUES (1, 2)");
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "USE app");
    assert_eq!(
        ints(ok(&mut s, r#"SELECT "Total", "my col" FROM q"#)),
        [[2, 1]]
    );
}

/// `l` and `r` both have `id` and `k`; `only_l` / `only_r` are unique.
fn two_tables(tag: &str) -> (common::TestWorkspace, Session) {
    let (ws, mut s) = setup(tag);
    ok(&mut s, "CREATE TABLE l (id INT, k INT, only_l INT)");
    ok(&mut s, "CREATE TABLE r (id INT, k INT, only_r INT)");
    ok(&mut s, "INSERT INTO l VALUES (1, 10, 100), (2, 20, 200)");
    ok(&mut s, "INSERT INTO r VALUES (7, 10, 700), (8, 20, 800)");
    (ws, s)
}

fn assert_ambiguous(s: &mut Session, sql: &str) {
    match run(s, sql) {
        Err(e) => assert!(
            e.contains("Column reference 'id' is ambiguous"),
            "{}: wrong error: {}",
            sql,
            e
        ),
        Ok(rows) => panic!("{} should be ambiguous, returned {:?}", sql, rows),
    }
}

#[test]
fn unqualified_name_shared_by_two_joined_tables_is_ambiguous() {
    let (_ws, mut s) = two_tables("ambiguous");
    assert_ambiguous(&mut s, "SELECT id FROM l JOIN r ON l.k = r.k");
    assert_ambiguous(&mut s, "SELECT only_l FROM l JOIN r ON id = 1");
    assert_ambiguous(
        &mut s,
        "SELECT only_l FROM l JOIN r ON l.k = r.k WHERE id > 0",
    );
    assert_ambiguous(
        &mut s,
        "SELECT only_l FROM l JOIN r ON l.k = r.k ORDER BY id",
    );
    assert_ambiguous(&mut s, "SELECT id + 1 FROM l, r WHERE l.k = r.k");
    assert_ambiguous(
        &mut s,
        "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k GROUP BY id",
    );
    assert_ambiguous(
        &mut s,
        "SELECT l.k FROM l JOIN r ON l.k = r.k GROUP BY l.k HAVING MAX(id) > 0",
    );
    assert_ambiguous(&mut s, "SELECT MAX(id) FROM l JOIN r ON l.k = r.k");
    // Left side of a later join in a chain.
    ok(&mut s, "CREATE TABLE m (mk INT)");
    assert_ambiguous(
        &mut s,
        "SELECT m.mk FROM l JOIN r ON l.k = r.k JOIN m ON m.mk = id",
    );
}

#[test]
fn names_that_are_not_ambiguous_still_resolve() {
    let (_ws, mut s) = two_tables("unambiguous");
    // Only one input has the column.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT only_l, only_r FROM l JOIN r ON l.k = r.k ORDER BY only_l"
        )),
        [[100, 700], [200, 800]]
    );
    // Qualified names.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT l.id, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.id"
        )),
        [[1, 7], [2, 8]]
    );
    // ORDER BY may name a select-list alias.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT l.id AS x, r.id AS y FROM l JOIN r ON l.k = r.k ORDER BY y DESC"
        )),
        [[2, 8], [1, 7]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT l.id AS id FROM l JOIN r ON l.k = r.k ORDER BY id DESC"
        )),
        [[2], [1]]
    );
    // USING and NATURAL merge the shared column.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT k, only_l FROM l JOIN r USING (k) WHERE k > 10"
        )),
        [[20, 200]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT only_r FROM l LEFT JOIN r USING (k) ORDER BY k"
        )),
        [[700], [800]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT k, only_r FROM l RIGHT JOIN r USING (k) ORDER BY k"
        )),
        [[10, 700], [20, 800]]
    );
    ok(&mut s, "CREATE TABLE n (k INT, tag INT)");
    ok(&mut s, "INSERT INTO n VALUES (10, 5)");
    assert_eq!(
        ints(ok(&mut s, "SELECT k, tag FROM l JOIN n USING (k)")),
        [[10, 5]]
    );
    // `id` is still shared by l and r, so only k may go unqualified.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT k, tag, only_r FROM l JOIN r USING (k) JOIN n USING (k)"
        )),
        [[10, 5, 700]]
    );
    assert_ambiguous(&mut s, "SELECT id FROM l JOIN r USING (k)");
    let natural = ok(&mut s, "SELECT k FROM n NATURAL JOIN l");
    assert_eq!(ints(natural), [[10]]);
}

#[test]
fn aliases_subqueries_and_ctes_bind_by_their_own_names() {
    let (_ws, mut s) = two_tables("scopes");
    // Self-join through aliases.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT a.id, b.id FROM l a JOIN l b ON a.k = b.k ORDER BY a.id"
        )),
        [[1, 1], [2, 2]]
    );
    assert_ambiguous(&mut s, "SELECT id FROM l a JOIN l b ON a.k = b.k");
    // Derived table and CTE outputs with unique names.
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT lid, rid FROM (SELECT id AS lid, k FROM l) x JOIN (SELECT id AS rid, k AS k2 FROM r) y ON x.k = y.k2 ORDER BY lid"
        )),
        [[1, 7], [2, 8]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "WITH c AS (SELECT id AS cid, k FROM r) SELECT cid, only_l FROM l JOIN c ON l.k = c.k ORDER BY cid"
        )),
        [[7, 100], [8, 200]]
    );
    // A derived table that outputs `id` twice makes `id` ambiguous outside.
    assert_ambiguous(
        &mut s,
        "SELECT id FROM (SELECT l.id, r.id FROM l JOIN r ON l.k = r.k) x",
    );
}

// ── USING: the merged column means what SQL says it means ───────────────────

type Row = Vec<Option<i64>>;

/// Rows of integers (NULL as `None`), sorted so no ORDER BY is needed.
fn sorted(rows: Vec<Vec<Option<DataValue>>>) -> Vec<Row> {
    let mut out: Vec<Row> = rows
        .into_iter()
        .map(|r| {
            r.into_iter()
                .map(|v| {
                    v.map(|v| match v {
                        DataValue::Int(i) => i as i64,
                        DataValue::BigInt(i) => i,
                        other => panic!("not an integer: {:?}", other),
                    })
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn n(v: i64) -> Option<i64> {
    Some(v)
}

/// `l` and `r` each have rows without a partner and a NULL key.
fn using_tables(tag: &str) -> (common::TestWorkspace, Session) {
    let (ws, mut s) = setup(tag);
    ok(&mut s, "CREATE TABLE l (k INT, a INT)");
    ok(&mut s, "CREATE TABLE r (k INT, b INT)");
    ok(&mut s, "INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30)");
    ok(
        &mut s,
        "INSERT INTO r VALUES (2, 200), (3, 300), (NULL, 400)",
    );
    (ws, s)
}

fn query(s: &mut Session, sql: &str) -> Vec<Row> {
    sorted(ok(s, sql))
}

#[test]
fn using_merged_column_per_join_type() {
    let (_ws, mut s) = using_tables("using_types");
    let none = None;
    let cases: [(&str, Vec<Row>); 4] = [
        ("INNER", vec![vec![n(2), n(20), n(200)]]),
        (
            "LEFT",
            vec![
                vec![none, n(30), none],
                vec![n(1), n(10), none],
                vec![n(2), n(20), n(200)],
            ],
        ),
        (
            "RIGHT",
            vec![
                vec![none, none, n(400)],
                vec![n(2), n(20), n(200)],
                vec![n(3), none, n(300)],
            ],
        ),
        (
            "FULL",
            vec![
                vec![none, none, n(400)],
                vec![none, n(30), none],
                vec![n(1), n(10), none],
                vec![n(2), n(20), n(200)],
                vec![n(3), none, n(300)],
            ],
        ),
    ];
    for (jt, expected) in &cases {
        let sql = format!("SELECT k, a, b FROM l {} JOIN r USING (k)", jt);
        assert_eq!(&query(&mut s, &sql), expected, "{}", sql);
        // SELECT * shows the merged column once, first.
        let star = format!("SELECT * FROM l {} JOIN r USING (k)", jt);
        assert_eq!(&query(&mut s, &star), expected, "{}", star);
    }
}

#[test]
fn using_merged_column_in_where_group_and_order() {
    let (_ws, mut s) = using_tables("using_clauses");
    let none = None;
    // WHERE reads the merged column: a right row with no partner is found
    // through a RIGHT or FULL join, never through a LEFT join.
    for (jt, want) in [("RIGHT", true), ("FULL", true), ("LEFT", false)] {
        let sql = format!("SELECT k, a, b FROM l {} JOIN r USING (k) WHERE k = 3", jt);
        let expected = if want {
            vec![vec![n(3), none, n(300)]]
        } else {
            vec![]
        };
        assert_eq!(query(&mut s, &sql), expected, "{}", sql);
    }
    for (jt, want) in [("LEFT", true), ("FULL", true), ("RIGHT", false)] {
        let sql = format!("SELECT k, a, b FROM l {} JOIN r USING (k) WHERE k = 1", jt);
        let expected = if want {
            vec![vec![n(1), n(10), none]]
        } else {
            vec![]
        };
        assert_eq!(query(&mut s, &sql), expected, "{}", sql);
    }
    assert_eq!(
        query(
            &mut s,
            "SELECT a, b FROM l FULL JOIN r USING (k) WHERE k IS NULL"
        ),
        vec![vec![none, n(400)], vec![n(30), none]]
    );
    assert_eq!(
        query(
            &mut s,
            "SELECT a, b FROM l RIGHT JOIN r USING (k) WHERE k IS NULL"
        ),
        vec![vec![none, n(400)]]
    );
    // GROUP BY and ORDER BY on the merged column.
    assert_eq!(
        query(
            &mut s,
            "SELECT k, COUNT(*) FROM l FULL JOIN r USING (k) GROUP BY k"
        ),
        vec![
            vec![none, n(2)],
            vec![n(1), n(1)],
            vec![n(2), n(1)],
            vec![n(3), n(1)]
        ]
    );
    assert_eq!(
        ints_opt(ok(
            &mut s,
            "SELECT k FROM l FULL JOIN r USING (k) WHERE k IS NOT NULL ORDER BY k DESC"
        )),
        vec![n(3), n(2), n(1)]
    );
    // ORDER BY a merged column that is not in the select list.
    assert_eq!(
        ints_opt(ok(
            &mut s,
            "SELECT b FROM l RIGHT JOIN r USING (k) WHERE k IS NOT NULL ORDER BY k DESC"
        )),
        vec![n(300), n(200)]
    );
    assert_eq!(
        ints_opt(ok(
            &mut s,
            "SELECT COALESCE(a, 0) FROM l FULL JOIN r USING (k) WHERE k IS NOT NULL ORDER BY k"
        )),
        vec![n(10), n(20), n(0)]
    );
}

fn ints_opt(rows: Vec<Vec<Option<DataValue>>>) -> Vec<Option<i64>> {
    rows.into_iter()
        .map(|mut r| match r.remove(0) {
            Some(DataValue::Int(i)) => Some(i as i64),
            None => None,
            other => panic!("not an INT: {:?}", other),
        })
        .collect()
}

#[test]
fn using_chain_compares_the_merged_column_of_everything_before() {
    let (_ws, mut s) = setup("using_chain");
    ok(&mut s, "CREATE TABLE a (k INT, x INT)");
    ok(&mut s, "CREATE TABLE b (k INT, y INT)");
    ok(&mut s, "CREATE TABLE c (k INT, z INT)");
    ok(&mut s, "INSERT INTO a VALUES (1, 1), (2, 2), (NULL, 3)");
    ok(&mut s, "INSERT INTO b VALUES (2, 20), (3, 30)");
    ok(&mut s, "INSERT INTO c VALUES (2, 200), (3, 300), (1, 100)");
    let none = None;
    let cases: [(&str, Vec<Row>); 4] = [
        ("JOIN", vec![vec![n(2), n(2), n(20), n(200)]]),
        (
            "LEFT JOIN",
            vec![
                vec![n(1), n(1), none, n(100)],
                vec![n(2), n(2), n(20), n(200)],
            ],
        ),
        (
            "RIGHT JOIN",
            vec![
                vec![n(2), n(2), n(20), n(200)],
                vec![n(3), none, n(30), n(300)],
            ],
        ),
        (
            "FULL JOIN",
            vec![
                vec![n(1), n(1), none, n(100)],
                vec![n(2), n(2), n(20), n(200)],
                vec![n(3), none, n(30), n(300)],
            ],
        ),
    ];
    for (jt, expected) in &cases {
        let sql = format!(
            "SELECT k, x, y, z FROM a {} b USING (k) JOIN c USING (k)",
            jt
        );
        assert_eq!(&query(&mut s, &sql), expected, "{}", sql);
        let star = format!("SELECT * FROM a {} b USING (k) JOIN c USING (k)", jt);
        assert_eq!(&query(&mut s, &star), expected, "{}", star);
    }
    // The chain's second join still hashes: its keys name different tables.
    let plan = rook_parser::parse_sql("EXPLAIN SELECT k FROM a JOIN b USING (k) JOIN c USING (k)")
        .unwrap();
    let result = s.execute(plan).unwrap();
    assert!(result.plan.unwrap().root.contains("HashJoin"));
}

#[test]
fn correlated_subquery_outer_reference_is_checked_for_ambiguity() {
    let (_ws, mut s) = two_tables("correlated");
    ok(&mut s, "CREATE TABLE t (x INT, y INT)");
    ok(&mut s, "INSERT INTO t VALUES (1, 1), (8, 8)");
    // `id` is in both l and r: an outer reference to it must be qualified.
    assert_ambiguous(
        &mut s,
        "SELECT l.k FROM l JOIN r ON l.k = r.k WHERE EXISTS (SELECT 1 FROM t WHERE t.x = id)",
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT l.id FROM l JOIN r ON l.k = r.k WHERE EXISTS (SELECT 1 FROM t WHERE t.x = l.id)"
        )),
        [[1]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            "SELECT r.id FROM l JOIN r ON l.k = r.k WHERE EXISTS (SELECT 1 FROM t WHERE t.x = r.id)"
        )),
        [[8]]
    );
}

#[test]
fn quoted_table_names_keep_their_spelling_and_survive_a_reload() {
    let (_ws, mut s) = setup("quoted_table");
    ok(
        &mut s,
        r#"CREATE TABLE "My Table" (id INT PRIMARY KEY, v INT)"#,
    );
    ok(&mut s, r#"INSERT INTO "My Table" VALUES (1, 10), (2, 20)"#);
    ok(&mut s, r#"UPDATE "My Table" SET v = 11 WHERE id = 1"#);
    ok(
        &mut s,
        r#"CREATE TABLE child (cid INT, pid INT, FOREIGN KEY (pid) REFERENCES "My Table" (id))"#,
    );
    ok(&mut s, "INSERT INTO child VALUES (1, 2)");
    assert!(run(&mut s, "INSERT INTO child VALUES (2, 99)").is_err());
    assert_eq!(
        ints(ok(
            &mut s,
            r#"SELECT t.v FROM "My Table" t JOIN child ON child.pid = t.id"#
        )),
        [[20]]
    );
    assert_eq!(
        ints(ok(
            &mut s,
            r#"SELECT "My Table".v FROM "My Table" ORDER BY "My Table".id"#
        )),
        [[11], [20]]
    );

    // Reload from disk.
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "USE app");
    assert_eq!(
        ints(ok(&mut s, r#"SELECT v FROM "My Table" ORDER BY id"#)),
        [[11], [20]]
    );
    assert!(run(&mut s, "INSERT INTO child VALUES (3, 98)").is_err());
    ok(&mut s, r#"DELETE FROM "My Table" WHERE id = 1"#);
    assert_eq!(ints(ok(&mut s, r#"SELECT id FROM "My Table""#)), [[2]]);
    ok(&mut s, r#"ALTER TABLE "My Table" RENAME TO "Other Name""#);
    assert_eq!(ints(ok(&mut s, r#"SELECT id FROM "Other Name""#)), [[2]]);
    ok(&mut s, "DROP TABLE child");
    ok(&mut s, r#"TRUNCATE TABLE "Other Name""#);
    assert!(ints(ok(&mut s, r#"SELECT id FROM "Other Name""#)).is_empty());
    ok(&mut s, r#"DROP TABLE "Other Name""#);
    assert!(run(&mut s, r#"SELECT * FROM "Other Name""#).is_err());
}

#[test]
fn table_names_a_file_cannot_hold_are_rejected() {
    let (_ws, mut s) = setup("bad_table_names");
    for name in [r#""a/b""#, r#""a:b""#, r#""a|b""#, r#""..""#, r#""a*b""#] {
        let err = run(&mut s, &format!("CREATE TABLE {} (id INT)", name)).unwrap_err();
        assert!(err.contains("Invalid table name"), "{}: {}", name, err);
    }
}
