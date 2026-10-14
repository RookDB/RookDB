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
