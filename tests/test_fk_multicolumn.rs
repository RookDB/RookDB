//! Multi-column FOREIGN KEYs are enforced like single-column ones, and a
//! renamed column is followed by the foreign keys that name it.

mod common;

use storage_manager::backend::session::{Session, StatementResult};

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
    common::init_catalog();
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    s
}

/// A new session on the same data directory, as after a restart.
fn restart(session: Session) -> Session {
    drop(session);
    common::engine().invalidate_metadata();
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "USE app");
    s
}

fn run(session: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    session.execute(plan).map_err(|e| e.to_string())
}

fn ok(session: &mut Session, sql: &str) {
    run(session, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    let (_, rows) = run(session, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .unwrap();
    rows.iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| v.as_ref().map_or("NULL".to_string(), |v| v.to_string()))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect()
}

fn parent(s: &mut Session) {
    ok(s, "CREATE TABLE p (a INT, b INT, PRIMARY KEY (a, b))");
    ok(s, "INSERT INTO p VALUES (1, 1), (1, 2), (2, 1)");
}

fn child(s: &mut Session, actions: &str) {
    ok(
        s,
        &format!(
            "CREATE TABLE c (id INT PRIMARY KEY, x INT, y INT, \
             FOREIGN KEY (x, y) REFERENCES p (a, b) {actions})"
        ),
    );
}

#[test]
fn child_insert_needs_the_whole_key_in_the_parent() {
    let _ws = common::TestWorkspace::new("fkmulti", "insert");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");
    ok(&mut s, "INSERT INTO c VALUES (2, 2, 1)");
    // Each column exists in the parent, the pair does not.
    let err = run(&mut s, "INSERT INTO c VALUES (3, 2, 2)").unwrap_err();
    assert!(err.contains("FOREIGN KEY"), "{err}");
    assert!(run(&mut s, "INSERT INTO c VALUES (4, 9, 9)").is_err());
    assert_eq!(rows(&mut s, "SELECT id FROM c ORDER BY id"), ["1", "2"]);
}

#[test]
fn a_null_in_one_column_is_not_checked() {
    let _ws = common::TestWorkspace::new("fkmulti", "null");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 9, NULL)");
    ok(&mut s, "INSERT INTO c VALUES (2, NULL, 9)");
    ok(&mut s, "INSERT INTO c VALUES (3, NULL, NULL)");
    assert_eq!(rows(&mut s, "SELECT id FROM c ORDER BY id").len(), 3);
}

#[test]
fn child_update_is_checked() {
    let _ws = common::TestWorkspace::new("fkmulti", "childupdate");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 1)");
    assert!(run(&mut s, "UPDATE c SET y = 3 WHERE id = 1").is_err());
    ok(&mut s, "UPDATE c SET y = 2 WHERE id = 1");
    ok(&mut s, "UPDATE c SET x = 2, y = 1 WHERE id = 1");
    assert_eq!(rows(&mut s, "SELECT x, y FROM c"), ["2,1"]);
}

#[test]
fn referenced_columns_may_be_listed_in_another_order() {
    let _ws = common::TestWorkspace::new("fkmulti", "order");
    let mut s = session();
    parent(&mut s);
    ok(
        &mut s,
        "CREATE TABLE c (y INT, x INT, FOREIGN KEY (y, x) REFERENCES p (b, a))",
    );
    ok(&mut s, "INSERT INTO c VALUES (2, 1)");
    assert!(run(&mut s, "INSERT INTO c VALUES (1, 2)").is_ok());
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 2)").is_err());
    assert!(run(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2").is_err());
}

#[test]
fn the_referenced_columns_must_be_a_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "notkey");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (a INT, b INT, PRIMARY KEY (a, b))");
    // Only part of the key, and columns that are no key at all.
    for refs in ["p (a)", "p (b)"] {
        let err = run(
            &mut s,
            &format!("CREATE TABLE c (x INT, FOREIGN KEY (x) REFERENCES {refs})"),
        )
        .unwrap_err();
        assert!(err.contains("PRIMARY KEY or UNIQUE"), "{err}");
    }
    let err = run(
        &mut s,
        "CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p (a))",
    )
    .unwrap_err();
    assert!(err.contains("column(s)"), "{err}");
    let err = run(
        &mut s,
        "CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p (a, z))",
    )
    .unwrap_err();
    assert!(err.contains("'z'"), "{err}");
    // None of the failed statements left a table behind.
    ok(&mut s, "CREATE TABLE c (x INT)");
    // UNIQUE (a, b) is as good as a PRIMARY KEY.
    ok(&mut s, "CREATE TABLE u (a INT, b INT, UNIQUE (a, b))");
    ok(
        &mut s,
        "CREATE TABLE d (x INT, y INT, FOREIGN KEY (x, y) REFERENCES u (a, b))",
    );
}

#[test]
fn parent_delete_restrict_blocks_while_a_child_references_the_pair() {
    let _ws = common::TestWorkspace::new("fkmulti", "restrict");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");
    let err = run(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2").unwrap_err();
    assert!(err.contains("referenced"), "{err}");
    // (1, 1) shares each column with the referenced pair but is not it.
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 1");
    // Nothing was deleted by the blocked statement.
    assert!(run(&mut s, "DELETE FROM p WHERE a = 1").is_err());
    assert_eq!(
        rows(&mut s, "SELECT a, b FROM p ORDER BY a, b"),
        ["1,2", "2,1"]
    );
}

#[test]
fn parent_delete_cascade_removes_children_of_the_pair_only() {
    let _ws = common::TestWorkspace::new("fkmulti", "cascade");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON DELETE CASCADE");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 1), (2, 1, 2), (3, 1, 2), (4, 2, 1)",
    );
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
    assert_eq!(rows(&mut s, "SELECT id FROM c ORDER BY id"), ["1", "4"]);
}

#[test]
fn cascade_follows_two_column_keys_to_grandchildren() {
    let _ws = common::TestWorkspace::new("fkmulti", "grand");
    let mut s = session();
    parent(&mut s);
    ok(
        &mut s,
        "CREATE TABLE c (x INT, y INT, PRIMARY KEY (x, y), \
         FOREIGN KEY (x, y) REFERENCES p (a, b) ON DELETE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE g (id INT PRIMARY KEY, m INT, n INT, \
         FOREIGN KEY (m, n) REFERENCES c (x, y) ON DELETE CASCADE)",
    );
    ok(&mut s, "INSERT INTO c VALUES (1, 1), (1, 2)");
    ok(
        &mut s,
        "INSERT INTO g VALUES (1, 1, 1), (2, 1, 2), (3, 1, 2)",
    );
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
    assert_eq!(rows(&mut s, "SELECT x, y FROM c"), ["1,1"]);
    assert_eq!(rows(&mut s, "SELECT id FROM g"), ["1"]);
}

#[test]
fn a_restricting_grandchild_blocks_the_whole_cascade() {
    let _ws = common::TestWorkspace::new("fkmulti", "grandrestrict");
    let mut s = session();
    parent(&mut s);
    ok(
        &mut s,
        "CREATE TABLE c (x INT, y INT, PRIMARY KEY (x, y), \
         FOREIGN KEY (x, y) REFERENCES p (a, b) ON DELETE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE g (m INT, n INT, FOREIGN KEY (m, n) REFERENCES c (x, y))",
    );
    ok(&mut s, "INSERT INTO c VALUES (1, 1), (1, 2)");
    ok(&mut s, "INSERT INTO g VALUES (1, 2)");
    assert!(run(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2").is_err());
    assert_eq!(
        rows(&mut s, "SELECT x, y FROM c ORDER BY y"),
        ["1,1", "1,2"]
    );
    assert_eq!(rows(&mut s, "SELECT a, b FROM p ORDER BY a, b").len(), 3);
}

#[test]
fn parent_delete_set_null_clears_every_fk_column() {
    let _ws = common::TestWorkspace::new("fkmulti", "setnull");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON DELETE SET NULL");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1)",
    );
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
    assert_eq!(
        rows(&mut s, "SELECT id, x, y FROM c ORDER BY id"),
        ["1,1,1", "2,NULL,NULL", "3,2,1"]
    );
}

#[test]
fn updating_one_key_column_of_a_referenced_parent_row() {
    let _ws = common::TestWorkspace::new("fkmulti", "parentupdate");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");
    // Restrict: the referenced pair may not change, an unreferenced one may.
    assert!(run(&mut s, "UPDATE p SET b = 3 WHERE a = 1 AND b = 2").is_err());
    ok(&mut s, "UPDATE p SET b = 3 WHERE a = 1 AND b = 1");
    assert_eq!(rows(&mut s, "SELECT x, y FROM c"), ["1,2"]);
}

#[test]
fn parent_update_cascade_rewrites_the_changed_column_of_the_children() {
    let _ws = common::TestWorkspace::new("fkmulti", "updatecascade");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON UPDATE CASCADE");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 2), (2, 1, 1), (3, 2, 1)",
    );
    ok(&mut s, "UPDATE p SET b = 5 WHERE a = 1 AND b = 2");
    assert_eq!(
        rows(&mut s, "SELECT id, x, y FROM c ORDER BY id"),
        ["1,1,5", "2,1,1", "3,2,1"]
    );
    ok(&mut s, "INSERT INTO c VALUES (4, 1, 5)");
}

#[test]
fn parent_update_set_null() {
    let _ws = common::TestWorkspace::new("fkmulti", "updatenull");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON UPDATE SET NULL");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2), (2, 1, 1)");
    ok(&mut s, "UPDATE p SET a = 7 WHERE a = 1 AND b = 2");
    assert_eq!(
        rows(&mut s, "SELECT id, x, y FROM c ORDER BY id"),
        ["1,NULL,NULL", "2,1,1"]
    );
}

#[test]
fn enforcement_survives_a_restart_and_later_ddl() {
    let _ws = common::TestWorkspace::new("fkmulti", "restart");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON DELETE CASCADE");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2), (2, 2, 1)");
    ok(&mut s, "CREATE TABLE other (z INT)");
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO c VALUES (3, 2, 2)").is_err());
    ok(&mut s, "INSERT INTO c VALUES (3, 1, 1)");
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
    assert_eq!(rows(&mut s, "SELECT id FROM c ORDER BY id"), ["2", "3"]);
}

#[test]
fn rename_and_drop_keep_the_foreign_key_working() {
    let _ws = common::TestWorkspace::new("fkmulti", "ddl");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");

    ok(&mut s, "ALTER TABLE p RENAME TO p2");
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 2, 2)").is_err());
    assert!(run(&mut s, "DELETE FROM p2 WHERE a = 1 AND b = 2").is_err());
    ok(&mut s, "ALTER TABLE c RENAME TO c2");
    assert!(run(&mut s, "INSERT INTO c2 VALUES (2, 2, 2)").is_err());
    ok(&mut s, "INSERT INTO c2 VALUES (2, 2, 1)");
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO c2 VALUES (3, 2, 2)").is_err());

    // The referenced table cannot go while a child points at it.
    assert!(run(&mut s, "DROP TABLE p2").is_err());
    // Dropping the child frees the parent.
    ok(&mut s, "DROP TABLE c2");
    ok(&mut s, "DELETE FROM p2 WHERE a = 1 AND b = 2");
}

#[test]
fn dropping_a_child_column_removes_the_foreign_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "dropchildcol");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");
    ok(&mut s, "ALTER TABLE c DROP COLUMN y");
    ok(&mut s, "INSERT INTO c VALUES (2, 9)");
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
}

#[test]
fn a_referenced_column_cannot_be_dropped() {
    let _ws = common::TestWorkspace::new("fkmulti", "dropparentcol");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "");
    let err = run(&mut s, "ALTER TABLE p DROP COLUMN b").unwrap_err();
    assert!(err.contains("referenced by a foreign key"), "{err}");
    assert_eq!(rows(&mut s, "SELECT a, b FROM p").len(), 3);
}

// ── Renaming columns ──────────────────────────────────────────────────────

fn single_pair(s: &mut Session) {
    ok(s, "CREATE TABLE p (id INT PRIMARY KEY, label VARCHAR(10))");
    ok(s, "INSERT INTO p VALUES (1, 'one'), (2, 'two')");
    ok(
        s,
        "CREATE TABLE c (n INT, pid INT, FOREIGN KEY (pid) REFERENCES p (id) ON DELETE CASCADE)",
    );
    ok(s, "INSERT INTO c VALUES (10, 1), (20, 2)");
}

fn assert_single_pair_enforced(s: &mut Session, parent_col: &str) {
    assert!(run(s, "INSERT INTO c VALUES (30, 9)").is_err());
    ok(s, "INSERT INTO c VALUES (30, 1)");
    ok(s, &format!("DELETE FROM p WHERE {parent_col} = 1"));
    assert_eq!(rows(s, "SELECT n FROM c ORDER BY n"), ["20"]);
}

#[test]
fn renaming_a_referenced_column_updates_the_children() {
    let _ws = common::TestWorkspace::new("fkmulti", "renparent");
    let mut s = session();
    single_pair(&mut s);
    ok(&mut s, "ALTER TABLE p RENAME COLUMN id TO key");
    assert_single_pair_enforced(&mut s, "key");
}

#[test]
fn a_renamed_referenced_column_survives_a_restart() {
    let _ws = common::TestWorkspace::new("fkmulti", "renparentrestart");
    let mut s = session();
    single_pair(&mut s);
    ok(&mut s, "ALTER TABLE p RENAME COLUMN id TO key");
    ok(&mut s, "CREATE TABLE other (z INT)");
    let mut s = restart(s);
    assert_single_pair_enforced(&mut s, "key");
}

#[test]
fn renaming_a_referenced_column_of_a_two_column_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "renparent2");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON DELETE CASCADE");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2), (2, 2, 1)");
    ok(&mut s, "ALTER TABLE p RENAME COLUMN b TO bb");
    ok(&mut s, "ALTER TABLE p RENAME COLUMN a TO aa");
    let check = |s: &mut Session| {
        assert!(run(s, "INSERT INTO c VALUES (3, 2, 2)").is_err());
        ok(s, "INSERT INTO c VALUES (3, 1, 1)");
    };
    check(&mut s);
    let mut s = restart(s);
    ok(&mut s, "DELETE FROM c WHERE id = 3");
    check(&mut s);
    ok(&mut s, "DELETE FROM p WHERE aa = 1 AND bb = 2");
    assert_eq!(rows(&mut s, "SELECT id FROM c ORDER BY id"), ["2", "3"]);
}

#[test]
fn renaming_a_child_foreign_key_column() {
    let _ws = common::TestWorkspace::new("fkmulti", "renchild");
    let mut s = session();
    single_pair(&mut s);
    ok(&mut s, "ALTER TABLE c RENAME COLUMN pid TO parent_id");
    assert_single_pair_enforced(&mut s, "id");
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO c VALUES (40, 9)").is_err());
    ok(&mut s, "DELETE FROM p WHERE id = 2");
    assert_eq!(rows(&mut s, "SELECT n FROM c"), Vec::<String>::new());
}

#[test]
fn renaming_a_child_column_of_a_two_column_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "renchild2");
    let mut s = session();
    parent(&mut s);
    child(&mut s, "ON DELETE CASCADE");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 2)");
    ok(&mut s, "ALTER TABLE c RENAME COLUMN y TO yy");
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 2, 2)").is_err());
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 2, 2)").is_err());
    ok(&mut s, "DELETE FROM p WHERE a = 1 AND b = 2");
    assert_eq!(rows(&mut s, "SELECT id FROM c").len(), 0);
}

#[test]
fn renaming_a_column_of_a_self_referencing_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "renself");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE t (id INT PRIMARY KEY, boss INT, \
         FOREIGN KEY (boss) REFERENCES t (id) ON DELETE CASCADE)",
    );
    ok(&mut s, "INSERT INTO t VALUES (1, NULL), (2, 1)");
    ok(&mut s, "ALTER TABLE t RENAME COLUMN id TO eid");
    ok(&mut s, "ALTER TABLE t RENAME COLUMN boss TO chief");
    assert!(run(&mut s, "INSERT INTO t VALUES (3, 9)").is_err());
    let mut s = restart(s);
    assert!(run(&mut s, "INSERT INTO t VALUES (3, 9)").is_err());
    ok(&mut s, "DELETE FROM t WHERE eid = 1");
    assert_eq!(rows(&mut s, "SELECT eid FROM t").len(), 0);
}

#[test]
fn auto_named_indexes_take_the_new_column_name() {
    let _ws = common::TestWorkspace::new("fkmulti", "indexnames");
    let mut s = session();
    single_pair(&mut s);
    let names = |s: &mut Session| {
        let mut v = rows(s, "SELECT index_name FROM information_schema.statistics");
        v.iter_mut().for_each(|n| *n = n.replace('\'', ""));
        v.sort();
        v
    };
    assert_eq!(names(&mut s), ["fk_c_pid", "pk_p_id"]);
    ok(&mut s, "ALTER TABLE p RENAME COLUMN id TO key");
    ok(&mut s, "ALTER TABLE c RENAME COLUMN pid TO parent_id");
    assert_eq!(names(&mut s), ["fk_c_parent_id", "pk_p_key"]);
    // The renamed index files are the ones lookups use.
    assert_single_pair_enforced(&mut s, "key");
}

#[test]
fn a_parent_created_after_its_child_must_make_the_target_a_key() {
    let _ws = common::TestWorkspace::new("fkmulti", "forward");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p (a, b))",
    );
    let err = run(&mut s, "CREATE TABLE p (a INT, b INT)").unwrap_err();
    assert!(err.contains("PRIMARY KEY or UNIQUE"), "{err}");
    let err = run(&mut s, "CREATE TABLE p (a INT PRIMARY KEY, b INT)").unwrap_err();
    assert!(err.contains("PRIMARY KEY or UNIQUE"), "{err}");
    ok(&mut s, "CREATE TABLE p (a INT, b INT, PRIMARY KEY (b, a))");
    ok(&mut s, "INSERT INTO p VALUES (1, 2)");
    ok(&mut s, "INSERT INTO c VALUES (1, 2)");
    assert!(run(&mut s, "INSERT INTO c VALUES (2, 1)").is_err());
}
