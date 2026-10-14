//! Constraint checks find rows through indexes (P3): every PRIMARY KEY,
//! UNIQUE and foreign-key column has one, a table that lacks one gets it on
//! first use, and the FK actions reach child rows through the child index.

mod common;

use storage_manager::backend::executor::create_index::load_table_indexes_multi;
use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    common::init_catalog();
    let mut s = Session::new(common::load_catalog());
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    s
}

fn run(s: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    s.execute(plan).map_err(|e| e.to_string())
}

fn ok(s: &mut Session, sql: &str) {
    run(s, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter().map(|t| t.values).collect()
}

fn count(s: &mut Session, sql: &str) -> usize {
    rows(s, sql).len()
}

/// The table's indexes as `name(col,col)`, sorted.
fn indexes(table: &str) -> Vec<String> {
    let mut v: Vec<String> = load_table_indexes_multi(&common::engine(), "app", table)
        .unwrap()
        .into_iter()
        .map(|(name, cols, unique)| {
            format!(
                "{}({}){}",
                name,
                cols.join(","),
                if unique { " unique" } else { "" }
            )
        })
        .collect();
    v.sort();
    v
}

fn index_file_exists(table: &str, name: &str) -> bool {
    common::engine().paths().index("app", table, name).exists()
}

/// Make the table look like one from before its constraint indexes existed:
/// the index files and their `sys_indexes` rows are gone.
fn forget_indexes(table: &str) {
    let engine = common::engine();
    for (name, _, _) in load_table_indexes_multi(&engine, "app", table).unwrap() {
        storage_manager::backend::system_table::delete_index_metadata(&engine, "app", table, &name)
            .unwrap();
        let _ = std::fs::remove_file(engine.paths().index("app", table, &name));
    }
    engine.invalidate_metadata();
}

fn parent_child(s: &mut Session, action: &str) {
    ok(s, "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(10))");
    ok(
        s,
        &format!(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id) {action}, tag INT UNIQUE)"
        ),
    );
}

#[test]
fn every_constraint_has_an_index() {
    let _ws = common::TestWorkspace::new("cidx", "every");
    let mut s = session();
    ok(
        &mut s,
        "CREATE TABLE p (id INT PRIMARY KEY, code VARCHAR(5) UNIQUE, a INT UNIQUE, b INT, UNIQUE (a, b))",
    );
    ok(
        &mut s,
        "CREATE TABLE c (id INT, pid INT REFERENCES p(id), x INT, y INT, PRIMARY KEY (id), \
         FOREIGN KEY (x) REFERENCES p(a))",
    );
    assert_eq!(
        indexes("p"),
        [
            "pk_p_id(id) unique",
            "uq_p_a(a) unique",
            "uq_p_a_b(a,b) unique",
            "uq_p_code(code) unique"
        ]
    );
    assert_eq!(
        indexes("c"),
        ["fk_c_pid(pid)", "fk_c_x(x)", "pk_c_id(id) unique"]
    );
}

#[test]
fn a_composite_unique_index_is_not_a_parent_index_for_one_column() {
    let _ws = common::TestWorkspace::new("cidx", "composite");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (a INT, b INT, UNIQUE (a, b))");
    // One column of the pair is no key, so it cannot be referenced alone...
    let err = run(&mut s, "CREATE TABLE c (x INT REFERENCES p(a))").unwrap_err();
    assert!(err.contains("PRIMARY KEY or UNIQUE"), "{err}");
    // ...but the pair can, through that same index.
    ok(
        &mut s,
        "CREATE TABLE c (x INT, y INT, FOREIGN KEY (x, y) REFERENCES p(a, b))",
    );
    assert_eq!(indexes("p"), ["uq_p_a_b(a,b) unique"]);
    ok(&mut s, "INSERT INTO p VALUES (1, 10), (2, 20)");
    ok(&mut s, "INSERT INTO c VALUES (1, 10)");
    assert!(run(&mut s, "INSERT INTO c VALUES (1, 20)").is_err());
    assert!(run(&mut s, "DELETE FROM p WHERE a = 1").is_err());
    ok(&mut s, "DELETE FROM p WHERE a = 2");
}

#[test]
fn child_index_follows_renames_and_schema_changes() {
    let _ws = common::TestWorkspace::new("cidx", "ddl");
    let mut s = session();
    parent_child(&mut s, "ON DELETE CASCADE");
    ok(&mut s, "INSERT INTO p VALUES (1, 'a'), (2, 'b')");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 1), (2, 1, 2), (3, 2, 3)",
    );

    ok(&mut s, "ALTER TABLE c ADD COLUMN note INT");
    ok(&mut s, "TRUNCATE TABLE c");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 1, NULL), (2, 1, 2, NULL), (3, 2, 3, NULL)",
    );
    ok(&mut s, "VACUUM c");
    ok(&mut s, "ALTER TABLE c RENAME COLUMN pid TO parent_id");
    assert!(indexes("c").contains(&"fk_c_parent_id(parent_id)".to_string()));
    ok(&mut s, "ALTER TABLE c RENAME TO kids");
    assert!(index_file_exists("kids", "fk_c_parent_id"));

    ok(&mut s, "DELETE FROM p WHERE id = 1");
    assert_eq!(count(&mut s, "SELECT * FROM kids"), 1);
    assert!(run(&mut s, "INSERT INTO kids VALUES (9, 7, 9, NULL)").is_err());

    ok(&mut s, "ALTER TABLE kids DROP COLUMN parent_id");
    assert!(!indexes("kids").iter().any(|i| i.starts_with("fk_")));
}

#[test]
fn the_child_index_goes_with_the_foreign_key() {
    let _ws = common::TestWorkspace::new("cidx", "dropfk");
    let mut s = session();
    parent_child(&mut s, "");
    assert!(index_file_exists("c", "fk_c_pid"));
    ok(&mut s, "DROP TABLE p CASCADE");
    assert!(!index_file_exists("c", "fk_c_pid"));
    assert_eq!(indexes("c"), ["pk_c_id(id) unique", "uq_c_tag(tag) unique"]);
    ok(&mut s, "INSERT INTO c VALUES (1, 99, 1)");
}

#[test]
fn a_column_declared_unique_twice_has_one_index() {
    let _ws = common::TestWorkspace::new("cidx", "twice");
    let mut s = session();
    ok(&mut s, "CREATE TABLE t (a INT UNIQUE, b INT, UNIQUE (a))");
    assert_eq!(indexes("t"), ["uq_t_a(a) unique"]);
}

#[test]
fn a_table_without_indexes_gets_them_on_first_use() {
    let _ws = common::TestWorkspace::new("cidx", "lazy");
    let mut s = session();
    parent_child(&mut s, "ON DELETE CASCADE");
    ok(&mut s, "INSERT INTO p VALUES (1, 'a'), (2, 'b')");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30)",
    );
    forget_indexes("p");
    forget_indexes("c");
    assert!(indexes("c").is_empty());

    // UNIQUE, FK existence and the cascade each work, and leave their index.
    let err = run(&mut s, "INSERT INTO c VALUES (4, 2, 10)").unwrap_err();
    assert!(err.contains("UNIQUE"), "{err}");
    assert!(index_file_exists("c", "uq_c_tag"));
    let err = run(&mut s, "INSERT INTO c VALUES (4, 7, 40)").unwrap_err();
    assert!(err.contains("FOREIGN KEY"), "{err}");
    assert!(index_file_exists("p", "fkp_p_id"));
    ok(&mut s, "DELETE FROM p WHERE id = 1");
    assert!(index_file_exists("c", "fk_c_pid"));
    assert_eq!(count(&mut s, "SELECT * FROM c"), 1);
}

#[test]
fn a_lost_index_file_is_rebuilt_not_scanned_around() {
    let _ws = common::TestWorkspace::new("cidx", "lostfile");
    let mut s = session();
    parent_child(&mut s, "");
    ok(&mut s, "INSERT INTO p VALUES (1, 'a')");
    ok(&mut s, "INSERT INTO c VALUES (1, 1, 10)");
    let engine = common::engine();
    std::fs::remove_file(engine.paths().index("app", "c", "uq_c_tag")).unwrap();
    std::fs::remove_file(engine.paths().index("app", "c", "fk_c_pid")).unwrap();
    engine.invalidate_metadata();

    assert!(run(&mut s, "INSERT INTO c VALUES (2, 1, 10)").is_err());
    assert!(index_file_exists("c", "uq_c_tag"));
    assert!(run(&mut s, "DELETE FROM p WHERE id = 1").is_err());
    assert!(index_file_exists("c", "fk_c_pid"));
    // One row each in sys_indexes, however often a file was rebuilt.
    assert_eq!(indexes("c").len(), 3, "{:?}", indexes("c"));
}

#[test]
fn actions_reach_children_on_many_pages() {
    let _ws = common::TestWorkspace::new("cidx", "pages");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (id INT PRIMARY KEY)");
    ok(
        &mut s,
        "CREATE TABLE cas (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE nul (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE SET NULL ON UPDATE SET NULL)",
    );
    ok(&mut s, "INSERT INTO p VALUES (1), (2), (3)");
    for table in ["cas", "nul"] {
        // Three parents' rows interleaved, so a parent's children sit on
        // every page.
        let values: Vec<String> = (0..2000)
            .map(|i| format!("({}, {})", i, i % 3 + 1))
            .collect();
        ok(
            &mut s,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
        );
    }

    ok(&mut s, "UPDATE p SET id = 30 WHERE id = 3");
    assert_eq!(count(&mut s, "SELECT * FROM nul WHERE pid IS NULL"), 666);
    assert_eq!(count(&mut s, "SELECT * FROM cas WHERE pid = 30"), 666);
    assert_eq!(count(&mut s, "SELECT * FROM cas WHERE pid = 3"), 0);
    // The child index moved with the rows.
    assert_eq!(count(&mut s, "SELECT * FROM cas WHERE pid = 1"), 667);

    ok(&mut s, "DELETE FROM p WHERE id = 1");
    assert_eq!(count(&mut s, "SELECT * FROM cas"), 1333);
    assert_eq!(count(&mut s, "SELECT * FROM nul WHERE pid IS NULL"), 1333);
    assert_eq!(count(&mut s, "SELECT * FROM nul"), 2000);
}

#[test]
fn restrict_is_checked_before_any_action() {
    let _ws = common::TestWorkspace::new("cidx", "restrict");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (id INT PRIMARY KEY)");
    ok(
        &mut s,
        "CREATE TABLE mid (id INT PRIMARY KEY, pid INT REFERENCES p(id) ON DELETE CASCADE)",
    );
    ok(
        &mut s,
        "CREATE TABLE leaf (id INT PRIMARY KEY, mid_id INT REFERENCES mid(id))",
    );
    ok(&mut s, "INSERT INTO p VALUES (1), (2)");
    ok(&mut s, "INSERT INTO mid VALUES (10, 1), (20, 2)");
    ok(&mut s, "INSERT INTO leaf VALUES (100, 20)");

    // Parent 1 would cascade fine, parent 2's grandchild blocks: nothing goes.
    assert!(run(&mut s, "DELETE FROM p WHERE id = 1 OR id = 2").is_err());
    assert_eq!(count(&mut s, "SELECT * FROM p"), 2);
    assert_eq!(count(&mut s, "SELECT * FROM mid"), 2);
}

#[test]
fn text_keys_spelled_null_are_ordinary_keys() {
    let _ws = common::TestWorkspace::new("cidx", "textnull");
    let mut s = session();
    ok(&mut s, "CREATE TABLE p (code VARCHAR(8) PRIMARY KEY)");
    ok(
        &mut s,
        "CREATE TABLE c (id INT PRIMARY KEY, code VARCHAR(8) REFERENCES p(code) ON DELETE CASCADE)",
    );
    ok(&mut s, "INSERT INTO p VALUES ('NULL'), ('x')");
    ok(
        &mut s,
        "INSERT INTO c VALUES (1, 'NULL'), (2, NULL), (3, 'x')",
    );
    ok(&mut s, "DELETE FROM p WHERE code = 'NULL'");
    let left: Vec<i32> = rows(&mut s, "SELECT id FROM c ORDER BY id")
        .into_iter()
        .map(|r| match r[0] {
            Some(DataValue::Int(n)) => n,
            _ => panic!("id"),
        })
        .collect();
    assert_eq!(left, [2, 3]);
}
