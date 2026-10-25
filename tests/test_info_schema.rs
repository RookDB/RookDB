//! INFORMATION_SCHEMA integration tests.
//!
//! Metadata views are ordinary SQL: they read their backing system tables
//! (sys_databases, sys_tables, ...) and expose the SQL-99 column names.
//!
//! Run with: cargo test --test test_info_schema -- --test-threads=1

mod common;

use common::load_catalog;
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::types::DataType;

/// One database with one two-column table.
fn setup() -> storage_manager::catalog::Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, "meta_db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "meta_db",
        "widgets",
        vec![
            storage_manager::catalog::Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                nullable: false,
                constraints: Default::default(),
            },
            storage_manager::catalog::Column {
                name: "label".to_string(),
                data_type: DataType::Varchar(40),
                nullable: true,
                constraints: Default::default(),
            },
        ],
    )
    .unwrap();
    save_catalog(&catalog).unwrap();
    load_catalog()
}

fn query(catalog: &storage_manager::catalog::Catalog, sql: &str) -> Vec<Vec<String>> {
    let plan = parse_sql(sql).unwrap();
    let logical =
        storage_manager::planner::plan_query(&plan, catalog, "meta_db").expect("planning failed");
    execute_plan_collect(&logical, catalog, "meta_db")
        .expect("execution failed")
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(dv) => format!("{}", dv),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn schemata_lists_databases() {
    let _ws = common::TestWorkspace::new("info", "schemata");
    let catalog = setup();

    let out = query(
        &catalog,
        "SELECT schema_name FROM information_schema.schemata",
    );
    assert!(out.iter().any(|r| r[0] == "'meta_db'"), "meta_db listed");
}

#[test]
fn tables_view_reports_user_tables() {
    let _ws = common::TestWorkspace::new("info", "tables");
    let catalog = setup();

    let out = query(&catalog, "SELECT table_name FROM information_schema.tables");
    assert!(out.iter().any(|r| r[0] == "'widgets'"), "widgets listed");
}

#[test]
fn columns_view_exposes_sql99_names() {
    let _ws = common::TestWorkspace::new("info", "columns");
    let catalog = setup();

    // The COLUMNS view exposes SQL-99 names; its positional mapping makes
    // `column_name` report the actual column names of the table.
    let out = query(
        &catalog,
        "SELECT column_name FROM information_schema.columns",
    );
    let names: Vec<&String> = out.iter().map(|r| &r[0]).collect();
    assert!(
        names.contains(&&"'id'".to_string()),
        "id present: {:?}",
        names
    );
    assert!(
        names.contains(&&"'label'".to_string()),
        "label present: {:?}",
        names
    );
}

#[test]
fn where_filtering_works_on_views() {
    let _ws = common::TestWorkspace::new("info", "filter");
    let catalog = setup();

    let out = query(
        &catalog,
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'widgets'",
    );
    assert_eq!(out.len(), 1);
}

// ── Each column holds what its name says ─────────────────────────────────────

mod through_a_session {
    use super::common;
    use storage_manager::backend::session::Session;
    use storage_manager::types::DataValue;

    fn session() -> Session {
        common::init_catalog();
        let mut s = Session::new(common::load_catalog());
        for sql in [
            "CREATE DATABASE shop",
            "USE shop",
            "CREATE TABLE p (id INT PRIMARY KEY, code VARCHAR(5) UNIQUE NOT NULL, n INT DEFAULT 5 CHECK (n > 0))",
            "CREATE TABLE c (a INT, b INT, pid INT REFERENCES p(id), PRIMARY KEY (a, b), CHECK (a < b))",
            "CREATE INDEX ix ON c (pid, b)",
            "CREATE VIEW v AS SELECT id FROM p",
        ] {
            s.execute(rook_parser::parse_sql(sql).unwrap())
                .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        }
        s
    }

    /// Each row as text, NULL as `NULL`.
    fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
        let result = s
            .execute(rook_parser::parse_sql(sql).unwrap())
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
        let (_, rows) = result.rows.expect("a query returns rows");
        rows.into_iter()
            .map(|t| {
                t.values
                    .into_iter()
                    .map(|v| match v {
                        Some(DataValue::Varchar(s)) => s,
                        Some(other) => other.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn tables_name_their_schema_and_kind() {
        let _ws = common::TestWorkspace::new("info", "tables_kind");
        let mut s = session();
        assert_eq!(
            rows(&mut s, "SELECT * FROM information_schema.tables"),
            [
                ["rookdb", "shop", "c", "BASE TABLE"],
                ["rookdb", "shop", "p", "BASE TABLE"],
                ["rookdb", "shop", "v", "VIEW"],
            ]
        );
        assert_eq!(
            rows(&mut s, "SELECT * FROM information_schema.schemata"),
            [["rookdb", "shop"]]
        );
    }

    #[test]
    fn columns_are_filtered_by_their_table() {
        let _ws = common::TestWorkspace::new("info", "columns_rows");
        let mut s = session();
        assert_eq!(
            rows(
                &mut s,
                "SELECT table_name, column_name, ordinal_position, data_type, is_nullable, column_default \
                 FROM information_schema.columns WHERE table_name = 'p'"
            ),
            [
                ["p", "id", "1", "INT", "NO", "NULL"],
                ["p", "code", "2", "VARCHAR(5)", "NO", "NULL"],
                ["p", "n", "3", "INT", "YES", "5"],
            ]
        );
    }

    #[test]
    fn constraints_have_names_types_and_columns() {
        let _ws = common::TestWorkspace::new("info", "constraints_rows");
        let mut s = session();
        assert_eq!(
            rows(
                &mut s,
                "SELECT constraint_name, table_name, constraint_type, constraint_columns, check_clause \
                 FROM information_schema.table_constraints"
            ),
            [
                ["pk_c_a_b", "c", "PRIMARY KEY", "a, b", "NULL"],
                ["ck_c_1", "c", "CHECK", "NULL", "a < b"],
                ["fk_c_pid", "c", "FOREIGN KEY", "pid", "NULL"],
                ["pk_p_id", "p", "PRIMARY KEY", "id", "NULL"],
                ["uq_p_code", "p", "UNIQUE", "code", "NULL"],
                ["ck_p_1", "p", "CHECK", "NULL", "n > 0"],
            ]
        );
        assert_eq!(
            rows(
                &mut s,
                "SELECT constraint_name, column_name, ordinal_position \
                 FROM information_schema.key_column_usage WHERE table_name = 'c'"
            ),
            [
                ["pk_c_a_b", "a", "1"],
                ["pk_c_a_b", "b", "2"],
                ["fk_c_pid", "pid", "1"],
            ]
        );
    }

    #[test]
    fn indexes_and_views_read_as_words() {
        let _ws = common::TestWorkspace::new("info", "indexes_rows");
        let mut s = session();
        assert_eq!(
            rows(
                &mut s,
                "SELECT index_name, \"unique\", \"primary\", column_names \
                 FROM information_schema.indexes WHERE table_name = 'c'"
            ),
            [
                ["pk_c_a_b", "YES", "YES", "a, b"],
                ["fk_c_pid", "NO", "NO", "pid"],
                ["ix", "NO", "NO", "pid, b"],
            ]
        );
        assert_eq!(
            rows(
                &mut s,
                "SELECT table_schema, table_name FROM information_schema.views"
            ),
            [["shop", "v"]]
        );
    }

    #[test]
    fn an_unknown_view_is_an_error() {
        let _ws = common::TestWorkspace::new("info", "unknown_view");
        let mut s = session();
        let err = s
            .execute(rook_parser::parse_sql("SELECT * FROM information_schema.nope").unwrap())
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("INFORMATION_SCHEMA view 'nope' not found"),
            "{}",
            err
        );
    }
}
