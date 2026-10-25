//! INFORMATION_SCHEMA views: their columns, and their rows computed from the
//! catalog and the system tables.
//!
//! A view's rows are built when a query scans it ([`info_schema_rows`]), so
//! every column holds what its name says: `table_schema` is a database name,
//! not a database id; `ordinal_position` counts from 1; `is_nullable` is
//! `YES` or `NO`. The databases of the data directory are its schemas, and
//! the catalog is named [`CATALOG_NAME`]. Rows are sorted, so results are
//! repeatable.

use std::collections::HashMap;

use rook_ast::logical::{ColumnInfo, ColumnSchema};

use super::{
    SYS_CONSTRAINTS_SCHEMA, SYS_DATABASES_SCHEMA, SYS_INDEXES_SCHEMA, SYS_TABLES_SCHEMA,
    scan_system_table, split_column_list, strip_check_wrapper, value_to_string,
};
use crate::backend::error::{RookError, RookResult};
use crate::catalog::types::Catalog;
use crate::types::DataValue;

/// The catalog name INFORMATION_SCHEMA reports (`table_catalog`, ...).
pub const CATALOG_NAME: &str = "rookdb";

/// The columns of an INFORMATION_SCHEMA view, or `None` for a view that
/// does not exist.
pub fn info_schema_column_schema(view_name: &str) -> Option<ColumnSchema> {
    let cols: &[(&str, &str)] = match view_name.to_ascii_lowercase().as_str() {
        "schemata" => &[
            ("catalog_name", "VARCHAR(255)"),
            ("schema_name", "VARCHAR(255)"),
        ],
        "tables" => &[
            ("table_catalog", "VARCHAR(255)"),
            ("table_schema", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("table_type", "VARCHAR(50)"),
        ],
        "columns" => &[
            ("table_catalog", "VARCHAR(255)"),
            ("table_schema", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("column_name", "VARCHAR(255)"),
            ("ordinal_position", "INT"),
            ("data_type", "VARCHAR(100)"),
            ("is_nullable", "VARCHAR(3)"),
            ("column_default", "VARCHAR(255)"),
        ],
        "table_constraints" => &[
            ("constraint_catalog", "VARCHAR(255)"),
            ("constraint_schema", "VARCHAR(255)"),
            ("constraint_name", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("constraint_type", "VARCHAR(50)"),
            ("constraint_columns", "VARCHAR(1024)"),
            ("check_clause", "VARCHAR(1024)"),
        ],
        "key_column_usage" => &[
            ("table_catalog", "VARCHAR(255)"),
            ("table_schema", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("column_name", "VARCHAR(255)"),
            ("ordinal_position", "INT"),
            ("constraint_catalog", "VARCHAR(255)"),
            ("constraint_schema", "VARCHAR(255)"),
            ("constraint_name", "VARCHAR(255)"),
        ],
        // Not in SQL-99; as in MySQL, one row per index.
        "statistics" | "indexes" => &[
            ("table_catalog", "VARCHAR(255)"),
            ("table_schema", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("index_name", "VARCHAR(255)"),
            ("unique", "VARCHAR(3)"),
            ("primary", "VARCHAR(3)"),
            ("column_names", "VARCHAR(255)"),
        ],
        "views" => &[
            ("table_catalog", "VARCHAR(255)"),
            ("table_schema", "VARCHAR(255)"),
            ("table_name", "VARCHAR(255)"),
            ("view_definition", "VARCHAR(8192)"),
        ],
        _ => return None,
    };
    Some(ColumnSchema {
        columns: cols
            .iter()
            .map(|(name, dt)| ColumnInfo {
                name: name.to_string(),
                data_type: dt.to_string(),
                nullable: true,
            })
            .collect(),
    })
}

/// A constraint of a table, as INFORMATION_SCHEMA shows it.
struct Constraint {
    name: String,
    kind: &'static str,
    /// Key columns; empty for a CHECK.
    columns: Vec<String>,
    check: Option<String>,
}

/// One index of a table: name, unique, primary, columns.
type Index = (String, bool, bool, Vec<String>);

/// What the system tables say about each table, by `(database, table)`.
#[derive(Default)]
struct TableMetadata {
    constraints: HashMap<(String, String), Vec<Constraint>>,
    indexes: HashMap<(String, String), Vec<Index>>,
}

fn text(v: &Option<DataValue>) -> Option<String> {
    match v {
        Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => Some(s.clone()),
        _ => None,
    }
}

fn int(v: &Option<DataValue>) -> Option<i32> {
    match v {
        Some(DataValue::Int(i)) => Some(*i),
        _ => None,
    }
}

fn flag(v: &Option<DataValue>) -> bool {
    matches!(v, Some(DataValue::Bool(true)))
}

fn same_columns(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Read the constraints and indexes of every table from the system tables.
fn table_metadata(catalog: &Catalog) -> RookResult<TableMetadata> {
    let engine = catalog.engine();
    let databases: HashMap<i32, String> =
        scan_system_table(engine, "databases", SYS_DATABASES_SCHEMA)?
            .iter()
            .filter_map(|r| Some((int(r.first()?)?, text(r.get(1)?)?)))
            .collect();
    let tables: HashMap<i32, (String, String)> =
        scan_system_table(engine, "tables", SYS_TABLES_SCHEMA)?
            .iter()
            .filter_map(|r| {
                let db = databases.get(&int(r.get(1)?)?)?;
                Some((int(r.first()?)?, (db.clone(), text(r.get(2)?)?)))
            })
            .collect();

    let mut meta = TableMetadata::default();
    let mut index_rows = scan_system_table(engine, "indexes", SYS_INDEXES_SCHEMA)?;
    index_rows.sort_by_key(|r| int(&r[0]));
    for row in &index_rows {
        let (Some(key), Some(name), Some(columns)) = (
            int(&row[1]).and_then(|t| tables.get(&t)),
            text(&row[2]),
            text(&row[5]),
        ) else {
            continue;
        };
        meta.indexes.entry(key.clone()).or_default().push((
            name,
            flag(&row[3]),
            flag(&row[4]),
            split_column_list(&columns),
        ));
    }

    let mut constraint_rows = scan_system_table(engine, "constraints", SYS_CONSTRAINTS_SCHEMA)?;
    constraint_rows.sort_by_key(|r| int(&r[0]));
    for row in &constraint_rows {
        let (Some(key), Some(stored_type), Some(columns_field)) = (
            int(&row[1]).and_then(|t| tables.get(&t)),
            text(&row[2]),
            text(&row[3]),
        ) else {
            continue;
        };
        let table = &key.1;
        let stored_type = stored_type.to_ascii_uppercase();
        let indexes = meta.indexes.get(key).map(Vec::as_slice).unwrap_or(&[]);
        let list = meta.constraints.entry(key.clone()).or_default();
        let constraint = if stored_type == "CHECK" {
            let n = list.iter().filter(|c| c.kind == "CHECK").count() + 1;
            Constraint {
                name: format!("ck_{}_{}", table, n),
                kind: "CHECK",
                columns: Vec::new(),
                check: strip_check_wrapper(&columns_field),
            }
        } else if stored_type == "PRIMARY KEY" || stored_type == "UNIQUE" {
            // A column PRIMARY KEY is stored as UNIQUE + NOT NULL; its index
            // says it is the primary key.
            let columns = split_column_list(&columns_field);
            let index = indexes
                .iter()
                .find(|(_, unique, _, cols)| *unique && same_columns(cols, &columns));
            let primary = stored_type == "PRIMARY KEY" || index.is_some_and(|ix| ix.2);
            let prefix = if primary { "pk" } else { "uq" };
            Constraint {
                name: index
                    .map(|ix| ix.0.clone())
                    .unwrap_or_else(|| format!("{}_{}_{}", prefix, table, columns.join("_"))),
                kind: if primary { "PRIMARY KEY" } else { "UNIQUE" },
                columns,
                check: None,
            }
        } else if stored_type.starts_with("FOREIGN KEY") {
            let columns = split_column_list(&columns_field);
            Constraint {
                name: format!("fk_{}_{}", table, columns.join("_")),
                kind: "FOREIGN KEY",
                columns,
                check: None,
            }
        } else {
            // NOT NULL is a column property: `columns.is_nullable`.
            continue;
        };
        if !list
            .iter()
            .any(|c| c.kind == constraint.kind && same_columns(&c.columns, &constraint.columns))
            || constraint.kind == "CHECK"
        {
            list.push(constraint);
        }
    }
    Ok(meta)
}

/// The rows of an INFORMATION_SCHEMA view, in the column order of
/// [`info_schema_column_schema`].
pub fn info_schema_rows(
    catalog: &Catalog,
    view_name: &str,
) -> RookResult<Vec<Vec<Option<DataValue>>>> {
    let view = view_name.to_ascii_lowercase();
    if info_schema_column_schema(&view).is_none() {
        return Err(RookError::NotFound {
            entity: "INFORMATION_SCHEMA view",
            name: view_name.to_string(),
        });
    }
    let s = |v: &str| Some(DataValue::Varchar(v.to_string()));
    let yes_no = |b: bool| s(if b { "YES" } else { "NO" });

    let mut databases: Vec<(&String, &crate::catalog::types::Database)> =
        catalog.databases.iter().collect();
    databases.sort_by(|a, b| a.0.cmp(b.0));
    let tables = || {
        databases.iter().flat_map(|(db, d)| {
            let mut tables: Vec<_> = d.tables.iter().collect();
            tables.sort_by(|a, b| a.0.cmp(b.0));
            tables.into_iter().map(move |(t, table)| (*db, t, table))
        })
    };

    let mut rows = Vec::new();
    match view.as_str() {
        "schemata" => {
            for (db, _) in &databases {
                rows.push(vec![s(CATALOG_NAME), s(db)]);
            }
        }
        "tables" => {
            for (db, d) in &databases {
                let mut entries: Vec<(&String, &str)> = d
                    .tables
                    .keys()
                    .map(|t| (t, "BASE TABLE"))
                    .chain(d.views.keys().map(|v| (v, "VIEW")))
                    .collect();
                entries.sort();
                for (name, kind) in entries {
                    rows.push(vec![s(CATALOG_NAME), s(db), s(name), s(kind)]);
                }
            }
        }
        "columns" => {
            for (db, t, table) in tables() {
                for (i, col) in table.columns.iter().enumerate() {
                    rows.push(vec![
                        s(CATALOG_NAME),
                        s(db),
                        s(t),
                        s(&col.name),
                        Some(DataValue::Int(i as i32 + 1)),
                        s(&col.data_type.to_string()),
                        yes_no(col.nullable),
                        col.constraints
                            .default
                            .as_ref()
                            .map(|v| DataValue::Varchar(value_to_string(v))),
                    ]);
                }
            }
        }
        "views" => {
            for (db, d) in &databases {
                let mut views: Vec<_> = d.views.iter().collect();
                views.sort_by(|a, b| a.0.cmp(b.0));
                for (name, def) in views {
                    rows.push(vec![s(CATALOG_NAME), s(db), s(name), s(&def.query_json)]);
                }
            }
        }
        _ => {
            let meta = table_metadata(catalog)?;
            for (db, t, _) in tables() {
                let key = (db.clone(), t.clone());
                match view.as_str() {
                    "table_constraints" => {
                        for c in meta.constraints.get(&key).into_iter().flatten() {
                            rows.push(vec![
                                s(CATALOG_NAME),
                                s(db),
                                s(&c.name),
                                s(t),
                                s(c.kind),
                                (!c.columns.is_empty())
                                    .then(|| DataValue::Varchar(c.columns.join(", "))),
                                c.check.as_deref().and_then(s),
                            ]);
                        }
                    }
                    "key_column_usage" => {
                        for c in meta.constraints.get(&key).into_iter().flatten() {
                            for (i, col) in c.columns.iter().enumerate() {
                                rows.push(vec![
                                    s(CATALOG_NAME),
                                    s(db),
                                    s(t),
                                    s(col),
                                    Some(DataValue::Int(i as i32 + 1)),
                                    s(CATALOG_NAME),
                                    s(db),
                                    s(&c.name),
                                ]);
                            }
                        }
                    }
                    _ => {
                        for (name, unique, primary, cols) in
                            meta.indexes.get(&key).into_iter().flatten()
                        {
                            rows.push(vec![
                                s(CATALOG_NAME),
                                s(db),
                                s(t),
                                s(name),
                                yes_no(*unique),
                                yes_no(*primary),
                                s(&cols.join(", ")),
                            ]);
                        }
                    }
                }
            }
        }
    }
    Ok(rows)
}
