//! Unified projection engine: single entry-point `project()`.
//!
//! Pipeline: load_rows → filter_rows → eval_projection_list → apply_distinct → ResultTable

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io;

use crate::catalog::types::{Catalog, Column, DataType};
use crate::disk::read_page;
use crate::executor::expr::{eval_expr, Expr, Row};
use crate::executor::tuple_codec::decode_tuple;
use crate::executor::value::Value;
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

// ─── Public types ───────────────────────────────────────────────────────────

/// Metadata for a single column of the result.
#[derive(Debug, Clone)]
pub struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
}

/// The result of a projection: schema + rows.
#[derive(Debug, Clone)]
pub struct ResultTable {
    pub columns: Vec<OutputColumn>,
    pub rows: Vec<Row>,
}

impl ResultTable {
    pub fn empty(columns: Vec<OutputColumn>) -> Self {
        Self { columns, rows: vec![] }
    }

    /// Pretty-print the table to stdout.
    pub fn print(&self) {
        let headers: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        println!("{}", headers.join(" | "));
        println!("{}", "-".repeat(headers.join(" | ").len()));
        for row in &self.rows {
            let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            println!("{}", cells.join(" | "));
        }
        println!("({} row{})", self.rows.len(), if self.rows.len() == 1 { "" } else { "s" });
    }
}

/// One item in the SELECT list.
#[derive(Debug, Clone)]
pub enum ProjectionItem {
    /// SELECT *
    Star,
    /// SELECT <expr> [AS alias]
    Expr(Expr, String),
}

/// Everything the caller supplies to `project()`.
pub struct ProjectionInput<'a> {
    pub catalog: &'a Catalog,
    pub db_name: &'a str,
    pub table_name: &'a str,
    /// SELECT list
    pub items: Vec<ProjectionItem>,
    /// WHERE clause – None means "no filter"
    pub predicate: Option<Expr>,
    /// DISTINCT flag
    pub distinct: bool,
    /// Pre-computed CTE tables (WITH clause).
    /// If `table_name` matches a key here, those rows are used instead of disk.
    pub cte_tables: HashMap<String, ResultTable>,
}

// ─── Main entry point ───────────────────────────────────────────────────────

/// Execute a projection (SELECT) over a table.
///
/// This is the single function the SQL parser / storage manager calls.
/// It composes all internal helpers and returns a `ResultTable`.
pub fn project(input: ProjectionInput) -> io::Result<ResultTable> {
    // Step 1: resolve schema
    let (schema, is_cte) = resolve_schema(input.catalog, input.db_name, input.table_name, &input.cte_tables)?;

    // Step 2: load all rows
    let rows = if is_cte {
        input.cte_tables[input.table_name].rows.clone()
    } else {
        load_rows(input.catalog, input.db_name, input.table_name)?
    };

    // Step 3: filter (WHERE)
    let rows = filter_rows(rows, &input.predicate)?;

    // Step 4: evaluate SELECT list
    let (out_cols, rows) = eval_projection_list(rows, &input.items, &schema)?;

    // Step 5: DISTINCT
    let rows = if input.distinct {
        apply_distinct(rows)
    } else {
        rows
    };

    Ok(ResultTable { columns: out_cols, rows })
}

// ─── Step 1: resolve schema ─────────────────────────────────────────────────

fn resolve_schema<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
    cte_tables: &HashMap<String, ResultTable>,
) -> io::Result<(Vec<Column>, bool)> {
    if cte_tables.contains_key(table_name) {
        let cte = &cte_tables[table_name];
        let cols: Vec<Column> = cte.columns.iter().map(|c| Column {
            name: c.name.clone(),
            data_type: c.data_type.as_legacy_str(),
        }).collect();
        return Ok((cols, true));
    }

    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    Ok((table.columns.clone(), false))
}

// ─── Step 2: load rows from disk ───────────────────────────────────────────

pub fn load_rows(catalog: &Catalog, db_name: &str, table_name: &str) -> io::Result<Vec<Row>> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = &table.columns;

    let path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    let mut file = OpenOptions::new().read(true).open(&path).map_err(|e| {
        io::Error::new(e.kind(), format!("Cannot open table file '{}': {}", path, e))
    })?;

    let total_pages = page_count(&mut file)?;

    // Empty table: only header page exists
    if total_pages <= 1 {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_bytes = &page.data[offset..offset + length];
            let row = decode_tuple(tuple_bytes, schema);
            rows.push(row);
        }
    }

    Ok(rows)
}

// ─── Step 3: filter rows ────────────────────────────────────────────────────

pub fn filter_rows(rows: Vec<Row>, predicate: &Option<Expr>) -> io::Result<Vec<Row>> {
    let pred = match predicate {
        None => return Ok(rows),
        Some(p) => p,
    };

    let mut out = Vec::new();
    for row in rows {
        match eval_expr(pred, &row) {
            Ok(Value::Bool(true)) => out.push(row),
            Ok(_) => {} // false, null, or non-bool -> filtered out
            Err(e) => return Err(io::Error::from(e)),
        }
    }
    Ok(out)
}

// ─── Step 4: evaluate SELECT list ───────────────────────────────────────────

pub fn eval_projection_list(
    rows: Vec<Row>,
    items: &[ProjectionItem],
    schema: &[Column],
) -> io::Result<(Vec<OutputColumn>, Vec<Row>)> {
    // Build output column metadata
    let out_cols: Vec<OutputColumn> = items
        .iter()
        .flat_map(|item| match item {
            ProjectionItem::Star => schema
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.parsed_type(),
                })
                .collect::<Vec<_>>(),
            ProjectionItem::Expr(_, alias) => vec![OutputColumn {
                name: alias.clone(),
                data_type: DataType::Text, // inferred at runtime; Text is a safe default
            }],
        })
        .collect();

    if rows.is_empty() {
        return Ok((out_cols, vec![]));
    }

    let num_schema_cols = schema.len();
    let mut out_rows: Vec<Row> = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut out_row = Vec::new();
        for item in items {
            match item {
                ProjectionItem::Star => {
                    // Expand to all columns
                    for i in 0..num_schema_cols {
                        out_row.push(row.get(i).cloned().unwrap_or(Value::Null));
                    }
                }
                ProjectionItem::Expr(expr, _) => {
                    let v = eval_expr(expr, row).map_err(io::Error::from)?;
                    out_row.push(v);
                }
            }
        }
        out_rows.push(out_row);
    }

    Ok((out_cols, out_rows))
}

// ─── Step 5: DISTINCT ───────────────────────────────────────────────────────

pub fn apply_distinct(rows: Vec<Row>) -> Vec<Row> {
    let mut seen: HashSet<Vec<Value>> = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

// ─── Selection convenience wrapper ──────────────────────────────────────────

/// SELECT * FROM table WHERE predicate.
/// Convenience function that delegates to `project()`.
pub fn select(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    predicate: Option<Expr>,
) -> io::Result<ResultTable> {
    project(ProjectionInput {
        catalog,
        db_name,
        table_name,
        items: vec![ProjectionItem::Star],
        predicate,
        distinct: false,
        cte_tables: HashMap::new(),
    })
}
