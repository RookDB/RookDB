//! Streaming (iterator-based) scan — constant RAM regardless of table size.
//!
//! Every public function returns a StreamResult that includes:
//!   - matched rows
//!   - total_scanned  (every tuple visited)
//!   - total_matched  (tuples that passed the predicate)
//!   - elapsed_ms     (wall-clock time for the whole operation)
//!   - pages_read     (I/O page count)

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io;
use std::time::Instant;

use crate::catalog::types::{Catalog, Column, DataType};
use crate::disk::read_page;
use crate::executor::expr::{eval_expr, Expr, Row};
use crate::executor::projection::{
    eval_projection_list, OutputColumn, ProjectionItem,
};
use crate::executor::tuple_codec::decode_tuple;
use crate::executor::value::Value;
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

// ─── Result type ─────────────────────────────────────────────────────────────

/// Result of a streaming operation, with full timing info.
#[derive(Debug)]
pub struct StreamResult {
    pub columns: Vec<OutputColumn>,
    pub rows: Vec<Row>,
    /// Total tuples visited (including non-matching ones).
    pub total_scanned: u64,
    /// Total tuples that passed the predicate.
    pub total_matched: u64,
    /// Pages read from disk.
    pub pages_read: u64,
    /// Wall-clock milliseconds for the whole operation.
    pub elapsed_ms: u128,
}

impl StreamResult {
    /// Pretty-print result with timing line.
    pub fn print(&self) {
        let headers: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        let sep = "-".repeat(headers.join(" | ").len().max(40));
        println!("{}", headers.join(" | "));
        println!("{}", sep);
        for row in &self.rows {
            let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            println!("{}", cells.join(" | "));
        }
        println!("{}", sep);
        println!(
            "  {} matched / {} scanned | {} pages | {} ms",
            self.total_matched, self.total_scanned, self.pages_read, self.elapsed_ms
        );
    }

    /// Print only the timing summary (no rows).
    pub fn print_timing(&self) {
        println!(
            "  matched={} scanned={} pages={} time={}ms  ({} rows/s)",
            self.total_matched,
            self.total_scanned,
            self.pages_read,
            self.elapsed_ms,
            if self.elapsed_ms > 0 {
                self.total_scanned * 1000 / self.elapsed_ms as u64
            } else {
                self.total_scanned * 1_000_000
            }
        );
    }
}

// ─── stream_select ───────────────────────────────────────────────────────────

/// Streaming SELECT * with optional WHERE predicate and LIMIT.
///
/// RAM: O(matched rows) — never loads the whole table.
/// Supports ALL predicate types: =, !=, <, <=, >, >=, LIKE, BETWEEN,
/// IN, IS NULL, IS NOT NULL, AND, OR, NOT, arithmetic, cast, date ops.
pub fn stream_select(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    predicate: Option<&Expr>,
    limit: Option<usize>,
) -> io::Result<StreamResult> {
    let t0 = Instant::now();
    let (schema, out_cols) = resolve(catalog, db_name, table_name)?;
    let (mut file, total_pages) = open_table(db_name, table_name)?;

    let mut matched: Vec<Row> = Vec::new();
    let mut total_scanned: u64 = 0;
    let mut pages_read: u64 = 0;
    let cap = limit.unwrap_or(usize::MAX);

    'outer: for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        pages_read += 1;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot in 0..num_items {
            let row = read_slot(&page, slot);
            let row = decode_tuple(&row, &schema);
            total_scanned += 1;

            if eval_predicate(predicate, &row) {
                matched.push(row);
                if matched.len() >= cap {
                    break 'outer;
                }
            }
        }
    }

    let total_matched = matched.len() as u64;
    Ok(StreamResult {
        columns: out_cols,
        rows: matched,
        total_scanned,
        total_matched,
        pages_read,
        elapsed_ms: t0.elapsed().as_millis(),
    })
}

// ─── stream_project ──────────────────────────────────────────────────────────

/// Streaming projection: SELECT cols/exprs WHERE predicate DISTINCT? LIMIT?
///
/// Only the projected columns are kept in memory — saves RAM vs. keeping full rows.
pub fn stream_project(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    items: &[ProjectionItem],
    predicate: Option<&Expr>,
    distinct: bool,
    limit: Option<usize>,
) -> io::Result<StreamResult> {
    let t0 = Instant::now();
    let (schema, _) = resolve(catalog, db_name, table_name)?;
    let (mut file, total_pages) = open_table(db_name, table_name)?;

    let mut projected: Vec<Row> = Vec::new();
    let mut out_cols_opt: Option<Vec<OutputColumn>> = None;
    let mut seen: HashSet<Vec<Value>> = HashSet::new();
    let mut total_scanned: u64 = 0;
    let mut pages_read: u64 = 0;
    let cap = limit.unwrap_or(usize::MAX);

    'outer: for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        pages_read += 1;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        let mut batch: Vec<Row> = Vec::new();

        for slot in 0..num_items {
            let raw = read_slot(&page, slot);
            let row = decode_tuple(&raw, &schema);
            total_scanned += 1;

            if eval_predicate(predicate, &row) {
                batch.push(row);
            }
        }

        if !batch.is_empty() {
            let (cols, rows) =
                eval_projection_list(std::mem::take(&mut batch), items, &schema)?;
            if out_cols_opt.is_none() {
                out_cols_opt = Some(cols);
            }
            for row in rows {
                if distinct {
                    if !seen.insert(row.clone()) {
                        continue;
                    }
                }
                projected.push(row);
                if projected.len() >= cap {
                    break 'outer;
                }
            }
        }
    }

    let total_matched = projected.len() as u64;
    let out_cols = out_cols_opt.unwrap_or_else(|| build_star_cols(&schema));

    Ok(StreamResult {
        columns: out_cols,
        rows: projected,
        total_scanned,
        total_matched,
        pages_read,
        elapsed_ms: t0.elapsed().as_millis(),
    })
}

// ─── stream_count ────────────────────────────────────────────────────────────

/// COUNT(*) WHERE predicate — no rows stored, minimal RAM.
pub fn stream_count(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    predicate: Option<&Expr>,
) -> io::Result<StreamResult> {
    let t0 = Instant::now();
    let (schema, _) = resolve(catalog, db_name, table_name)?;
    let (mut file, total_pages) = open_table(db_name, table_name)?;

    let mut count: u64 = 0;
    let mut total_scanned: u64 = 0;
    let mut pages_read: u64 = 0;

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        pages_read += 1;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot in 0..num_items {
            let raw = read_slot(&page, slot);
            let row = decode_tuple(&raw, &schema);
            total_scanned += 1;
            if eval_predicate(predicate, &row) {
                count += 1;
            }
        }
    }

    let out_cols = vec![OutputColumn {
        name: "count".to_string(),
        data_type: DataType::Int,
    }];
    let rows = vec![vec![Value::Int(count as i64)]];

    Ok(StreamResult {
        columns: out_cols,
        rows,
        total_scanned,
        total_matched: count,
        pages_read,
        elapsed_ms: t0.elapsed().as_millis(),
    })
}

// ─── stream_dedup_scan ───────────────────────────────────────────────────────

/// Streaming scan that skips duplicate tuples (based on full decoded value equality).
/// Uses a HashSet of seen content keys — RAM grows with distinct row count, not total.
pub fn stream_dedup_scan(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    predicate: Option<&Expr>,
    limit: Option<usize>,
) -> io::Result<StreamResult> {
    let t0 = Instant::now();
    let (schema, out_cols) = resolve(catalog, db_name, table_name)?;
    let (mut file, total_pages) = open_table(db_name, table_name)?;

    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut matched: Vec<Row> = Vec::new();
    let mut total_scanned: u64 = 0;
    let mut pages_read: u64 = 0;
    let cap = limit.unwrap_or(usize::MAX);

    'outer: for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        pages_read += 1;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot in 0..num_items {
            let raw = read_slot(&page, slot);
            let row = decode_tuple(&raw, &schema);
            total_scanned += 1;

            // Content fingerprint
            let key: Vec<u8> = row
                .iter()
                .flat_map(|v| format!("{:?}|", v).into_bytes())
                .collect();

            if !seen.insert(key) {
                continue; // duplicate — skip
            }

            if eval_predicate(predicate, &row) {
                matched.push(row);
                if matched.len() >= cap {
                    break 'outer;
                }
            }
        }
    }

    let total_matched = matched.len() as u64;
    Ok(StreamResult {
        columns: out_cols,
        rows: matched,
        total_scanned,
        total_matched,
        pages_read,
        elapsed_ms: t0.elapsed().as_millis(),
    })
}

// ─── private helpers ─────────────────────────────────────────────────────────

fn eval_predicate(pred: Option<&Expr>, row: &[Value]) -> bool {
    match pred {
        None => true,
        Some(p) => matches!(eval_expr(p, row), Ok(Value::Bool(true))),
    }
}

fn read_slot(page: &Page, slot: u32) -> Vec<u8> {
    let base = (PAGE_HEADER_SIZE + slot * ITEM_ID_SIZE) as usize;
    let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
    let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
    page.data[offset..offset + length].to_vec()
}

fn open_table(db_name: &str, table_name: &str) -> io::Result<(std::fs::File, u32)> {
    let path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);
    let mut file = OpenOptions::new().read(true).open(&path).map_err(|e| {
        io::Error::new(e.kind(), format!("Cannot open '{}': {}", path, e))
    })?;
    let pages = page_count(&mut file)?;
    Ok((file, pages))
}

fn resolve(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<(Vec<Column>, Vec<OutputColumn>)> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = table.columns.clone();
    let out_cols = build_star_cols(&schema);
    Ok((schema, out_cols))
}

fn build_star_cols(schema: &[Column]) -> Vec<OutputColumn> {
    schema
        .iter()
        .map(|c| OutputColumn {
            name: c.name.clone(),
            data_type: c.parsed_type(),
        })
        .collect()
}
