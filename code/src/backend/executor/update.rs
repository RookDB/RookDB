//! Implements UPDATE with SET assignments, optional WHERE conditions, and RETURNING support.
//!
//! Supports:
//!   UPDATE table SET col = val;
//!   UPDATE table SET col = val WHERE other = x;
//!   UPDATE table SET col1 = val1, col2 = val2 WHERE id > 5;
//!   UPDATE table SET col = val WHERE (a = 1 AND b = 2) OR c = 3;
//!
//! UPDATE is implemented using delete + insert semantics for latest-version wins.
//! Rows with the SLOT_FLAG_DELETED bit set are invisible and are never updated.

use std::fs::File;
use std::io;

use crate::backend::heap::autovacuum::notify_table_write;
use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::heap::{insert_tuple, soft_delete_tuple_at, TuplePointer};
use crate::backend::log::operation_log::log_update;
use crate::backend::log::operation_log::current_timestamp_iso;
use crate::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE, SLOT_FLAG_DELETED};
use crate::backend::page::page_lock::PageWriteLock;
use crate::table::page_count;
use crate::table::increment_dead_tuple_count;
use serde_json::{Value, json};

use super::delete::{Condition, ColumnValue, condition_to_json, matches_condition_groups_pub};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Arithmetic operator used in a SET expression (e.g. `salary = salary * 1.10`).
#[derive(Debug, Clone)]
pub enum ArithOp {
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
}

/// Right-hand side of a SET assignment — either a literal or an expression
/// that references the column's current value.
///
/// Examples:
///   `age = 25`           → `SetExpr::Literal(Int(25))`
///   `age = age + 1`      → `SetExpr::Expr { col: "age", op: Add, rhs: Int(1) }`
///   `salary = salary * 1.10` → `SetExpr::Expr { col: "salary", op: Mul, rhs_f: 1.10 }`
#[derive(Debug, Clone)]
pub enum SetExpr {
    /// A constant value.
    Literal(ColumnValue),
    /// `<src_col> <op> <rhs>` evaluated against the current row.
    /// `rhs_f` is used for floating-point multipliers (Mul / Div);
    /// `rhs_i` is used for integer Add / Sub.
    Expr {
        src_col: String,
        op:      ArithOp,
        rhs_i:   i64,   // used for Add/Sub
        rhs_f:   f64,   // used for Mul/Div
    },
}

/// One `col = <expr>` assignment from the SET clause.
#[derive(Debug, Clone)]
pub struct SetAssignment {
    pub column: String,
    pub expr:   SetExpr,
}

/// Result returned by `update_tuples`.
pub struct UpdateResult {
    /// How many rows were modified.
    pub updated_count:   usize,
    /// The rows **after** update (only populated when `returning = true`).
    pub returning_rows:  Vec<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Decode a raw tuple byte slice into `(column_name, ColumnValue)` pairs
/// following the schema stored in `columns`.
fn decode_tuple(
    data:    &[u8],
    columns: &[crate::catalog::types::Column],
) -> Vec<(String, ColumnValue)> {
    let mut result = Vec::new();
    let mut cursor = 0usize;
    for col in columns {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= data.len() {
                    let v = i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                    result.push((col.name.clone(), ColumnValue::Int(v)));
                    cursor += 4;
                }
            }
            "TEXT" => {
                if cursor + 10 <= data.len() {
                    let t = String::from_utf8_lossy(&data[cursor..cursor + 10])
                        .trim_end_matches('\0')
                        .trim()
                        .to_string();
                    result.push((col.name.clone(), ColumnValue::Text(t)));
                    cursor += 10;
                }
            }
            _ => {}
        }
    }
    result
}

/// Re-encode a decoded tuple (after applying SET assignments) back to raw bytes.
/// The layout must match exactly what the buffer manager originally wrote.
fn encode_tuple(
    decoded: &[(String, ColumnValue)],
    columns: &[crate::catalog::types::Column],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    for col in columns {
        // Find the (possibly updated) value for this column
        let val = decoded
            .iter()
            .find(|(name, _)| name == &col.name)
            .map(|(_, v)| v);

        match col.data_type.as_str() {
            "INT" => {
                let n = match val {
                    Some(ColumnValue::Int(n))   => *n,
                    Some(ColumnValue::Text(s))  => s.parse::<i32>().unwrap_or(0),
                    Some(ColumnValue::List(_))  => 0,
                    None                        => 0,
                };
                bytes.extend_from_slice(&n.to_le_bytes());
            }
            "TEXT" => {
                let s = match val {
                    Some(ColumnValue::Text(s))  => s.clone(),
                    Some(ColumnValue::Int(n))   => n.to_string(),
                    Some(ColumnValue::List(_))  => String::new(),
                    None                        => String::new(),
                };
                let mut b = s.into_bytes();
                b.truncate(10);
                b.resize(10, b' ');
                bytes.extend_from_slice(&b);
            }
            _ => {}
        }
    }
    bytes
}

/// Apply `assignments` to a decoded row, returning the new row.
/// Arithmetic expressions are evaluated against the *current* column values.
fn apply_assignments(
    mut decoded: Vec<(String, ColumnValue)>,
    assignments: &[SetAssignment],
) -> Vec<(String, ColumnValue)> {
    for asgn in assignments {
        let new_val = match &asgn.expr {
            SetExpr::Literal(v) => v.clone(),
            SetExpr::Expr { src_col, op, rhs_i, rhs_f } => {
                // Look up current value of src_col
                let cur = decoded.iter().find(|(name, _)| name == src_col)
                    .map(|(_, v)| v.clone());
                match cur {
                    Some(ColumnValue::Int(n)) => {
                        let result = match op {
                            ArithOp::Add => (n as i64 + rhs_i) as i32,
                            ArithOp::Sub => (n as i64 - rhs_i) as i32,
                            ArithOp::Mul => (n as f64 * rhs_f) as i32,
                            ArithOp::Div => if *rhs_f == 0.0 { n } else { (n as f64 / rhs_f) as i32 },
                        };
                        ColumnValue::Int(result)
                    }
                    Some(ColumnValue::Text(s)) => {
                        // Text + int → append; Text * int → repeat
                        let result = match op {
                            ArithOp::Add => format!("{}{}", s, rhs_i),
                            ArithOp::Sub => s.chars().take(
                                (s.len() as i64 - rhs_i).max(0) as usize
                            ).collect(),
                            _ => s.clone(),
                        };
                        ColumnValue::Text(result)
                    }
                    _ => continue, // column not found, skip
                }
            }
        };
        if let Some(entry) = decoded.iter_mut().find(|(name, _)| *name == asgn.column) {
            entry.1 = new_val;
        }
    }
    decoded
}

struct PendingUpdate {
    pointer: TuplePointer,
    new_bytes: Vec<u8>,
    updated_decoded: Vec<(String, ColumnValue)>,
}

fn update_log_details(
    condition_groups: &[Vec<Condition>],
    returning: bool,
    updated_count: Option<usize>,
    error: Option<&str>,
) -> Value {
    let groups_json: Vec<Value> = condition_groups
        .iter()
        .map(|group| {
            let conds: Vec<Value> = group.iter().map(condition_to_json).collect();
            json!(conds)
        })
        .collect();

    json!({
        "timestamp": current_timestamp_iso(),
        "condition_groups": groups_json,
        "returning": returning,
        "updated_count": updated_count,
        "error": error,
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Update every live tuple in `table_name` that satisfies `condition_groups`,
/// applying each `SetAssignment`.
///
/// - `condition_groups` empty → UPDATE all rows (no WHERE clause).
/// - `returning = true`       → populate `UpdateResult::returning_rows` with
///                              the rows **after** update.
///
pub fn update_tuples(
    catalog:           &Catalog,
    db_name:           &str,
    table_name:        &str,
    file:              &mut File,
    assignments:       &[SetAssignment],
    condition_groups:  &[Vec<Condition>],
    returning:         bool,
) -> io::Result<UpdateResult> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let columns = &table.columns;

    let total_pages = page_count(file)?;
    let file_identity = crate::table::file_identity_from_file(file)?;
    let mut updated_count  = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();
    let mut pending_updates: Vec<PendingUpdate> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower     = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        for i in 0..num_items {
            let base   = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
            let flags  = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

            if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                continue;
            }

            let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();
            let decoded    = decode_tuple(&tuple_data, columns);

            if !matches_condition_groups_pub(&decoded, condition_groups) {
                continue;
            }

            let updated_decoded = apply_assignments(decoded, assignments);
            let new_bytes       = encode_tuple(&updated_decoded, columns);

            pending_updates.push(PendingUpdate {
                pointer: TuplePointer {
                    page_id: page_num,
                    slot_index: i as u16,
                },
                new_bytes,
                updated_decoded,
            });

            updated_count += 1;
        }
    }

    if !pending_updates.is_empty() {
        for update in &pending_updates {
            let _page_lock = PageWriteLock::acquire(file_identity, update.pointer.page_id);
            soft_delete_tuple_at(file, update.pointer)?;
        }
        increment_dead_tuple_count(file, pending_updates.len() as u32)?;
    }

    if !pending_updates.is_empty() {
        for update in pending_updates {
            insert_tuple(file, &update.new_bytes)?;

            if returning {
                let row: Vec<(String, String)> = update
                    .updated_decoded
                    .iter()
                    .map(|(col, val)| {
                        let s = match val {
                            ColumnValue::Int(n) => n.to_string(),
                            ColumnValue::Text(t) => t.clone(),
                            ColumnValue::List(_) => String::from("[list]"),
                        };
                        (col.clone(), s)
                    })
                    .collect();
                returning_rows.push(row);
            }
        }
    }

    notify_table_write(db_name, table_name, updated_count, total_pages as usize);

    let result: io::Result<UpdateResult> = Ok(UpdateResult { updated_count, returning_rows });

    match &result {
        Ok(update_result) => {
            let details = update_log_details(
                condition_groups,
                returning,
                Some(update_result.updated_count),
                None,
            );
            let _ = log_update(db_name, table_name, details, "success");
        }
        Err(err) => {
            let details = update_log_details(
                condition_groups,
                returning,
                None,
                Some(&err.to_string()),
            );
            let _ = log_update(db_name, table_name, details, "failed");
        }
    }

    result
}

// ---------------------------------------------------------------------------
// SET clause parser
// ---------------------------------------------------------------------------

/// Parse a comma-separated SET clause string into `SetAssignment`s.
///
/// Handles both literal and expression-based assignments:
///   `salary = 60000`               → `Literal(Int(60000))`
///   `name = 'Alice'`               → `Literal(Text("Alice"))`
///   `age = age + 1`                → `Expr { src: age, op: Add, rhs_i: 1 }`
///   `salary = salary * 1.10`       → `Expr { src: salary, op: Mul, rhs_f: 1.10 }`
///   `score = score - 5, age = age + 1`  → two assignments
///
/// Returns `None` if the string is empty or no valid assignment could be parsed.
pub fn parse_set_clause(input: &str) -> Option<Vec<SetAssignment>> {
    let input = input.trim();
    if input.is_empty() { return None; }

    let mut assignments = Vec::new();

    for part in split_set_parts(input) {
        let part = part.trim();
        // Find the FIRST '=' (the assignment operator)
        let eq_pos = part.find('=')?;
        let col = part[..eq_pos].trim().to_string();
        let rhs = part[eq_pos + 1..].trim().to_string();

        if col.is_empty() || rhs.is_empty() { continue; }

        // Try to detect arithmetic expression: `src_col OP value`
        // where OP is one of + - * /
        // e.g. "age + 1", "salary * 1.10", "score - 5"
        let expr = try_parse_arith_expr(&rhs)
            .and_then(|(src, op, rhs_str)| {
                let rhs_f: f64 = rhs_str.parse().ok()?;
                let rhs_i: i64 = rhs_f as i64;
                Some(SetExpr::Expr { src_col: src, op, rhs_i, rhs_f })
            })
            .unwrap_or_else(|| {
                // Literal value
                let v = rhs.trim_matches('\'').to_string();
                if let Ok(n) = v.parse::<i32>() {
                    SetExpr::Literal(ColumnValue::Int(n))
                } else {
                    SetExpr::Literal(ColumnValue::Text(v))
                }
            });

        assignments.push(SetAssignment { column: col, expr });
    }

    if assignments.is_empty() { None } else { Some(assignments) }
}

/// Split a SET clause by commas, but NOT commas inside parentheses.
fn split_set_parts(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => { parts.push(&s[start..i]); start = i + 1; }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Try to parse an arithmetic expression like `age + 1`, `age+1`, or `salary * 1.10`.
/// Returns `(src_col, op, rhs_str)` or `None`.
fn try_parse_arith_expr(rhs: &str) -> Option<(String, ArithOp, String)> {
    // .trim() on both sides makes spaced and unspaced forms identical,
    // so we only need the bare operator symbol.
    // pos > 0 guard prevents treating a leading sign (e.g. "-5") as an expression.
    let ops: &[(&str, ArithOp)] = &[
        ("+", ArithOp::Add),
        ("-", ArithOp::Sub),
        ("*", ArithOp::Mul),
        ("/", ArithOp::Div),
    ];

    for (sym, op) in ops {
        if let Some(pos) = rhs.find(sym) {
            if pos == 0 { continue; } // leading sign — not an expression
            let src = rhs[..pos].trim().trim_matches('\'').to_string();
            let val = rhs[pos + sym.len()..].trim().trim_matches('\'').to_string();
            // src must look like a column name (non-numeric, non-empty)
            if !src.is_empty() && src.parse::<f64>().is_err() && !val.is_empty() {
                return Some((src, op.clone(), val));
            }
        }
    }
    None
}

// Re-export parse_where_clause so callers only need to import from this module.
pub use super::delete::parse_where_clause as parse_where_clause_update;
