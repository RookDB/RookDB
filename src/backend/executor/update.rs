//! Implements UPDATE with SET assignments and optional WHERE conditions.
//!
//! Supports:
//!   UPDATE table SET col = val;
//!   UPDATE table SET col = val WHERE other = x;
//!   UPDATE table SET col1 = val1, col2 = val2 WHERE id > 5;
//!   UPDATE table SET col = val WHERE (a = 1 AND b = 2) OR c = 3;
//!
//! ("Print updated rows?" is an interactive frontend prompt, not SQL RETURNING.)
//!
//! UPDATE is implemented using delete + insert semantics for latest-version wins.
//! Rows with the SLOT_FLAG_DELETED bit set are invisible and are never updated.

use std::io;

use crate::backend::log::operation_log::current_timestamp_iso;
use crate::backend::page::page_lock::PageWriteLock;
use crate::catalog::types::Catalog;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, SLOT_FLAG_DELETED};
use crate::types::datatype::DataType;
use crate::types::row::deserialize_nullable_row;
use crate::types::row::serialize_nullable_typed_row;
use crate::types::value::{DataValue, OrderedF32, OrderedF64};
use serde_json::{Value, json};

use super::delete::ColumnValue;

/// (page_num, slot_index) pair that uniquely identifies a stored tuple.
#[derive(Debug, Clone, Copy)]
struct TuplePointer {
    page_id: u32,
    slot_index: u16,
}

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
///   `tag = tag`          → `SetExpr::Column("tag")`
///   `tag = NULL`         → `SetExpr::Null`
#[derive(Debug, Clone)]
pub enum SetExpr {
    /// A constant value.
    Literal(ColumnValue),
    /// A reference to another column (or self) in the current row.
    Column(String),
    /// SQL NULL literal.
    Null,
    /// `<src_col> <op> <rhs>` evaluated against the current row.
    /// `rhs_f` is used for floating-point multipliers (Mul / Div);
    /// `rhs_i` is used for integer Add / Sub.
    Expr {
        src_col: String,
        op: ArithOp,
        rhs_i: i64, // used for Add/Sub
        rhs_f: f64, // used for Mul/Div
    },
}

/// One `col = <expr>` assignment from the SET clause.
#[derive(Debug, Clone)]
pub struct SetAssignment {
    pub column: String,
    pub expr: SetExpr,
}

/// Result returned by the UPDATE entry points.
pub struct UpdateResult {
    /// How many rows were modified.
    pub updated_count: usize,
    /// The rows **after** update (only populated when `returning = true`).
    pub returning_rows: Vec<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn values_to_column_values(
    columns: &[crate::catalog::types::Column],
    values: &[Option<DataValue>],
) -> Vec<(String, ColumnValue)> {
    columns
        .iter()
        .zip(values.iter())
        .map(|(col, val)| {
            let cv = match val {
                Some(DataValue::Int(n)) => ColumnValue::Int(*n),
                Some(DataValue::SmallInt(n)) => ColumnValue::Int(*n as i32),
                Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => {
                    ColumnValue::Text(s.clone())
                }
                Some(other) => ColumnValue::Text(format!("{}", other)),
                None => ColumnValue::Text("NULL".to_string()),
            };
            (col.name.clone(), cv)
        })
        .collect()
}

/// Apply `assignments` directly to typed row values, preserving all types natively.
fn apply_assignments_typed(
    columns: &[crate::catalog::types::Column],
    mut values: Vec<Option<DataValue>>,
    assignments: &[SetAssignment],
) -> Result<Vec<Option<DataValue>>, String> {
    for asgn in assignments {
        let target_idx = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&asgn.column))
            .ok_or_else(|| format!("Column '{}' not found", asgn.column))?;
        let target_type = &columns[target_idx].data_type;

        let new_val: Option<DataValue> = match &asgn.expr {
            SetExpr::Null => None,
            SetExpr::Literal(cv) => match cv {
                ColumnValue::Int(n) => match target_type {
                    DataType::SmallInt => {
                        let val = i16::try_from(*n)
                            .map_err(|_| format!("Value {} out of range for SMALLINT", n))?;
                        Some(DataValue::SmallInt(val))
                    }
                    DataType::Int => Some(DataValue::Int(*n)),
                    DataType::BigInt => Some(DataValue::BigInt(*n as i64)),
                    DataType::Real => Some(DataValue::Real(OrderedF32(*n as f32))),
                    DataType::DoublePrecision => {
                        Some(DataValue::DoublePrecision(OrderedF64(*n as f64)))
                    }
                    DataType::Numeric { scale, .. } | DataType::Decimal { scale, .. } => {
                        let factor = 10_i128
                            .checked_pow(*scale as u32)
                            .ok_or_else(|| "Numeric scale overflow".to_string())?;
                        let unscaled = (*n as i128)
                            .checked_mul(factor)
                            .ok_or_else(|| "Numeric overflow".to_string())?;
                        Some(DataValue::Numeric(crate::types::value::NumericValue {
                            unscaled,
                            scale: *scale,
                        }))
                    }
                    _ => Some(DataValue::Int(*n)),
                },
                ColumnValue::Text(s) => match target_type {
                    DataType::Char(n) | DataType::Character(n) => {
                        if s.len() > *n as usize {
                            return Err(format!(
                                "CHAR payload length {} exceeds declared limit {}",
                                s.len(),
                                n
                            ));
                        }
                        let mut padded = s.clone();
                        if padded.len() < *n as usize {
                            padded.push_str(&" ".repeat(*n as usize - padded.len()));
                        }
                        Some(DataValue::Char(padded))
                    }
                    DataType::Varchar(max_len) => {
                        if s.len() > *max_len as usize {
                            return Err(format!(
                                "VARCHAR payload length {} exceeds declared limit {}",
                                s.len(),
                                max_len
                            ));
                        }
                        Some(DataValue::Varchar(s.clone()))
                    }
                    DataType::Numeric { precision, scale }
                    | DataType::Decimal { precision, scale } => {
                        let num = crate::types::value::parse_numeric_literal(s, *precision, *scale)
                            .map_err(|e| format!("Invalid numeric literal '{}': {}", s, e))?;
                        Some(DataValue::Numeric(num))
                    }
                    _ => {
                        let dv = super::create_index::parse_string_to_value(target_type, s)
                            .map_err(|e| {
                                format!("Invalid value '{}' for type {:?}: {}", s, target_type, e)
                            })?;
                        Some(dv)
                    }
                },
                ColumnValue::List(_) => {
                    return Err("Cannot assign list literal to column".to_string());
                }
            },
            SetExpr::Column(src_col) => {
                let src_idx = columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(src_col))
                    .ok_or_else(|| format!("Source column '{}' not found", src_col))?;
                values[src_idx].clone()
            }
            SetExpr::Expr {
                src_col,
                op,
                rhs_i,
                rhs_f,
            } => {
                let src_idx = columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(src_col))
                    .ok_or_else(|| format!("Source column '{}' not found", src_col))?;
                let src_val = values.get(src_idx).cloned().flatten();

                match src_val {
                    None => None, // SQL semantics: arithmetic on NULL produces NULL!
                    Some(DataValue::Int(n)) => {
                        let result = match op {
                            ArithOp::Add => {
                                let rhs = i32::try_from(*rhs_i)
                                    .map_err(|_| "Integer addition overflow".to_string())?;
                                n.checked_add(rhs)
                                    .ok_or_else(|| "Integer addition overflow".to_string())?
                            }
                            ArithOp::Sub => {
                                let rhs = i32::try_from(*rhs_i)
                                    .map_err(|_| "Integer subtraction overflow".to_string())?;
                                n.checked_sub(rhs)
                                    .ok_or_else(|| "Integer subtraction overflow".to_string())?
                            }
                            ArithOp::Mul => {
                                if *rhs_f == (*rhs_i as f64) {
                                    let rhs = i32::try_from(*rhs_i).map_err(|_| {
                                        "Integer multiplication overflow".to_string()
                                    })?;
                                    n.checked_mul(rhs).ok_or_else(|| {
                                        "Integer multiplication overflow".to_string()
                                    })?
                                } else {
                                    let f = (n as f64) * rhs_f;
                                    if f.is_nan() || f < i32::MIN as f64 || f > i32::MAX as f64 {
                                        return Err("Integer multiplication overflow".to_string());
                                    }
                                    f as i32
                                }
                            }
                            ArithOp::Div => {
                                if *rhs_f == 0.0 || *rhs_i == 0 {
                                    return Err("Division by zero".to_string());
                                }
                                if *rhs_f == (*rhs_i as f64) {
                                    let rhs = i32::try_from(*rhs_i)
                                        .map_err(|_| "Integer division overflow".to_string())?;
                                    n.checked_div(rhs)
                                        .ok_or_else(|| "Integer division overflow".to_string())?
                                } else {
                                    let f = (n as f64) / rhs_f;
                                    if f.is_nan() || f < i32::MIN as f64 || f > i32::MAX as f64 {
                                        return Err("Integer division overflow".to_string());
                                    }
                                    f as i32
                                }
                            }
                        };
                        Some(DataValue::Int(result))
                    }
                    Some(DataValue::SmallInt(n)) => {
                        let result = match op {
                            ArithOp::Add => {
                                let rhs = i16::try_from(*rhs_i)
                                    .map_err(|_| "SmallInt addition overflow".to_string())?;
                                n.checked_add(rhs)
                                    .ok_or_else(|| "SmallInt addition overflow".to_string())?
                            }
                            ArithOp::Sub => {
                                let rhs = i16::try_from(*rhs_i)
                                    .map_err(|_| "SmallInt subtraction overflow".to_string())?;
                                n.checked_sub(rhs)
                                    .ok_or_else(|| "SmallInt subtraction overflow".to_string())?
                            }
                            ArithOp::Mul => {
                                if *rhs_f == (*rhs_i as f64) {
                                    let rhs = i16::try_from(*rhs_i).map_err(|_| {
                                        "SmallInt multiplication overflow".to_string()
                                    })?;
                                    n.checked_mul(rhs).ok_or_else(|| {
                                        "SmallInt multiplication overflow".to_string()
                                    })?
                                } else {
                                    let f = (n as f64) * rhs_f;
                                    if f.is_nan() || f < i16::MIN as f64 || f > i16::MAX as f64 {
                                        return Err("SmallInt multiplication overflow".to_string());
                                    }
                                    f as i16
                                }
                            }
                            ArithOp::Div => {
                                if *rhs_f == 0.0 || *rhs_i == 0 {
                                    return Err("Division by zero".to_string());
                                }
                                if *rhs_f == (*rhs_i as f64) {
                                    let rhs = i16::try_from(*rhs_i)
                                        .map_err(|_| "SmallInt division overflow".to_string())?;
                                    n.checked_div(rhs)
                                        .ok_or_else(|| "SmallInt division overflow".to_string())?
                                } else {
                                    let f = (n as f64) / rhs_f;
                                    if f.is_nan() || f < i16::MIN as f64 || f > i16::MAX as f64 {
                                        return Err("SmallInt division overflow".to_string());
                                    }
                                    f as i16
                                }
                            }
                        };
                        Some(DataValue::SmallInt(result))
                    }
                    Some(DataValue::BigInt(n)) => {
                        let result = match op {
                            ArithOp::Add => n
                                .checked_add(*rhs_i)
                                .ok_or_else(|| "BigInt addition overflow".to_string())?,
                            ArithOp::Sub => n
                                .checked_sub(*rhs_i)
                                .ok_or_else(|| "BigInt subtraction overflow".to_string())?,
                            ArithOp::Mul => {
                                if *rhs_f == (*rhs_i as f64) {
                                    n.checked_mul(*rhs_i).ok_or_else(|| {
                                        "BigInt multiplication overflow".to_string()
                                    })?
                                } else {
                                    let f = (n as f64) * rhs_f;
                                    if f.is_nan() || f < i64::MIN as f64 || f > i64::MAX as f64 {
                                        return Err("BigInt multiplication overflow".to_string());
                                    }
                                    f as i64
                                }
                            }
                            ArithOp::Div => {
                                if *rhs_f == 0.0 || *rhs_i == 0 {
                                    return Err("Division by zero".to_string());
                                }
                                if *rhs_f == (*rhs_i as f64) {
                                    n.checked_div(*rhs_i)
                                        .ok_or_else(|| "BigInt division overflow".to_string())?
                                } else {
                                    let f = (n as f64) / rhs_f;
                                    if f.is_nan() || f < i64::MIN as f64 || f > i64::MAX as f64 {
                                        return Err("BigInt division overflow".to_string());
                                    }
                                    f as i64
                                }
                            }
                        };
                        Some(DataValue::BigInt(result))
                    }
                    Some(DataValue::Real(r)) => {
                        let cur = r.0 as f64;
                        let result = match op {
                            ArithOp::Add => cur + *rhs_f,
                            ArithOp::Sub => cur - *rhs_f,
                            ArithOp::Mul => cur * *rhs_f,
                            ArithOp::Div => {
                                if *rhs_f == 0.0 {
                                    return Err("Division by zero".to_string());
                                }
                                cur / *rhs_f
                            }
                        };
                        Some(DataValue::Real(OrderedF32(result as f32)))
                    }
                    Some(DataValue::DoublePrecision(d)) => {
                        let cur = d.0;
                        let result = match op {
                            ArithOp::Add => cur + *rhs_f,
                            ArithOp::Sub => cur - *rhs_f,
                            ArithOp::Mul => cur * *rhs_f,
                            ArithOp::Div => {
                                if *rhs_f == 0.0 {
                                    return Err("Division by zero".to_string());
                                }
                                cur / *rhs_f
                            }
                        };
                        Some(DataValue::DoublePrecision(OrderedF64(result)))
                    }
                    Some(DataValue::Numeric(num)) => {
                        if matches!(op, ArithOp::Div) && (*rhs_f == 0.0 || *rhs_i == 0) {
                            return Err("Division by zero".to_string());
                        }
                        Some(DataValue::Numeric(num))
                    }
                    Some(DataValue::Varchar(s)) => {
                        let result = match op {
                            ArithOp::Add => format!("{}{}", s, rhs_i),
                            ArithOp::Sub => {
                                let total_chars = s.chars().count();
                                let take_count = (total_chars as i64 - rhs_i).max(0) as usize;
                                s.chars().take(take_count).collect()
                            }
                            _ => s.clone(),
                        };
                        Some(DataValue::Varchar(result))
                    }
                    Some(DataValue::Char(s)) => {
                        let trimmed = s.trim_end();
                        let result = match op {
                            ArithOp::Add => format!("{}{}", trimmed, rhs_i),
                            ArithOp::Sub => {
                                let total_chars = trimmed.chars().count();
                                let take_count = (total_chars as i64 - rhs_i).max(0) as usize;
                                trimmed.chars().take(take_count).collect()
                            }
                            _ => trimmed.to_string(),
                        };
                        let width = if let DataType::Char(w) | DataType::Character(w) = target_type
                        {
                            *w as usize
                        } else {
                            result.len()
                        };
                        let mut padded = result;
                        if padded.len() < width {
                            padded.push_str(&" ".repeat(width - padded.len()));
                        }
                        Some(DataValue::Char(padded))
                    }
                    Some(other) => Some(other),
                }
            }
        };

        if let Some(ref dv) = new_val {
            match (target_type, dv) {
                (DataType::Varchar(max_len), DataValue::Varchar(s))
                    if s.len() > *max_len as usize =>
                {
                    return Err(format!(
                        "VARCHAR payload length {} exceeds declared limit {}",
                        s.len(),
                        max_len
                    ));
                }
                (DataType::Char(n) | DataType::Character(n), DataValue::Char(s))
                    if s.len() > *n as usize =>
                {
                    return Err(format!(
                        "CHAR payload length {} exceeds declared limit {}",
                        s.len(),
                        n
                    ));
                }
                _ => {}
            }
        }

        values[target_idx] = new_val;
    }
    Ok(values)
}

struct PendingUpdate {
    pointer: TuplePointer,
    /// The raw bytes of the OLD tuple (before update), for index key extraction.
    old_tuple_data: Vec<u8>,
    /// The raw bytes of the NEW tuple (after update).
    new_bytes: Vec<u8>,
    /// The decoded row BEFORE the update, for FK cascade propagation.
    old_decoded: Vec<(String, ColumnValue)>,
    /// The decoded row AFTER the update, for FK cascade propagation.
    updated_decoded: Vec<(String, ColumnValue)>,
    /// The updated typed values, used for intra-batch constraint validation.
    updated_values: Vec<Option<DataValue>>,
}

fn update_log_details(updated_count: Option<usize>, error: Option<&str>) -> Value {
    json!({
        "timestamp": current_timestamp_iso(),
        "updated_count": updated_count,
        "error": error,
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SET clause parser
// ---------------------------------------------------------------------------

/// Parse a comma-separated SET clause string into `SetAssignment`s.
///
/// Handles literal, column-reference, and expression-based assignments:
///   `salary = 60000`               → `Literal(Int(60000))`
///   `name = 'Alice'`               → `Literal(Text("Alice"))`
///   `tag = tag`                    → `Column("tag")`
///   `tag = NULL`                   → `Null`
///   `age = age + 1`                → `Expr { src: age, op: Add, rhs_i: 1 }`
///   `salary = salary * 1.10`       → `Expr { src: salary, op: Mul, rhs_f: 1.10 }`
///   `score = score - 5, age = age + 1`  → two assignments
///
/// Returns `None` if the string is empty or no valid assignment could be parsed.
pub fn parse_set_clause(input: &str) -> Option<Vec<SetAssignment>> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    let mut assignments = Vec::new();

    for part in split_set_parts(input) {
        let part = part.trim();
        // Find the FIRST '=' (the assignment operator)
        let eq_pos = part.find('=')?;
        let col = part[..eq_pos].trim().to_string();
        let rhs = part[eq_pos + 1..].trim();

        if col.is_empty() || rhs.is_empty() {
            continue;
        }

        let expr = if rhs.starts_with('\'') && rhs.ends_with('\'') && rhs.len() >= 2 {
            let inner = &rhs[1..rhs.len() - 1];
            SetExpr::Literal(ColumnValue::Text(inner.replace("''", "'")))
        } else if rhs.starts_with('"') && rhs.ends_with('"') && rhs.len() >= 2 {
            let inner = &rhs[1..rhs.len() - 1];
            SetExpr::Literal(ColumnValue::Text(inner.replace("\"\"", "\"")))
        } else if rhs.eq_ignore_ascii_case("null") {
            SetExpr::Null
        } else if let Some((src, op, rhs_str)) = try_parse_arith_expr(rhs) {
            let rhs_f: f64 = rhs_str.parse().ok()?;
            let rhs_i: i64 = rhs_f as i64;
            SetExpr::Expr {
                src_col: src,
                op,
                rhs_i,
                rhs_f,
            }
        } else if let Ok(n) = rhs.parse::<i32>() {
            SetExpr::Literal(ColumnValue::Int(n))
        } else if let Ok(n) = rhs.parse::<i64>() {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                SetExpr::Literal(ColumnValue::Int(n as i32))
            } else {
                SetExpr::Literal(ColumnValue::Text(rhs.to_string()))
            }
        } else if rhs.parse::<f64>().is_ok() {
            SetExpr::Literal(ColumnValue::Text(rhs.to_string()))
        } else {
            // Unquoted string that is not numeric or NULL: column reference!
            SetExpr::Column(rhs.to_string())
        };

        assignments.push(SetAssignment { column: col, expr });
    }

    if assignments.is_empty() {
        None
    } else {
        Some(assignments)
    }
}

/// Split a SET clause by commas, but NOT commas inside parentheses or string literals.
fn split_set_parts(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut start = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '(' if !in_single_quote && !in_double_quote => depth += 1,
            ')' if !in_single_quote && !in_double_quote => depth = depth.saturating_sub(1),
            ',' if depth == 0 && !in_single_quote && !in_double_quote => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Try to parse an arithmetic expression like `age + 1`, `age+1`, or `salary * 1.10`.
/// Returns `(src_col, op, rhs_str)` or `None`.
fn try_parse_arith_expr(rhs: &str) -> Option<(String, ArithOp, String)> {
    if rhs.starts_with('\'') || rhs.starts_with('"') {
        return None;
    }

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
            if pos == 0 {
                continue;
            } // leading sign — not an expression
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

/// Update rows identified by explicit heap pointers (page_id, slot_id).
///
/// This is the Volcano-aware UPDATE path: instead of scanning the heap and
/// evaluating WHERE conditions here, the caller (Volcano engine) has already
/// identified the matching rows. This function applies the SET assignments to
/// the rows at the given pointers (soft-delete + rewrite).
pub fn update_by_pointers(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    pointers: &[(u32, u32)],
    assignments: &[SetAssignment],
) -> io::Result<UpdateResult> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db_name),
        )
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", table_name),
        )
    })?;
    let columns = &table.columns;

    for asgn in assignments {
        if !columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&asgn.column))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Assignment column '{}' not found in table '{}'",
                    asgn.column, table_name
                ),
            ));
        }
        match &asgn.expr {
            SetExpr::Column(src_col)
                if !columns.iter().any(|c| c.name.eq_ignore_ascii_case(src_col)) =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Source column '{}' not found in table '{}'",
                        src_col, table_name
                    ),
                ));
            }
            SetExpr::Expr { src_col, .. }
                if !columns.iter().any(|c| c.name.eq_ignore_ascii_case(src_col)) =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Source column '{}' not found in table '{}'",
                        src_col, table_name
                    ),
                ));
            }
            _ => {}
        }
    }

    let path = format!("database/base/{}/{}.dat", db_name, table_name);
    // UPDATE rewrites pages via direct I/O — flush/evict cached pool state
    // first so the raw reads observe every prior insert.
    crate::backend::cache::quiesce_for_direct_io(std::path::Path::new(&path))
        .map_err(|e| io::Error::other(format!("Failed to flush cache: {}", e)))?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| io::Error::other(format!("Failed to open table file: {}", e)))?;
    let file_identity = crate::table::file_identity_from_file(&file)?;

    let mut updated_count = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();
    let mut pending_updates: Vec<PendingUpdate> = Vec::new();

    // Consecutive pointers usually share a heap page; nothing in this pass
    // writes to the heap, so a page is read once for all of its rows.
    let mut page = Page::new();
    let mut loaded_page: Option<u32> = None;

    for &(page_num, slot_idx) in pointers {
        let slot_index = slot_idx as u16;

        if loaded_page != Some(page_num) {
            crate::disk::read_page(&mut file, &mut page, page_num)?;
            loaded_page = Some(page_num);
        }

        let base = (PAGE_HEADER_SIZE + slot_index as u32 * ITEM_ID_SIZE) as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
        let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
        let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

        if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
            continue;
        }

        let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();
        let schema: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
        let decoded = match deserialize_nullable_row(&schema, &tuple_data) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let updated_values = match apply_assignments_typed(columns, decoded.clone(), assignments) {
            Ok(v) => v,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("assignment error: {}", e),
                ));
            }
        };
        let new_bytes = match serialize_nullable_typed_row(&schema, &updated_values) {
            Ok(b) => b,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("serialization error: {}", e),
                ));
            }
        };

        let old_decoded = values_to_column_values(columns, &decoded);
        let updated_decoded = values_to_column_values(columns, &updated_values);

        // Constraint validation against disk, excluding all pointers in this batch
        let new_strings: Vec<String> = updated_values
            .iter()
            .map(|val| match val {
                Some(dv) => dv.to_string(),
                None => "NULL".to_string(),
            })
            .collect();
        let new_values: Vec<&str> = new_strings.iter().map(|s| s.as_str()).collect();
        if let Err(e) = crate::backend::constraint::validate_row_update_with_excludes(
            catalog,
            db_name,
            table_name,
            &new_values,
            pointers,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("constraint violation: {}", e),
            ));
        }

        if let Err(e) = crate::backend::constraint::validate_update_restrict(
            db_name,
            table_name,
            &old_decoded,
            &updated_decoded,
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("FOREIGN KEY RESTRICT constraint: {}", e),
            ));
        }

        log::info!(
            "[Update] Validated row at (page={}, slot={}): updated_decoded={:?}",
            page_num,
            slot_idx,
            updated_decoded
        );

        pending_updates.push(PendingUpdate {
            pointer: TuplePointer {
                page_id: page_num,
                slot_index,
            },
            old_tuple_data: tuple_data,
            new_bytes,
            old_decoded,
            updated_decoded,
            updated_values,
        });

        updated_count += 1;
    }

    // Intra-batch uniqueness validation: ensure no two updated rows in the same
    // statement produce duplicate keys for any UNIQUE constraint or index.
    let meta = crate::backend::cache::metadata(db_name, table_name);
    let mut has_intra_batch_conflict = false;

    // Check column-level UNIQUE constraints
    for (col_idx, col) in columns.iter().enumerate() {
        if !col.constraints.unique {
            continue;
        }
        let mut seen = std::collections::HashSet::new();
        for pu in &pending_updates {
            if let Some(Some(dv)) = pu.updated_values.get(col_idx) {
                let val_str = dv.to_string();
                if !seen.insert(val_str) {
                    has_intra_batch_conflict = true;
                    log::warn!(
                        "[UpdateByPointers] Batch UNIQUE violation on column '{}': duplicate value '{}'",
                        col.name,
                        dv
                    );
                    break;
                }
            }
        }
        if has_intra_batch_conflict {
            break;
        }
    }

    // Check unique indexes (including composite)
    if !has_intra_batch_conflict && let Some(ref m) = meta {
        for (idx_name, idx_cols) in &m.unique_indexes {
            let col_indices: Vec<usize> = idx_cols
                .iter()
                .filter_map(|c| {
                    columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if col_indices.len() != idx_cols.len() {
                continue;
            }

            let mut seen = std::collections::HashSet::new();
            for pu in &pending_updates {
                let mut has_null = false;
                let mut key_parts = Vec::with_capacity(col_indices.len());
                for &idx in &col_indices {
                    match pu.updated_values.get(idx) {
                        Some(Some(dv)) => key_parts.push(dv.to_string()),
                        _ => {
                            has_null = true;
                            break;
                        }
                    }
                }
                if !has_null {
                    let composite_key = key_parts.join("|||");
                    if !seen.insert(composite_key) {
                        has_intra_batch_conflict = true;
                        log::warn!(
                            "[UpdateByPointers] Batch UNIQUE violation on index '{}': duplicate key ({})",
                            idx_name,
                            key_parts.join(", ")
                        );
                        break;
                    }
                }
            }
            if has_intra_batch_conflict {
                break;
            }
        }
    }

    if has_intra_batch_conflict {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "UNIQUE constraint violated: the update gives two rows the same key".to_string(),
        ));
    }

    // An index rejects keys above its size limit. Check before Phase 2: once
    // rows are rewritten, a failed index update would leave them invisible
    // to index scans.
    if let Some(ref m) = meta {
        for (idx_name, idx_cols, _) in &m.named_indexes {
            let col_indices: Vec<usize> = idx_cols
                .iter()
                .filter_map(|c| {
                    columns
                        .iter()
                        .position(|col| col.name.eq_ignore_ascii_case(c))
                })
                .collect();
            if col_indices.len() != idx_cols.len() {
                continue;
            }
            for pu in &pending_updates {
                let key: Option<Vec<DataValue>> = col_indices
                    .iter()
                    .map(|&i| pu.updated_values.get(i).cloned().flatten())
                    .collect();
                // A NULL in the key is not indexed.
                let Some(key) = key else { continue };
                let len = crate::backend::index::btree::encoded_key_len(&key)?;
                if len > crate::backend::index::btree::MAX_ENCODED_KEY_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "index key of {} bytes for index '{}' exceeds the maximum of {} bytes",
                            len,
                            idx_name,
                            crate::backend::index::btree::MAX_ENCODED_KEY_SIZE
                        ),
                    ));
                }
            }
        }
    }

    // Phase 2: apply every pending update.
    //
    // A new row image of the same length is written over the old one: the row
    // keeps its (page, slot), no dead tuple is left behind, and indexes whose
    // key did not change need no maintenance. Any other row is replaced by
    // delete+insert as before.
    let mut in_place = vec![false; pending_updates.len()];
    if !pending_updates.is_empty() {
        let mut moved = 0u32;
        // Each run of updates on one page is applied to a single page image:
        // one read, one write and one lock per run rather than per row.
        let mut start = 0;
        while start < pending_updates.len() {
            let page_id = pending_updates[start].pointer.page_id;
            let mut end = start + 1;
            while end < pending_updates.len() && pending_updates[end].pointer.page_id == page_id {
                end += 1;
            }

            let _page_lock = PageWriteLock::acquire(file_identity, page_id);
            let mut page = Page::new();
            crate::disk::read_page(&mut file, &mut page, page_id)?;
            let mut moved_on_page = false;

            for (i, update) in pending_updates.iter().enumerate().take(end).skip(start) {
                let base =
                    (PAGE_HEADER_SIZE + update.pointer.slot_index as u32 * ITEM_ID_SIZE) as usize;
                let offset =
                    u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
                let length =
                    u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as usize;
                if update.new_bytes.len() == update.old_tuple_data.len()
                    && length == update.new_bytes.len()
                    && offset + length <= page.data.len()
                {
                    page.data[offset..offset + length].copy_from_slice(&update.new_bytes);
                    in_place[i] = true;
                    continue;
                }
                let mut flags =
                    u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                flags |= SLOT_FLAG_DELETED;
                page.data[base + 6..base + 8].copy_from_slice(&flags.to_le_bytes());
                moved_on_page = true;
                moved += 1;
            }

            crate::disk::write_page(&mut file, &mut page, page_id)?;
            if moved_on_page {
                let _ = crate::backend::visibility_map::vm_clear_page(db_name, table_name, page_id);
            }
            start = end;
        }
        if moved > 0 {
            crate::table::increment_dead_tuple_count(&mut file, moved)?;
        }
    }

    if !pending_updates.is_empty() {
        for (i, update) in pending_updates.iter().enumerate() {
            let (new_page_id, new_slot_id) = if in_place[i] {
                (update.pointer.page_id, update.pointer.slot_index as u32)
            } else {
                crate::backend::executor::compaction_api::insert_raw_tuple(
                    db_name,
                    table_name,
                    &update.new_bytes,
                )?
            };

            log::info!(
                "[Update] Row at (page={}, slot={}) now at (page={}, slot={})",
                update.pointer.page_id,
                update.pointer.slot_index,
                new_page_id,
                new_slot_id
            );

            if let Err(e) = crate::backend::executor::create_index::update_index_on_update(
                db_name,
                table_name,
                columns,
                &update.old_tuple_data,
                &update.new_bytes,
                update.pointer.page_id,
                update.pointer.slot_index as u32,
                new_page_id,
                new_slot_id,
            ) {
                log::warn!("Failed to update index for updated tuple: {}", e);
            }

            returning_rows.push(
                update
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
                    .collect(),
            );
        }
    }

    // FK cascade propagation
    if !pending_updates.is_empty() {
        for update in &pending_updates {
            if let Err(e) = crate::backend::constraint::propagate_update_to_children(
                catalog,
                db_name,
                table_name,
                &update.old_decoded,
                &update.updated_decoded,
            ) {
                log::warn!("[UpdateByPointers] Failed to propagate FK cascade: {}", e);
            }
        }
    }

    let result = UpdateResult {
        updated_count,
        returning_rows,
    };
    let details = update_log_details(Some(result.updated_count), None);
    let _ = crate::backend::log::operation_log::log_update(db_name, table_name, details, "success");

    Ok(result)
}
