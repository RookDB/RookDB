//! Implements DELETE with optional WHERE conditions and RETURNING support.
//!
//! Supports:
//!   DELETE FROM table;
//!   DELETE FROM table WHERE col = val;
//!   DELETE FROM table WHERE col > val AND col2 = val2;
//!   DELETE FROM table WHERE col = val OR col2 = val2;
//!   DELETE FROM table WHERE col = val AND col2 = val2 OR col3 = val3;
//!   DELETE FROM table WHERE col = val RETURNING *;
//!
//! Condition logic uses Disjunctive Normal Form (DNF):
//!   condition_groups is Vec<Vec<Condition>>
//!   Outer Vec → OR  (row matches if ANY group matches)
//!   Inner Vec → AND (group matches if ALL conditions in it match)

use std::fs::File;
use std::io;

use crate::catalog::types::{Catalog, Column};
use crate::disk::{read_page, write_page};
use crate::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE, PAGE_SIZE, SLOT_FLAG_DELETED};
use crate::table::page_count;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// SQL comparison operators supported in WHERE conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Eq,      // =
    Ne,      // !=
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
    Like,    // LIKE  '%pattern%'
    NotLike, // NOT LIKE '%pattern%'
    In,      // IN (v1, v2, ...)
    NotIn,   // NOT IN (v1, v2, ...)
}

/// A column value that can appear on the right-hand side of a condition.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int(i32),
    Text(String),
    /// Used by IN / NOT IN: list of literal values.
    List(Vec<ColumnValue>),
}

/// A single WHERE condition: `column operator value`.
/// Conditions are grouped in DNF: groups are OR-connected, conditions within a group are AND-connected.
#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: ColumnValue,
}

/// Result returned by `delete_tuples`.
pub struct DeleteResult {
    /// How many rows were deleted.
    pub deleted_count: usize,
    /// The deleted rows (only populated when `returning = true`).
    pub returning_rows: Vec<Vec<(String, String)>>,
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Decode a raw tuple byte slice into column name / value pairs.
fn decode_tuple(
    tuple_data: &[u8],
    columns: &[crate::catalog::types::Column],
) -> Vec<(String, ColumnValue)> {
    let mut result = Vec::new();
    let mut cursor = 0usize;

    for col in columns {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= tuple_data.len() {
                    let val =
                        i32::from_le_bytes(tuple_data[cursor..cursor + 4].try_into().unwrap());
                    result.push((col.name.clone(), ColumnValue::Int(val)));
                    cursor += 4;
                }
            }
            "TEXT" => {
                if cursor + 10 <= tuple_data.len() {
                    let text = String::from_utf8_lossy(&tuple_data[cursor..cursor + 10])
                        .trim_end_matches('\0')
                        .trim()
                        .to_string();
                    result.push((col.name.clone(), ColumnValue::Text(text)));
                    cursor += 10;
                }
            }
            _ => {}
        }
    }

    result
}

/// Returns `true` when a decoded tuple satisfies **all** conditions in the group (AND logic).
fn matches_and_group(decoded: &[(String, ColumnValue)], conditions: &[Condition]) -> bool {
    for cond in conditions {
        let col_val = decoded.iter().find(|(name, _)| *name == cond.column);

        match col_val {
            None => return false, // column not found → no match
            Some((_, actual)) => {
                let matched = match &cond.operator {
                    // ------ IN / NOT IN ----------------------------------------
                    Operator::In | Operator::NotIn => {
                        let list = match &cond.value {
                            ColumnValue::List(l) => l,
                            _ => return false,
                        };
                        let found = list.iter().any(|item| match (actual, item) {
                            (ColumnValue::Int(a),  ColumnValue::Int(b))  => a == b,
                            (ColumnValue::Text(a), ColumnValue::Text(b)) =>
                                a.eq_ignore_ascii_case(b),
                            _ => false,
                        });
                        if cond.operator == Operator::In { found } else { !found }
                    }

                    // ------ LIKE / NOT LIKE ------------------------------------
                    Operator::Like | Operator::NotLike => {
                        let pattern = match &cond.value {
                            ColumnValue::Text(p) => p.as_str(),
                            _ => return false,
                        };
                        let text = match actual {
                            ColumnValue::Text(t) => t.to_lowercase(),
                            ColumnValue::Int(_)  => return false, // LIKE on INT is an error — never matches
                            _ => return false,
                        };
                        let matched_like = like_match(&text, &pattern.to_lowercase());
                        if cond.operator == Operator::Like { matched_like } else { !matched_like }
                    }

                    // ------ Standard comparison --------------------------------
                    op => match (actual, &cond.value) {
                        (ColumnValue::Int(a), ColumnValue::Int(b)) => match op {
                            Operator::Eq => a == b,
                            Operator::Ne => a != b,
                            Operator::Lt => a < b,
                            Operator::Le => a <= b,
                            Operator::Gt => a > b,
                            Operator::Ge => a >= b,
                            _ => false,
                        },
                        (ColumnValue::Text(a), ColumnValue::Text(b)) => match op {
                            Operator::Eq => a.eq_ignore_ascii_case(b),
                            Operator::Ne => !a.eq_ignore_ascii_case(b),
                            Operator::Lt => a < b,
                            Operator::Le => a <= b,
                            Operator::Gt => a > b,
                            Operator::Ge => a >= b,
                            _ => false,
                        },
                        _ => false,
                    },
                };

                if !matched {
                    return false;
                }
            }
        }
    }
    true
}

/// SQL LIKE pattern matching.  Supports `%` (any sequence) and `_` (any single char).
/// Both `text` and `pattern` should already be lowercased by the caller.
fn like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_inner(&t, &p)
}

fn like_match_inner(t: &[char], p: &[char]) -> bool {
    match (t, p) {
        (_, [])            => t.is_empty(),
        (_, ['%', rest @ ..]) => {
            // % matches zero or more chars
            for i in 0..=t.len() {
                if like_match_inner(&t[i..], rest) { return true; }
            }
            false
        }
        ([], _)            => false,
        ([_tc, tr @ ..], ['_', pr @ ..]) => like_match_inner(tr, pr), // _ matches one char
        ([tc, tr @ ..], [pc, pr @ ..])   => tc == pc && like_match_inner(tr, pr),
    }
}

/// Returns `true` when a decoded tuple satisfies the DNF expression:
///   ANY of the AND-groups must match (OR between groups).
///
/// Empty `condition_groups` → matches everything (DELETE all).
/// Public so `update.rs` can reuse the same logic.
pub fn matches_condition_groups_pub(decoded: &[(String, ColumnValue)], condition_groups: &[Vec<Condition>]) -> bool {
    if condition_groups.is_empty() {
        return true;
    }
    condition_groups.iter().any(|group| matches_and_group(decoded, group))
}

// keep the private alias for delete_tuples internal use
fn matches_condition_groups(decoded: &[(String, ColumnValue)], condition_groups: &[Vec<Condition>]) -> bool {
    matches_condition_groups_pub(decoded, condition_groups)
}

/// Physically compact a page: remove all slots whose DELETED flag is set.
/// Called only by `compaction_table()`, not by `delete_tuples()`.
///
/// Slot layout: [offset: u32][length: u16][flags: u16]
fn compact_page(page: &mut Page, num_items: usize) {
    // Collect surviving tuple data (live slots only)
    let mut surviving: Vec<Vec<u8>> = Vec::new();

    for i in 0..num_items {
        let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
        let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap());
        let flags  = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

        // Skip empty or soft-deleted slots
        if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
            continue;
        }

        surviving.push(
            page.data[offset as usize..(offset as usize + length as usize)].to_vec(),
        );
    }

    // Zero out the entire page
    page.data.iter_mut().for_each(|b| *b = 0);

    // Re-initialise header pointers
    let mut lower = PAGE_HEADER_SIZE;
    let mut upper = PAGE_SIZE as u32;

    // Re-insert surviving tuples with fresh (unflagged) slots
    for tuple in &surviving {
        let start = upper - tuple.len() as u32;

        // Write tuple data
        page.data[start as usize..upper as usize].copy_from_slice(tuple);
        upper = start;

        // Write slot entry: [offset: u32][length: u16][flags: u16 = 0]
        page.data[lower as usize..lower as usize + 4]
            .copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 6]
            .copy_from_slice(&(tuple.len() as u16).to_le_bytes());
        page.data[lower as usize + 6..lower as usize + 8]
            .copy_from_slice(&0u16.to_le_bytes()); // flags = 0 (live)

        lower += ITEM_ID_SIZE;
    }

    // Persist updated lower and upper pointers
    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Delete tuples from a table that satisfy the DNF condition expression.
///
/// - `condition_groups` empty  → DELETE FROM table (all rows)
/// - Each inner `Vec<Condition>` is an AND-group; groups are OR-connected.
/// - `returning = true`  → collect deleted rows for RETURNING *
///
/// Examples:
///   WHERE a=1 AND b=2           → `[[{a=1},{b=2}]]`
///   WHERE a=1 OR b=2            → `[[{a=1}],[{b=2}]]`
///   WHERE a=1 AND b=2 OR c=3   → `[[{a=1},{b=2}],[{c=3}]]`
pub fn delete_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    condition_groups: &[Vec<Condition>],
    returning: bool,
) -> io::Result<DeleteResult> {
    // Resolve schema from catalog
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
    let total_pages = page_count(file)?;
    let mut deleted_count = 0usize;
    let mut returning_rows: Vec<Vec<(String, String)>> = Vec::new();

    // Iterate every data page (page 0 is the table header)
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        let mut slots_to_delete: Vec<usize> = Vec::new();

        for i in 0..num_items {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u16::from_le_bytes(page.data[base + 4..base + 6].try_into().unwrap()) as u32;
            let flags  = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

            // Skip empty or already soft-deleted slots
            if (offset == 0 && length == 0) || (flags & SLOT_FLAG_DELETED != 0) {
                continue;
            }

            let tuple_data = &page.data[offset as usize..(offset + length) as usize];
            let decoded = decode_tuple(tuple_data, columns);

            // Check conditions via DNF (empty groups = match everything)
            if matches_condition_groups(&decoded, condition_groups) {
                if returning {
                    let row: Vec<(String, String)> = decoded
                        .iter()
                        .map(|(col, val)| {
                            let s = match val {
                                ColumnValue::Int(n)    => n.to_string(),
                                ColumnValue::Text(t)   => t.clone(),
                                ColumnValue::List(_)   => String::from("[list]"),
                            };
                            (col.clone(), s)
                        })
                        .collect();
                    returning_rows.push(row);
                }

                slots_to_delete.push(i);
                deleted_count += 1;
            }
        }

        // Soft-delete: set SLOT_FLAG_DELETED on each matched slot (no physical removal)
        if !slots_to_delete.is_empty() {
            for idx in &slots_to_delete {
                let base = PAGE_HEADER_SIZE as usize + idx * ITEM_ID_SIZE as usize;
                let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
                let new_flags = flags | SLOT_FLAG_DELETED;
                page.data[base + 6..base + 8].copy_from_slice(&new_flags.to_le_bytes());
            }
            write_page(file, &mut page, page_num)?;
        }
    }

    Ok(DeleteResult {
        deleted_count,
        returning_rows,
    })
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// Physically remove all soft-deleted slots from every data page in a table.
///
/// Call this periodically (e.g. via menu option) rather than on every DELETE.
/// Returns the number of pages that were actually rewritten.
pub fn compaction_table(
    db_name: &str,
    table_name: &str,
) -> io::Result<usize> {
    use std::fs::OpenOptions;

    let path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

    let total_pages = page_count(&mut file)?;
    let mut pages_compacted = 0usize;

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;

        // Only rewrite the page if it has at least one soft-deleted slot
        let has_deleted = (0..num_items).any(|i| {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
            flags & SLOT_FLAG_DELETED != 0
        });

        if has_deleted {
            let upper_before = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
            println!(
                "  Page {:>3} BEFORE → lower={:>5}  upper={:>5}  slots={}",
                page_num, lower, upper_before, num_items
            );

            compact_page(&mut page, num_items);

            let lower_after = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let upper_after = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
            let slots_after = ((lower_after - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;
            println!(
                "  Page {:>3}  AFTER → lower={:>5}  upper={:>5}  slots={}  freed={}B",
                page_num, lower_after, upper_after, slots_after,
                (upper_after as i64 - upper_before as i64).abs()
                    + (lower as i64 - lower_after as i64).abs()
            );

            write_page(&mut file, &mut page, page_num)?;
            pages_compacted += 1;
        }
    }

    Ok(pages_compacted)
}

// ---------------------------------------------------------------------------
// WHERE clause parser  (tokenizer → AST → DNF)
// ---------------------------------------------------------------------------
//
// The user types the full WHERE expression as a single string, e.g.:
//   "price > 10"
//   "dept = HR AND salary < 50000"
//   "(dept = HR AND salary < 50000) OR (dept = Sales AND salary < 30000)"
//   "(c1 = 1 AND c2 = 2) AND (c3 = 3 OR c4 = 4)"
//
// Grammar (AND binds tighter than OR, same as SQL):
//   expr   := term  ( OR  term  )*
//   term   := factor ( AND factor )*
//   factor := '(' expr ')' | condition
//
// DNF conversion:
//   Or(L, R)  → concat  dnf(L) + dnf(R)
//   And(L, R) → cross-product  dnf(L) × dnf(R)
//   Leaf(c)   → [[c]]
//
// Example:
//   (c1=1 AND c2=2) AND (c3=3 OR c4=4)
//     AST : And( And(c1,c2), Or(c3,c4) )
//     DNF : [[c1,c2,c3], [c1,c2,c4]]

// -- tokens ----------------------------------------------------------

#[derive(Debug)]
enum Token {
    Cond(Condition),
    And,
    Or,
    LParen,
    RParen,
}

// -- AST -------------------------------------------------------------

enum BoolExpr {
    Leaf(Condition),
    And(Box<BoolExpr>, Box<BoolExpr>),
    Or(Box<BoolExpr>,  Box<BoolExpr>),
}

// -- atomic condition parser -----------------------------------------

/// Parse an IN-list string like `(1, 2, 'foo')` into a `Vec<ColumnValue>`.
/// Resolve a raw string to the correct `ColumnValue` type using the column's
/// declared schema type.  Falls back to try-parse-as-i32 when `columns` is empty
/// (e.g. in unit tests that don't supply a schema).
fn resolve_value(col_name: &str, raw: &str, columns: &[Column]) -> ColumnValue {
    if let Some(col) = columns.iter().find(|c| c.name.eq_ignore_ascii_case(col_name)) {
        return match col.data_type.as_str() {
            "INT" => raw.parse::<i32>()
                .map(ColumnValue::Int)
                .unwrap_or_else(|_| ColumnValue::Text(raw.to_string())),
            _ => ColumnValue::Text(raw.to_string()),
        };
    }
    // No schema info — heuristic fallback
    if let Ok(n) = raw.parse::<i32>() { ColumnValue::Int(n) } else { ColumnValue::Text(raw.to_string()) }
}

fn parse_in_list(col_name: &str, s: &str, columns: &[Column]) -> Vec<ColumnValue> {
    let inner = s.trim().trim_start_matches('(').trim_end_matches(')');
    inner.split(',').filter_map(|part| {
        let v = part.trim().trim_matches('\'').to_string();
        if v.is_empty() { return None; }
        Some(resolve_value(col_name, &v, columns))
    }).collect()
}

/// Find word-boundary position of `keyword` inside `upper` (already uppercased).
/// Returns the byte position of the keyword start, or `None`.
fn find_word(upper: &str, keyword: &str) -> Option<usize> {
    let klen = keyword.len();
    let mut start = 0;
    while let Some(pos) = upper[start..].find(keyword) {
        let abs = start + pos;
        // check boundary before
        let before_ok = abs == 0 || {
            let b = upper.as_bytes()[abs - 1];
            b == b' ' || b == b'\t' || b == b'('
        };
        // check boundary after
        let after_ok = abs + klen >= upper.len() || {
            let b = upper.as_bytes()[abs + klen];
            b == b' ' || b == b'\t' || b == b')' || b == b'('
        };
        if before_ok && after_ok { return Some(abs); }
        start = abs + 1;
    }
    None
}

/// Try to parse `col BETWEEN low AND high` from a buffer string.
/// Returns `(cond_ge, cond_le)` or `None`.
fn parse_between_condition(s: &str, columns: &[Column]) -> Option<(Condition, Condition)> {
    let upper = s.to_uppercase();
    let bet = find_word(&upper, "BETWEEN")?;
    let col = s[..bet].trim().to_string();
    let rest = &s[bet + 7..].trim_start(); // skip "BETWEEN"
    let rest_upper = rest.to_uppercase();
    let and_pos = find_word(&rest_upper, "AND")?;
    let low_str  = rest[..and_pos].trim().trim_matches('\'').to_string();
    let high_str = rest[and_pos + 3..].trim().trim_matches('\'').to_string();
    if col.is_empty() || low_str.is_empty() || high_str.is_empty() { return None; }
    let low  = resolve_value(&col, &low_str,  columns);
    let high = resolve_value(&col, &high_str, columns);
    Some((
        Condition { column: col.clone(), operator: Operator::Ge, value: low  },
        Condition { column: col,         operator: Operator::Le, value: high },
    ))
}

/// Parse a single atomic condition.
///
/// Handles (in priority order):
///   `col NOT LIKE 'pat'`  → Operator::NotLike
///   `col LIKE 'pat'`      → Operator::Like
///   `col NOT IN (v,...)`  → Operator::NotIn
///   `col IN (v,...)`      → Operator::In
///   `col >= val` etc.     → standard operators
///
/// Single quotes around text values are stripped automatically.
pub fn parse_condition(input: &str, columns: &[Column]) -> Option<Condition> {
    let input = input.trim();
    let upper = input.to_uppercase();

    // NOT LIKE
    if let Some(pos) = find_word(&upper, "NOT LIKE") {
        let col     = input[..pos].trim().to_string();
        let pattern = input[pos + 8..].trim().trim_matches('\'').to_string();
        if !col.is_empty() && !pattern.is_empty() {
            return Some(Condition { column: col, operator: Operator::NotLike, value: ColumnValue::Text(pattern) });
        }
    }
    // LIKE
    if let Some(pos) = find_word(&upper, "LIKE") {
        let col     = input[..pos].trim().to_string();
        let pattern = input[pos + 4..].trim().trim_matches('\'').to_string();
        if !col.is_empty() && !pattern.is_empty() {
            return Some(Condition { column: col, operator: Operator::Like, value: ColumnValue::Text(pattern) });
        }
    }
    // NOT IN
    if let Some(pos) = find_word(&upper, "NOT IN") {
        let col      = input[..pos].trim().to_string();
        let list_str = input[pos + 6..].trim();
        if !col.is_empty() && !list_str.is_empty() {
            return Some(Condition { column: col.clone(), operator: Operator::NotIn, value: ColumnValue::List(parse_in_list(&col, list_str, columns)) });
        }
    }
    // IN
    if let Some(pos) = find_word(&upper, "IN") {
        let col      = input[..pos].trim().to_string();
        let list_str = input[pos + 2..].trim();
        if !col.is_empty() && list_str.starts_with('(') {
            return Some(Condition { column: col.clone(), operator: Operator::In, value: ColumnValue::List(parse_in_list(&col, list_str, columns)) });
        }
    }

    // Standard comparison operators (try longest first)
    let ops: &[(&str, Operator)] = &[
        (">=", Operator::Ge),
        ("<=", Operator::Le),
        ("!=", Operator::Ne),
        (">" , Operator::Gt),
        ("<" , Operator::Lt),
        ("=" , Operator::Eq),
    ];
    for (op_str, op) in ops {
        if let Some(pos) = input.find(op_str) {
            let col     = input[..pos].trim().to_string();
            let val_str = input[pos + op_str.len()..].trim().trim_matches('\'').to_string();
            if col.is_empty() || val_str.is_empty() { continue; }
            let value = resolve_value(&col, &val_str, columns);
            return Some(Condition { column: col, operator: op.clone(), value });
        }
    }
    None
}

// -- tokenizer -------------------------------------------------------

/// Scan the WHERE string into a flat list of `Token`s.
///
/// - `(` / `)` become `LParen` / `RParen`.
/// - `AND` / `OR` (case-insensitive, word-boundary) become `And` / `Or`.
/// - `BETWEEN low AND high` is expanded to two `Cond` tokens with an `And` between them.
/// - LIKE / NOT LIKE / IN / NOT IN are handled inside `parse_condition`.
/// - When inside a BETWEEN clause, the separator AND is kept in the buffer.
fn tokenize(input: &str, columns: &[Column]) -> Vec<Token> {
    let mut tokens: Vec<Token> = Vec::new();
    let chars: Vec<char>       = input.chars().collect();
    let n                      = chars.len();
    let mut i                  = 0usize;
    let mut buf                = String::new();

    macro_rules! flush_buf {
        () => {{
            let s = buf.trim().to_string();
            buf.clear();
            if !s.is_empty() {
                if let Some((c_ge, c_le)) = parse_between_condition(&s, columns) {
                    // BETWEEN expands to two conditions joined by AND
                    tokens.push(Token::Cond(c_ge));
                    tokens.push(Token::And);
                    tokens.push(Token::Cond(c_le));
                } else if let Some(c) = parse_condition(&s, columns) {
                    tokens.push(Token::Cond(c));
                }
            }
        }};
    }

    while i < n {
        // Parentheses — but don't break on '(' that is part of IN (...)
        if chars[i] == '(' {
            // Peek back: if the buffer (trimmed) ends with IN or NOT IN, keep '(' in buffer
            let buf_upper = buf.trim().to_uppercase();
            if buf_upper.ends_with(" IN") || buf_upper.ends_with("NOT IN") || buf_upper == "IN" {
                buf.push('(');
                i += 1;
                continue;
            }
            flush_buf!();
            tokens.push(Token::LParen);
            i += 1;
            continue;
        }
        if chars[i] == ')' {
            // If buffer contains IN (, keep ')' in buffer to close the list
            let buf_upper = buf.to_uppercase();
            if buf_upper.contains(" IN (") || buf_upper.contains("NOT IN (") || buf_upper.starts_with("IN (") {
                buf.push(')');
                i += 1;
                continue;
            }
            flush_buf!();
            tokens.push(Token::RParen);
            i += 1;
            continue;
        }

        // AND / OR keyword detection (case-insensitive, word-boundary)
        let remaining: String = chars[i..].iter().collect();
        let upper = remaining.to_uppercase();

        if upper.starts_with("AND") {
            let after = upper.chars().nth(3).unwrap_or(' ');
            if after.is_whitespace() || after == '(' {
                // If we're currently inside a BETWEEN clause, this AND is its separator
                let buf_upper = buf.to_uppercase();
                if find_word(&buf_upper, "BETWEEN").is_some() {
                    buf.push_str(" AND ");
                    i += 3;
                    while i < n && chars[i].is_whitespace() { i += 1; }
                    continue;
                }
                flush_buf!();
                tokens.push(Token::And);
                i += 3;
                while i < n && chars[i].is_whitespace() { i += 1; }
                continue;
            }
        }
        if upper.starts_with("OR") {
            let after = upper.chars().nth(2).unwrap_or(' ');
            if after.is_whitespace() || after == '(' {
                flush_buf!();
                tokens.push(Token::Or);
                i += 2;
                while i < n && chars[i].is_whitespace() { i += 1; }
                continue;
            }
        }

        buf.push(chars[i]);
        i += 1;
    }
    flush_buf!();
    tokens
}

// -- recursive-descent parser ----------------------------------------

struct Parser { tokens: Vec<Token>, pos: usize }

impl Parser {
    fn new(tokens: Vec<Token>) -> Self { Parser { tokens, pos: 0 } }
    fn peek(&self) -> Option<&Token>   { self.tokens.get(self.pos) }

    /// expr := term ( OR term )*
    fn parse_expr(&mut self) -> Option<BoolExpr> {
        let mut left = self.parse_term()?;
        while let Some(Token::Or) = self.peek() {
            self.pos += 1;
            let right = self.parse_term()?;
            left = BoolExpr::Or(Box::new(left), Box::new(right));
        }
        Some(left)
    }

    /// term := factor ( AND factor )*
    fn parse_term(&mut self) -> Option<BoolExpr> {
        let mut left = self.parse_factor()?;
        while let Some(Token::And) = self.peek() {
            self.pos += 1;
            let right = self.parse_factor()?;
            left = BoolExpr::And(Box::new(left), Box::new(right));
        }
        Some(left)
    }

    /// factor := '(' expr ')' | condition
    fn parse_factor(&mut self) -> Option<BoolExpr> {
        match self.peek()? {
            Token::LParen => {
                self.pos += 1;
                let expr = self.parse_expr()?;
                if let Some(Token::RParen) = self.peek() { self.pos += 1; }
                Some(expr)
            }
            Token::Cond(_) => {
                // Replace the token with a dummy so we can take ownership.
                let tok = std::mem::replace(&mut self.tokens[self.pos], Token::And);
                self.pos += 1;
                if let Token::Cond(c) = tok { Some(BoolExpr::Leaf(c)) } else { None }
            }
            _ => None,
        }
    }
}

// -- DNF conversion --------------------------------------------------

/// Convert a `BoolExpr` AST to Disjunctive Normal Form.
///
/// Outer Vec = OR-connected groups; inner Vec = AND-connected conditions.
///
/// `And(L, R)` is handled by cross-product, which correctly distributes AND
/// over OR.  Example:
///   `(c1 AND c2) AND (c3 OR c4)` → `[[c1,c2,c3], [c1,c2,c4]]`
fn to_dnf(expr: BoolExpr) -> Vec<Vec<Condition>> {
    match expr {
        BoolExpr::Leaf(c)    => vec![vec![c]],
        BoolExpr::Or(l, r)   => { let mut d = to_dnf(*l); d.extend(to_dnf(*r)); d }
        BoolExpr::And(l, r)  => {
            let l_dnf = to_dnf(*l);
            let r_dnf = to_dnf(*r);
            let mut result = Vec::new();
            for lg in &l_dnf {
                for rg in &r_dnf {
                    let mut group = lg.clone();
                    group.extend(rg.iter().cloned());
                    result.push(group);
                }
            }
            result
        }
    }
}

// -- public API ------------------------------------------------------

/// Parse a full WHERE clause string into DNF groups ready for `delete_tuples`.
///
/// Returns `None` for empty input (caller treats as DELETE ALL).
///
/// # Examples
/// ```
/// "price > 10"
///   → [[price>10]]
///
/// "dept = HR AND salary < 50000"
///   → [[dept=HR, salary<50000]]
///
/// "(dept = HR AND salary < 50000) OR dept = Sales"
///   → [[dept=HR, salary<50000], [dept=Sales]]
///
/// "(c1 = 1 AND c2 = 2) AND (c3 = 3 OR c4 = 4)"
///   → [[c1=1, c2=2, c3=3], [c1=1, c2=2, c4=4]]
/// ```
/// Schema-aware WHERE parser.  Values are typed according to the column's
/// declared type in `columns`.  Pass `&[]` to fall back to the try-parse-as-i32
/// heuristic (used by tests and any caller without schema info).
pub fn parse_where_clause_with_schema(input: &str, columns: &[Column]) -> Option<Vec<Vec<Condition>>> {
    let s = input.trim();
    if s.is_empty() { return None; }
    let tokens = tokenize(s, columns);
    if tokens.is_empty() { return None; }
    let ast = Parser::new(tokens).parse_expr()?;
    Some(to_dnf(ast))
}

/// Convenience wrapper — schema-less, falls back to try-parse heuristic.
/// Kept for backward compatibility with tests and callers that don't supply schema.
pub fn parse_where_clause(input: &str) -> Option<Vec<Vec<Condition>>> {
    parse_where_clause_with_schema(input, &[])
}
