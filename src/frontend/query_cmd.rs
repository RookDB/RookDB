//! All query commands — in-memory and streaming paths.
//! Every operator supported: =, !=, <, <=, >, >=, LIKE, NOT LIKE,
//! BETWEEN, IN, IS NULL, IS NOT NULL, AND, OR, NOT.

use std::collections::HashMap;
use std::io::{self, Write};

use storage_manager::catalog::load_catalog;
use storage_manager::catalog::types::Column;
use storage_manager::executor::expr::Expr;
use storage_manager::executor::projection::{
    project, select, ProjectionInput, ProjectionItem, ResultTable,
};
use storage_manager::executor::set_ops::{except, intersect, union};
use storage_manager::executor::streaming::{
    stream_count, stream_dedup_scan, stream_project, stream_select,
};
use storage_manager::executor::value::Value;

// ─── I/O helpers ─────────────────────────────────────────────────────────────

fn prompt(msg: &str) -> io::Result<String> {
    print!("{}", msg);
    io::stdout().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(s.trim().to_string())
}

fn require_db(current_db: &Option<String>) -> Option<String> {
    match current_db {
        Some(db) => Some(db.clone()),
        None => { println!("  No database selected. Choose option 3 first."); None }
    }
}

fn get_schema(db: &str, table: &str) -> io::Result<Vec<Column>> {
    let catalog = load_catalog();
    let db_entry = catalog.databases.get(db)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
            format!("Database '{}' not found", db)))?;
    let tbl = db_entry.tables.get(table)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound,
            format!("Table '{}' not found", table)))?;
    Ok(tbl.columns.clone())
}

// ─── Value parser ────────────────────────────────────────────────────────────

fn parse_value(s: &str) -> Value {
    let s = s.trim();
    if s.eq_ignore_ascii_case("null")  { return Value::Null; }
    if s.eq_ignore_ascii_case("true")  { return Value::Bool(true); }
    if s.eq_ignore_ascii_case("false") { return Value::Bool(false); }
    if let Ok(i) = s.parse::<i64>()   { return Value::Int(i); }
    if let Ok(f) = s.parse::<f64>()   { return Value::Float(f); }
    Value::Text(s.trim_matches('\'').trim_matches('"').to_string())
}

// ─── WHERE builder — all operators ───────────────────────────────────────────

fn show_schema(cols: &[Column]) {
    println!("\n  Columns:");
    for (i, c) in cols.iter().enumerate() {
        println!("    [{}] {}  ({})", i, c.name, c.data_type);
    }
}

fn show_operators() {
    println!("  Operators:");
    println!("    =  !=  <  <=  >  >=");
    println!("    like        (% and _ wildcards)");
    println!("    notlike");
    println!("    between     (will ask for low and high)");
    println!("    in          (will ask for comma-separated list)");
    println!("    notin");
    println!("    isnull");
    println!("    isnotnull");
    println!("  Logical: after each condition choose  and / or / not / done");
}

/// Interactively build one atomic condition.
fn build_one_condition(schema: &[Column]) -> io::Result<Option<Expr>> {
    let col_name = prompt("    Column name: ")?;
    if col_name.is_empty() { return Ok(None); }

    let idx = match schema.iter().position(|c| c.name.eq_ignore_ascii_case(&col_name)) {
        Some(i) => i,
        None => { println!("    Column '{}' not found.", col_name); return Ok(None); }
    };

    let op = prompt("    Operator: ")?.to_lowercase();
    let op = op.trim().to_string();

    let expr: Option<Expr> = match op.as_str() {
        "isnull" | "is null" =>
            Some(Expr::IsNull(Box::new(Expr::col(idx)))),
        "isnotnull" | "is not null" =>
            Some(Expr::IsNotNull(Box::new(Expr::col(idx)))),
        "between" => {
            let lo_s = prompt("    Low value:  ")?;
            let hi_s = prompt("    High value: ")?;
            let lo = parse_value(&lo_s);
            let hi = parse_value(&hi_s);
            Some(Expr::Between(
                Box::new(Expr::col(idx)),
                Box::new(Expr::Const(lo)),
                Box::new(Expr::Const(hi)),
            ))
        }
        "in" => {
            let list_s = prompt("    Values (comma-separated): ")?;
            let items: Vec<Expr> = list_s
                .split(',')
                .map(|v| Expr::Const(parse_value(v.trim())))
                .collect();
            Some(Expr::In(Box::new(Expr::col(idx)), items))
        }
        "notin" | "not in" => {
            let list_s = prompt("    Values (comma-separated): ")?;
            let items: Vec<Expr> = list_s
                .split(',')
                .map(|v| Expr::Const(parse_value(v.trim())))
                .collect();
            Some(Expr::NotIn(Box::new(Expr::col(idx)), items))
        }
        "like" => {
            let pat = prompt("    Pattern: ")?;
            Some(Expr::Like(
                Box::new(Expr::col(idx)),
                Box::new(Expr::Const(Value::Text(pat))),
            ))
        }
        "notlike" | "not like" => {
            let pat = prompt("    Pattern: ")?;
            Some(Expr::NotLike(
                Box::new(Expr::col(idx)),
                Box::new(Expr::Const(Value::Text(pat))),
            ))
        }
        _ => {
            let val_s = prompt("    Value: ")?;
            let val = parse_value(&val_s);
            let lhs = Expr::col(idx);
            let rhs = Expr::Const(val);
            match op.as_str() {
                "="  | "==" | "eq" => Some(Expr::eq(lhs, rhs)),
                "!=" | "<>" | "ne" => Some(Expr::ne(lhs, rhs)),
                "<"  | "lt"        => Some(Expr::lt(lhs, rhs)),
                "<=" | "le"        => Some(Expr::le(lhs, rhs)),
                ">"  | "gt"        => Some(Expr::gt(lhs, rhs)),
                ">=" | "ge"        => Some(Expr::ge(lhs, rhs)),
                _ => {
                    println!("    Unknown operator '{}'.", op);
                    None
                }
            }
        }
    };
    Ok(expr)
}

/// Build a full predicate with AND / OR / NOT chaining.
pub fn build_predicate(schema: &[Column]) -> io::Result<Option<Expr>> {
    show_schema(schema);
    show_operators();
    println!("  (press Enter with blank column to finish / skip WHERE)\n");

    let mut stack: Vec<Expr> = Vec::new();

    loop {
        let cond = build_one_condition(schema)?;
        let cond = match cond {
            Some(c) => c,
            None => break,
        };

        // Ask how to combine
        let combinator = if stack.is_empty() {
            "first".to_string()
        } else {
            prompt("    Combine with previous (and/or): ")?.to_lowercase()
        };

        // Wrap in NOT?
        let negate = prompt("    Negate this condition? (y/n): ")?.to_lowercase();
        let cond = if negate == "y" { Expr::not(cond) } else { cond };

        if stack.is_empty() {
            stack.push(cond);
        } else {
            let prev = stack.pop().unwrap();
            let combined = if combinator == "or" {
                Expr::or(prev, cond)
            } else {
                Expr::and(prev, cond)
            };
            stack.push(combined);
        }

        println!("    Condition added (total depth: {}).", stack.len());
        let more = prompt("    Add another condition? (y/n): ")?.to_lowercase();
        if more != "y" { break; }
    }

    Ok(stack.into_iter().next())
}

/// Pick columns / * for SELECT list.
fn build_projection(schema: &[Column]) -> io::Result<Vec<ProjectionItem>> {
    show_schema(schema);
    println!("  Enter column names (comma-separated) or * for all:");
    let input = prompt("  Columns: ")?;
    if input.trim() == "*" || input.trim().is_empty() {
        return Ok(vec![ProjectionItem::Star]);
    }
    let mut items = Vec::new();
    for part in input.split(',') {
        let name = part.trim();
        match schema.iter().position(|c| c.name.eq_ignore_ascii_case(name)) {
            Some(idx) => items.push(ProjectionItem::Expr(Expr::col(idx), name.to_string())),
            None => println!("  Warning: column '{}' not found, skipped.", name),
        }
    }
    if items.is_empty() {
        println!("  No valid columns — defaulting to *");
        Ok(vec![ProjectionItem::Star])
    } else {
        Ok(items)
    }
}

fn ask_limit() -> io::Result<Option<usize>> {
    let s = prompt("  LIMIT (blank = no limit): ")?;
    if s.is_empty() { return Ok(None); }
    Ok(s.parse::<usize>().ok())
}

// ─── Option 8: SELECT * ──────────────────────────────────────────────────────

pub fn show_all_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let catalog = load_catalog();
    let result = select(&catalog, &db, &table, None)?;
    result.print();
    Ok(())
}

// ─── Option 9: SELECT * WHERE ────────────────────────────────────────────────

pub fn select_where_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let pred = build_predicate(&schema)?;
    let catalog = load_catalog();
    let result = select(&catalog, &db, &table, pred)?;
    result.print();
    Ok(())
}

// ─── Option 10: PROJECT ──────────────────────────────────────────────────────

pub fn project_columns_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let items = build_projection(&schema)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let catalog = load_catalog();
    let result = project(ProjectionInput {
        catalog: &catalog, db_name: &db, table_name: &table,
        items, predicate: pred, distinct: false, cte_tables: HashMap::new(),
    })?;
    result.print();
    Ok(())
}

// ─── Option 11: DISTINCT ─────────────────────────────────────────────────────

pub fn select_distinct_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let items = build_projection(&schema)?;
    let catalog = load_catalog();
    let result = project(ProjectionInput {
        catalog: &catalog, db_name: &db, table_name: &table,
        items, predicate: None, distinct: true, cte_tables: HashMap::new(),
    })?;
    println!("  [DISTINCT result]");
    result.print();
    Ok(())
}

// ─── Option 12: COUNT ────────────────────────────────────────────────────────

pub fn count_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let catalog = load_catalog();
    let result = select(&catalog, &db, &table, pred)?;
    println!("  COUNT = {}", result.rows.len());
    Ok(())
}

// ─── Set ops (13-15) ─────────────────────────────────────────────────────────

fn load_table(db: &str, table: &str) -> io::Result<ResultTable> {
    let catalog = load_catalog();
    select(&catalog, db, table, None)
}

pub fn union_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let a_name = prompt("  First table:  ")?;
    let b_name = prompt("  Second table: ")?;
    if a_name.is_empty() || b_name.is_empty() { return Ok(()); }
    let all = prompt("  UNION ALL? (y/n): ")?.to_lowercase() == "y";
    let result = union(load_table(&db, &a_name)?, load_table(&db, &b_name)?, all)?;
    println!("  [UNION{}]", if all { " ALL" } else { "" });
    result.print();
    Ok(())
}

pub fn intersect_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let a_name = prompt("  First table:  ")?;
    let b_name = prompt("  Second table: ")?;
    if a_name.is_empty() || b_name.is_empty() { return Ok(()); }
    let all = prompt("  INTERSECT ALL? (y/n): ")?.to_lowercase() == "y";
    let result = intersect(load_table(&db, &a_name)?, load_table(&db, &b_name)?, all)?;
    println!("  [INTERSECT{}]", if all { " ALL" } else { "" });
    result.print();
    Ok(())
}

pub fn except_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let a_name = prompt("  First table:  ")?;
    let b_name = prompt("  Second table: ")?;
    if a_name.is_empty() || b_name.is_empty() { return Ok(()); }
    let all = prompt("  EXCEPT ALL? (y/n): ")?.to_lowercase() == "y";
    let result = except(load_table(&db, &a_name)?, load_table(&db, &b_name)?, all)?;
    println!("  [EXCEPT{}]", if all { " ALL" } else { "" });
    result.print();
    Ok(())
}

// ─── Streaming (16-19) ───────────────────────────────────────────────────────

pub fn stream_select_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let limit = ask_limit()?;
    let catalog = load_catalog();

    println!("  [STREAM SELECT — reading page by page]");
    let result = stream_select(&catalog, &db, &table, pred.as_ref(), limit)?;
    result.print();
    Ok(())
}

pub fn stream_project_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let items = build_projection(&schema)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let distinct = prompt("  DISTINCT? (y/n): ")?.to_lowercase() == "y";
    let limit = ask_limit()?;
    let catalog = load_catalog();

    println!("  [STREAM PROJECT — reading page by page]");
    let result = stream_project(&catalog, &db, &table, &items, pred.as_ref(), distinct, limit)?;
    result.print();
    Ok(())
}

pub fn stream_count_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let catalog = load_catalog();

    println!("  [STREAM COUNT — counting without loading rows]");
    let result = stream_count(&catalog, &db, &table, pred.as_ref())?;
    result.print();
    Ok(())
}

pub fn stream_dedup_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match require_db(current_db) { Some(d) => d, None => return Ok(()) };
    let table = prompt("  Table: ")?;
    if table.is_empty() { return Ok(()); }
    let schema = get_schema(&db, &table)?;
    let add_where = prompt("  Add WHERE? (y/n): ")?.to_lowercase();
    let pred = if add_where == "y" { build_predicate(&schema)? } else { None };
    let limit = ask_limit()?;
    let catalog = load_catalog();

    println!("  [STREAM DEDUP SCAN — skips duplicate rows on the fly]");
    let result = stream_dedup_scan(&catalog, &db, &table, pred.as_ref(), limit)?;
    result.print();
    Ok(())
}
