use std::fs::OpenOptions;
use std::io::{self, Write};

use serde_json_path::JsonPath;
use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::executor::predicate::{CmpOp, Datum, Expr, Predicate};
use storage_manager::executor::show_tuples;
use storage_manager::table::page_count;

pub fn load_csv_cmd(
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let mut csv_path = String::new();
    print!("Enter CSV path: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut csv_path)?;
    let csv_path = csv_path.trim();

    let catalog = load_catalog();
    buffer_manager.load_csv_to_buffer(&catalog, &db, table, csv_path)?;

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    println!("Page Count: {}", page_count(&mut file)?);

    Ok(())
}

pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let catalog = load_catalog();
    show_tuples(&catalog, &db, table, &mut file, None)?;

    Ok(())
}

fn read_line(prompt: &str) -> io::Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(buf.trim().to_string())
}

/// Filter Tuples: prompts for a column, optional JSONPath, `=`/`!=`, and a
/// literal, then runs `show_tuples` with the resulting predicate.
pub fn filter_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            return Ok(());
        }
    };

    let table = read_line("Enter table name: ")?;

    let catalog = load_catalog();
    let columns = match catalog
        .databases
        .get(&db)
        .and_then(|d| d.tables.get(&table))
        .map(|t| t.columns.clone())
    {
        Some(cols) => cols,
        None => {
            println!("Table '{}.{}' not found", db, table);
            return Ok(());
        }
    };

    println!("Columns:");
    for (i, c) in columns.iter().enumerate() {
        println!("  {}. {} ({})", i + 1, c.name, c.data_type);
    }

    let col_choice = read_line("Pick column number: ")?;
    let col_idx: usize = match col_choice.parse::<usize>() {
        Ok(n) if n >= 1 && n <= columns.len() => n - 1,
        _ => {
            println!("Invalid column number");
            return Ok(());
        }
    };
    let col = &columns[col_idx];

    // For JSON / JSONB columns, prompt for an optional JSONPath.
    let lhs = if col.data_type == "JSON" || col.data_type == "JSONB" {
        let path_str = read_line("JSONPath (e.g. $.score, blank for whole value): ")?;
        if path_str.is_empty() {
            Expr::Column(col_idx)
        } else {
            match JsonPath::parse(&path_str) {
                Ok(path) => Expr::JsonPath(col_idx, path),
                Err(e) => {
                    println!("Invalid JSONPath: {}", e);
                    return Ok(());
                }
            }
        }
    } else {
        Expr::Column(col_idx)
    };

    let op_choice = read_line("Operator (= or !=): ")?;
    let op = match op_choice.as_str() {
        "=" | "==" | "eq" => CmpOp::Eq,
        "!=" | "<>" | "ne" => CmpOp::Ne,
        _ => {
            println!("Only '=' and '!=' are supported.");
            return Ok(());
        }
    };

    let literal_str = read_line("Value: ")?;
    let literal = parse_literal(&literal_str, &col.data_type, matches!(lhs, Expr::JsonPath(..)));

    let predicate = Predicate::Cmp(lhs, op, Expr::Literal(literal));

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    show_tuples(&catalog, &db, &table, &mut file, Some(&predicate))?;

    Ok(())
}

/// Parse a literal string from the prompt into a `Datum`. For JSONPath
/// predicates we always coerce to text/number/bool because that's what the
/// path will return; for plain columns we follow the column's type.
fn parse_literal(raw: &str, col_type: &str, is_path: bool) -> Datum {
    if raw.eq_ignore_ascii_case("null") {
        return Datum::Null;
    }
    if is_path {
        if let Ok(n) = raw.parse::<f64>() {
            return Datum::Number(n);
        }
        match raw {
            "true" => return Datum::Bool(true),
            "false" => return Datum::Bool(false),
            _ => {}
        }
        return Datum::Text(raw.to_string());
    }
    match col_type {
        "INT" => raw.parse::<i32>().map(Datum::Int).unwrap_or(Datum::Null),
        "BOOLEAN" => match raw.to_lowercase().as_str() {
            "true" | "1" | "yes" => Datum::Bool(true),
            "false" | "0" | "no" => Datum::Bool(false),
            _ => Datum::Null,
        },
        _ => Datum::text(raw),
    }
}
