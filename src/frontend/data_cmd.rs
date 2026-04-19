use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::executor::show_tuples;
use storage_manager::table::page_count;

use storage_manager::executor::{AggFunc, AggReq};
use storage_manager::executor::hash_aggregator::execute_aggregation;

use storage_manager::executor::expr::{Expr, ComparisonOperator, BinaryOperator};
use storage_manager::executor::value::Value;

pub fn aggregate_query_cmd(buffer_manager: &mut BufferManager, current_db: &Option<String>) -> io::Result<()> {
    // if current_db.is_none() {
    //     println!("No database selected. Please select a database first");
    //     return Ok(());
    let db = match current_db {
        Some(d) => d.clone(),
        None => {
            println!("{{Status: 400, Message: \"Failed\", Error: \"No database selected. Please select a database first\"}}");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let _table = table.trim();

    let catalog = load_catalog();
    let db_obj = match catalog.databases.get(&db) {
        Some(d) => d,
        None => { println!("{{Status: 400, Message: \"Failed\", Error: \"Database not found.\"}}"); return Ok(()); }
    };
    let table_obj = match db_obj.tables.get(_table) {
        Some(t) => t,
        None => { println!("{{Status: 400, Message: \"Failed\", Error: \"Table '{}' not found.\"}}", _table); return Ok(()); }
    };
    let schema = table_obj.columns.clone();

    let mut agg_str = String::new();
    print!("Enter aggregation function (COUNT*, COUNT, SUM, MIN, MAX, AVG, COUNT DISTINCT, SUM DISTINCT, VARIANCE, STDDEV, BOOL_AND, BOOL_OR): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut agg_str)?;
    let agg_str = agg_str.trim().to_uppercase();

    let mut col_str = String::new();
    print!("Enter target column name (e.g., id or score): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut col_str)?;
    let col_name = col_str.trim();
    let col_idx: usize = match schema.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)) {
        Some(idx) => idx,
        None => {
            if agg_str == "COUNT*" { 0 } else {
                println!("{{Status: 400, Message: \"Failed\", Error: \"Column '{}' not found.\"}}", col_name);
                return Ok(());
            }
        }
    };

    let agg_type = match agg_str.as_str() {
        "COUNT*" => AggFunc::CountStar,
        "COUNT" => AggFunc::Count,
        "SUM" => AggFunc::Sum,
        "MIN" => AggFunc::Min,
        "MAX" => AggFunc::Max,
        "AVG" => AggFunc::Avg,
        "COUNT DISTINCT" => AggFunc::CountDistinct,
        "SUM DISTINCT" => AggFunc::SumDistinct,
        "VARIANCE" => AggFunc::Variance,
        "STDDEV" => AggFunc::StdDev,
        "BOOL_AND" => AggFunc::BoolAnd,
        "BOOL_OR" => AggFunc::BoolOr,
        _ => {
            println!("{{Status: 400, Message: \"Failed\", Error: \"Unsupported aggregation function: {}\"}}", agg_str);
            return Ok(());
        }
    };

    let reqs = vec![AggReq {
        agg_type,
        col_index: Some(col_idx),
    }];

    if let Err(e) = buffer_manager.load_table_from_disk(&db, _table) {
        println!("Buffer load error: {}", e);
        return Err(e);
    }

    let path = format!("database/base/{}/{}.dat", db, _table);
    let mut file = std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
    let total_pages = page_count(&mut file)?;

    let seq_scan = storage_manager::executor::seq_scan_iter::SeqScan::new(
        _table.to_string(),
        std::sync::Arc::new(buffer_manager.clone()),
        total_pages,
        schema.clone(),
    );

    let child_node = Box::new(seq_scan);
    
    let mut group_str = String::new();
    print!("Enter group by column names (comma-separated, e.g., id, department) [Leave empty for ALL columns]: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut group_str)?;
    
    let mut group_by_cols = Vec::new();
    let group_str = group_str.trim();
    if !group_str.is_empty() {
        for col_n in group_str.split(',') {
            let col_name_trimmed = col_n.trim();
            if let Some(idx) = schema.iter().position(|c| c.name.eq_ignore_ascii_case(col_name_trimmed)) {
                group_by_cols.push(idx);
            } else {
                println!("{{Status: 400, Message: \"Failed\", Error: \"Group By Column '{}' not found.\"}}", col_name_trimmed);
                return Ok(());
            }
        }
    } else {
        for i in 0..table_obj.columns.len() {
            if i != col_idx {
                group_by_cols.push(i);
            }
        }
    }

    // Determine output columns for having parsing
    let mut out_cols = Vec::new();
    for &idx in &group_by_cols {
        out_cols.push(schema[idx].name.clone());
    }
    out_cols.push(agg_str.clone()); // "SUM", "COUNT", etc.

    let out_col_names = out_cols.join(", ");
    let mut having_str = String::new();
    print!("Enter HAVING expression (e.g., id + 1 > SUM, or {} > 100) Output columns are [{}], [Leave empty to skip]: ", agg_str, out_col_names);
    io::stdout().flush()?;
    io::stdin().read_line(&mut having_str)?;
    
    let mut having_expr: Option<Expr> = None;
    let having_trimmed = having_str.trim();
    if !having_trimmed.is_empty() {
        having_expr = parse_having(having_trimmed, &out_cols);
        if having_expr.is_none() {
            println!("Failed to parse HAVING expression. Proceeding without it.");
        }
    }

    println!("
Executing Aggregation...");
    let result = execute_aggregation(child_node, reqs, group_by_cols, having_expr);
    match result {
        Ok(data) => {
            if data.is_empty() {
                println!("\nResult:\n(Empty Table)");
            } else {
                println!("\nResult:");
                let mut widths = vec![0; data[0].values.len()];
                let mut str_data: Vec<Vec<String>> = Vec::new();
                
                for tuple in &data {
                    let mut row = Vec::new();
                    for (i, val) in tuple.values.iter().enumerate() {
                        let s = match val {
                            Value::Int(v) => v.to_string(),
                            Value::Text(t) => t.clone(),
                            Value::Float(f) => format!("{:.2}", f.into_inner()),
                            Value::Boolean(b) => b.to_string(),
                            Value::Null => "NULL".to_string(),
                        };
                        if i < widths.len() {
                            widths[i] = std::cmp::max(widths[i], s.len());
                        } else {
                            widths.push(s.len());
                        }
                        row.push(s);
                    }
                    str_data.push(row);
                }

                let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w + 2)).collect();
                let separator = format!("+{}+", sep.join("+"));
                
                println!("{}", separator);
                for row in str_data {
                    let mut formatted_row = Vec::new();
                    for (i, s) in row.iter().enumerate() {
                        let w = if i < widths.len() { widths[i] } else { s.len() };
                        formatted_row.push(format!(" {:<width$} ", s, width = w));
                    }
                    println!("|{}|", formatted_row.join("|"));
                }
                println!("{}", separator);
            }
            println!("{{Status: 200, Message: \"Success\", Error: \"None\"}}");
        }
        Err(e) => {
            println!("{{Status: {}, Message: \"Failed\", Error: \"{}\"}}", e.status_code(), e);
        }
    }

    Ok(())
}

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
    show_tuples(&catalog, &db, table, &mut file)?;

    Ok(())
}


fn parse_expr_side(s: &str, cols: &[String]) -> Option<Expr> {
    let s = s.trim();
    if s.is_empty() { return None; }
    
    for (op_str, op_enum) in [("+", BinaryOperator::Add), ("-", BinaryOperator::Sub)] {
        if let Some(idx) = s.rfind(op_str) {
            let left = parse_expr_side(&s[..idx], cols)?;
            let right = parse_expr_side(&s[idx + op_str.len()..], cols)?;
            return Some(Expr::BinaryOp { left: Box::new(left), op: op_enum, right: Box::new(right) });
        }
    }
    
    for (op_str, op_enum) in [("*", BinaryOperator::Mul), ("/", BinaryOperator::Div)] {
        if let Some(idx) = s.rfind(op_str) {
            let left = parse_expr_side(&s[..idx], cols)?;
            let right = parse_expr_side(&s[idx + op_str.len()..], cols)?;
            return Some(Expr::BinaryOp { left: Box::new(left), op: op_enum, right: Box::new(right) });
        }
    }
    
    if let Some(idx) = cols.iter().position(|name| name.eq_ignore_ascii_case(s)) {
        return Some(Expr::ColumnRef(idx));
    }
    
    if s.starts_with('c') || s.starts_with('C') {
        if let Ok(idx) = s[1..].trim().parse::<usize>() {
            return Some(Expr::ColumnRef(idx)); // Fallback backward compatibility
        }
    }
    
    if let Ok(val) = s.parse::<i32>() {
        return Some(Expr::Constant(Value::Int(val)));
    }
    
    None
}

fn parse_having(s: &str, cols: &[String]) -> Option<Expr> {
    let comp_ops = [
        (">=", ComparisonOperator::Geq),
        ("<=", ComparisonOperator::Leq),
        ("!=", ComparisonOperator::Neq),
        ("==", ComparisonOperator::Eq),
        ("=", ComparisonOperator::Eq),
        (">", ComparisonOperator::Gt),
        ("<", ComparisonOperator::Lt),
    ];
    
    for (op_str, op_enum) in comp_ops {
        if let Some(idx) = s.find(op_str) {
            let left = parse_expr_side(&s[..idx], cols)?;
            let right = parse_expr_side(&s[idx + op_str.len()..], cols)?;
            return Some(Expr::Comparison { left: Box::new(left), op: op_enum, right: Box::new(right) });
        }
    }
    None
}
