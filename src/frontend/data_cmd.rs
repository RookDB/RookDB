use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::executor::show_tuples;
use storage_manager::table::page_count;

use storage_manager::executor::{AggFunc, AggReq};
use storage_manager::executor::iterator::Executor;
use storage_manager::executor::tuple::Tuple;
use storage_manager::executor::hash_aggregator::execute_aggregation;
use storage_manager::executor::expr::{Expr, ComparisonOperator};

struct MockScanner {
    tuples: std::vec::IntoIter<Tuple>,
}

impl Executor for MockScanner {
    fn next(&mut self) -> Option<Tuple> {
        self.tuples.next()
    }
}

pub fn aggregate_query_cmd(_current_db: &Option<String>) -> io::Result<()> {
    // if current_db.is_none() {
    //     println!("No database selected. Please select a database first");
    //     return Ok(());
    // }

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let _table = table.trim();

    let mut agg_str = String::new();
    print!("Enter aggregation function (e.g., SUM, COUNT, AVG): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut agg_str)?;
    let agg_str = agg_str.trim().to_uppercase();

    let mut col_str = String::new();
    print!("Enter target column index (e.g., 0): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut col_str)?;
    let col_idx: usize = match col_str.trim().parse() {
        Ok(idx) => idx,
        Err(_) => {
            println!("Invalid column index.");
            return Ok(());
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
            println!("Unsupported aggregation function: {}", agg_str);
            return Ok(());
        }
    };

    let reqs = vec![AggReq {
        agg_type,
        col_index: Some(col_idx),
    }];

    // Generate dummy data since complete tuple mapping isn't fully integrated here
    #[allow(unused_imports)]
    use storage_manager::executor::value::Value;
    
    let dummy_data = vec![
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Manager".to_string()), Value::Int(100)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(200)], is_null_bitmap: vec![] },
        Tuple { values: vec![Value::Text("HR".to_string()), Value::Text("Staff".to_string()), Value::Int(210)], is_null_bitmap: vec![] },
    ];

    let having_expr = Expr::Comparison {
        left: Box::new(Expr::ColumnRef(2)), 
        op: ComparisonOperator::Gt,
        right: Box::new(Expr::Constant(Value::Int(150))),
    };

    let child_node = Box::new(MockScanner {
        tuples: dummy_data.into_iter(),
    });

    println!("\nExecuting Aggregation...");
    let result = execute_aggregation(child_node, reqs, vec![0,1], Some(having_expr));
    println!("Result: {:?}", result);

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
