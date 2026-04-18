//! Load CSV data into a table file using the new TupleHeader-based encoding.

use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::catalog::types::Catalog;
use crate::executor::tuple_codec::encode_tuple;
use crate::executor::value::Value;
use crate::heap::insert_tuple;

pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<()> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = &table.columns;

    if schema.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Table has no columns"));
    }

    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);
    let mut lines = reader.lines();

    // Skip header line
    lines.next();

    let mut inserted = 0usize;
    for (i, line) in lines.enumerate() {
        let row = line?;
        if row.trim().is_empty() {
            continue;
        }

        let raw_vals: Vec<&str> = row.split(',').map(|v| v.trim()).collect();
        if raw_vals.len() != schema.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                schema.len(),
                raw_vals.len()
            );
            continue;
        }

        // Parse each field into a typed Value
        let values: Vec<Value> = raw_vals
            .iter()
            .zip(schema.iter())
            .map(|(raw, col)| {
                if raw.is_empty() || raw.to_uppercase() == "NULL" {
                    return Value::Null;
                }
                match col.data_type.to_uppercase().as_str() {
                    "INT" | "INTEGER" => raw
                        .parse::<i64>()
                        .map(Value::Int)
                        .unwrap_or(Value::Null),
                    "FLOAT" | "REAL" | "DOUBLE" => raw
                        .parse::<f64>()
                        .map(Value::Float)
                        .unwrap_or(Value::Null),
                    "BOOL" | "BOOLEAN" => match raw.to_lowercase().as_str() {
                        "true" | "1" | "yes" => Value::Bool(true),
                        "false" | "0" | "no" => Value::Bool(false),
                        _ => Value::Null,
                    },
                    "DATE" => raw.parse::<i32>().map(Value::Date).unwrap_or(Value::Null),
                    "TIMESTAMP" => raw.parse::<i64>().map(Value::Timestamp).unwrap_or(Value::Null),
                    _ => Value::Text(raw.to_string()), // TEXT, VARCHAR(n), unknown
                }
            })
            .collect();

        let tuple_bytes = encode_tuple(&values, schema);

        if let Err(e) = insert_tuple(file, &tuple_bytes) {
            println!("Failed to insert row {}: {}", i + 1, e);
        } else {
            inserted += 1;
        }
    }

    println!("Total rows inserted: {}", inserted);
    Ok(())
}
