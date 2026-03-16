///! This file is to test load CSV file without using Buffer Manager.
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::catalog::types::Catalog;
use crate::catalog::serialize_value;
use crate::heap::insert_tuple;

pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<()> {
    // --- 1. Fetch table schema from catalog ---
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
    if columns.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Table has no columns",
        ));
    }

    // --- 2. Open and read the CSV file ---
    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);

    let mut lines = reader.lines();

    // Skip header line
    if let Some(Ok(_header)) = lines.next() {
        // println!("Header: {}", header);
    }

    // --- 3. Iterate through rows ---
    let mut inserted = 0;
    for (i, line) in lines.enumerate() {
        let row = line?;
        if row.trim().is_empty() {
            continue;
        }

        // Split CSV fields by comma
        let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();

        // Validate number of columns
        if values.len() != columns.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                columns.len(),
                values.len()
            );
            continue;
        }

        // --- 4. Serialize row based on schema (INT fixed; TEXT/VARCHAR variable-length) ---
        let mut tuple_bytes: Vec<u8> = Vec::new();
        let mut skip_row = false;
        for (val, col) in values.iter().zip(columns.iter()) {
            let bytes = serialize_value(col, val);
            if bytes.is_empty() {
                println!(
                    "Unsupported column type '{}' in column '{}'",
                    col.data_type, col.name
                );
                skip_row = true;
                break;
            }
            tuple_bytes.extend_from_slice(&bytes);
        }
        if skip_row {
            continue;
        }

        // --- 5. Insert tuple into page system ---
        if let Err(e) = insert_tuple(file, &tuple_bytes) {
            println!("Failed to insert row {}: {}", i + 1, e);
        } else {
            inserted += 1;
        }
    }
    println!("Total Number of rows inserted: {}", inserted);
    Ok(())
}
