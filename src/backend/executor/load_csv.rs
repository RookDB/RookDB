//! This file is to test load CSV file without using Buffer Manager.
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::buffer_manager::BufferManager;
use crate::catalog::page_manager::CatalogPageManager;
use crate::catalog::types::Catalog;
use crate::heap::insert_tuple;
use crate::types::DataValue;
use crate::types::datatype::DataType;

pub fn load_csv(
    catalog: &Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
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
    if let Some(Ok(_header)) = lines.next() {}

    // --- 3. Iterate through rows ---
    let mut inserted = 0;
    for (i, line) in lines.enumerate() {
        let row = line?;
        if row.trim().is_empty() {
            continue;
        }

        let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();

        if values.len() != columns.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                columns.len(),
                values.len()
            );
            continue;
        }

        // --- 4. Encode each field using the column's DataType ---
        let mut tuple_bytes: Vec<u8> = Vec::new();
        let mut row_ok = true;

        for (val, col) in values.iter().zip(columns.iter()) {
            let exec_type: DataType = (&col.data_type).into();

            match DataValue::parse_and_encode(&exec_type, val) {
                Ok(bytes) => tuple_bytes.extend_from_slice(&bytes),
                Err(e) => {
                    println!("Skipping row {}: column '{}' — {}", i + 1, col.name, e);
                    row_ok = false;
                    break;
                }
            }
        }

        if !row_ok {
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
