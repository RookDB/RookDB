///! This file is to test load CSV file without using Buffer Manager.
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::catalog::types::Catalog;
use crate::heap::insert_tuple;

/// Parse a CSV line while respecting bracket-delimited arrays and quoted fields.
/// 
/// Standard CSV parsers treat every comma as a field delimiter, but this breaks
/// array fields like [item1,item2,item3]. This function correctly identifies
/// field boundaries while ignoring commas inside brackets and quotes.
/// 
/// # Examples
/// - `1,True,"text",0xABCD,[1,2,3]` → 5 fields
/// - `42,"hello, world",@file.bin` → 3 fields (comma in quoted string ignored)
/// - `[[1,2],[3,4]]` → 1 field (nested brackets handled)
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_brackets: i32 = 0;  // Track nested brackets
    let mut in_quotes = false;
    let mut quote_char = ' ';
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current_field.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_quotes => {
                escaped = true;
                current_field.push(ch);
            }
            '"' | '\'' if !in_quotes => {
                // Enter quoted string
                in_quotes = true;
                quote_char = ch;
                current_field.push(ch);
            }
            c if in_quotes && c == quote_char => {
                // Exit quoted string
                in_quotes = false;
                current_field.push(ch);
            }
            '[' if !in_quotes => {
                in_brackets += 1;
                current_field.push(ch);
            }
            ']' if !in_quotes => {
                in_brackets = in_brackets.saturating_sub(1);
                current_field.push(ch);
            }
            ',' if !in_quotes && in_brackets == 0 => {
                // Field delimiter: only if not in quotes or brackets
                fields.push(current_field.trim().to_string());
                current_field.clear();
            }
            _ => current_field.push(ch),
        }
    }

    // Don't forget the last field
    if !current_field.is_empty() || !fields.is_empty() {
        fields.push(current_field.trim().to_string());
    }

    fields
}

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

        // Parse CSV fields, respecting bracket-delimited arrays and quoted fields
        let values = parse_csv_line(&row);
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // Validate number of columns
        if values_refs.len() != columns.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                columns.len(),
                values_refs.len()
            );
            continue;
        }

        // --- 4. Serialize row based on schema ---
        let mut tuple_bytes: Vec<u8> = Vec::new();

        for (val, col) in values_refs.iter().zip(columns.iter()) {
            let type_str = col.data_type.as_ref().map(|s| s.as_str()).unwrap_or("UNKNOWN");
            match type_str {
                "INT" => {
                    let num: i32 = val.parse().unwrap_or_default();
                    tuple_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "TEXT" => {
                    let mut text_bytes = val.as_bytes().to_vec();
                    if text_bytes.len() > 10 {
                        text_bytes.truncate(10);
                    } else if text_bytes.len() < 10 {
                        text_bytes.extend(vec![b' '; 10 - text_bytes.len()]);
                    }
                    tuple_bytes.extend_from_slice(&text_bytes);
                }
                _ => {
                    println!(
                        "Unsupported column type '{}' in column '{}'",
                        type_str, col.name
                    );
                    continue;
                }
            }
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
