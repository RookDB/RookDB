use std::fs::File;
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::catalog::data_type::{DataType, Value};
use crate::backend::disk::read_page;
use crate::backend::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::backend::storage::tuple_codec::TupleCodec;
use crate::backend::storage::toast::ToastManager;
use crate::backend::table::page_count;

pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
) -> io::Result<()> {
    // 1. Get schema from catalog
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

    // 2. Load TOAST chunks from disk if available
    let toast_path = format!("database/base/{}/{}.toast", db_name, table_name);
    let toast_manager = ToastManager::load_from_disk(&toast_path)
        .unwrap_or_else(|_| ToastManager::new());

    // 3. Read total number of pages
    let total_pages = page_count(file)?; // total pages currently in file

    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);

    // 4. Loop through each page
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;
        println!("\n-- Page {} --", page_num);

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        println!("Lower: {}, Upper: {}", lower, upper);
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        println!("Lower: {}, Upper: {}, Tuples: {}", lower, upper, num_items);

        // 5. For each tuple
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("Tuple {}: ", i + 1);

            // 6. Build schema array for TupleCodec
            let schema: Vec<(String, DataType)> = columns
                .iter()
                .map(|col| {
                    let data_type = col
                        .data_type
                        .as_ref()
                        .and_then(|type_str| DataType::parse(type_str).ok())
                        .unwrap_or(DataType::Text);
                    (col.name.clone(), data_type)
                })
                .collect();

            // 7. Decode tuple using TupleCodec with TOAST support for proper type handling
            match TupleCodec::decode_tuple_with_toast(tuple_data, &schema, &toast_manager) {
                Ok(values) => {
                    for (col, value) in columns.iter().zip(values.iter()) {
                        display_value(col.name.as_str(), value);
                    }
                }
                Err(_e) => {
                    // Fallback to manual decoding for legacy format
                    let mut cursor = 0usize;
                    for col in columns {
                        let type_str = col.data_type.as_ref().map(|s| s.as_str()).unwrap_or("UNKNOWN");
                        match type_str {
                            "INT" => {
                                if cursor + 4 <= tuple_data.len() {
                                    let val = i32::from_le_bytes(
                                        tuple_data[cursor..cursor + 4].try_into().unwrap(),
                                    );
                                    print!("{}={} ", col.name, val);
                                    cursor += 4;
                                }
                            }
                            "TEXT" => {
                                if cursor + 10 <= tuple_data.len() {
                                    let text_bytes = &tuple_data[cursor..cursor + 10];
                                    let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                                    print!("{}='{}' ", col.name, text);
                                    cursor += 10;
                                }
                            }
                            _ => {
                                print!("{}=<unsupported> ", col.name);
                            }
                        }
                    }
                }
            }
            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}

/// Sequential scan that returns `(page_num, slot_index, decoded_values)` for every
/// live (non-deleted) tuple.  Used by the delete and update interactive commands.
pub fn scan_tuples_indexed(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
) -> io::Result<Vec<(u32, u32, Vec<Value>)>> {
    // 1. Get schema from catalog
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

    // 2. Build typed schema
    let schema: Vec<(String, DataType)> = columns
        .iter()
        .map(|col| {
            let data_type = col
                .data_type
                .as_ref()
                .and_then(|type_str| DataType::parse(type_str).ok())
                .unwrap_or(DataType::Text);
            (col.name.clone(), data_type)
        })
        .collect();

    // 3. Load TOAST manager
    let toast_path = format!("database/base/{}/{}.toast", db_name, table_name);
    let toast_manager = ToastManager::load_from_disk(&toast_path)
        .unwrap_or_else(|_| ToastManager::new());

    // 4. Iterate pages
    let total_pages = page_count(file)?;
    let mut results: Vec<(u32, u32, Vec<Value>)> = Vec::new();

    for page_num in 1..total_pages {
        let mut page = crate::backend::page::Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot_index in 0..num_items {
            let base = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());

            // Skip deleted slots (length == 0)
            if length == 0 {
                continue;
            }

            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            match TupleCodec::decode_tuple_with_toast(tuple_data, &schema, &toast_manager) {
                Ok(values) => {
                    results.push((page_num, slot_index, values));
                }
                Err(_) => {
                    // Skip undecodable tuples gracefully
                }
            }
        }
    }

    Ok(results)
}

/// Helper function to display a value based on its type
fn display_value(col_name: &str, value: &Value) {
    match value {
        Value::Null => {
            print!("{}=NULL ", col_name);
        }
        Value::Int32(n) => {
            print!("{}={} ", col_name, n);
        }
        Value::Boolean(b) => {
            print!("{}={} ", col_name, if *b { "true" } else { "false" });
        }
        Value::Text(s) => {
            print!("{}='{}' ", col_name, s);
        }
        Value::Blob(bytes) => {
            // Display blob size and first few bytes in hex
            if bytes.is_empty() {
                print!("{}=<empty blob> ", col_name);
            } else {
                let preview = bytes
                    .iter()
                    .take(8)
                    .map(|b| format!("{:02X}", b))
                    .collect::<Vec<_>>()
                    .join("");
                if bytes.len() > 8 {
                    print!("{}=<blob:{} bytes, starts:0x{}...> ", col_name, bytes.len(), preview);
                } else {
                    print!("{}=<blob:{} bytes:0x{}> ", col_name, bytes.len(), preview);
                }
            }
        }
        Value::Array(elements) => {
            // Display array element count and types
            if elements.is_empty() {
                print!("{}=<empty array> ", col_name);
            } else {
                print!("{}=<array:{} elements> ", col_name, elements.len());
            }
        }
    }
}
