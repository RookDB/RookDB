//! Sequential scan executor – read and print all tuples in a table.

use std::fs::File;
use std::io;

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
) -> io::Result<()> {
    let db    = catalog.databases.get(db_name).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name)))?;
    let table = db.tables.get(table_name).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name)))?;
    let columns = &table.columns;

    let total_pages = page_count(file)?;
    println!("\n=== Tuples in '{}.{}' ({} pages) ===", db_name, table_name, total_pages);

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;
        println!("\n-- Page {} --", page_num);

        let lower     = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        println!("Lower: {}, Tuples: {}", lower, num_items);

        for i in 0..num_items {
            let base   = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base+4].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(page.data[base+4..base+8].try_into().unwrap()) as usize;
            let tuple  = &page.data[offset..offset+length];

            print!("Tuple {}: ", i + 1);
            let mut cursor = 0usize;
            for col in columns {
                let type_name = col.data_type.type_name.to_uppercase();
                match type_name.as_str() {
                    "INT" | "INTEGER" => {
                        if cursor + 4 <= tuple.len() {
                            let v = i32::from_le_bytes(tuple[cursor..cursor+4].try_into().unwrap());
                            print!("{}={} ", col.name, v); cursor += 4;
                        }
                    }
                    "BIGINT" => {
                        if cursor + 8 <= tuple.len() {
                            let v = i64::from_le_bytes(tuple[cursor..cursor+8].try_into().unwrap());
                            print!("{}={} ", col.name, v); cursor += 8;
                        }
                    }
                    "FLOAT" | "REAL" => {
                        if cursor + 4 <= tuple.len() {
                            let v = f32::from_le_bytes(tuple[cursor..cursor+4].try_into().unwrap());
                            print!("{}={:.4} ", col.name, v); cursor += 4;
                        }
                    }
                    "DOUBLE" => {
                        if cursor + 8 <= tuple.len() {
                            let v = f64::from_le_bytes(tuple[cursor..cursor+8].try_into().unwrap());
                            print!("{}={:.4} ", col.name, v); cursor += 8;
                        }
                    }
                    "BOOL" | "BOOLEAN" => {
                        if cursor + 1 <= tuple.len() {
                            let v = tuple[cursor] != 0;
                            print!("{}={} ", col.name, v); cursor += 1;
                        }
                    }
                    t if t.starts_with("VARCHAR") => {
                        if cursor + 2 <= tuple.len() {
                            let len = u16::from_le_bytes(tuple[cursor..cursor+2].try_into().unwrap()) as usize;
                            cursor += 2;
                            if cursor + len <= tuple.len() {
                                let text = String::from_utf8_lossy(&tuple[cursor..cursor+len]);
                                print!("{}='{}' ", col.name, text);
                                cursor += len;
                            }
                        }
                    }
                    _ => {
                        // Legacy TEXT: fixed 10-byte field
                        if cursor + 10 <= tuple.len() {
                            let text = String::from_utf8_lossy(&tuple[cursor..cursor+10]).trim().to_string();
                            print!("{}='{}' ", col.name, text); cursor += 10;
                        } else {
                            print!("{}=<unsupported> ", col.name);
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
