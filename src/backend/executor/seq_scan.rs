use std::fs::File;
use std::io::{self};

use crate::catalog::types::{Catalog, SortDirection};
use crate::disk::read_page;
use crate::ordered::ordered_scan;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

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

    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);

    if table.file_type.as_deref() == Some("ordered") {
        if let Some(sort_keys) = &table.sort_keys {
            let parts: Vec<String> = sort_keys
                .iter()
                .map(|sk| {
                    let col_name = if (sk.column_index as usize) < columns.len() {
                        &columns[sk.column_index as usize].name
                    } else {
                        "?"
                    };
                    let dir = match sk.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {}", col_name, dir)
                })
                .collect();
            println!("[Ordered by: {}]", parts.join(", "));
        }

        let total_pages = page_count(file)?;
        let tuples = ordered_scan(file, catalog, db_name, table_name)?;
        println!("Total pages: {}", total_pages);
        println!("Logical tuples (base + delta): {}", tuples.len());

        for (i, tuple) in tuples.iter().enumerate() {
            print!("Tuple {}: ", i + 1);
            print_decoded_tuple(tuple, columns);
            println!();
        }

        println!("\n=== End of tuples ===\n");
        return Ok(());
    }

    // Heap table: page-by-page physical layout scan.
    let total_pages = page_count(file)?;
    println!("Total pages: {}", total_pages);

    // 3. Loop through each page
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;
        println!("\n-- Page {} --", page_num);

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        println!("Lower: {}, Upper: {}", lower, upper);
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        println!("Lower: {}, Upper: {}, Tuples: {}", lower, upper, num_items);

        // 4. For each tuple
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("Tuple {}: ", i + 1);
            print_decoded_tuple(tuple_data, columns);
            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}

fn print_decoded_tuple(tuple_data: &[u8], columns: &[crate::catalog::types::Column]) {
    let mut cursor = 0usize;
    for col in columns {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= tuple_data.len() {
                    let val =
                        i32::from_le_bytes(tuple_data[cursor..cursor + 4].try_into().unwrap());
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
