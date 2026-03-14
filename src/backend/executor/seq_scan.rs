use std::fs::File;
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::backend::types_validator::DataType;

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

    // 2. Read total number of pages
    let total_pages = page_count(file)?; // total pages currently in file

    println!("\n╔════════════════════════════════════════════╗");
    println!("║   Tuples in '{}.{}'", db_name, table_name);
    println!("║   Total pages: {}", total_pages);
    println!("╚════════════════════════════════════════════╝");

    // Display column headers
    println!("\n[TABLE DISPLAY] Columns:");
    for (idx, col) in columns.iter().enumerate() {
        println!("  {}: {} ({})", idx + 1, col.name, col.data_type);
    }

    // 3. Print table header
    println!("\n┌─────┬──────────────────────────────────────────────────┐");
    print!("│ ID  │ ");
    for (idx, col) in columns.iter().enumerate() {
        let col_display = format!("{}: {}", col.name, col.data_type);
        if idx < columns.len() - 1 {
            print!("{:<25} │ ", col_display);
        } else {
            print!("{:<17} │", col_display);
        }
    }
    println!();
    println!("├─────┼──────────────────────────────────────────────────┤");

    let mut total_tuples = 0u32;

    // 3. Loop through each page
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        println!("[PAGE {}] Lower: {}, Upper: {}, Tuples: {}", page_num, lower, upper, num_items);

        // 4. For each tuple
        for i in 0..num_items {
            total_tuples += 1;
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("│ {:>3} │ ", total_tuples);

            // 5. Decode each column
            let mut cursor = 0usize;
            for (col_idx, col) in columns.iter().enumerate() {
                match DataType::from_str(&col.data_type) {
                    Ok(data_type) => {
                        let byte_size = data_type.byte_size();
                        if cursor + byte_size <= tuple_data.len() {
                            match data_type.deserialize_value(&tuple_data[cursor..cursor + byte_size]) {
                                Ok(value) => {
                                    if col_idx < columns.len() - 1 {
                                        print!("{:<25} │ ", value);
                                    } else {
                                        print!("{:<17} │", value);
                                    }
                                }
                                Err(e) => {
                                    println!("[ERROR] Failed to deserialize: {}", e);
                                    if col_idx < columns.len() - 1 {
                                        print!("{:<25} │ ", "<error>");
                                    } else {
                                        print!("{:<17} │", "<error>");
                                    }
                                }
                            }
                            cursor += byte_size;
                        } else {
                            if col_idx < columns.len() - 1 {
                                print!("{:<25} │ ", "<incomplete>");
                            } else {
                                print!("{:<17} │", "<incomplete>");
                            }
                        }
                    }
                    Err(_) => {
                        if col_idx < columns.len() - 1 {
                            print!("{:<25} │ ", "<unsupported>");
                        } else {
                            print!("{:<17} │", "<unsupported>");
                        }
                    }
                }
            }
            println!();
        }
    }

    println!("└─────┴──────────────────────────────────────────────────┘");
    println!("\nTotal tuples displayed: {}\n", total_tuples);

    Ok(())
}
