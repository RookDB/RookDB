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

    println!("\n════════════════════════════════════════════");
    println!("   Tuples in '{}.{}'", db_name, table_name);
    println!("   Total pages: {}", total_pages);          
    println!("════════════════════════════════════════════");

    // Display column headers
    println!("\n[TABLE DISPLAY] Columns:");
    for (idx, col) in columns.iter().enumerate() {
        println!("  {}: {} ({})", idx + 1, col.name, col.data_type);
    }

    // 3. Print table header dynamically
    let col_width = 22usize;
    let mut top_border = String::from("┌─────┬");
    let mut mid_border = String::from("├─────┼");
    let mut bot_border = String::from("└─────┴");

    for idx in 0..columns.len() {
        let line = "─".repeat(col_width + 2);
        if idx < columns.len() - 1 {
            top_border.push_str(&format!("{}┬", line));
            mid_border.push_str(&format!("{}┼", line));
            bot_border.push_str(&format!("{}┴", line));
        } else {
            top_border.push_str(&format!("{}┐", line));
            mid_border.push_str(&format!("{}┤", line));
            bot_border.push_str(&format!("{}┘", line));
        }
    }

    println!("\n{}", top_border);
    print!("│ ID  │");
    for col in columns.iter() {
        let col_display = format!("{}: {}", col.name, col.data_type);
        let display = if col_display.len() > col_width {
            format!("{}…", &col_display[..col_width - 1])
        } else {
            col_display
        };
        print!(" {:<width$} │", display, width = col_width);
    }
    println!();
    println!("{}", mid_border);

    let mut total_tuples = 0u32;

    // 3. Loop through each page
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

        if lower < PAGE_HEADER_SIZE || lower > upper || upper > page.data.len() as u32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Corrupted page header on page {}: lower={}, upper={}, page_size={}",
                    page_num,
                    lower,
                    upper,
                    page.data.len()
                ),
            ));
        }

        if (lower - PAGE_HEADER_SIZE) % ITEM_ID_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Corrupted slot directory alignment on page {}: lower={}, header={}, item_size={}",
                    page_num,
                    lower,
                    PAGE_HEADER_SIZE,
                    ITEM_ID_SIZE
                ),
            ));
        }

        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        log::trace!("[PAGE {}] Lower: {}, Upper: {}, Tuples: {}", page_num, lower, upper, num_items);

        // 4. For each tuple
        for i in 0..num_items {
            total_tuples += 1;
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());

            if offset > page.data.len() as u32 || length > page.data.len() as u32 || offset + length > page.data.len() as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Corrupted slot bounds on page {} slot {}: offset={}, length={}, page_size={}",
                        page_num,
                        i,
                        offset,
                        length,
                        page.data.len()
                    ),
                ));
            }

            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("│ {:>3} │", total_tuples);

            // 5. Decode each column
            let mut cursor = 0usize;
            for col in columns.iter() {
                match DataType::from_str(&col.data_type) {
                    Ok(data_type) => {
                        let byte_size = data_type.byte_size();
                        if cursor + byte_size <= tuple_data.len() {
                            match data_type.deserialize_value(&tuple_data[cursor..cursor + byte_size]) {
                                Ok(value) => {
                                    let val_str = value.to_string();
                                    let display = if val_str.len() > col_width {
                                        format!("{}…", &val_str[..col_width - 1])
                                    } else {
                                        val_str
                                    };
                                    print!(" {:<width$} │", display, width = col_width);
                                }
                                Err(e) => {
                                    log::error!("[ERROR] Failed to deserialize: {}", e);
                                    print!(" {:<width$} │", "<error>", width = col_width);
                                }
                            }
                            cursor += byte_size;
                        } else {
                            print!(" {:<width$} │", "<incomplete>", width = col_width);
                        }
                    }
                    Err(_) => {
                        print!(" {:<width$} │", "<unsupported>", width = col_width);
                    }
                }
            }
            println!();
        }
    }

    println!("{}", bot_border);
    println!("\nTotal tuples displayed: {}\n", total_tuples);

    Ok(())
}
