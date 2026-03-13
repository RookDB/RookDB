use std::fs::File;
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::types::DataValue;

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
    let total_pages = page_count(file)?;

    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);

    // Print column header
    let header: Vec<String> = columns
        .iter()
        .map(|c| format!("{} ({})", c.name, c.data_type))
        .collect();
    println!("{}", header.join(" | "));

    // 3. Loop through each data page (skip page 0 = table header)
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        // 4. For each tuple
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("Tuple {}: ", i + 1);

            // 5. Decode each column using its DataType
            let mut cursor = 0usize;
            for col in columns {
                let field = &tuple_data[cursor..];
                let sz = match col.data_type.encoded_len(field) {
                    Ok(sz) => sz,
                    Err(_) => {
                        print!("{}=<truncated> ", col.name);
                        break;
                    }
                };

                if cursor + sz <= tuple_data.len() {
                    match DataValue::from_bytes(&col.data_type, &tuple_data[cursor..cursor + sz]) {
                        Ok(val) => print!("{}={} ", col.name, val),
                        Err(_) => {
                            print!("{}=<decode-error> ", col.name);
                            break;
                        }
                    }
                    cursor += sz;
                } else {
                    print!("{}=<truncated> ", col.name);
                    break;
                }
            }
            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}
