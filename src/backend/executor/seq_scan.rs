use std::fs::File;
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::types::{deserialize_nullable_row};
use crate::types::datatype::DataType;

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
        .map(|c| format!("{} ({:?})", c.name, c.data_type))
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
            let schema_types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();
            let exec_types: Vec<DataType> = schema_types.iter().map(|t| t.into()).collect();
            match deserialize_nullable_row(&exec_types, tuple_data) {
                Ok(values) => {
                    for (col, val_opt) in columns.iter().zip(values.iter()) {
                        match val_opt {
                            Some(val) => print!("{}={} ", col.name, val),
                            None => print!("{}=NULL ", col.name),
                        }
                    }
                }
                Err(e) => print!("<decode-error: {}> ", e),
            }

            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}
