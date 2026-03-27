//! Sequential scan: decode every tuple in a table and print it.
//! Updated to use TupleHeader-based decoding via tuple_codec.

use std::fs::File;
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::executor::tuple_codec::decode_tuple;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
) -> io::Result<()> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = &table.columns;

    let total_pages = page_count(file)?;
    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);

    // Print header row
    let header: Vec<&str> = schema.iter().map(|c| c.name.as_str()).collect();
    println!("\n{}", header.join(" | "));
    println!("{}", "-".repeat(header.join(" | ").len()));

    let mut total_tuples = 0u32;

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_bytes = &page.data[offset..offset + length];

            let values = decode_tuple(tuple_bytes, schema);
            let cells: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            println!("{}", cells.join(" | "));
            total_tuples += 1;
        }
    }

    println!("\n({} row{})", total_tuples, if total_tuples == 1 { "" } else { "s" });
    println!("=== End of tuples ===\n");
    Ok(())
}
