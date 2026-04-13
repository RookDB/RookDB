use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::disk::read_page;
use storage_manager::executor::selection::{SelectionExecutor, filter_tuples};
use storage_manager::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE};
use storage_manager::table::page_count;
use storage_manager::types::deserialize_nullable_row;
use storage_manager::query::build_predicate_from_sql;

pub fn load_csv_cmd(
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let mut csv_path = String::new();
    print!("Enter CSV path: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut csv_path)?;
    let csv_path = csv_path.trim();

    let catalog = load_catalog();
    buffer_manager.load_csv_to_buffer(&catalog, &db, table, csv_path)?;

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    println!("Page Count: {}", page_count(&mut file)?);

    Ok(())
}

pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let catalog = load_catalog();

    // Step A — read SQL from user
    print!("Enter SQL (single SELECT with WHERE): ");
    io::stdout().flush()?;
    let mut sql = String::new();
    io::stdin().read_line(&mut sql)?;

    // Step B — build predicate from SQL
    let predicate = build_predicate_from_sql(sql.trim())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // Get the table schema (borrowed from catalog)
    let table_schema = catalog
        .databases
        .get(&db)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Database not found"))?
        .tables
        .get(table)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Table not found"))?;

    // Step C — create SelectionExecutor
    // table_schema is &Table; SelectionExecutor::new takes owned Table, so we clone.
    let executor = SelectionExecutor::new(predicate, table_schema.clone())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // Step D — read all raw tuple bytes from every data page
    let total_pages = page_count(&mut file)?;
    let columns = &table_schema.columns;
    let schema_types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();

    println!("\n=== Tuples in '{}.{}' ===", db, table);
    println!("Total pages: {}", total_pages);

    let header: Vec<String> = columns
        .iter()
        .map(|c| format!("{} ({})", c.name, c.data_type))
        .collect();
    println!("{}", header.join(" | "));

    let mut raw_tuples: Vec<Vec<u8>> = Vec::new();

    // Skip page 0 (table header), iterate data pages
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_bytes = page.data[offset as usize..(offset + length) as usize].to_vec();
            raw_tuples.push(tuple_bytes);
        }
    }

    // Step E — apply predicate filtering
    let matching = filter_tuples(&executor, &raw_tuples)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    // Step F — print only the filtered tuples
    for (i, tuple_bytes) in matching.iter().enumerate() {
        print!("Tuple {}: ", i + 1);
        match deserialize_nullable_row(&schema_types, tuple_bytes) {
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

    println!("\n=== End of tuples ===\n");
    Ok(())
}
