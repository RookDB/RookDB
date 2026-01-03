use std::io::{self, Write};

use storage_manager::buffer::BufferManager;
use storage_manager::catalog::{Catalog, Column, create_table, show_tables};

pub fn show_tables_cmd(catalog: &Catalog, current_db: &Option<String>) {
    let db = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected. Please select a database first.");
            return;
        }
    };
    show_tables(catalog, db);
}

pub fn create_table_cmd(
    catalog: &mut Catalog,
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    let mut table_name = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("\nEnter columns in the format:- column_name:data_type\n");
    print!("Press Enter on an empty line to finish\n");


    let mut columns = Vec::new();
    loop {
        let mut input = String::new();
        print!("Enter column (name:type): ");
        io::stdout().flush()?;
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let parts: Vec<&str> = input.split(':').collect();
        if parts.len() != 2 {
            println!("Invalid format. Please use name:type (e.g. id:INT)");
            continue;
        }

        columns.push(Column {
            name: parts[0].to_string(),
            data_type: parts[1].to_string(),
        });
    }

    create_table(catalog, &db, &table_name, columns);
    buffer_manager.load_table_on_create(&db, &table_name)?;

    Ok(())
}
