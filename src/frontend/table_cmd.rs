//! Handles table-related user commands such as listing tables,
//! creating tables, and displaying table statistics.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{Catalog, Column, DataType, create_table, show_tables};
use storage_manager::statistics::print_table_page_count;

/// Displays tables in the currently selected database
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

        let type_str = parts[1].trim();
        let data_type = match type_str.parse::<DataType>() {
            Ok(dt) => dt,
            Err(e) => {
                println!(
                    "Unknown type '{}': {}. Supported: SMALLINT, INT, BIGINT, BOOLEAN, VARCHAR(n), DATE, BIT(n)",
                    type_str, e
                );
                continue;
            }
        };

        columns.push(Column::new(parts[0].trim().to_string(), data_type));
    }

    create_table(catalog, &db, &table_name, columns);
    buffer_manager.load_table_from_disk(&db, &table_name)?;

    Ok(())
}

pub fn show_table_statistics_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Enter table name: ");
    io::stdout().flush()?;

    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim();

    print_table_page_count(db_name, table_name)?;

    Ok(())
}
