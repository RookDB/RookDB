//! Handles table-related user commands such as listing tables,
//! creating tables, and displaying table statistics.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{Catalog, create_table, show_tables};
use storage_manager::catalog::types::{Column, DataType};
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

    let mut columns: Vec<Column> = Vec::new();
    let mut position: u16 = 1;
    loop {
        let mut input = String::new();
        print!("Enter column (name:type): ");
        io::stdout().flush()?;
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let parts: Vec<&str> = input.splitn(2, ':').collect();
        if parts.len() != 2 {
            println!("Invalid format. Please use name:type (e.g. id:INT)");
            continue;
        }

        let col_name = parts[0].trim().to_string();
        let type_str = parts[1].trim();
        let data_type = DataType::from_name(type_str).unwrap_or_else(|| {
            println!("Unknown type '{}', defaulting to TEXT", type_str);
            DataType::text()
        });

        columns.push(Column {
            column_oid: 0,
            name: col_name,
            column_position: position,
            data_type,
            type_modifier: None,
            is_nullable: true,
            default_value: None,
            constraints: vec![],
        });
        position += 1;
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
