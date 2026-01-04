use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{init_catalog, load_catalog};

use crate::frontend::{
    database_cmd,
    table_cmd,
    data_cmd,
};

pub fn run() -> io::Result<()> {
    println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    println!("Initializing Catalog File...\n");
    init_catalog();

    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();
    let mut buffer_manager = BufferManager::new();
    let mut current_db: Option<String> = None;

    loop {
        println!("\n=============================");
        println!("Choose an option:");
        println!("1. Show Databases");
        println!("2. Create Database");
        println!("3. Select Database");
        println!("4. Show Tables");
        println!("5. Create Table");
        println!("6. Load CSV");
        println!("7. Show Tuples");
        println!("8. Exit");
        println!("=============================");

        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            "1" => database_cmd::show_databases_cmd(&catalog),
            "2" => database_cmd::create_database_cmd(&mut catalog)?,
            "3" => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "4" => table_cmd::show_tables_cmd(&catalog, &current_db),
            "5" => table_cmd::create_table_cmd(
                &mut catalog,
                &mut buffer_manager,
                &current_db,
            )?,
            "6" => data_cmd::load_csv_cmd(
                &mut buffer_manager,
                &current_db,
            )?,
            "7" => data_cmd::show_tuples_cmd(&current_db)?,
            "8" => {
                println!("Exiting RookDB. Goodbye!");
                break;
            }
            _ => println!("Invalid option."),
        }
    }

    Ok(())
}
