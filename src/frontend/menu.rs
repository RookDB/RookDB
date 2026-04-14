//! Handles the interactive command-line menu and routes user input
//! to the appropriate operations.

use std::io::{self, Write};

// Core storage manager components
use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{init_catalog, load_catalog, save_catalog};
use storage_manager::storage::toast_logger;
use storage_manager::storage::database_logger;

// Frontend command handlers
use crate::frontend::{data_cmd, database_cmd, table_cmd};

/// Runs the main interactive menu loop
pub fn run() -> io::Result<()> {
    // Initialize TOAST logger
    toast_logger::init_toast_logger();
    
    // Initialize database operations logger
    database_logger::init_database_logger();
    
    // Log session start
    database_logger::log_session("DATABASE_SESSION_START");

    println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    // Ensure catalog file exists
    println!("Initializing Catalog File...\n");
    init_catalog();

    // Load catalog metadata into memory
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();
    
    // Log catalog initialization
    let db_count = catalog.databases.len();
    let table_count = catalog.databases.values().map(|db| db.tables.len()).sum::<usize>();
    database_logger::log_catalog_init(db_count, table_count);

    // Initialize buffer manager
    let mut buffer_manager = BufferManager::new();

    // Tracks the currently selected database
    let mut current_db: Option<String> = None;

    loop {
        println!("\n=============================");
        println!("Choose an option:");
        println!("1.  Show Databases");
        println!("2.  Create Database");
        println!("3.  Select Database");
        println!("4.  Show Tables");
        println!("5.  Create Table");
        println!("6.  Load CSV");
        println!("7.  Show Tuples");
        println!("8.  Insert Tuple");
        println!("9.  Delete Tuple");
        println!("10. Update Tuple");
        println!("11. Show Table Statistics");
        println!("12. Exit");
        println!("=============================");

        // Read user input
        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        // Dispatch command based on user selection
        match choice {
            "1"  => database_cmd::show_databases_cmd(&catalog),
            "2"  => {
                database_cmd::create_database_cmd(&mut catalog)?;
                save_catalog(&catalog);
            }
            "3"  => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "4"  => table_cmd::show_tables_cmd(&catalog, &current_db),
            "5"  => {
                table_cmd::create_table_cmd(&mut catalog, &mut buffer_manager, &current_db)?;
                save_catalog(&catalog);
            }
            "6"  => data_cmd::load_csv_cmd(&mut buffer_manager, &current_db)?,
            "7"  => data_cmd::show_tuples_cmd(&current_db)?,
            "8"  => data_cmd::insert_tuple_cmd(&current_db)?,
            "9"  => data_cmd::delete_tuple_cmd(&current_db)?,
            "10" => data_cmd::update_tuple_cmd(&current_db)?,
            "11" => table_cmd::show_table_statistics_cmd(&current_db)?,
            "12" => {
                save_catalog(&catalog);
                
                // Log catalog save and session end
                let db_count = catalog.databases.len();
                let table_count = catalog.databases.values().map(|db| db.tables.len()).sum::<usize>();
                database_logger::log_catalog_save(db_count, table_count);
                database_logger::log_session("DATABASE_SESSION_END");
                
                println!("Catalog saved. Exiting RookDB. Goodbye!");
                break;
            }
            _ => println!("Invalid option."),
        }
    }

    Ok(())
}
