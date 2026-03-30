use std::io::{self, Write};

// Core storage manager components
use crate::buffer_manager::BufferManager;
use crate::catalog::{init_catalog, load_catalog};

// Frontend command handlers
use crate::frontend::{data_cmd, database_cmd, table_cmd};

/// Runs the main interactive menu loop
pub fn run() -> io::Result<()> {
    println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    // Ensure catalog file exists
    println!("Initializing Catalog File...\n");
    init_catalog();

    // Load catalog metadata into memory
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();

    // Initialize buffer manager
    let mut buffer_manager = BufferManager::new();

    // Tracks the currently selected database
    let mut current_db: Option<String> = None;

    loop {
        println!("\n╔════════════════════════════════════════╗");
        println!("║          ROOKDB MAIN MENU              ║");
        println!("╠════════════════════════════════════════╣");
        println!("║  Database Operations:                  ║");
        println!("║    1. Show Databases                   ║");
        println!("║    2. Create Database                  ║");
        println!("║    3. Select Database                  ║");
        println!("║                                        ║");
        println!("║  Table Operations:                     ║");
        println!("║    4. Show Tables                      ║");
        println!("║    5. Create Table                     ║");
        println!("║                                        ║");
        println!("║  Data Operations:                      ║");
        println!("║    6. Load CSV                         ║");
        println!("║    7. Insert Single Tuple              ║");
        println!("║    8. Show Tuples                      ║");
        println!("║    9. Show Table Statistics            ║");
        println!("║                                        ║");
        println!("║  Maintenance:                          ║");
        println!("║    10. Check Heap Health               ║");
        println!("║    11. Exit                            ║");
        println!("╚════════════════════════════════════════╝");

        // Read user input
        print!("\nEnter your choice (1-11): ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        // Dispatch command based on user selection
        match choice {
            "1" => database_cmd::show_databases_cmd(&catalog),
            "2" => database_cmd::create_database_cmd(&mut catalog)?,
            "3" => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "4" => table_cmd::show_tables_cmd(&catalog, &current_db),
            "5" => table_cmd::create_table_cmd(&mut catalog, &mut buffer_manager, &current_db)?,
            "6" => data_cmd::load_csv_cmd(&current_db)?,
            "7" => data_cmd::insert_tuple_cmd(&current_db)?,
            "8" => data_cmd::show_tuples_cmd(&current_db)?,
            "9" => table_cmd::show_table_statistics_cmd(&current_db)?,
            "10" => data_cmd::check_heap_cmd(&current_db)?,
            "11" => {
                println!("\n╔═══════════════════════════════════╗");
                println!("║   Exiting RookDB. Goodbye!        ║");
                println!("╚═══════════════════════════════════╝\n");
                break;
            }
            _ => println!(" Invalid option. Please enter a number between 1 and 11."),
        }
    }

    Ok(())
}
