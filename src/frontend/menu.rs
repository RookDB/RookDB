use std::io::{self, Write};

// Core storage manager components
use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{init_catalog, load_catalog};

// Frontend command handlers
use crate::frontend::{data_cmd, database_cmd, join_cmd, table_cmd};

/// Runs the main interactive menu loop
pub fn run() -> io::Result<()> {
    println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    // Ensure catalog file exists
    log::info!("Initializing Catalog File...");
    init_catalog();

    // Load catalog metadata into memory
    log::info!("Loading Catalog...");
    let mut catalog = load_catalog();

    // Initialize buffer manager
    let mut buffer_manager = BufferManager::new();

    // Tracks the currently selected database
    let mut current_db: Option<String> = None;

    loop {
        println!("╔════════════════════════════════════════╗");
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
        println!("║    8. Select tuples                    ║");
        println!("║    9. Delete Tuples                    ║");
        println!("║    10. Update Tuples                   ║");
        println!("║    11. Compact Table                   ║");
        println!("║    12. Show Table Statistics           ║");
        println!("║                                        ║");
        println!("║  Query:                                ║");
        println!("║    15. Join Operations                 ║");
        println!("║                                        ║");
        println!("║  Maintenance:                          ║");
        println!("║    13. Check Heap Health               ║");
        println!("║    14. Exit                            ║");
        println!("╚════════════════════════════════════════╝");

        // Read user input
        print!("\nEnter your choice (1-15): ");
        io::stdout().flush()?;

        let mut choice = String::new();
        // `Ok(0)` is end of input - Ctrl-D, or a piped script running out.
        // Without this the empty string falls through to the catch-all arm and
        // the menu loops forever.
        if io::stdin().read_line(&mut choice)? == 0 {
            println!("\n╔═══════════════════════════════════╗");
            println!("║   Exiting RookDB. Goodbye!        ║");
            println!("╚═══════════════════════════════════╝\n");
            break;
        }
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
            "9" => data_cmd::delete_tuples_cmd(&current_db)?,
            "10" => data_cmd::update_tuples_cmd(&current_db)?,
            "11" => data_cmd::compact_table_cmd(&current_db)?,
            "12" => table_cmd::show_table_statistics_cmd(&catalog, &current_db)?,
            "13" => data_cmd::check_heap_cmd(&current_db)?,
            "15" => join_cmd::join_menu(&catalog, &current_db)?,
            "14" => {
                println!("\n╔═══════════════════════════════════╗");
                println!("║   Exiting RookDB. Goodbye!        ║");
                println!("╚═══════════════════════════════════╝\n");
                break;
            }
            _ => println!(" Invalid option. Please enter a number between 1 and 15."),
        }
    }

    Ok(())
}
