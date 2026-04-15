//! menu_test_buffer.rs
//! Interactive CLI to test BufferPool with LRU / Clock policies

use std::fs::OpenOptions;
use std::io::{self, Write};

// Catalog
use storage_manager::catalog::{init_catalog, load_catalog};


use storage_manager::{BufferPool, PageId, ReplacementPolicy, LRUPolicy, ClockPolicy};
// Command implementations
use crate::frontend::buffer_test_cmd;
use crate::frontend::database_cmd;

/// Runs the buffer pool test menu
pub fn run() -> io::Result<()> {
    println!("--------------------------------------");
    println!("RookDB Buffer Pool Testing Interface");
    println!("--------------------------------------\n");

     // -----------------------------
    // INIT CATALOG
    // -----------------------------
    println!("Initializing Catalog...\n");
    init_catalog();

    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();


    let mut input = String::new();

    // -----------------------------
    // BUFFER SIZE INPUT
    // -----------------------------
    print!("Enter buffer pool size: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    let pool_size: usize = input.trim().parse().unwrap_or(3);
    input.clear();

    // -----------------------------
    // FILE PATH INPUT
    // -----------------------------
    print!("Enter table file path (e.g., database/base/db1/table.dat): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    let file_path = input.trim().to_string();
    input.clear();

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;

    // -----------------------------
    // POLICY SELECTION
    // -----------------------------
    println!("\nChoose Replacement Policy:");
    println!("1. LRU");
    println!("2. Clock");

    print!("Enter choice: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    let policy_choice = input.trim().to_string();
input.clear();

    let policy: Box<dyn ReplacementPolicy> = match policy_choice.as_str() {
        "2" => {
            println!("Using Clock Replacement Policy");
            Box::new(ClockPolicy::new())
        }
        _ => {
            println!("Using LRU Replacement Policy");
            Box::new(LRUPolicy::new())
        }
    };

    // -----------------------------
    // INIT BUFFER POOL
    // -----------------------------
    let mut buffer_manager =    BufferPool::new(policy);
     println!("Initializing Catalog File...\n");
    init_catalog(&mut buffer_manager);

    // Load catalog metadata into memory
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog(&mut buffer_manager);

    // Tracks the currently selected database
    let mut current_db: Option<String> = None;

 

    println!("\n✅ Buffer Pool initialized successfully!");
    // -----------------------------
    // COMMAND LOOP
    // -----------------------------
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
        println!("8. Show Table Statistics");
        println!("9. Exit");
        println!("=============================");

        // Read user input
        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        // Dispatch command based on user selection
        match choice {
            "1" => database_cmd::show_databases_cmd(&catalog, &mut buffer_manager)?,
            "2" => database_cmd::create_database_cmd(&mut catalog, &mut buffer_manager)?,
            "3" => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "4" => table_cmd::show_tables_cmd(&catalog, &mut buffer_manager, &current_db)?,
            "5" => table_cmd::create_table_cmd(&mut catalog, &mut buffer_manager, &current_db)?,
            "6" => data_cmd::load_csv_cmd(&mut buffer_manager, &current_db)?,
            "7" => data_cmd::show_tuples_cmd(&mut buffer_manager, &current_db)?,
            "8" => table_cmd::show_table_statistics_cmd(&current_db)?,
            "9" => {
                println!("Exiting RookDB. Goodbye!");
                break;
            }
            _ => println!("Invalid option."),
        }
    }


    Ok(())
}
