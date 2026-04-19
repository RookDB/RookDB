use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{PathBuf, Path};

use storage_manager::catalog::load_catalog;
use storage_manager::executor::{show_tuples, load_csv, insert_single_tuple};
use storage_manager::backend::disk::read_header_page;

/// Gracefully load CSV file with comprehensive validation and error handling
pub fn load_csv_cmd(
    current_db: &Option<String>,
) -> io::Result<()> {
    log::info!("Starting CSV load operation");

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
    let table = table.trim().to_string();

    if table.is_empty() {
        println!("Table name cannot be empty");
        return Ok(());
    }

    let mut csv_path = String::new();
    print!("Enter CSV path: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut csv_path)?;
    let csv_path = csv_path.trim();

    // --- 1. VALIDATE CSV PATH FIRST ---
    log::info!("Verifying CSV path: '{}'", csv_path);
    
    if csv_path.is_empty() {
        println!("CSV path cannot be empty");
        return Ok(());
    }

    let csv_file_path = Path::new(csv_path);
    if !csv_file_path.exists() {
        println!("CSV file not found at: '{}'", csv_path);
        println!("Please check the file path and try again.");
        println!("Make sure the file exists and the path is correct.");
        return Ok(());
    }

    if !csv_file_path.is_file() {
        println!("Path is not a file: '{}'", csv_path);
        println!("Please provide a path to a file, not a directory.");
        return Ok(());
    }

    log::info!("CSV file verified successfully: '{}'", csv_path);

    // Load catalog and insert data using the improved load_csv function
    let catalog = load_catalog();
    
    log::info!("Starting data insertion...\n");

    // Use the improved load_csv function with validation (HeapManager handles FSM)
    match load_csv(&catalog, &db, &table, csv_path) {
        Ok(inserted_count) => {
            if inserted_count == 0 {
                log::warn!("No data was inserted from the CSV file.");
                println!("   Please check:");
                println!("   1. CSV file is not empty (excluding header)");
                println!("   2. Data types match the table schema");
                println!("   3. Each row has the correct number of columns");
            } else {
                log::info!("Successfully inserted {} rows from CSV", inserted_count);
                println!("\n FSM fork file has been created/updated");
            }
        }
        Err(e) => {
            log::error!("Error during CSV loading: {}", e);
            println!("\nThis usually means:");
            if e.kind() == io::ErrorKind::NotFound {
                println!("  - The CSV file path is incorrect");
                println!("  - The file no longer exists");
            } else if e.kind() == io::ErrorKind::PermissionDenied {
                println!("  - Permission denied accessing the file");
                println!("  - Try running with appropriate permissions");
            } else if e.kind() == io::ErrorKind::InvalidData {
                println!("  - Data validation failed");
                println!("  - Check your CSV format and data types");
            } else {
                println!("  - An I/O error occurred: {}", e);
            }
            println!("\nPlease fix the issue and try again.");
        }
    }

    Ok(())
}

/// Insert a single tuple manually
pub fn insert_tuple_cmd(
    current_db: &Option<String>,
) -> io::Result<()> {
    log::info!("Starting single tuple insertion");

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

    if table.is_empty() {
        println!("Table name cannot be empty");
        return Ok(());
    }

    // Load catalog to get schema
    let catalog = load_catalog();
    
    let db_obj = match catalog.databases.get(&db) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found", db);
            return Ok(());
        }
    };

    let table_schema = match db_obj.tables.get(table) {
        Some(t) => t,
        None => {
            println!("Table '{}' not found in database '{}'", table, db);
            return Ok(());
        }
    };

    // Display schema
    log::trace!("Table schema:");
    for (idx, col) in table_schema.columns.iter().enumerate() {
        println!("  {}: {} (type: {})", idx + 1, col.name, col.data_type);
    }

    // Collect values
    println!("Enter values for each column:");
    let mut values = Vec::new();
    
    for col in &table_schema.columns {
        print!("  {} [{}]: ", col.name, col.data_type);
        io::stdout().flush()?;
        
        let mut value = String::new();
        io::stdin().read_line(&mut value)?;
        values.push(value.trim().to_string());
    }

    // Convert to string references
    let value_refs: Vec<&str> = values.iter().map(|v| v.as_str()).collect();

    // Insert tuple using HSM-aware insert (with FSM)
    log::info!("Inserting tuple...");
    match insert_single_tuple(&catalog, &db, table, &value_refs) {
        Ok(success) => {
            if success {
                log::info!("Tuple inserted successfully!");
                log::info!("FSM fork file updated");
            } else {
                log::error!("Failed to insert tuple. Please check your data types and values.");
            }
        }
        Err(e) => {
            log::error!("Error inserting tuple: {}", e);
        }
    }

    Ok(())
}

pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    log::debug!("Starting tuple display");

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

    if table.is_empty() {
        println!("Table name cannot be empty");
        return Ok(());
    }

    let path = format!("database/base/{}/{}.dat", db, table);
    
    if !Path::new(&path).exists() {
        log::warn!("Table file not found: '{}'", path);
        println!("Make sure the table exists. Try creating the table first.");
        return Ok(());
    }

    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            log::error!("Failed to open table file: {}", e);
            return Ok(());
        }
    };

    let catalog = load_catalog();
    
    match show_tuples(&catalog, &db, table, &mut file) {
        Ok(_) => {
            println!("Tuple display completed");
        }
        Err(e) => {
            log::error!("Error displaying tuples: {}", e);
        }
    }

    Ok(())
}

/// Check heap health and display FSM statistics for a table.
pub fn check_heap_cmd(current_db: &Option<String>) -> io::Result<()> {

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

    let heap_path = PathBuf::from(format!("database/base/{}/{}.dat", db, table));
    
    if !heap_path.exists() {
        log::warn!("Heap file not found: {:?}", heap_path);
        println!("Table may not exist. Try creating the table first.");
        return Ok(());
    }

    println!("\n╔════════════════════════════════════════╗");
    println!("║         HEAP DIAGNOSTICS               ║"); 
    println!("╚════════════════════════════════════════╝");
    

    println!("\nHeap Info: {}.{}", db, table);
    // println!("════════════════════════════════════════");

    // Try to read header
    match OpenOptions::new()
        .read(true)
        .write(true)
        .open(&heap_path)
    {
        Ok(mut file) => {
            match read_header_page(&mut file) {
                Ok(header) => {
                    println!("Total Heap Pages:  {}", header.page_count);
                    println!("FSM Fork Pages:    {}", header.fsm_page_count);
                    println!("Total Tuples:      {}", header.total_tuples);
                    println!("Last Vacuum:       {}", 
                             if header.last_vacuum == 0 { 
                                 "Never".to_string() 
                             } else { 
                                 format!("{}s ago", std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs() - header.last_vacuum as u64) 
                             });
                    
                    
                    
                    println!("\nHeap is healthy and accessible");
                    storage_manager::backend::instrumentation::StatsSnapshot::capture().print_table();
                }
                Err(e) => {
                    log::warn!("Could not read header: {}", e);
                    println!("   Heap file may need FSM rebuild");
                }
            }
        }
        Err(e) => {
            log::error!("Error opening heap file: {}", e);
        }
    }

    // Check FSM fork file
    let fsm_path = PathBuf::from(format!("{}.fsm", heap_path.to_string_lossy()));
    if fsm_path.exists() {
        match std::fs::metadata(&fsm_path) {
            Ok(meta) => {
                let fsm_pages = meta.len() / 8192;
                println!("\nFSM Fork File:");
                println!("  Path: {:?}", fsm_path);
                println!("  Size: {} bytes ({} pages)", meta.len(), fsm_pages);
            }
            Err(e) => {
                println!("\nFSM Fork file exists but cannot stat: {}", e);
            }
        }
    } else {
        println!("\n FSM Fork file not yet created (will be created on first insert)");
    }

    println!("════════════════════════════════════════\n");

    Ok(())
}
