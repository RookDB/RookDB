/// Test to verify FSM tree search correctly allocates tuples across pages
/// Based on user issue: tuples were being forced to sequential pages (1, 2, 3...)
/// instead of being distributed across pages with available free space

#[test]
fn test_fsm_page_allocation() {
    use std::fs;
    
    // Clean up from previous runs
    let _ = fs::remove_dir_all("database/base/test_fsm_alloc");
    
    // Use the provided load_csv with HeapManager for proper FSM integration
    use storage_manager::catalog::{init_catalog, load_catalog, create_database, create_table, Column, save_catalog};
    use storage_manager::executor::load_csv;
    
    // Setup
    init_catalog();
    let mut catalog = load_catalog();
    
    // Create database
    let db_name = "test_fsm_alloc";
    let _ = create_database(&mut catalog, db_name);
    
    // Create table with INT id and TEXT name
    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];
    
    // Add table to catalog via create_table and save
    create_table(&mut catalog, db_name, "pages_test", columns);
    let _ = save_catalog(&catalog);
    
    // IMPORTANT: Create the heap file first
    use storage_manager::heap::HeapManager;
    use std::path::PathBuf;
    
    // Note: HeapManager expects just "database/base/{db_name}/{table_name}.dat"
    let heap_file_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, "pages_test"));
    let _= fs::create_dir_all(heap_file_path.parent().unwrap());
    let _= HeapManager::create(heap_file_path);
    
    // Load CSV - this should use HeapManager with FSM tree search
    // The gd.csv file has 500 rows which should fit in 2-3 pages with proper FSM allocation
    println!("Loading 500 rows from gd.csv...");
    match load_csv(&catalog, db_name, "pages_test", "gd.csv") {
        Ok(count) => {
            println!("Inserted {} tuples", count);
            
            // Each tuple is approximately 4 (id) + 10 (name) + 8 (slot entry) = 22 bytes
            // Page has 8192 - 8 (header) = 8184 bytes usable
            // So one page can hold approximately 8184 / 22 = 372 tuples
            
            // With 500 tuples, we should see 2 pages:
            // - Page 1:  ~372 tuples (8184 - 72 bytes free approximately)
            // - Page 2:  ~128 tuples (8184 - 3200 bytes free approximately)
            
            // NOT the previous behavior:
            // - 7+ pages with 1-73 tuples each
            
            assert!(count == 500, "Expected 500 tuples, got {}", count);
            println!("✓ Correct number of tuples inserted: {}", count);
        }
        Err(e) => {
            panic!("Failed to load CSV: {}", e);
        }
    }
    
    // Check heap structure to verify pages are being used efficiently
    use storage_manager::disk::read_all_pages;
    use storage_manager::page;
    
    let heap_file_path = format!("database/base/{}/{}.dat", db_name, "pages_test");
    match std::fs::OpenOptions::new()
        .read(true)
        .open(&heap_file_path)
    {
        Ok(mut file) => {
            let pages = read_all_pages(&mut file).expect("Failed to read pages");
            
            println!("\nPage Usage Summary:");
            let mut total_tuples = 0;
            let mut pages_used = 0;
            
            for (idx, p) in pages.iter().skip(1).enumerate() { // Skip page 0 (header)
                let tuples_in_page: u32 = page::get_tuple_count(p).unwrap_or(0);
                let free_space: u32 = page::page_free_space(p).unwrap_or(0);
                
                if tuples_in_page > 0 {
                    pages_used += 1;
                    total_tuples += tuples_in_page;
                    println!(
                        "  Page {}: {} tuples, {} bytes free",
                        idx + 1,
                        tuples_in_page,
                        free_space
                    );
                }
            }
            
            println!("\nTotal pages used: {}", pages_used);
            println!("Total tuples allocated: {}", total_tuples);
            
            // With proper FSM tree search, we should use 2-3 pages, not 7+
            // Allow up to 4 pages for some variance in tuple size encoding
            assert!(
                pages_used <= 4,
                "Expected <= 4 pages with proper FSM, got {} pages",
                pages_used
            );
            
            println!("✓ FSM page allocation is working correctly!");
            println!("✓ Tuples are distributed efficiently across {} page(s)", pages_used);
        }
        Err(e) => {
            panic!("Failed to open heap file: {}", e);
        }
    }
}
