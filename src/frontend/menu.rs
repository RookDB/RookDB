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
    let mut buffer_pool = BufferPool::new(pool_size, policy, file);

    println!("\n✅ Buffer Pool initialized successfully!");

    // Current DB
    let mut current_db: Option<String> = None;

    // -----------------------------
    // COMMAND LOOP
    // -----------------------------
    loop {
        println!("\n=============================");
        println!("Buffer Pool Test Menu:");
        println!("1. Fetch Page");
        println!("2. Create New Page");
        println!("3. Unpin Page");
        println!("4. Mark Page Dirty");
        println!("5. Flush Page");
        println!("6. Flush All Pages");
        println!("7. Delete Page");
        println!("8. Show Buffer Stats");
        println!("9. Show Frame Table");
        println!("10. Exit");
        println!("11. Show Databases");
        println!("12. Create Database");
        println!("13. Select Database");
        println!("14. Perform Benchmarking");
        println!("15. Create Table");
        println!("16. Load CSV");
        println!("17. Show Tuples");
        println!("18. Show Table Statistics");
        println!("=============================");

        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            // -----------------------------
            // FETCH PAGE
            // -----------------------------
            "1" => {
                let (table_name, page_number) = get_page_input()?;
                buffer_test_cmd::fetch_page_cmd(
                    &mut buffer_pool,
                    &catalog,
                    &current_db,
                    table_name,
                    page_number,
                );
            }

            // -----------------------------
            // CREATE PAGE
            // -----------------------------
            "2" => {
                let table_name = get_table_name()?;
                buffer_test_cmd::new_page_cmd(&mut buffer_pool, table_name);
            }

            // -----------------------------
            // UNPIN PAGE
            // -----------------------------
            "3" => {
                let page_id = build_page_id()?;
                buffer_test_cmd::unpin_page_cmd(&mut buffer_pool, page_id, false);
            }

            // -----------------------------
            // MARK DIRTY
            // -----------------------------
            "4" => {
                let page_id = build_page_id()?;
                buffer_test_cmd::unpin_page_cmd(&mut buffer_pool, page_id, true);
            }

            // -----------------------------
            // FLUSH PAGE
            // -----------------------------
            "5" => {
                let page_id = build_page_id()?;
                buffer_test_cmd::flush_page_cmd(&mut buffer_pool, page_id);
            }

            // -----------------------------
            // FLUSH ALL
            // -----------------------------
            "6" => {
                buffer_test_cmd::flush_all_pages_cmd(&mut buffer_pool);
            }

            // -----------------------------
            // DELETE PAGE
            // -----------------------------
            "7" => {
                let page_id = build_page_id()?;
                buffer_test_cmd::delete_page_cmd(&mut buffer_pool, page_id);
            }

            // -----------------------------
            // SHOW STATS
            // -----------------------------
            "8" => {
                buffer_test_cmd::show_stats_cmd(&buffer_pool);
            }

            // -----------------------------
            // SHOW FRAME TABLE
            // -----------------------------
            "9" => {
                buffer_test_cmd::show_frames_cmd(&buffer_pool);
            }

            // -----------------------------
            // EXIT
            // -----------------------------
            "10" => {
                println!("Exiting Buffer Pool Test. Goodbye!");
                break;
            }

            "11" => database_cmd::show_databases_cmd(&catalog),
            "12" => database_cmd::create_database_cmd(&mut catalog)?,
            "13" => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "14" => {
    run_benchmark(&catalog, &current_db)?;
},
            _ => println!("Invalid option."),
        }
    }

    Ok(())
}

//
// -----------------------------
// HELPER FUNCTIONS
// -----------------------------
//

fn get_table_name() -> io::Result<String> {
    let mut table_name = String::new();

    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;

    Ok(table_name.trim().to_string())
}

fn get_page_input() -> io::Result<(String, u32)> {
    let table_name = get_table_name()?;

    let mut page_number = String::new();
    print!("Enter page number: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut page_number)?;

    let page_number: u32 = page_number.trim().parse().unwrap_or(0);

    Ok((table_name, page_number))
}

fn build_page_id() -> io::Result<PageId> {
    let (table_name, page_number) = get_page_input()?;

    Ok(PageId {
        table_name,
        page_number,
    })
}

use std::fs::{File};
use std::io::{BufRead, BufReader};
use std::time::Instant;



fn run_benchmark(
    catalog: &storage_manager::catalog::Catalog,
    current_db: &Option<String>,
) -> io::Result<()> {

    println!("🚀 Starting Benchmark...");

    // -----------------------------
    // LOAD RANDOM PAGE NUMBERS
    // -----------------------------
    let file = File::open("random_numbers.txt")?;
    let reader = BufReader::new(file);

    let page_numbers: Vec<u32> = reader
        .lines()
        .filter_map(|line| line.ok()?.trim().parse().ok())
        .collect();

    if page_numbers.len() < 1000 {
        println!("❌ Need at least 1000 page numbers");
        return Ok(());
    }

  

    // -----------------------------
    // CSV OUTPUT FILE
    // -----------------------------
    let mut results_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("benchmark_results.csv")?;

    writeln!(
        results_file,
        "policy,buffer_size,avg_time_ms,hits,misses,hit_ratio,evictions,dirty_flushes"
    )?;

    // -----------------------------
    // CONFIGS
    // -----------------------------
    let policies = vec!["LRU", "Clock"];
    let pool_sizes = vec![1, 2, 32, 128, 256, 1024];

    let table_name = "ug1".to_string();
    let file_path = "/home/pratham-omkar-pattanayak/SEM 8/Data Systems/Project/RookDB/database/base/iiit/ug1.dat";

    // -----------------------------
    // BENCHMARK LOOPS
    // -----------------------------
    for policy_name in policies {
        for &pool_size in &pool_sizes {

            println!(
                "\n⚙️ Running: Policy = {}, Pool Size = {}",
                policy_name, pool_size
            );

            // -----------------------------
            // REOPEN FILE (IMPORTANT)
            // -----------------------------
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(file_path)?;

            // -----------------------------
            // INIT POLICY
            // -----------------------------
            let policy: Box<dyn ReplacementPolicy> = match policy_name {
                "Clock" => Box::new(ClockPolicy::new()),
                _ => Box::new(LRUPolicy::new()),
            };

            // -----------------------------
            // INIT BUFFER POOL
            // -----------------------------
            let mut buffer_pool = BufferPool::new(pool_size, policy, file);

            // -----------------------------
            // TIMING START
            // -----------------------------
            let start = Instant::now();

            for &page_number in &page_numbers {

                // FETCH
                buffer_test_cmd::fetch_page_cmd(
                    &mut buffer_pool,
                    catalog,
                    current_db,
                    table_name.clone(),
                    page_number,
                );

                // UNPIN (mark dirty)
                buffer_test_cmd::unpin_page_cmd(
                    &mut buffer_pool,
                    PageId {
                        table_name: table_name.clone(),
                        page_number,
                    },
                    true,
                );
            }

            // -----------------------------
            // TIMING END
            // -----------------------------
            let duration = start.elapsed();
            let total_ms = duration.as_millis() as f64;
            let total_requests = page_numbers.len() as f64;
            let avg_time = total_ms / total_requests;

            // -----------------------------
            // STATS
            // -----------------------------
            let stats = &buffer_pool.stats;

            let hits = stats.hit_count;
            let misses = stats.miss_count;
            let evictions = stats.eviction_count;
            let dirty_flushes = stats.dirty_flush_count;

            let hit_ratio = if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            };

            // -----------------------------
            // WRITE CSV
            // -----------------------------
            writeln!(
                results_file,
                "{},{},{:.4},{},{},{:.4},{},{}",
                policy_name,
                pool_size,
                avg_time,
                hits,
                misses,
                hit_ratio,
                evictions,
                dirty_flushes
            )?;

            println!(
                "✅ Done: Avg Time = {:.4} ms | Hit Ratio = {:.4}",
                avg_time, hit_ratio
            );
        }
    }

    println!("\n🎯 Benchmark Completed! Results saved to benchmark_results.csv");

    Ok(())
}