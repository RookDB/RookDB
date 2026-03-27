//! Interactive command-line menu — full feature set.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{init_catalog, load_catalog};

use crate::frontend::{data_cmd, database_cmd, duplicate_cmd, query_cmd, table_cmd};

pub fn run() -> io::Result<()> {
    println!("╔══════════════════════════════════════╗");
    println!("║           Welcome to RookDB          ║");
    println!("╚══════════════════════════════════════╝");

    init_catalog();
    let mut catalog = load_catalog();
    let mut buffer_manager = BufferManager::new();
    let mut current_db: Option<String> = None;

    loop {
        let db_label = current_db.as_deref().unwrap_or("none");

        println!("\n┌─────────────────────────────────────────────┐");
        println!("│  Database: [{:<36}]│", db_label);
        println!("├──── Database & Table ───────────────────────┤");
        println!("│  1. Show Databases                          │");
        println!("│  2. Create Database                         │");
        println!("│  3. Select Database                         │");
        println!("│  4. Show Tables                             │");
        println!("│  5. Create Table                            │");
        println!("│  6. Show Table Statistics                   │");
        println!("├──── Data ───────────────────────────────────┤");
        println!("│  7. Load CSV                                │");
        println!("├──── Query (in-memory) ──────────────────────┤");
        println!("│  8. SELECT *                                │");
        println!("│  9. SELECT * WHERE ...                      │");
        println!("│ 10. SELECT columns / expressions            │");
        println!("│ 11. SELECT DISTINCT                         │");
        println!("│ 12. COUNT rows (with optional WHERE)        │");
        println!("├──── Set Operations ─────────────────────────┤");
        println!("│ 13. UNION two tables                        │");
        println!("│ 14. INTERSECT two tables                    │");
        println!("│ 15. EXCEPT two tables                       │");
        println!("├──── Streaming (constant RAM) ───────────────┤");
        println!("│ 16. STREAM SELECT * WHERE ...               │");
        println!("│ 17. STREAM PROJECT columns WHERE ...        │");
        println!("│ 18. STREAM COUNT WHERE ...                  │");
        println!("│ 19. STREAM SELECT DISTINCT (dedup scan)     │");
        println!("├──── Duplicates ─────────────────────────────┤");
        println!("│ 20. Find & report duplicate tuples          │");
        println!("│ 21. Export deduped table (no dups)          │");
        println!("│ 22. Export duplicates-only file             │");
        println!("│ 23. Show duplicate index (.dup file)        │");
        println!("├─────────────────────────────────────────────┤");
        println!("│  0. Exit                                    │");
        println!("└─────────────────────────────────────────────┘");
        print!("  Choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;

        match choice.trim() {
            "1"  => database_cmd::show_databases_cmd(&catalog),
            "2"  => database_cmd::create_database_cmd(&mut catalog)?,
            "3"  => database_cmd::select_database_cmd(&catalog, &mut current_db)?,
            "4"  => table_cmd::show_tables_cmd(&catalog, &current_db),
            "5"  => table_cmd::create_table_cmd(&mut catalog, &mut buffer_manager, &current_db)?,
            "6"  => table_cmd::show_table_statistics_cmd(&current_db)?,
            "7"  => data_cmd::load_csv_cmd(&mut buffer_manager, &current_db)?,
            "8"  => query_cmd::show_all_cmd(&current_db)?,
            "9"  => query_cmd::select_where_cmd(&current_db)?,
            "10" => query_cmd::project_columns_cmd(&current_db)?,
            "11" => query_cmd::select_distinct_cmd(&current_db)?,
            "12" => query_cmd::count_cmd(&current_db)?,
            "13" => query_cmd::union_cmd(&current_db)?,
            "14" => query_cmd::intersect_cmd(&current_db)?,
            "15" => query_cmd::except_cmd(&current_db)?,
            "16" => query_cmd::stream_select_cmd(&current_db)?,
            "17" => query_cmd::stream_project_cmd(&current_db)?,
            "18" => query_cmd::stream_count_cmd(&current_db)?,
            "19" => query_cmd::stream_dedup_cmd(&current_db)?,
            "20" => duplicate_cmd::find_duplicates_cmd(&current_db)?,
            "21" => duplicate_cmd::export_deduped_cmd(&current_db)?,
            "22" => duplicate_cmd::export_dups_only_cmd(&current_db)?,
            "23" => duplicate_cmd::show_dup_index_cmd(&current_db)?,
            "0"  => { println!("  Goodbye!"); break; }
            _    => println!("  Invalid option."),
        }
    }
    Ok(())
}
