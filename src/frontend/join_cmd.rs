// Frontend CLI commands for JOIN operations.

use std::io::{self, Write};

use storage_manager::catalog::types::Catalog;
use storage_manager::catalog::load_catalog;
use storage_manager::join::{
    JoinType, JoinOp, NLJMode,
    JoinCondition, JoinMetrics,
    NLJExecutor, SMJExecutor, HashJoinExecutor,
};
use storage_manager::table::page_count;
use std::fs::OpenOptions;

// Interactive join command (Option 9).
pub fn run_join_cmd(
    _catalog: &Catalog,
    current_db: &Option<String>,
) -> io::Result<()> {
    // Guard: database must be selected
    let db = match current_db {
        Some(d) => d,
        None => {
            println!("No database selected. Use option 3 first.");
            return Ok(());
        }
    };

    // Reload catalog to get latest state
    let catalog = load_catalog();

    let database = match catalog.databases.get(db.as_str()) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found in catalog.", db);
            return Ok(());
        }
    };

    // Step 1: left table
    print!("Enter left table name: ");
    io::stdout().flush()?;
    let mut left_table = String::new();
    io::stdin().read_line(&mut left_table)?;
    let left_table = left_table.trim().to_string();

    if !database.tables.contains_key(&left_table) {
        println!("Table '{}' not found in database '{}'.", left_table, db);
        return Ok(());
    }

    // Step 2: right table
    print!("Enter right table name: ");
    io::stdout().flush()?;
    let mut right_table = String::new();
    io::stdin().read_line(&mut right_table)?;
    let right_table = right_table.trim().to_string();

    if !database.tables.contains_key(&right_table) {
        println!("Table '{}' not found in database '{}'.", right_table, db);
        return Ok(());
    }

    // Show columns for left table
    let left_cols = &database.tables.get(&left_table).unwrap().columns;
    println!("\nColumns in '{}': {}", left_table,
        left_cols.iter().map(|c| format!("{} ({})", c.name, c.data_type)).collect::<Vec<_>>().join(", "));

    // Step 3: left join column
    print!("Enter join column from '{}': ", left_table);
    io::stdout().flush()?;
    let mut left_col = String::new();
    io::stdin().read_line(&mut left_col)?;
    let left_col = left_col.trim().to_string();

    if !left_cols.iter().any(|c| c.name == left_col) {
        println!("Column '{}' not found in table '{}'.", left_col, left_table);
        return Ok(());
    }

    // Step 4: operator
    println!("\nSelect join operator:");
    println!("  1. =  (Equal)");
    println!("  2. != (Not Equal)");
    println!("  3. <  (Less Than)");
    println!("  4. >  (Greater Than)");
    println!("  5. <= (Less Than or Equal)");
    println!("  6. >= (Greater Than or Equal)");
    print!("Enter operator choice (1-6): ");
    io::stdout().flush()?;
    let mut op_choice = String::new();
    io::stdin().read_line(&mut op_choice)?;
    let op = match op_choice.trim() {
        "1" => JoinOp::Eq,
        "2" => JoinOp::Ne,
        "3" => JoinOp::Lt,
        "4" => JoinOp::Gt,
        "5" => JoinOp::Le,
        "6" => JoinOp::Ge,
        _ => {
            println!("Invalid operator choice. Defaulting to '='.");
            JoinOp::Eq
        }
    };

    // Show columns for right table
    let right_cols = &database.tables.get(&right_table).unwrap().columns;
    println!("\nColumns in '{}': {}", right_table,
        right_cols.iter().map(|c| format!("{} ({})", c.name, c.data_type)).collect::<Vec<_>>().join(", "));

    // Step 5: right join column
    print!("Enter join column from '{}': ", right_table);
    io::stdout().flush()?;
    let mut right_col = String::new();
    io::stdin().read_line(&mut right_col)?;
    let right_col = right_col.trim().to_string();

    if !right_cols.iter().any(|c| c.name == right_col) {
        println!("Column '{}' not found in table '{}'.", right_col, right_table);
        return Ok(());
    }

    // Step 6: join type
    println!("\nSelect join type:");
    println!("  1. Inner Join (default)");
    println!("  2. Left Outer Join");
    println!("  3. Right Outer Join");
    println!("  4. Full Outer Join");
    println!("  5. Cross Join");
    print!("Enter join type (1-5): ");
    io::stdout().flush()?;
    let mut jt_choice = String::new();
    io::stdin().read_line(&mut jt_choice)?;
    let join_type = match jt_choice.trim() {
        "1" => JoinType::Inner,
        "2" => JoinType::LeftOuter,
        "3" => JoinType::RightOuter,
        "4" => JoinType::FullOuter,
        "5" => JoinType::Cross,
        _ => {
            println!("Invalid join type. Defaulting to Inner Join.");
            JoinType::Inner
        }
    };

    // Step 7: algorithm selection
    println!("\nSelect algorithm:");
    println!("  1. Auto-select (recommended)");
    println!("  2. Nested Loop Join (NLJ)");
    println!("  3. Sort-Merge Join (SMJ)");
    println!("  4. Hash Join (HJ)");
    print!("Enter algorithm choice (1-4): ");
    io::stdout().flush()?;
    let mut algo_choice = String::new();
    io::stdin().read_line(&mut algo_choice)?;
    let algo = algo_choice.trim().to_string();

    // Build join condition
    let condition = JoinCondition {
        left_table: left_table.clone(),
        left_col: left_col.clone(),
        operator: op,
        right_table: right_table.clone(),
        right_col: right_col.clone(),
    };

    // Resolve algorithm
    let algo_name = match algo.as_str() {
        "1" => auto_select_algorithm(&condition, db, &left_table, &right_table),
        "2" => "NLJ".to_string(),
        "3" => "SMJ".to_string(),
        "4" => "HJ".to_string(),
        _ => {
            println!("Invalid algorithm choice. Using Auto-select.");
            auto_select_algorithm(&condition, db, &left_table, &right_table)
        }
    };

    println!("\n>>> Using algorithm: {}", algo_name);
    println!(">>> Join: {}.{} {} {}.{}", left_table, left_col, op, right_table, right_col);
    println!(">>> Join type: {:?}", join_type);

    // Execute join with chosen algorithm
    let mut metrics = JoinMetrics::start(&algo_name);

    let result = match algo_name.as_str() {
        "NLJ" => {
            let executor = NLJExecutor {
                outer_table: left_table.clone(),
                inner_table: right_table.clone(),
                conditions: vec![condition],
                join_type,
                block_size: 2,
                mode: NLJMode::Simple,
            };
            executor.execute(db, &catalog)?
        }
        "SMJ" => {
            let executor = SMJExecutor {
                left_table: left_table.clone(),
                right_table: right_table.clone(),
                conditions: vec![condition],
                join_type,
                memory_pages: 10,
            };
            executor.execute(db, &catalog)?
        }
        "HJ" => {
            let executor = HashJoinExecutor {
                build_table: right_table.clone(),
                probe_table: left_table.clone(),
                conditions: vec![condition],
                join_type,
                memory_pages: 10,
                num_partitions: 4,
            };
            executor.execute(db, &catalog)?
        }
        _ => {
            println!("Unknown algorithm: {}", algo_name);
            return Ok(());
        }
    };

    metrics.tuples_output = result.tuples.len() as u64;
    metrics.stop();

    // Display results
    result.display();
    metrics.display();

    Ok(())
}

/// Benchmark join command (Option 10).
pub fn run_benchmark_cmd(
    _catalog: &Catalog,
    current_db: &Option<String>,
) -> io::Result<()> {
    // Guard: database must be selected
    let db = match current_db {
        Some(d) => d,
        None => {
            println!("No database selected. Use option 3 first.");
            return Ok(());
        }
    };

    let catalog = load_catalog();

    let database = match catalog.databases.get(db.as_str()) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found in catalog.", db);
            return Ok(());
        }
    };

    // Collect the same inputs as run_join_cmd
    print!("Enter left table name: ");
    io::stdout().flush()?;
    let mut left_table = String::new();
    io::stdin().read_line(&mut left_table)?;
    let left_table = left_table.trim().to_string();

    if !database.tables.contains_key(&left_table) {
        println!("Table '{}' not found in database '{}'.", left_table, db);
        return Ok(());
    }

    print!("Enter right table name: ");
    io::stdout().flush()?;
    let mut right_table = String::new();
    io::stdin().read_line(&mut right_table)?;
    let right_table = right_table.trim().to_string();

    if !database.tables.contains_key(&right_table) {
        println!("Table '{}' not found in database '{}'.", right_table, db);
        return Ok(());
    }

    let left_cols = &database.tables.get(&left_table).unwrap().columns;
    println!("\nColumns in '{}': {}", left_table,
        left_cols.iter().map(|c| format!("{} ({})", c.name, c.data_type)).collect::<Vec<_>>().join(", "));

    print!("Enter join column from '{}': ", left_table);
    io::stdout().flush()?;
    let mut left_col = String::new();
    io::stdin().read_line(&mut left_col)?;
    let left_col = left_col.trim().to_string();

    if !left_cols.iter().any(|c| c.name == left_col) {
        println!("Column '{}' not found in table '{}'.", left_col, left_table);
        return Ok(());
    }

    println!("\nSelect join operator:");
    println!("  1. =  (Equal)");
    println!("  2. != (Not Equal)");
    println!("  3. <  (Less Than)");
    println!("  4. >  (Greater Than)");
    println!("  5. <= (Less Than or Equal)");
    println!("  6. >= (Greater Than or Equal)");
    print!("Enter operator choice (1-6): ");
    io::stdout().flush()?;
    let mut op_choice = String::new();
    io::stdin().read_line(&mut op_choice)?;
    let op = match op_choice.trim() {
        "1" => JoinOp::Eq,
        "2" => JoinOp::Ne,
        "3" => JoinOp::Lt,
        "4" => JoinOp::Gt,
        "5" => JoinOp::Le,
        "6" => JoinOp::Ge,
        _ => JoinOp::Eq,
    };

    let right_cols = &database.tables.get(&right_table).unwrap().columns;
    println!("\nColumns in '{}': {}", right_table,
        right_cols.iter().map(|c| format!("{} ({})", c.name, c.data_type)).collect::<Vec<_>>().join(", "));

    print!("Enter join column from '{}': ", right_table);
    io::stdout().flush()?;
    let mut right_col = String::new();
    io::stdin().read_line(&mut right_col)?;
    let right_col = right_col.trim().to_string();

    if !right_cols.iter().any(|c| c.name == right_col) {
        println!("Column '{}' not found in table '{}'.", right_col, right_table);
        return Ok(());
    }

    println!("\nSelect join type:");
    println!("  1. Inner Join (default)");
    println!("  2. Left Outer Join");
    println!("  3. Right Outer Join");
    println!("  4. Full Outer Join");
    println!("  5. Cross Join");
    print!("Enter join type (1-5): ");
    io::stdout().flush()?;
    let mut jt_choice = String::new();
    io::stdin().read_line(&mut jt_choice)?;
    let join_type = match jt_choice.trim() {
        "1" => JoinType::Inner,
        "2" => JoinType::LeftOuter,
        "3" => JoinType::RightOuter,
        "4" => JoinType::FullOuter,
        "5" => JoinType::Cross,
        _ => JoinType::Inner,
    };

    let condition = JoinCondition {
        left_table: left_table.clone(),
        left_col: left_col.clone(),
        operator: op,
        right_table: right_table.clone(),
        right_col: right_col.clone(),
    };

    println!("\n========== Running all three algorithms for benchmarking ==========\n");

    // NLJ
    let mut nlj_metrics = JoinMetrics::start("Nested Loop Join (NLJ)");
    let nlj_result = {
        let executor = NLJExecutor {
            outer_table: left_table.clone(),
            inner_table: right_table.clone(),
            conditions: vec![condition.clone()],
            join_type,
            block_size: 2,
            mode: NLJMode::Simple,
        };
        executor.execute(db, &catalog)?
    };
    nlj_metrics.tuples_output = nlj_result.tuples.len() as u64;
    nlj_metrics.stop();

    // SMJ
    let mut smj_metrics = JoinMetrics::start("Sort-Merge Join (SMJ)");
    let smj_result = {
        let executor = SMJExecutor {
            left_table: left_table.clone(),
            right_table: right_table.clone(),
            conditions: vec![condition.clone()],
            join_type,
            memory_pages: 10,
        };
        executor.execute(db, &catalog)?
    };
    smj_metrics.tuples_output = smj_result.tuples.len() as u64;
    smj_metrics.stop();

    // HJ
    let mut hj_metrics = JoinMetrics::start("Hash Join (HJ)");
    let hj_result = {
        let executor = HashJoinExecutor {
            build_table: right_table.clone(),
            probe_table: left_table.clone(),
            conditions: vec![condition.clone()],
            join_type,
            memory_pages: 10,
            num_partitions: 4,
        };
        executor.execute(db, &catalog)?
    };
    hj_metrics.tuples_output = hj_result.tuples.len() as u64;
    hj_metrics.stop();

    // Print comparison table
    println!("\n{:<25} {:>12} {:>14}", "Algorithm", "Time (ms)", "Tuples Output");
    println!("{}", "-".repeat(55));
    nlj_metrics.display_row();
    smj_metrics.display_row();
    hj_metrics.display_row();

    // Verify correctness parity
    if nlj_result.tuples.len() == smj_result.tuples.len() && nlj_result.tuples.len() == hj_result.tuples.len() {
        println!("\n✓ All three algorithms produced the same number of result tuples ({}).", nlj_result.tuples.len());
    } else {
        println!("\n⚠ Result counts differ: NLJ={}, SMJ={}, HJ={}",
            nlj_result.tuples.len(), smj_result.tuples.len(), hj_result.tuples.len());
    }

    println!("\n=================================================================\n");

    Ok(())
}

/// Rule-based algorithm auto-selector.
fn auto_select_algorithm(
    condition: &JoinCondition,
    db: &str,
    left_table: &str,
    right_table: &str,
) -> String {
    // If inequality operator, use NLJ (hash/SMJ only benefit from equi-joins)
    if !condition.is_equality() {
        println!("Auto-selected: NLJ (inequality operator)");
        return "NLJ".to_string();
    }

    // Check table sizes
    let left_pages = get_table_page_count(db, left_table).unwrap_or(0);
    let right_pages = get_table_page_count(db, right_table).unwrap_or(0);
    let memory_pages: u32 = 10; // default memory buffer

    // Both tables fit in memory → use HJ (simple in-memory, lowest I/O)
    if left_pages + right_pages <= memory_pages {
        println!("Auto-selected: Hash Join (both tables fit in memory)");
        return "HJ".to_string();
    }

    // Either table is large and condition is equality → use HJ (Grace Hash Join)
    if left_pages > memory_pages || right_pages > memory_pages {
        println!("Auto-selected: Hash Join (Grace partitioning for large tables)");
        return "HJ".to_string();
    }

    // Fallback for large data → use SMJ
    println!("Auto-selected: Sort-Merge Join (SMJ)");
    "SMJ".to_string()
}

fn get_table_page_count(db: &str, table: &str) -> io::Result<u32> {
    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    page_count(&mut file)
}
