// Frontend CLI commands for JOIN operations.
// Uses the Cost-Based Optimizer (CBO) for algorithm selection.

use std::io::{self, Write};
use std::time::Instant;

use storage_manager::catalog::types::Catalog;
use storage_manager::catalog::load_catalog;
use storage_manager::join::{
    JoinType, JoinOp, JoinAlgorithmType, NLJMode,
    JoinCondition,
    NLJExecutor, SMJExecutor, HashJoinExecutor, HashJoinMode,
    SymmetricHashJoinExecutor, DirectJoinExecutor,
    JoinPlanner, JoinPlannerConfig,
};

/// Interactive join command (Option 9).
/// Supports all join types and uses the CBO planner for algorithm selection.
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

    // Show available tables to select from
    println!("\nAvailable tables in '{}':", db);
    if database.tables.is_empty() {
        println!("  (No tables found)");
    } else {
        let mut table_names: Vec<&String> = database.tables.keys().collect();
        table_names.sort();
        for name in table_names {
            println!("  - {}", name);
        }
    }
    println!();

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

    // Get columns for left table
    let left_cols = &database.tables.get(&left_table).unwrap().columns;

    // Step 3: join type
    println!("\nSelect join type:");
    println!("  1. Inner Join (default)");
    println!("  2. Left Outer Join");
    println!("  3. Right Outer Join");
    println!("  4. Full Outer Join");
    println!("  5. Cross Join");
    println!("  6. Semi Join (EXISTS)");
    println!("  7. Anti Join (NOT EXISTS)");
    println!("  8. Natural Join");
    print!("Enter join type (1-8): ");
    io::stdout().flush()?;
    let mut jt_choice = String::new();
    io::stdin().read_line(&mut jt_choice)?;
    let join_type = match jt_choice.trim() {
        "1" => JoinType::Inner,
        "2" => JoinType::LeftOuter,
        "3" => JoinType::RightOuter,
        "4" => JoinType::FullOuter,
        "5" => JoinType::Cross,
        "6" => JoinType::SemiJoin,
        "7" => JoinType::AntiJoin,
        "8" => JoinType::Natural,
        _ => {
            println!("Invalid join type. Defaulting to Inner Join.");
            JoinType::Inner
        }
    };

    // For Cross joins, no condition is needed
    let (condition, left_col, right_col, op) = if join_type == JoinType::Cross {
        // Cross join: no condition, generate a dummy that won't be used
        let dummy = JoinCondition {
            left_table: left_table.clone(),
            left_col: String::new(),
            operator: JoinOp::Eq,
            right_table: right_table.clone(),
            right_col: String::new(),
        };
        (dummy, String::new(), String::new(), JoinOp::Eq)
    } else {
        // Show columns for left table
        println!("\nAvailable columns in '{}':", left_table);
        for c in left_cols {
            println!("  - {} ({})", c.name, c.data_type);
        }
        println!();

        // Step 4: left join column
        print!("Enter join column from '{}': ", left_table);
        io::stdout().flush()?;
        let mut left_col = String::new();
        io::stdin().read_line(&mut left_col)?;
        let left_col = left_col.trim().to_string();

        if !left_cols.iter().any(|c| c.name == left_col) {
            println!("Column '{}' not found in table '{}'.", left_col, left_table);
            return Ok(());
        }

        // Step 5: operator
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
        println!("\nAvailable columns in '{}':", right_table);
        for c in right_cols {
            println!("  - {} ({})", c.name, c.data_type);
        }
        println!();

        // Step 6: right join column
        print!("Enter join column from '{}': ", right_table);
        io::stdout().flush()?;
        let mut right_col = String::new();
        io::stdin().read_line(&mut right_col)?;
        let right_col = right_col.trim().to_string();

        if !right_cols.iter().any(|c| c.name == right_col) {
            println!("Column '{}' not found in table '{}'.", right_col, right_table);
            return Ok(());
        }

        let cond = JoinCondition {
            left_table: left_table.clone(),
            left_col: left_col.clone(),
            operator: op,
            right_table: right_table.clone(),
            right_col: right_col.clone(),
        };

        (cond, left_col, right_col, op)
    };

    // Step 7: algorithm selection
    println!("\nSelect algorithm:");
    println!("  1. CBO Auto-select (recommended)");
    println!("  2. Simple Nested Loop Join (NLJ)");
    println!("  3. Block Nested Loop Join (BNLJ)");
    println!("  4. Sort-Merge Join (SMJ)");
    println!("  5. In-Memory Hash Join");
    println!("  6. Grace Hash Join");
    println!("  7. Hybrid Hash Join");
    println!("  8. Symmetric Hash Join");
    println!("  9. Direct Join");
    print!("Enter algorithm choice (1-9): ");
    io::stdout().flush()?;
    let mut algo_choice = String::new();
    io::stdin().read_line(&mut algo_choice)?;

    let conditions = if join_type == JoinType::Cross {
        vec![]
    } else {
        vec![condition.clone()]
    };

    // Resolve algorithm
    let (algo_name, selected_algo) = match algo_choice.trim() {
        "1" => {
            // Use CBO planner for optimal selection
            let config = JoinPlannerConfig::default();
            match JoinPlanner::select_best_join(
                &left_table,
                &right_table,
                &conditions,
                join_type,
                &catalog,
                &config,
            ) {
                Ok(plan) => {
                    println!("\n--- CBO Cost Estimation ---");
                    println!("  Algorithm:      {}", plan.algorithm);
                    println!("  Estimated cost: {}", plan.estimated_cost);
                    println!("  Est. output:    {} rows", plan.estimated_output_rows);
                    println!("---------------------------");
                    let name = format!("{}", plan.algorithm);
                    let algo = plan.algorithm;
                    (name, algo)
                }
                Err(e) => {
                    println!("CBO planner error: {}. Falling back to BNLJ.", e);
                    ("Block Nested Loop Join".to_string(), JoinAlgorithmType::BlockNLJ)
                }
            }
        }
        "2" => ("Simple Nested Loop Join".to_string(), JoinAlgorithmType::SimpleNLJ),
        "3" => ("Block Nested Loop Join".to_string(), JoinAlgorithmType::BlockNLJ),
        "4" => ("Sort-Merge Join".to_string(), JoinAlgorithmType::SortMergeJoin),
        "5" => ("In-Memory Hash Join".to_string(), JoinAlgorithmType::InMemoryHashJoin),
        "6" => ("Grace Hash Join".to_string(), JoinAlgorithmType::GraceHashJoin),
        "7" => ("Hybrid Hash Join".to_string(), JoinAlgorithmType::HybridHashJoin),
        "8" => ("Symmetric Hash Join".to_string(), JoinAlgorithmType::SymmetricHashJoin),
        "9" => ("Direct Join".to_string(), JoinAlgorithmType::DirectJoin),
        _ => {
            println!("Invalid algorithm choice. Using CBO Auto-select.");
            let config = JoinPlannerConfig::default();
            match JoinPlanner::select_best_join(
                &left_table,
                &right_table,
                &conditions,
                join_type,
                &catalog,
                &config,
            ) {
                Ok(plan) => {
                    let name = format!("{}", plan.algorithm);
                    let algo = plan.algorithm;
                    (name, algo)
                }
                Err(_) => ("Block Nested Loop Join".to_string(), JoinAlgorithmType::BlockNLJ),
            }
        }
    };

    if join_type == JoinType::Cross {
        println!("\n>>> Using algorithm: {}", algo_name);
        println!(">>> Join: {} CROSS JOIN {}", left_table, right_table);
    } else {
        println!("\n>>> Using algorithm: {}", algo_name);
        println!(">>> Join: {}.{} {} {}.{}", left_table, left_col, op, right_table, right_col);
        println!(">>> Join type: {}", join_type);
    }

    // Execute join
    let start = Instant::now();

    let result = match selected_algo {
        JoinAlgorithmType::SimpleNLJ => {
            let executor = NLJExecutor {
                outer_table: left_table.clone(),
                inner_table: right_table.clone(),
                conditions,
                join_type,
                block_size: 2,
                mode: NLJMode::Simple,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::BlockNLJ => {
            let executor = NLJExecutor {
                outer_table: left_table.clone(),
                inner_table: right_table.clone(),
                conditions,
                join_type,
                block_size: 10,
                mode: NLJMode::Block,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::SortMergeJoin => {
            let executor = SMJExecutor {
                left_table: left_table.clone(),
                right_table: right_table.clone(),
                conditions,
                join_type,
                memory_pages: 10,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::InMemoryHashJoin => {
            let executor = HashJoinExecutor {
                build_table: right_table.clone(),
                probe_table: left_table.clone(),
                conditions,
                join_type,
                mode: HashJoinMode::InMemory,
                memory_pages: 10,
                num_partitions: 4,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::GraceHashJoin => {
            let executor = HashJoinExecutor {
                build_table: right_table.clone(),
                probe_table: left_table.clone(),
                conditions,
                join_type,
                mode: HashJoinMode::Grace,
                memory_pages: 10,
                num_partitions: 4,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::HybridHashJoin => {
            let executor = HashJoinExecutor {
                build_table: right_table.clone(),
                probe_table: left_table.clone(),
                conditions,
                join_type,
                mode: HashJoinMode::Hybrid,
                memory_pages: 10,
                num_partitions: 4,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::SymmetricHashJoin => {
            let executor = SymmetricHashJoinExecutor {
                left_table: left_table.clone(),
                right_table: right_table.clone(),
                conditions,
                join_type,
            };
            executor.execute(db, &catalog)?
        }
        JoinAlgorithmType::DirectJoin => {
            let executor = DirectJoinExecutor {
                outer_table: left_table.clone(),
                inner_table: right_table.clone(),
                conditions,
                join_type,
            };
            executor.execute(db, &catalog)?
        }
        _ => {
            // Fallback: use Block NLJ for any unimplemented physical algorithms
            println!("Algorithm {} not directly executable. Falling back to Block NLJ.", algo_name);
            let executor = NLJExecutor {
                outer_table: left_table.clone(),
                inner_table: right_table.clone(),
                conditions,
                join_type,
                block_size: 10,
                mode: NLJMode::Block,
            };
            executor.execute(db, &catalog)?
        }
    };

    let elapsed = start.elapsed();

    // Display results
    result.display();

    println!("--- Execution Summary ---");
    println!("  Algorithm:     {}", algo_name);
    println!("  Tuples output: {}", result.tuples.len());
    println!("  Execution:     {:.2} ms", elapsed.as_secs_f64() * 1000.0);
    println!("------------------------");

    Ok(())
}
