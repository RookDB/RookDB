//! Interactive join operations.
//!
//! Everything is chosen from numbered prompts - relations, aliases, join type,
//! conditions - in the same style as the rest of the CLI. There is no query
//! language to learn and nothing to parse, so a mistyped entry is re-prompted
//! rather than producing a confusing error.
//!
//! No input path here may panic. Every index into a list is bounds-checked,
//! every parse is matched, and end-of-input returns to the menu instead of
//! spinning on an empty string.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{self, Write};
use std::time::{Duration, Instant};

use storage_manager::catalog::Catalog;
use storage_manager::executor::selection::{ComparisonOp, Constant, Expr, Predicate};
use storage_manager::join::index::JoinIndex;
use storage_manager::join::index::sorted_array::{create_index, drop_index};
use storage_manager::join::{
    ExecStats, JoinBuilder, JoinConfig, JoinType, RowCodec, TableRef, analyze_table,
    catalog_bridge, save_stats, spill,
};
use storage_manager::types::DataValue;

/// Rows printed before the output is truncated, so a large join does not
/// scroll the terminal away.
const MAX_DISPLAY_ROWS: usize = 200;

/// Read a line, trimmed. `None` means end of input.
fn prompt(label: &str) -> io::Result<Option<String>> {
    print!("{label}");
    io::stdout().flush()?;

    let mut line = String::new();
    // Ok(0) is end of input - Ctrl-D. Treating it as an empty string would
    // loop forever.
    if io::stdin().read_line(&mut line)? == 0 {
        println!();
        return Ok(None);
    }
    Ok(Some(line.trim().to_string()))
}

/// Offer a numbered list and return the chosen index.
fn choose(title: &str, options: &[String]) -> io::Result<Option<usize>> {
    if options.is_empty() {
        println!("  (nothing to choose from)");
        return Ok(None);
    }

    println!("\n{title}");
    for (index, option) in options.iter().enumerate() {
        println!("  {}. {option}", index + 1);
    }

    loop {
        let Some(answer) = prompt("  Choice (blank to cancel): ")? else {
            return Ok(None);
        };
        if answer.is_empty() {
            return Ok(None);
        }
        match answer.parse::<usize>() {
            Ok(number) if number >= 1 && number <= options.len() => return Ok(Some(number - 1)),
            _ => println!("  Enter a number between 1 and {}.", options.len()),
        }
    }
}

/// The join operations submenu.
pub fn join_menu(catalog: &Catalog, current_db: &Option<String>) -> io::Result<()> {
    let Some(database) = current_db.clone() else {
        println!("\n  Select a database first (main menu option 3).");
        return Ok(());
    };

    let mut config = JoinConfig::resolve();

    // Reclaim anything a previous run left behind after being killed. Normal
    // exits and panics clean up on their own.
    match spill::sweep_orphans(&config.spill_root, Duration::from_secs(3600)) {
        Ok(0) => {}
        Ok(removed) => println!("  Reclaimed {removed} abandoned spill director(ies)."),
        Err(e) => println!("  Could not check for abandoned spill files: {e}"),
    }

    loop {
        println!("\n╔════════════════════════════════════════╗");
        println!("║          JOIN OPERATIONS               ║");
        println!("╠════════════════════════════════════════╣");
        println!("║    1. Run a join                       ║");
        println!("║    2. Explain a join (plan only)       ║");
        println!("║    3. Analyze a table                  ║");
        println!("║    4. Build a join index               ║");
        println!("║    5. Drop a join index                ║");
        println!("║    6. Join settings                    ║");
        println!("║    7. Back to main menu                ║");
        println!("╚════════════════════════════════════════╝");

        let Some(choice) = prompt("\nEnter your choice (1-7): ")? else {
            return Ok(());
        };

        match choice.as_str() {
            "1" => run_join(catalog, &database, &config, true)?,
            "2" => run_join(catalog, &database, &config, false)?,
            "3" => analyze_cmd(catalog, &database)?,
            "4" => build_index_cmd(catalog, &database)?,
            "5" => drop_index_cmd(catalog, &database)?,
            "6" => settings_cmd(&mut config)?,
            "7" | "" => return Ok(()),
            _ => println!("  Invalid option. Please enter a number between 1 and 7."),
        }
    }
}

/// Everything the user chose for one join.
struct JoinRequestInput {
    left: TableRef,
    right: TableRef,
    join_type: JoinType,
    condition: Option<Predicate>,
}

fn gather(catalog: &Catalog, database: &str) -> io::Result<Option<JoinRequestInput>> {
    let tables = catalog_bridge::table_names(catalog, database);
    if tables.len() < 2 {
        println!("\n  A join needs at least two tables in '{database}'.");
        return Ok(None);
    }

    let Some(left_index) = choose("Left table:", &tables)? else {
        return Ok(None);
    };
    let Some(right_index) = choose("Right table:", &tables)? else {
        return Ok(None);
    };

    let left_name = &tables[left_index];
    let right_name = &tables[right_index];

    // A self-join needs distinct aliases, or no qualified name means anything.
    let (default_left, default_right) = if left_name == right_name {
        (format!("{left_name}_1"), format!("{right_name}_2"))
    } else {
        (left_name.clone(), right_name.clone())
    };

    let Some(left_alias) = prompt(&format!("  Alias for the left table [{default_left}]: "))?
    else {
        return Ok(None);
    };
    let Some(right_alias) = prompt(&format!("  Alias for the right table [{default_right}]: "))?
    else {
        return Ok(None);
    };

    let left_alias = if left_alias.is_empty() {
        default_left
    } else {
        left_alias
    };
    let right_alias = if right_alias.is_empty() {
        default_right
    } else {
        right_alias
    };

    if left_alias == right_alias {
        println!("  The two sides need different aliases.");
        return Ok(None);
    }

    let left = match catalog_bridge::resolve(catalog, database, left_name, &left_alias) {
        Ok(table) => table,
        Err(e) => {
            println!("  {e}");
            return Ok(None);
        }
    };
    let right = match catalog_bridge::resolve(catalog, database, right_name, &right_alias) {
        Ok(table) => table,
        Err(e) => {
            println!("  {e}");
            return Ok(None);
        }
    };

    let type_names: Vec<String> = JoinType::ALL
        .iter()
        .map(|join_type| join_type.name().to_string())
        .collect();
    let Some(type_index) = choose("Join type:", &type_names)? else {
        return Ok(None);
    };
    let Some(join_type) = JoinType::ALL.get(type_index).copied() else {
        return Ok(None);
    };

    // A CROSS join is the absence of a condition; asking for one and then
    // refusing it at plan time would be a poor conversation.
    let condition = if join_type == JoinType::Cross {
        None
    } else {
        match gather_condition(&left, &right)? {
            Some(condition) => condition,
            None => return Ok(None),
        }
    };

    Ok(Some(JoinRequestInput {
        left,
        right,
        join_type,
        condition,
    }))
}

const OPERATORS: [(&str, ComparisonOp); 6] = [
    ("=", ComparisonOp::Equals),
    ("<>", ComparisonOp::NotEquals),
    ("<", ComparisonOp::LessThan),
    ("<=", ComparisonOp::LessOrEqual),
    (">", ComparisonOp::GreaterThan),
    (">=", ComparisonOp::GreaterOrEqual),
];

/// Collect conditions one at a time, combined with AND.
fn gather_condition(left: &TableRef, right: &TableRef) -> io::Result<Option<Option<Predicate>>> {
    let mut conjuncts: Vec<Predicate> = Vec::new();

    loop {
        let actions = vec![
            "Compare a left column to a right column".to_string(),
            "Compare a column to a value".to_string(),
            if conjuncts.is_empty() {
                "Done (no condition)".to_string()
            } else {
                format!("Done ({} condition(s))", conjuncts.len())
            },
        ];

        let Some(action) = choose("Add a join condition:", &actions)? else {
            return Ok(None);
        };

        match action {
            0 => {
                let Some(predicate) = column_to_column(left, right)? else {
                    continue;
                };
                conjuncts.push(predicate);
            }
            1 => {
                let Some(predicate) = column_to_value(left, right)? else {
                    continue;
                };
                conjuncts.push(predicate);
            }
            _ => break,
        }
    }

    if conjuncts.is_empty() {
        return Ok(Some(None));
    }

    let mut iter = conjuncts.into_iter();
    let Some(first) = iter.next() else {
        return Ok(Some(None));
    };
    Ok(Some(Some(iter.fold(first, Predicate::and))))
}

fn qualified_columns(table: &TableRef) -> Vec<String> {
    table
        .columns
        .iter()
        .map(|column| format!("{}.{}", table.alias, column.name))
        .collect()
}

fn column_to_column(left: &TableRef, right: &TableRef) -> io::Result<Option<Predicate>> {
    let left_columns = qualified_columns(left);
    let right_columns = qualified_columns(right);

    let Some(left_index) = choose("Left column:", &left_columns)? else {
        return Ok(None);
    };
    let operator_names: Vec<String> = OPERATORS
        .iter()
        .map(|(symbol, _)| (*symbol).to_string())
        .collect();
    let Some(operator_index) = choose("Operator:", &operator_names)? else {
        return Ok(None);
    };
    let Some(right_index) = choose("Right column:", &right_columns)? else {
        return Ok(None);
    };

    let (Some(left_name), Some(right_name), Some((_, operator))) = (
        left_columns.get(left_index),
        right_columns.get(right_index),
        OPERATORS.get(operator_index),
    ) else {
        return Ok(None);
    };

    Ok(Some(Predicate::Compare(
        Box::new(column_expr(left_name)),
        *operator,
        Box::new(column_expr(right_name)),
    )))
}

fn column_to_value(left: &TableRef, right: &TableRef) -> io::Result<Option<Predicate>> {
    let mut columns = qualified_columns(left);
    columns.extend(qualified_columns(right));

    let Some(column_index) = choose("Column:", &columns)? else {
        return Ok(None);
    };
    let operator_names: Vec<String> = OPERATORS
        .iter()
        .map(|(symbol, _)| (*symbol).to_string())
        .collect();
    let Some(operator_index) = choose("Operator:", &operator_names)? else {
        return Ok(None);
    };
    let Some(literal) = prompt("  Value (blank for NULL): ")? else {
        return Ok(None);
    };

    let (Some(column), Some((_, operator))) =
        (columns.get(column_index), OPERATORS.get(operator_index))
    else {
        return Ok(None);
    };

    // An integer is recognised as one; everything else is text. There is no
    // parser here, so the value is taken exactly as typed.
    let constant = if literal.is_empty() {
        Constant::Null
    } else if let Ok(number) = literal.parse::<i32>() {
        Constant::Int(number)
    } else {
        Constant::Text(literal)
    };

    Ok(Some(Predicate::Compare(
        Box::new(column_expr(column)),
        *operator,
        Box::new(Expr::Constant(constant)),
    )))
}

fn column_expr(qualified: &str) -> Expr {
    Expr::Column(storage_manager::executor::selection::ColumnReference::new(
        qualified.to_string(),
    ))
}

/// Plan a join, and optionally run it.
fn run_join(
    catalog: &Catalog,
    database: &str,
    config: &JoinConfig,
    execute: bool,
) -> io::Result<()> {
    let Some(request) = gather(catalog, database)? else {
        return Ok(());
    };

    let mut builder = JoinBuilder::new(request.left, request.right, request.join_type)
        .with_config(config.clone());
    if let Some(condition) = request.condition {
        builder = builder.with_condition(condition);
    }

    let plan = match builder.plan() {
        Ok(plan) => plan,
        Err(e) => {
            println!("\n  Cannot plan this join: {e}");
            return Ok(());
        }
    };

    println!("\n{}", plan.render());

    if !execute {
        return Ok(());
    }

    let started = Instant::now();
    let mut stream = match builder.execute() {
        Ok(stream) => stream,
        Err(e) => {
            println!("  Cannot run this join: {e}");
            return Ok(());
        }
    };

    let codec = RowCodec::new(stream.schema().types.clone());
    let headers: Vec<String> = stream
        .schema()
        .columns
        .iter()
        .map(|column| column.qualified_name.clone())
        .collect();

    println!("  {}", headers.join(" | "));
    println!("  {}", "-".repeat(headers.join(" | ").len()));

    let mut produced = 0usize;
    let mut truncated = false;

    loop {
        let Some(row) = stream.next() else { break };
        let row = match row {
            Ok(row) => row,
            Err(e) => {
                println!("\n  The join stopped: {e}");
                break;
            }
        };

        produced += 1;
        if produced > MAX_DISPLAY_ROWS {
            truncated = true;
            continue;
        }

        match codec.decode(&row) {
            Ok(values) => println!("  {}", render_row(&values)),
            Err(e) => println!("  <undecodable row: {e}>"),
        }
    }

    if truncated {
        println!(
            "  ... {} more row(s) not shown",
            produced.saturating_sub(MAX_DISPLAY_ROWS)
        );
    }
    println!("\n  {produced} row(s) in {:.2?}", started.elapsed());
    print_stats(&stream.stats());

    Ok(())
}

fn render_row(values: &[Option<DataValue>]) -> String {
    values
        .iter()
        .map(|value| match value {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Report what the operator actually did, as distinct from what was predicted.
fn print_stats(stats: &ExecStats) {
    println!(
        "  Scanned {} outer row(s), {} inner row(s), checked {} pair(s).",
        stats.outer_rows, stats.inner_rows, stats.candidate_pairs
    );

    if stats.spilled_bytes > 0 {
        println!(
            "  Spilled {} byte(s) across {} partition(s); {} sort run(s), {} merge pass(es).",
            stats.spilled_bytes, stats.partitions, stats.sort_runs, stats.merge_passes
        );
    }
    if stats.oversized_partitions > 0 {
        println!(
            "  {} partition(s) could not be split further - one key dominates them.",
            stats.oversized_partitions
        );
    }
    if stats.spilled_groups > 0 {
        println!(
            "  {} duplicate group(s) had to spill.",
            stats.spilled_groups
        );
    }
    if stats.role_reversed {
        println!("  Built from the left relation: it turned out to be the smaller one.");
    }
    if stats.strategy_switches > 0 {
        println!(
            "  Reduced the memory budget {} time(s) under system memory pressure.",
            stats.strategy_switches
        );
    }
}

fn pick_table(catalog: &Catalog, database: &str, title: &str) -> io::Result<Option<TableRef>> {
    let tables = catalog_bridge::table_names(catalog, database);
    let Some(index) = choose(title, &tables)? else {
        return Ok(None);
    };
    let Some(name) = tables.get(index) else {
        return Ok(None);
    };

    match catalog_bridge::resolve(catalog, database, name, name) {
        Ok(table) => Ok(Some(table)),
        Err(e) => {
            println!("  {e}");
            Ok(None)
        }
    }
}

fn analyze_cmd(catalog: &Catalog, database: &str) -> io::Result<()> {
    let Some(table) = pick_table(catalog, database, "Analyze which table?")? else {
        return Ok(());
    };

    let started = Instant::now();
    let stats = match analyze_table(&table) {
        Ok(stats) => stats,
        Err(e) => {
            println!("  Could not analyze '{}': {e}", table.alias);
            return Ok(());
        }
    };

    match save_stats(&table, &stats) {
        Ok(path) => println!("\n  Wrote statistics to {}", path.display()),
        Err(e) => println!("  Collected statistics but could not save them: {e}"),
    }

    println!(
        "  {} row(s), {} data page(s), {:.1} bytes per row, in {:.2?}",
        stats.rows,
        stats.data_pages,
        stats.avg_row_bytes,
        started.elapsed()
    );
    println!("\n  Column            Distinct    NULLs   Histogram");
    for column in &stats.columns {
        println!(
            "  {:<16}  {:>8}  {:>6.1}%   {}",
            column.name,
            column.distinct,
            column.null_fraction * 100.0,
            if column.histogram.is_some() {
                "yes"
            } else {
                "no"
            }
        );
    }

    Ok(())
}

fn build_index_cmd(catalog: &Catalog, database: &str) -> io::Result<()> {
    let Some(table) = pick_table(catalog, database, "Index which table?")? else {
        return Ok(());
    };

    let names: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let Some(column) = choose("Index which column?", &names)? else {
        return Ok(());
    };

    match create_index(&table, &[column]) {
        Ok((index, path)) => {
            println!(
                "\n  Built an index of {} entries at {}",
                index.entry_count(),
                path.display()
            );
            println!("  It will be used automatically while the table is unchanged.");
        }
        Err(e) => println!("  Could not build the index: {e}"),
    }

    Ok(())
}

fn drop_index_cmd(catalog: &Catalog, database: &str) -> io::Result<()> {
    let Some(table) = pick_table(catalog, database, "Drop an index from which table?")? else {
        return Ok(());
    };

    let names: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let Some(column) = choose("Which column's index?", &names)? else {
        return Ok(());
    };

    match drop_index(&table, &[column]) {
        Ok(true) => println!("\n  Index removed."),
        Ok(false) => println!("\n  There was no index on that column."),
        Err(e) => println!("  Could not remove the index: {e}"),
    }

    Ok(())
}

fn settings_cmd(config: &mut JoinConfig) -> io::Result<()> {
    println!("\n  Working memory : {} bytes", config.work_memory_bytes);
    println!("  Spill directory: {}", config.spill_root.display());
    println!(
        "  (Set {} to change the default for new sessions.)",
        storage_manager::join::config::WORK_MEMORY_ENV
    );

    let Some(answer) = prompt("\n  New working memory in bytes (blank to keep): ")? else {
        return Ok(());
    };
    if answer.is_empty() {
        return Ok(());
    }

    match answer.parse::<u64>() {
        Ok(bytes) => {
            *config = config.clone().work_memory(bytes);
            println!(
                "  Working memory is now {} bytes.",
                config.work_memory_bytes
            );
            println!("  (Clamped into the supported range if the value was extreme.)");
        }
        Err(_) => println!("  That is not a byte count."),
    }

    Ok(())
}
