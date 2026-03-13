use std::io::{self, Write};
use std::fs::OpenOptions;

use storage_manager::catalog::{load_catalog, types::Column};
use storage_manager::buffer_manager::BufferManager;
use storage_manager::table::page_count;
use storage_manager::executor::{
    show_tuples, delete_tuples, parse_where_clause_with_schema,
    update_tuples, parse_set_clause, compaction_table,
};

pub fn load_csv_cmd(
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
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

    let mut csv_path = String::new();
    print!("Enter CSV path: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut csv_path)?;
    let csv_path = csv_path.trim();

    let catalog = load_catalog();
    buffer_manager.load_csv_to_buffer(&catalog, &db, table, csv_path)?;

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    println!("Page Count: {}", page_count(&mut file)?);

    Ok(())
}

pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
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

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let catalog = load_catalog();
    show_tuples(&catalog, &db, table, &mut file)?;

    Ok(())
}

/// Interactive DELETE command.
///
/// Accepts a single WHERE clause string with full AND / OR / parentheses support.
///
/// Examples:
///   (leave empty)                                       → DELETE ALL rows
///   price > 10                                          → simple condition
///   dept = HR AND salary < 50000                        → AND
///   dept = HR OR dept = Sales                           → OR
///   (dept = HR AND salary < 50000) OR dept = Sales      → mixed
///   (c1 = 1 AND c2 = 2) AND (c3 = 3 OR c4 = 4)        → nested (auto-expanded to DNF)
pub fn delete_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    // -- table name --
    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim().to_string();

    // -- resolve column schema for type-aware WHERE parsing --
    let catalog = load_catalog();
    let columns: Vec<Column> = catalog.databases
        .get(&db)
        .and_then(|d| d.tables.get(table.as_str()))
        .map(|t| t.columns.clone())
        .unwrap_or_default();

    // -- single-line WHERE clause --
    println!();
    println!("Supported operators : =  !=  <  <=  >  >=");
    println!("Logical connectors  : AND  OR");
    println!("Grouping            : use parentheses ( )");
    println!("Leave empty         : delete ALL rows");
    println!();
    print!("WHERE clause: ");
    io::stdout().flush()?;
    let mut where_input = String::new();
    io::stdin().read_line(&mut where_input)?;
    let where_input = where_input.trim();

    let condition_groups = match parse_where_clause_with_schema(where_input, &columns) {
        Some(groups) => {
            println!("Parsed into {} AND-group(s) connected by OR:", groups.len());
            for (i, group) in groups.iter().enumerate() {
                let desc: Vec<String> = group
                    .iter()
                    .map(|c| format!("{} {:?} {:?}", c.column, c.operator, c.value))
                    .collect();
                println!("  Group {}: {}", i + 1, desc.join(" AND "));
            }
            groups
        }
        None => {
            println!("No WHERE clause – this will delete ALL rows in '{}'.", table);
            print!("Are you sure? (yes/no): ");
            io::stdout().flush()?;
            let mut confirm = String::new();
            io::stdin().read_line(&mut confirm)?;
            if !confirm.trim().eq_ignore_ascii_case("yes") {
                println!("Aborted.");
                return Ok(());
            }
            vec![]
        }
    };

    // -- RETURNING --
    print!("\nPrint deleted rows? (y/n): ");
    io::stdout().flush()?;
    let mut ret_input = String::new();
    io::stdin().read_line(&mut ret_input)?;
    let returning = ret_input.trim().eq_ignore_ascii_case("y");

    // -- execute --
    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            println!("Could not open table '{}': {}", table, e);
            return Ok(());
        }
    };

    match delete_tuples(&catalog, &db, &table, &mut file, &condition_groups, returning) {
        Ok(result) => {
            println!("\nDeleted {} row(s).", result.deleted_count);

            if returning && !result.returning_rows.is_empty() {
                println!("\n=== Deleted rows ===");
                for row in &result.returning_rows {
                    let cols: Vec<String> =
                        row.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                    println!("  {}", cols.join("  |  "));
                }
                println!("===================");
            }
        }
        Err(e) => println!("Delete failed: {}", e),
    }

    Ok(())
}

/// Interactive UPDATE command.
///
/// Prompts for SET assignments and an optional WHERE clause.
///
/// Examples:
///   SET  : age = 25
///   SET  : age = age + 1
///   SET  : salary = salary * 1.10 , dept = Engineering
///   WHERE: id > 5 AND dept = HR
pub fn update_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim().to_string();

    // -- resolve column schema for type-aware WHERE parsing --
    let catalog = load_catalog();
    let columns: Vec<Column> = catalog.databases
        .get(&db)
        .and_then(|d| d.tables.get(table.as_str()))
        .map(|t| t.columns.clone())
        .unwrap_or_default();

    // -- SET clause --
    println!();
    println!("SET clause examples:");
    println!("  age = 25");
    println!("  age = age + 1");
    println!("  salary = salary * 1.10 , dept = Engineering");
    println!();
    print!("SET: ");
    io::stdout().flush()?;
    let mut set_input = String::new();
    io::stdin().read_line(&mut set_input)?;
    let set_input = set_input.trim();

    let assignments = match parse_set_clause(set_input) {
        Some(a) if !a.is_empty() => a,
        _ => {
            println!("Could not parse SET clause. Aborted.");
            return Ok(());
        }
    };

    // -- WHERE clause --
    println!();
    println!("WHERE clause (leave empty to update ALL rows):");
    print!("WHERE: ");
    io::stdout().flush()?;
    let mut where_input = String::new();
    io::stdin().read_line(&mut where_input)?;
    let where_input = where_input.trim();

    let condition_groups = match parse_where_clause_with_schema(where_input, &columns) {
        Some(groups) => groups,
        None => {
            println!("No WHERE clause – this will update ALL rows in '{}'.", table);
            print!("Are you sure? (yes/no): ");
            io::stdout().flush()?;
            let mut confirm = String::new();
            io::stdin().read_line(&mut confirm)?;
            if !confirm.trim().eq_ignore_ascii_case("yes") {
                println!("Aborted.");
                return Ok(());
            }
            vec![]
        }
    };

    // -- RETURNING --
    print!("\nPrint updated rows? (y/n): ");
    io::stdout().flush()?;
    let mut ret_input = String::new();
    io::stdin().read_line(&mut ret_input)?;
    let returning = ret_input.trim().eq_ignore_ascii_case("y");

    // -- execute --
    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            println!("Could not open table '{}': {}", table, e);
            return Ok(());
        }
    };

    match update_tuples(&catalog, &db, &table, &mut file, &assignments, &condition_groups, returning) {
        Ok(result) => {
            println!("\nUpdated {} row(s).", result.updated_count);

            if returning && !result.returning_rows.is_empty() {
                println!("\n=== Updated rows (after) ===");
                for row in &result.returning_rows {
                    let cols: Vec<String> =
                        row.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                    println!("  {}", cols.join("  |  "));
                }
                println!("========================");
            }
        }
        Err(e) => println!("Update failed: {}", e),
    }

    Ok(())
}

/// Manually triggers compaction on a table, printing BEFORE/AFTER page stats.
pub fn compact_table_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim().to_string();

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            println!("Could not open table '{}': {}", table, e);
            return Ok(());
        }
    };

    // file was opened just to validate the table exists; compaction_table opens it internally
    drop(file);
    let pages_compacted = compaction_table(&db, &table)?;
    println!("\nCompaction complete. {} page(s) had dead tuples removed.", pages_compacted);

    Ok(())
}
