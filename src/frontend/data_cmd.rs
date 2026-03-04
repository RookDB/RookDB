// use std::fs::OpenOptions;
// use std::io::{self, Write};

// use storage_manager::buffer_manager::BufferManager;
// use storage_manager::catalog::load_catalog;
// use storage_manager::executor::show_tuples;
// use storage_manager::table::page_count;

// pub fn load_csv_cmd(
//     buffer_manager: &mut BufferManager,
//     current_db: &Option<String>,
// ) -> io::Result<()> {
//     let db = match current_db {
//         Some(db) => db.clone(),
//         None => {
//             println!("No database selected. Please select a database first");
//             return Ok(());
//         }
//     };

//     let mut table = String::new();
//     print!("Enter table name: ");
//     io::stdout().flush()?;
//     io::stdin().read_line(&mut table)?;
//     let table = table.trim();

//     let mut csv_path = String::new();
//     print!("Enter CSV path: ");
//     io::stdout().flush()?;
//     io::stdin().read_line(&mut csv_path)?;
//     let csv_path = csv_path.trim();

//     let catalog = load_catalog();
//     buffer_manager.load_csv_to_buffer(&catalog, &db, table, csv_path)?;

//     let path = format!("database/base/{}/{}.dat", db, table);
//     let mut file = OpenOptions::new().read(true).write(true).open(path)?;
//     println!("Page Count: {}", page_count(&mut file)?);

//     Ok(())
// }

// pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
//     let db = match current_db {
//         Some(db) => db.clone(),
//         None => {
//             println!("No database selected. Please select a database first");
//             return Ok(());
//         }
//     };

//     let mut table = String::new();
//     print!("Enter table name: ");
//     io::stdout().flush()?;
//     io::stdin().read_line(&mut table)?;
//     let table = table.trim();

//     let path = format!("database/base/{}/{}.dat", db, table);
//     let mut file = OpenOptions::new().read(true).write(true).open(path)?;

//     let catalog = load_catalog();
//     show_tuples(&catalog, &db, table, &mut file)?;

//     Ok(())
// }
use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::executor::{select_tuples, ProjectionRequest};
use storage_manager::table::page_count;

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

    // ✅ NEW: projection input
    let mut proj_input = String::new();
    print!("Enter attributes (* or comma-separated list like id,name): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut proj_input)?;
    let proj_input = proj_input.trim();

    let projection = if proj_input.is_empty() || proj_input == "*" {
        ProjectionRequest::All
    } else {
        let cols: Vec<String> = proj_input
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        ProjectionRequest::List(cols)
    };

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let catalog = load_catalog();
    select_tuples(&catalog, &db, table, &mut file, projection)?;

    Ok(())
}
