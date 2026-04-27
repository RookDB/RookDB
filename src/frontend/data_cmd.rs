use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{init_catalog_page_storage, load_catalog};
use storage_manager::executor::{load_csv, show_tuples};
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

    let mut catalog = load_catalog(buffer_manager);
    let mut pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

    load_csv(
        &mut catalog,
        &mut pm,
        buffer_manager,
        &db,
        table,
        &mut file,
        csv_path,
    )?;

    println!("Page Count: {}", page_count(&mut file)?);

    Ok(())
}

pub fn show_tuples_cmd(
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

    let path = format!("database/base/{}/{}.dat", db, table);
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;

    let mut catalog = load_catalog(buffer_manager);
    let mut pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    
    show_tuples(&mut catalog, &mut pm, buffer_manager, &db, table, &mut file)?;

    Ok(())
}
