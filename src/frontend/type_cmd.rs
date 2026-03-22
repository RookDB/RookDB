use std::io::{self, Write};

use storage_manager::catalog::{Catalog, Column, create_type, show_types};

pub fn create_type_cmd(catalog: &mut Catalog, current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    let mut type_name = String::new();
    print!("Enter type name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut type_name)?;
    let type_name = type_name.trim().to_string();

    println!("\nEnter fields in the format:- name:type (INT, TEXT, BOOLEAN)");
    println!("Press Enter on an empty line to finish");

    let mut fields = Vec::new();
    loop {
        let mut input = String::new();
        print!("Enter field (name:type): ");
        io::stdout().flush()?;
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let parts: Vec<&str> = input.split(':').collect();
        if parts.len() != 2 {
            println!("Invalid format. Please use name:type (e.g. street:TEXT)");
            continue;
        }

        let data_type = parts[1].to_uppercase();
        if !["INT", "TEXT", "BOOLEAN"].contains(&data_type.as_str()) {
            println!("Invalid field type '{}'. Only INT, TEXT, BOOLEAN are allowed.", parts[1]);
            continue;
        }

        fields.push(Column {
            name: parts[0].to_string(),
            data_type,
        });
    }

    if fields.is_empty() {
        println!("No fields provided. Type not created.");
        return Ok(());
    }

    if let Err(e) = create_type(catalog, &db, &type_name, fields) {
        println!("Error: {}", e);
    }

    Ok(())
}

pub fn show_types_cmd(catalog: &Catalog, current_db: &Option<String>) {
    let db = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected. Please select a database first.");
            return;
        }
    };
    show_types(catalog, db);
}
