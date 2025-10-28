// use std::fs::OpenOptions;
// use std::io::{self, Read, Seek, SeekFrom};
use std::io::{self, Write};

// use storage_manager::disk::{create_page, read_page};
// use storage_manager::disk::create_page;
// use storage_manager::page::{page_add_data, Page};
use storage_manager::catalog::{Column, init_catalog, load_catalog, create_table, create_database};
// use storage_manager::table::init_table;

fn main() -> io::Result<()> {
   println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    // Initialize catalog if missing
    println!("Initializing Catalog File...\n");
    init_catalog();

    // Load catalog from disk
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();

    loop {
        println!("\n=============================");
        println!("Choose an option:");
        println!("1. Create Database");
        println!("2. Create Table");
        println!("3. Exit");
        println!("=============================");

        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            // -----------------------
            // Option 1: Create Database
            // -----------------------
            "1" => {
                let mut db_name = String::new();
                print!("\nEnter new database name: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut db_name)?;
                let db_name = db_name.trim();

                if db_name.is_empty() {
                    println!("Database name cannot be empty.");
                } else if create_database(&mut catalog, db_name) {
                    println!("Database '{}' created successfully.", db_name);
                } else {
                    println!("Failed to create database '{}'.", db_name);
                }
            }

            // -----------------------
            // Option 2: Create Table
            // -----------------------
            "2" => {
                if catalog.databases.is_empty() {
                    println!("No databases found. Please create a database first.");
                    continue;
                }

                println!("\nAvailable Databases:");
                for db in catalog.databases.keys() {
                    println!("- {}", db);
                }

                let mut db_name = String::new();
                print!("\nEnter database name to create table in: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut db_name)?;
                let db_name = db_name.trim();

                if !catalog.databases.contains_key(db_name) {
                    println!("Database '{}' does not exist.", db_name);
                    continue;
                }

                let mut table_name = String::new();
                print!("Enter new table name: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut table_name)?;
                let table_name = table_name.trim().to_string();

                println!("\nEnter columns in the format: column_name:data_type");
                println!("(Press Enter on an empty line to finish)\n");

                let mut columns: Vec<Column> = Vec::new();

                loop {
                    let mut input = String::new();
                    print!("Enter column (name:type): ");
                    io::stdout().flush()?;
                    io::stdin().read_line(&mut input)?;
                    let input = input.trim();

                    if input.is_empty() {
                        break;
                    }

                    let parts: Vec<&str> = input.split(':').collect();
                    if parts.len() != 2 {
                        println!("Invalid format. Please use name:type (e.g. id:INT)");
                        continue;
                    }

                    let column_name = parts[0].trim().to_string();
                    let column_type = parts[1].trim().to_string();

                    columns.push(Column {
                        name: column_name,
                        data_type: column_type,
                    });
                }

                if columns.is_empty() {
                    println!("No columns provided. Table not created.");
                    continue;
                }

                create_table(&mut catalog, db_name, &table_name, columns);
            }

            // -----------------------
            // Option 3: Exit
            // -----------------------
            "3" => {
                println!("\nExiting Storage Manager. Goodbye!");
                break;
            }

            // -----------------------
            // Invalid Option
            // -----------------------
            _ => {
                println!("Invalid choice. Please select 1, 2, or 3.");
            }
        }
    }

    // Create File Pointer
    // let mut file_pointer = OpenOptions::new()
    //     .read(true)
    //     .write(true)
    //     .create(true)
    //     .open(CATALOG_FILE)?;

    // println!("Initialising Table...");

    // Init Table
    // init_table(&mut file_pointer)?;

    // Move cursor to start
    // file_pointer.seek(SeekFrom::Start(0))?;

    // Read entire file
    // let mut buffer = Vec::new();
    // file_pointer.read_to_end(&mut buffer)?;

    // Table File Data
    // println!("Table Initialised with Table Header. Table Content: {:?}\n", buffer);

    // println!("Creating Page in File...");
    /*
    Create a Page in file
    */
    // create_page(&mut file_pointer)?;
    // println!("Page created successfully.");

    // Read entire file to verify create page
    // file_pointer.seek(SeekFrom::Start(0))?;
    // let mut buffer = Vec::new();
    // file_pointer.read_to_end(&mut buffer)?;

    // Table File Data
    // println!("File Data after page creation: \n{:?}\n", buffer);

    // Create a Page in Memory
    // let mut page: Page = Page::new();

    // let content = b"Hello, Storage Manager!";
    // page.data[..content.len()].copy_from_slice(content);

    // let page_num: u32 = 0;

    // Write page to file
    // write_page(&mut file_pointer, &mut page, page_num)?;
    // println!("Updated Page with content Successfully.");

    /*
    Reading a Page from Disk file to Memory Page
    File: Catalog
    Page: page
    PageNum: 0
    */
    // read_page(&mut file_pointer, &mut page, page_num)?;
    // let page_text = String::from_utf8_lossy(&page.data);
    // println!("Page Header Bytes in Little Endian: {:?}", &page.data[..8]);

    // Adding Data to the File
    // let data_to_add = b"This is some raw data to add to the file.";
    // page_add_data(&mut file_pointer, data_to_add)?;
    // println!("Data added to file.");

    // Read the page again to check data insertion status
    // Read page 0
    // let mut page: Page = Page::new();
    // read_page(&mut file_pointer, &mut page, 0)?;

    // Convert the full page buffer to a UTF-8 string (lossy-safe)
    // let page_text = String::from_utf8_lossy(&page.data);

    // Print the page in human-readable form
    // println!("Complete Page 0 (human-readable):");
    // println!("{}", page_text);
    Ok(())
}
