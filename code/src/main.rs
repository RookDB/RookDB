// use std::fs::OpenOptions;
// use std::io::{self, Read, Seek, SeekFrom};
use std::io::{self, Write};

// use storage_manager::disk::{create_page, read_page};
// use storage_manager::disk::create_page;
// use storage_manager::page::{page_add_data, Page};
use storage_manager::catalog::{Column, init_catalog, load_catalog, create_table};
// use storage_manager::table::init_table;

fn main() -> io::Result<()> {
    println!("----");
    println!("Welcome to Storage Manager\n");

    // Initialising Catalog File
    println!("Initialising Catalog File\n");
    init_catalog();

    // Load Catalog File
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();

    if catalog.tables.len() == 0 {
        println!("No tables found in catalog.\n");
    } else {
        println!("Loaded tables from catalog:\n");
        for (table_name, table) in &catalog.tables {
            println!("Table: {}", table_name);
            for column in &table.columns {
                println!("  Column: {} ({})", column.name, column.data_type);
            }
            println!();
        }
    }

    println!("--------------------------");
    println!("Create a Table");
    println!("--------------------------\n");

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
            println!("Invalid format. Please use name:type format (e.g. id:INT)");
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
        return Ok(());
    }

    // Pass collected input to the create_table function
    create_table(&mut catalog, &table_name, columns);


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
