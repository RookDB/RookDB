//! buffer_test_cmd.rs
//! Contains implementations for BufferPool test commands

use std::io;

use storage_manager::{BufferPool, PageId};
use storage_manager::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use storage_manager::catalog::types::Catalog;

// -----------------------------
// FETCH PAGE
// -----------------------------
// pub fn fetch_page_cmd(
//     buffer_pool: &mut BufferPool,
//     table_name: String,
//     page_number: u32,
// ) {
//     match buffer_pool.fetch_page(table_name, page_number) {
//         Ok(_) => println!("✅ Page fetched successfully."),
//         Err(e) => println!("❌ Error fetching page: {}", e),
//     }

// }

pub fn fetch_page_cmd(
    buffer_pool: &mut BufferPool,
    catalog: &Catalog,
    db_name: &Option<String>,
    table_name: String,
    page_number: u32,
) {
    match buffer_pool.fetch_page(table_name.clone(), page_number) {
        Ok(page) => {
            println!("✅ Page {} fetched successfully.\n", page_number);

            // 🔥 Call the new function
            display_page_tuples(page, catalog, db_name, &table_name);
        }
        Err(e) => println!("❌ Error fetching page: {}", e),
    }
}

// -----------------------------
// CREATE NEW PAGE
// -----------------------------
pub fn new_page_cmd(
    buffer_pool: &mut BufferPool,
    table_name: String,
) {
    match buffer_pool.new_page(table_name) {
        Ok((page_id, _)) => {
            println!(
                "✅ New page created -> {}:{}",
                page_id.table_name, page_id.page_number
            );
        }
        Err(e) => println!("❌ Error creating page: {}", e),
    }
}

// -----------------------------
// UNPIN PAGE
// -----------------------------
pub fn unpin_page_cmd(
    buffer_pool: &mut BufferPool,
    page_id: PageId,
    is_dirty: bool,
) {
    match buffer_pool.unpin_page(&page_id, is_dirty) {
        Ok(_) => {
            if is_dirty {
                println!("✅ Page unpinned and marked dirty.");
            } else {
                println!("✅ Page unpinned.");
            }
        }
        Err(e) => println!("❌ Error: {}", e),
    }
}

// -----------------------------
// FLUSH PAGE
// -----------------------------
pub fn flush_page_cmd(
    buffer_pool: &mut BufferPool,
    page_id: PageId,
) {
    match buffer_pool.flush_page(&page_id) {
        Ok(_) => println!("✅ Page flushed."),
        Err(e) => println!("❌ Error: {}", e),
    }
}

// -----------------------------
// FLUSH ALL PAGES
// -----------------------------
pub fn flush_all_pages_cmd(
    buffer_pool: &mut BufferPool,
) {
    match buffer_pool.flush_all_pages() {
        Ok(_) => println!("✅ All dirty pages flushed."),
        Err(e) => println!("❌ Error: {}", e),
    }
}

// -----------------------------
// DELETE PAGE
// -----------------------------
pub fn delete_page_cmd(
    buffer_pool: &mut BufferPool,
    page_id: PageId,
) {
    match buffer_pool.delete_page(&page_id) {
        Ok(_) => println!("✅ Page deleted from buffer."),
        Err(e) => println!("❌ Error: {}", e),
    }
}

// -----------------------------
// SHOW BUFFER STATS
// -----------------------------
pub fn show_stats_cmd(
    buffer_pool: &BufferPool,
) {
    let stats = &buffer_pool.stats;

    println!("\n📊 Buffer Statistics:");
    println!("Hits: {}", stats.hit_count);
    println!("Misses: {}", stats.miss_count);
    println!("Evictions: {}", stats.eviction_count);
    println!("Dirty Flushes: {}", stats.dirty_flush_count);
}

// -----------------------------
// SHOW FRAME TABLE
// -----------------------------
pub fn show_frames_cmd(
    buffer_pool: &BufferPool,
) {
    println!("\n🧠 Frame Table State:");

    for (i, frame) in buffer_pool.frames.iter().enumerate() {
        let meta = &frame.metadata;

        if let Some(page_id) = &meta.page_id {
            println!(
                "Frame {} -> Page {}:{}, Pin: {}, Dirty: {}, Usage: {}",
                i,
                page_id.table_name,
                page_id.page_number,
                meta.pin_count,
                meta.dirty,
                meta.usage_count
            );
        } else {
            println!("Frame {} -> Empty", i);
        }
    }
}


pub fn display_page_tuples(
    page: &Page,
    catalog: &Catalog,
    db_name: &Option<String>,
    table_name: &str,
) {

    let db_name = match db_name {
        Some(name) => name,
        None => {
            println!("❌ No database selected");
            return;
        }
    };
    
    // 1. Get schema
    let db = match catalog.databases.get(db_name) {
        Some(db) => db,
        None => {
            println!("❌ Database '{}' not found", db_name);
            return;
        }
    };

    let table = match db.tables.get(table_name) {
        Some(table) => table,
        None => {
            println!("❌ Table '{}' not found", table_name);
            return;
        }
    };

    let columns = &table.columns;

    // 2. Read page header
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

    println!("--- Page Metadata ---");
    println!("Lower Offset: {}", lower);
    println!("Upper Offset: {}", upper);
    println!("Number of Tuples: {}", num_items);

    // 3. Iterate tuples
    for i in 0..num_items {
        let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;

        let offset = u32::from_le_bytes(
            page.data[base..base + 4].try_into().unwrap(),
        );

        let length = u32::from_le_bytes(
            page.data[base + 4..base + 8].try_into().unwrap(),
        );

        let tuple_data =
            &page.data[offset as usize..(offset + length) as usize];

        print!("Tuple {}: ", i + 1);

        // 4. Decode tuple
        let mut cursor = 0usize;

        for col in columns {
            match col.data_type.as_str() {
                "INT" => {
                    if cursor + 4 <= tuple_data.len() {
                        let val = i32::from_le_bytes(
                            tuple_data[cursor..cursor + 4]
                                .try_into()
                                .unwrap(),
                        );
                        print!("{}={} ", col.name, val);
                        cursor += 4;
                    }
                }
                "TEXT" => {
                    if cursor + 10 <= tuple_data.len() {
                        let text_bytes =
                            &tuple_data[cursor..cursor + 10];
                        let text = String::from_utf8_lossy(text_bytes)
                            .trim()
                            .to_string();
                        print!("{}='{}' ", col.name, text);
                        cursor += 10;
                    }
                }
                _ => {
                    print!("{}=<unsupported> ", col.name);
                }
            }
        }

        println!();
    }

    println!("--- End of Page ---\n");
}