//! buffer_test_cmd.rs
//! Contains implementations for BufferPool test commands

use std::io;

use storage_manager::{BufferPool, PageId};

// -----------------------------
// FETCH PAGE
// -----------------------------
pub fn fetch_page_cmd(
    buffer_pool: &mut BufferPool,
    table_name: String,
    page_number: u32,
) {
    match buffer_pool.fetch_page(table_name, page_number) {
        Ok(_) => println!("✅ Page fetched successfully."),
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