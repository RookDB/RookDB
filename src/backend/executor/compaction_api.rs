use std::io;
use std::path::PathBuf;

use crate::backend::heap::HeapManager;

/// Facade API for the Compaction Team
/// 
/// These functions deliberately hide the `HeapManager` and `FSM` 
/// internal state implementations from other teams.

/// Rebuild the table's Free Space Map (FSM) from scratch.
/// Call this ONCE after a full table compaction/rewrite is completed.
/// This avoids the need to update free space continuously during rewriting.
pub fn rebuild_table_fsm(db_name: &str, table_name: &str) -> io::Result<()> {
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    let fsm_path = PathBuf::from(format!("database/base/{}/{}.dat.fsm", db_name, table_name));
    
    // 1. Delete the old FSM fork file
    if fsm_path.exists() {
        std::fs::remove_file(&fsm_path)?;
        log::info!("Dropped old FSM file for table '{}'", table_name);
    }
    
    // 2. Opening the HeapManager triggers `FSM::build_from_heap`
    // automatically when the .fsm file is missing.
    let _hm = HeapManager::open(table_path)?;
    log::info!("Successfully rebuilt FSM tree for table '{}'", table_name);
    
    Ok(())
}

/// Update the FSM after performing an in-place page compaction (VACUUM).
/// Call this after manually reorganizing tuples within a single page
/// to make the newly reclaimed space searchable for future inserts.
pub fn update_page_free_space(
    db_name: &str, 
    table_name: &str, 
    page_id: u32, 
    reclaimed_bytes: u32
) -> io::Result<()> {
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    
    // Open the HeapManager (loads the FSM)
    let mut hm = HeapManager::open(table_path)?;
    
    // Call the FSM vacuum update logic to make the reclaimed space searchable
    hm.vacuum_page(page_id, reclaimed_bytes)?; // Assumes vacuum_page or fsm.fsm_vacuum_update is accessible
    
    log::info!(
        "Vacuumed page {} in table '{}', reclaimed {} bytes", 
        page_id, table_name, reclaimed_bytes
    );
    
    Ok(())
}

/// Insert a raw tuple directly into the table.
/// This allows the compaction team to relocate a tuple without 
/// interacting with the FSM or raw page layout directly.
pub fn insert_raw_tuple(
    db_name: &str, 
    table_name: &str, 
    tuple_data: &[u8]
) -> io::Result<(u32, u32)> {
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    
    let mut hm = HeapManager::open(table_path)?;
    
    hm.insert_tuple(tuple_data)
}