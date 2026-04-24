use std::fs::OpenOptions;
use std::io;
use std::path::PathBuf;

use crate::backend::fsm::fsm::FSM;
use crate::backend::heap::HeapManager;
use crate::disk::{read_header_page, update_header_page};

/// Facade API for the Compaction Team
///
/// These functions deliberately hide the `HeapManager` and `FSM`
/// internal state implementations from other teams.

/// Rebuild the table's Free Space Map (FSM) from scratch by scanning the heap.
/// Call this ONCE after a full table compaction/rewrite is completed.
pub fn rebuild_table_fsm(db_name: &str, table_name: &str) -> io::Result<()> {
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    let fsm_path   = PathBuf::from(format!("database/base/{}/{}.dat.fsm", db_name, table_name));

    // Remove the old FSM so build_from_heap starts with a clean slate.
    if fsm_path.exists() {
        std::fs::remove_file(&fsm_path)?;
    }

    // Directly call FSM::build_from_heap to scan every heap page and write
    // accurate free-space categories.  HeapManager::open is NOT used here
    // because its fsm_page_count mismatch check does NOT trigger build_from_heap.
    let mut heap_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)?;

    let _fsm = FSM::build_from_heap(&mut heap_file, fsm_path)?;

    // Persist the updated fsm_page_count so the next HeapManager::open finds
    // a consistent header (avoiding an unnecessary re-rebuild).
    let mut header = read_header_page(&mut heap_file)?;
    header.fsm_page_count = FSM::calculate_fsm_page_count(header.page_count);
    update_header_page(&mut heap_file, &header)?;

    log::info!("Successfully rebuilt FSM tree for table '{}'", table_name);
    Ok(())
}

/// Update the FSM after performing an in-place page compaction (VACUUM).
/// Rebuilds the entire FSM for the table so reclaimed space is immediately
/// searchable for future inserts.
pub fn update_page_free_space(
    db_name: &str,
    table_name: &str,
    _page_id: u32,
    _reclaimed_bytes: u32
) -> io::Result<()> {
    rebuild_table_fsm(db_name, table_name)
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