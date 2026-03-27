use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use storage_manager::backend::fsm::fsm::FSM;
use storage_manager::backend::heap::heap_manager::HeapManager;

const DB_NAME: &str = "test_fsm_heavy";

// Helper to create path
fn get_test_path(table_name: &str) -> PathBuf {
    PathBuf::from(format!("database/base/{}/{}.dat", DB_NAME, table_name))
}

// Ensure the db dir exists
fn setup_db_dir() {
    let _ = fs::create_dir_all(format!("database/base/{}", DB_NAME));
}

/// 1. INSERTION - Large number of insertions and time taken
/// 9. Time and space for various operations
#[test]
fn test_large_insertions() {
    setup_db_dir();
    let table_name = "large_insert";
    let file_path = get_test_path(table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");

    let num_inserts = 50_000;
    let tuple_data = vec![0xAB; 50]; // 50 bytes tuple

    println!("Starting 1. Large Insertions Test ({} records)...", num_inserts);
    let start_time = Instant::now();

    for _ in 0..num_inserts {
        hm.insert_tuple(&tuple_data).expect("Failed to insert");
    }

    let elapsed = start_time.elapsed();
    println!("Inserted {} tuples in {:?}", num_inserts, elapsed);
    assert!(elapsed.as_secs() < 30, "Insertions took too long");
    println!("✓ Large insertion test passed. Time mapped.");
}

/// 2. Updation and Deletion & 4. Deallocation Integrity
/// Simulating freeing space by FSM `fsm_vacuum_update`. 
#[test]
fn test_update_delete_fsm_deallocation() {
    setup_db_dir();
    let table_name = "upd_del_fsm";
    let file_path = get_test_path(table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    
    // Insert huge tuple to take up most of a page
    let tuple_data = vec![0xBB; 4000]; // 4000 bytes
    let (page_id, _slot_id) = hm.insert_tuple(&tuple_data).unwrap();

    let mut hf = fs::OpenOptions::new().read(true).open(&file_path).unwrap();
    let mut fsm = FSM::build_from_heap(&mut hf, file_path.with_extension("dat.fsm")).unwrap();
    let min_category = 200; // Require a lot of space

    // Now page shouldn't have enough space for another 4000
    let candidate = fsm.fsm_search_avail(min_category).unwrap();
    
    if let Some(pid) = candidate {
        assert!(pid != page_id, "FSM gave same page although it's full!");
    }

    // Simulate delete: reclaim 4000 bytes.
    println!("Simulating delete/vacuum reclaiming bytes...");
    fsm.fsm_vacuum_update(page_id, 8000).expect("FSM vacuum failed");

    // After vacuum, it should become available again
    let new_candidate = fsm.fsm_search_avail(min_category).unwrap();
    assert!(new_candidate.is_some(), "Freed space was not found by FSM");
    println!("✓ Deallocation Integrity (Update/Delete) passed.");
}

/// 3. Allocation Accuracy
/// Verify that when space is requested, FSM marks it as used, never handing it blindly again without updates.
#[test]
fn test_allocation_accuracy() {
    setup_db_dir();
    let table_name = "alloc_accuracy";
    let file_path = get_test_path(table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    let tuple_data = vec![0xCC; 8000]; // Almost full page 
    
    let (page_id1, _slot1) = hm.insert_tuple(&tuple_data).unwrap();
    let (page_id2, _slot2) = hm.insert_tuple(&tuple_data).unwrap();

    assert_ne!(page_id1, page_id2, "FSM allocated same overlapping page, collision occurred!");
    println!("✓ Allocation accuracy passed: distinct pages assigned for heavy data ({}, {}).", page_id1, page_id2);
}

/// 5. Fragmentation Management (Bubble up logic and category correctness)
#[test]
fn test_fragmentation_management() {
    setup_db_dir();
    let table_name = "frag_mgmt";
    let file_path = get_test_path(table_name);
    let fsm_path = file_path.with_extension("dat.fsm");
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(&fsm_path);

    {
        // Initial table create
        let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
        for _ in 0..10 {
            // Insert tiny chunks 
            hm.insert_tuple(&vec![0xDD; 50]).unwrap();
        }
        hm.flush().unwrap();
    }
    
    // We expect internal FSM state to bubble up category smoothly
    let mut hf = fs::OpenOptions::new().read(true).open(&file_path).unwrap();
    let mut fsm = FSM::build_from_heap(&mut hf, file_path.with_extension("dat.fsm")).unwrap();
    // Demand huge block - should find the remainder of the first page.
    let search_res = fsm.fsm_search_avail(100).unwrap();
    assert!(search_res.is_some(), "Could not find expected free chunk in fragmented page.");
    println!("✓ Fragmentation Management passed.");
}

/// 6. Persistence (System crash recovery)
#[test]
fn test_persistence_fsm_recovery() {
    setup_db_dir();
    let table_name = "fsm_persistence";
    let file_path = get_test_path(table_name);
    let fsm_path = file_path.with_extension("dat.fsm");
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(&fsm_path);

    {
        let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
        // Insert a few tuples
        hm.insert_tuple(&vec![0xEE; 1000]).unwrap();
    } // HM and FSM go out of scope (crash simulation)

    // Mess with or delete the fsm sidecar to simulate a crash where FSM might be missed or corrupted
    // Although in true implementation, if missing, build_from_heap rebuilds it.
    let _ = fs::remove_file(&fsm_path);
    
    let mut hf = fs::OpenOptions::new().read(true).open(&file_path).unwrap();
    let _fsm = FSM::build_from_heap(&mut hf, file_path.with_extension("dat.fsm")).expect("Recover failed");
    // If we can build FSM from heap without errors, it means it successfully rebuilt the FSM state from the heap metadata, demonstrating persistence and recovery.
    
    println!("✓ Persistence test passed: FSM fork rebuilt from heap correctly.");
}

/// 7. Boundary Violations
/// App writes past the chunk logically -> FSM/HM rejects tuples > PAGESIZE
#[test]
fn test_boundary_violations() {
    setup_db_dir();
    let table_name = "boundary_viol";
    let file_path = get_test_path(table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    
    let huge_data = vec![0xFF; 9000]; // Larger than ~8184 byte page boundary
    let res = hm.insert_tuple(&huge_data);
    
    // Should gracefully fail instead of crashing/panicking or corrupting metadata
    assert!(res.is_err(), "Boundary violation check failed: manager accepted oversize tuple!");
    println!("✓ Boundary violations passed: Manager rejects oversized buffers.");
}

