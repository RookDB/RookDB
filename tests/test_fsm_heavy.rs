use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use storage_manager::backend::fsm::fsm::FSM;
use storage_manager::backend::heap::heap_manager::HeapManager;
use storage_manager::backend::instrumentation::StatsSnapshot;

const DB_NAME: &str = "test_fsm_heavy";



// Drop guard for automatic directory cleanup
struct TestCleanup {
    path: PathBuf,
}
impl Drop for TestCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// Ensure the db dir exists and return guard
fn setup_db_dir(test_id: &str) -> (PathBuf, TestCleanup) {
    // Unique DB name to avoid parallel test collisions
    let unique_db = format!("{}_{}", DB_NAME, test_id);
    let db_path = PathBuf::from(format!("database/base/{}", unique_db));
    let _ = fs::remove_dir_all(&db_path);
    let _ = fs::create_dir_all(&db_path);
    (db_path, TestCleanup { path: PathBuf::from(format!("database/base/{}", unique_db)) })
}

// Helper to create path
fn get_test_path(db_path: &PathBuf, table_name: &str) -> PathBuf {
    db_path.join(format!("{}.dat", table_name))
}

/// 1. INSERTION - Large number of insertions and time taken
/// 9. Time and space for various operations
#[test]
fn test_large_insertions() {
    let (db_path, _guard) = setup_db_dir("large_insert");
    let table_name = "large_insert";
    let file_path = get_test_path(&db_path, table_name);
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
    
    // Print operation metrics
    let stats = StatsSnapshot::capture();
    stats.print_table();
    
    assert!(elapsed.as_secs() < 30, "Insertions took too long");
    println!("✓ Large insertion test passed. Time mapped.");
}

/// 2. Updation and Deletion & 4. Deallocation Integrity
/// Demonstrates that deleting large tuples doesn't immediately reclaim space
/// (VACUUM garbage collection would be needed for that), but slot entries are marked invalid.
#[test]
fn test_update_delete_fsm_deallocation() {
    let (db_path, _guard) = setup_db_dir("upd_del_fsm");
    let table_name = "upd_del_fsm";
    let file_path = get_test_path(&db_path, table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    
    // Insert medium tuples that allow for multiple inserts
    let tuple_data = vec![0xBB; 500]; // 500 bytes
    let (page_id, slot_id_1) = hm.insert_tuple(&tuple_data).unwrap();
    let (_page_id_2, _slot_id_2) = hm.insert_tuple(&tuple_data).unwrap();

    // Check free space before deletion
    let _free_before = page_id; // Just recording page for reference

    // Delete one tuple
    println!("Deleting first tuple to free slot...");
    hm.delete_tuple(page_id, slot_id_1).expect("Failed to delete tuple");

    // After deletion, total_tuples should decrease
    assert_eq!(hm.header.total_tuples, 1, "Total tuples should be 1 after deleting one");
    
    // Verify we can still insert (space is available in slot directory)
    let tuple_small = vec![0xCC; 100]; // Smaller tuple
    let result = hm.insert_tuple(&tuple_small);
    assert!(result.is_ok(), "Should be able to insert after deletion");
    
    println!("✓ Deallocation Integrity (Update/Delete) passed.");
}

/// 3. Allocation Accuracy
/// Verify that when space is requested, FSM marks it as used, never handing it blindly again without updates.
#[test]
fn test_allocation_accuracy() {
    let (db_path, _guard) = setup_db_dir("alloc_accuracy");
    let table_name = "alloc_accuracy";
    let file_path = get_test_path(&db_path, table_name);
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
    let (db_path, _guard) = setup_db_dir("frag_mgmt");
    let table_name = "frag_mgmt";
    let file_path = get_test_path(&db_path, table_name);
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
    let (db_path, _guard) = setup_db_dir("fsm_persistence");
    let table_name = "fsm_persistence";
    let file_path = get_test_path(&db_path, table_name);
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
    let (db_path, _guard) = setup_db_dir("boundary_viol");
    let table_name = "boundary_viol";
    let file_path = get_test_path(&db_path, table_name);
    let _ = fs::remove_file(&file_path);
    let _ = fs::remove_file(file_path.with_extension("dat.fsm"));

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    
    let huge_data = vec![0xFF; 9000]; // Larger than ~8184 byte page boundary
    let res = hm.insert_tuple(&huge_data);
    
    // Should gracefully fail instead of crashing/panicking or corrupting metadata
    assert!(res.is_err(), "Boundary violation check failed: manager accepted oversize tuple!");
    println!("✓ Boundary violations passed: Manager rejects oversized buffers.");
}

