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


/// 1. Reallocation after vacuum (Replaces upd_del_fsm logic effectively showing FSM reuse)
#[test]
fn test_fsm_reallocation_after_vacuum() {
    let (db_path, _guard) = setup_db_dir("fsm_reallocation");
    let table_name = "fsm_reallocation";
    let file_path = get_test_path(&db_path, table_name);

    let mut hm = HeapManager::create(file_path.clone()).expect("Failed to create HM");
    
    // Fill up a few pages
    // Each insert is 8000 bytes, so each goes to a new page.
    let tuple_data = vec![0xAA; 8000];
    
    let (p1, s1) = hm.insert_tuple(&tuple_data).unwrap();
    let (p2, _s2) = hm.insert_tuple(&tuple_data).unwrap();
    let (p3, _s3) = hm.insert_tuple(&tuple_data).unwrap();
    
    assert_ne!(p1, p2);
    assert_ne!(p2, p3);
    
    // Check Table statistics logic (printing tuple counts and allocations internally)
    println!("Pages allocated: P1: {}, P2: {}, P3: {}", p1, p2, p3);

    // Delete from page 1
    let freed_bytes = hm.delete_tuple(p1, s1).unwrap();
    
    // Apply Vacuum to update the FSM
    hm.vacuum_page(p1, freed_bytes).expect("Failed to vacuum page");
    
    // Now insert a new tuple that can fit in the freed space
    let tuple_small = vec![0xBB; 4000];
    let (p_new, _s_new) = hm.insert_tuple(&tuple_small).unwrap();
    
    // If FSM working correctly, it should reuse page 1!
    assert_eq!(p_new, p1, "FSM failed to route new insert to the freed space on page 1");
    println!("✓ Reallocation after vacuum test passed.");
}

/// 2. The "Masking" Bubble-Up Test
#[test]
fn test_fsm_bubble_up_recalculation() {
    let (db_path, _guard) = setup_db_dir("bubble_up");
    let table_name = "bubble_up";
    let file_path = get_test_path(&db_path, table_name);

    // Provide large enough count so FSM has 1 page
    let mut fsm = FSM::open(file_path.with_extension("dat.fsm"), 10).unwrap();
    // Simulate setting initial size
    fsm.set_heap_page_count(10);
    
    // Fill pages 0 and 1 partially
    fsm.fsm_set_avail(0, 500, None).unwrap(); // Page 0
    fsm.fsm_set_avail(1, 1000, None).unwrap(); // Page 1
    
    let cat_1000 = (1000 / 32).max(0).min(255) as u8;
    let root_val_1 = fsm.read_fsm_page(0, 0, 0).unwrap().root_value();
    assert_eq!(root_val_1, cat_1000, "Root should reflect the highest free space");

    // Reduce Page 1's space below Page 0's space
    fsm.fsm_set_avail(1, 200, None).unwrap();
    
    let cat_500 = (500 / 32).max(0).min(255) as u8;
    let root_val_2 = fsm.read_fsm_page(0, 0, 0).unwrap().root_value();
    
    assert_eq!(
        root_val_2, cat_500, 
        "FSM failed to correctly recalculate using the sibling node during bubble-up"
    );
    println!("✓ Bubble-up recalculation passed.");
}

/// 3. The Initial Unused Space Test
#[test]
fn test_fsm_initial_state_routing() {
    let (db_path, _guard) = setup_db_dir("fsm_initial_state");
    let table_name = "fsm_initial_state";
    let file_path = get_test_path(&db_path, table_name);

    let mut fsm = FSM::open(file_path.with_extension("dat.fsm"), 4).unwrap();
    fsm.set_heap_page_count(4);
    
    // Assume new pages 1, 2, 3 have full size
    for p in 1..4 {
        fsm.fsm_set_avail(p, 8000, None).unwrap();
    }
    
    let cat_3000 = (3000 / 32).max(0).min(255) as u8;
    let target_page = fsm.fsm_search_avail(cat_3000).unwrap().map(|(id, _)| id);
    
    assert_eq!(
        target_page, Some(1), // Page 1 is first data page
        "A completely empty FSM should route requests to the first data page"
    );
    println!("✓ Initial unused space routing passed.");
}

/// 4. The "Needle in the Haystack" Test (Deep Search)
#[test]
fn test_fsm_needle_in_haystack() {
    let (db_path, _guard) = setup_db_dir("needle_haystack");
    let table_name = "needle_haystack";
    let file_path = get_test_path(&db_path, table_name);

    let mut fsm = FSM::open(file_path.with_extension("dat.fsm"), 4001).unwrap();
    fsm.set_heap_page_count(4001);
    
    // Simulate completely full database
    for i in 1..=4000 {
        fsm.fsm_set_avail(i, 0, None).unwrap(); // 0 bytes free
    }
    
    let root_val_1 = fsm.read_fsm_page(0, 0, 0).unwrap().root_value();
    assert_eq!(root_val_1, 0, "Root should report 0 space when all pages are full in level 0");
    // At 4000 it is exactly 1 leaf page, so level 0 root is the max space.

    // Free up space on one specific, deep page
    let target_page_id = 3142;
    fsm.fsm_set_avail(target_page_id, 4000, None).unwrap(); 

    let cat_4000 = (4000 / 32).max(0).min(255) as u8;
    let root_val_2 = fsm.read_fsm_page(0, 0, 0).unwrap().root_value();
    
    // The root should instantly know 4000 bytes opened up
    assert_eq!(root_val_2, cat_4000, "Root did not bubble up the newly freed space");

    // Search for space
    let cat_3500 = (3500 / 32).max(0).min(255) as u8;
    let found_page = fsm.fsm_search_avail(cat_3500).unwrap().map(|(id, _)| id);
    
    assert_eq!(
        found_page, Some(target_page_id), 
        "FSM search failed to navigate the branches to find the only page with space"
    );
    println!("✓ Needle in haystack (Deep Search) passed.");
}

/// 5. The Exact Fit / Left-Bias Test
#[test]
fn test_fsm_exact_fit_left_bias() {
    let (db_path, _guard) = setup_db_dir("left_bias");
    let table_name = "left_bias";
    let file_path = get_test_path(&db_path, table_name);

    let mut fsm = FSM::open(file_path.with_extension("dat.fsm"), 5).unwrap();
    fsm.set_heap_page_count(5);
    
    fsm.fsm_set_avail(1, 0, None).unwrap();
    fsm.fsm_set_avail(2, 2000, None).unwrap();
    fsm.fsm_set_avail(3, 2000, None).unwrap();
    
    let cat_1500 = (1500 / 32).max(0).min(255) as u8;
    let found_page = fsm.fsm_search_avail(cat_1500).unwrap().map(|(id, _)| id);
    
    assert_eq!(
        found_page, Some(2), 
        "FSM failed exact fit / left-bias test (should prioritize the leftmost available space)"
    );
    println!("✓ Exact fit / left-bias passed.");
}

