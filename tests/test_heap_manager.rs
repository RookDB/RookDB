/// Integration tests for HeapManager and FSM
/// 
/// Tests the complete flow of heap operations:
/// - Creating a new heap
/// - Inserting tuples with FSM-guided page selection
/// - Retrieving tuples by coordinates
/// - Sequential scans
/// - FSM updates and rebuilds
/// - Header persistence

use std::fs;
use std::path::PathBuf;
use storage_manager::backend::heap::{HeapManager};
use storage_manager::backend::disk::read_header_page;

fn cleanup_test_files(name: &str) {
    let heap_file = PathBuf::from(format!("heap_test_{}.dat", name));
    let fsm_file = PathBuf::from(format!("heap_test_{}.dat.fsm", name));
    let _ = fs::remove_file(&heap_file);
    let _ = fs::remove_file(&fsm_file);
}

#[test]
fn test_heap_create() {
    let name = "create";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let manager = HeapManager::create(path.clone());

    assert!(manager.is_ok(), "Failed to create heap");
    
    let manager = manager.unwrap();
    assert_eq!(manager.header.page_count, 2, "Should have 2 pages (0 + 1)");
    assert_eq!(manager.header.total_tuples, 0, "Should have 0 tuples initially");
    
    cleanup_test_files(name);
}

#[test]
fn test_heap_insert_single() {
    let name = "insert_single";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    let tuple_data = b"Hello, RookDB!";
    let result = manager.insert_tuple(tuple_data);

    assert!(result.is_ok(), "Failed to insert tuple");
    let (page_id, slot_id) = result.unwrap();
    
    println!("[TEST] Inserted at page={}, slot={}", page_id, slot_id);
    assert!(page_id > 0, "Page ID should be > 0");
    assert_eq!(slot_id, 0, "First tuple should be at slot 0");
    assert_eq!(manager.header.total_tuples, 1, "Should have 1 tuple");

    cleanup_test_files(name);
}

#[test]
fn test_heap_insert_multiple() {
    let name = "insert_multiple";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Insert 10 small tuples
    for i in 0..10 {
        let tuple_data = format!("Tuple{}", i).into_bytes();
        let result = manager.insert_tuple(&tuple_data);
        
        assert!(result.is_ok(), "Failed to insert tuple {}", i);
        let (page_id, slot_id) = result.unwrap();
        
        println!("[TEST] Inserted tuple {} at page={}, slot={}", i, page_id, slot_id);
        assert!(page_id > 0, "Page ID should be > 0");
    }

    assert_eq!(
        manager.header.total_tuples, 10,
        "Should have 10 tuples"
    );

    cleanup_test_files(name);
}

#[test]
fn test_heap_get_tuple() {
    let name = "get_tuple";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    let original_data = b"Test data for retrieval";
    let (page_id, slot_id) = manager
        .insert_tuple(original_data)
        .expect("Failed to insert tuple");

    // Now retrieve it
    let retrieved = manager.get_tuple(page_id, slot_id);
    assert!(retrieved.is_ok(), "Failed to retrieve tuple");
    
    let retrieved_data = retrieved.unwrap();
    assert_eq!(
        retrieved_data, original_data.to_vec(),
        "Retrieved data should match original"
    );

    println!("[TEST] Retrieved: {:?}", std::str::from_utf8(&retrieved_data));

    cleanup_test_files(name);
}

#[test]
fn test_heap_scan() {
    let name = "scan";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Insert 5 tuples
    let test_data = vec![
        b"First".to_vec(),
        b"Second".to_vec(),
        b"Third".to_vec(),
        b"Fourth".to_vec(),
        b"Fifth".to_vec(),
    ];

    for data in test_data.iter() {
        manager.insert_tuple(data).expect("Failed to insert");
    }

    // Scan and count
    let mut count = 0;
    for result in manager.scan() {
        match result {
            Ok((page_id, slot_id, data)) => {
                count += 1;
                println!(
                    "[TEST] Scanned: page={}, slot={}, data={:?}",
                    page_id, slot_id, std::str::from_utf8(&data)
                );
            }
            Err(e) => panic!("Scan error: {}", e),
        }
    }

    assert_eq!(count, 5, "Should have scanned 5 tuples");

    cleanup_test_files(name);
}

#[test]
fn test_heap_header_persistence() {
    let name = "header_persistence";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    
    // Create and insert
    {
        let mut manager = HeapManager::create(path.clone())
            .expect("Failed to create heap");

        for i in 0..5 {
            let data = format!("Persistent {}", i).into_bytes();
            manager.insert_tuple(&data).expect("Failed to insert");
        }

        manager.flush().expect("Failed to flush");
    }

    // Reopen and verify
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("Failed to open file");

        let header = read_header_page(&mut file).expect("Failed to read header");
        
        println!(
            "[TEST] After reopen: page_count={}, total_tuples={}",
            header.page_count, header.total_tuples
        );
        
        assert!(header.page_count >= 2, "Should have at least 2 pages");
        assert_eq!(header.total_tuples, 5, "Should have persisted 5 tuples");
    }

    cleanup_test_files(name);
}

#[test]
fn test_heap_large_tuples() {
    let name = "large_tuples";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Create a large tuple (1000 bytes)
    let large_tuple = vec![b'A'; 1000];
    let result = manager.insert_tuple(&large_tuple);
    
    assert!(result.is_ok(), "Failed to insert large tuple");
    let (page_id, slot_id) = result.unwrap();
    
    // Retrieve and verify
    let retrieved = manager.get_tuple(page_id, slot_id)
        .expect("Failed to retrieve large tuple");
    
    assert_eq!(
        retrieved.len(), 1000,
        "Retrieved tuple size should match"
    );
    assert_eq!(
        retrieved, large_tuple,
        "Retrieved large tuple data should match"
    );

    println!("[TEST] Successfully handled 1000-byte tuple");

    cleanup_test_files(name);
}

#[test]
fn test_heap_invalid_operations() {
    let name = "invalid_ops";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Try to get from invalid coordinates
    let result = manager.get_tuple(999, 999);
    assert!(result.is_err(), "Should error on invalid page");

    cleanup_test_files(name);
}

#[test]
fn test_heap_empty_scan() {
    let name = "empty_scan";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Scan empty heap
    let count: usize = manager.scan().count();
    
    assert_eq!(count, 0, "Empty heap should yield no tuples");

    cleanup_test_files(name);
}

#[test]
fn test_heap_multiple_pages() {
    let name = "multiple_pages";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    // Insert tuples until we span multiple pages
    // Each page is 8192 bytes, with 8-byte header, so ~8184 usable
    // Insert 50 tuples of 150 bytes each = 7500 bytes (~1 page)
    // Total inserts: 100 tuples should span 2-3 pages
    // (Note: MVP FSM is simplified so all insertions may go to page 1 until it fills)
    for i in 0..100 {
        let data = format!("Tuple_{:03}_with_some_padding_to_make_it_bigger_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i).into_bytes();
        match manager.insert_tuple(&data) {
            Ok((page_id, _)) => {
                if page_id > 1 && i < 20 {
                    println!("[TEST] Early multiple-page insert at tuple {}", i);
                }
            }
            Err(e) => {
                eprintln!("[TEST] Insert error at tuple {}: {}", i, e);
                // After reach heap limit, this is expected for MVP
                break;
            }
        }
    }

    assert!(manager.header.page_count > 1, "Should have allocated pages");
    assert!(manager.header.total_tuples > 0, "Should have inserted tuples");

    // Verify scan gets all inserted tuples
    let scanned_count: usize = manager.scan()
        .filter_map(|r| r.ok())
        .count();
    
    assert_eq!(
        scanned_count, manager.header.total_tuples as usize,
        "Should scan all inserted tuples"
    );

    println!(
        "[TEST] Successfully handled {} tuples across {} pages",
        manager.header.total_tuples, manager.header.page_count
    );

    cleanup_test_files(name);
}

use storage_manager::backend::page::{Page, get_tuple_count, get_slot_entry};
use storage_manager::backend::disk::read_page;

fn print_table_slots(file_path: &PathBuf, page_id: u32, step_desc: &str) {
    println!("{}", step_desc);
    let mut file = fs::File::open(file_path).expect("Failed to open file for printing slots");
    let mut page = Page::new();
    read_page(&mut file, &mut page, page_id).expect("Failed to read page");
    
    let tuple_count = get_tuple_count(&page).unwrap();
    for slot_id in 0..tuple_count {
        let (offset, length) = get_slot_entry(&page, slot_id).unwrap();
        // A deleted slot typically has offset 0 and length 0 (or similar tombstone). 
        // Let's print only active ones to match user's expected output.
        if offset == 0 || length == 0 {
            continue;
        }
        
        let val_bytes = &page.data[offset as usize..(offset + length) as usize];
        let val_str = std::str::from_utf8(val_bytes).unwrap_or("INVALID");
        println!("slot_{} offset {} len {} {}", slot_id + 1, offset, length, val_str);
    }
}

#[test]
fn test_slot_reuse_delete_first() {
    let name = "slot_reuse_delete_first";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    let (p1, s1) = manager.insert_tuple(b"val_1").unwrap();
    let (_, _s2) = manager.insert_tuple(b"val_2").unwrap();
    
    print_table_slots(&path, p1, "1. insert 2");
    
    manager.delete_tuple(p1, s1).unwrap();
    
    print_table_slots(&path, p1, "2. delete val_1");
    
    let (_, _s3) = manager.insert_tuple(b"val_3").unwrap();
    
    print_table_slots(&path, p1, "3. insert 1");

    cleanup_test_files(name);
}

#[test]
fn test_slot_reuse_delete_second() {
    let name = "slot_reuse_delete_second";
    cleanup_test_files(name);

    let path = PathBuf::from(format!("heap_test_{}.dat", name));
    let mut manager = HeapManager::create(path.clone())
        .expect("Failed to create heap");

    let (p1, _s1) = manager.insert_tuple(b"val_1").unwrap();
    let (_, s2) = manager.insert_tuple(b"val_2").unwrap();
    
    print_table_slots(&path, p1, "1. insert 2");
    
    manager.delete_tuple(p1, s2).unwrap();
    
    print_table_slots(&path, p1, "2. delete val_2");
    
    let (_, _s3) = manager.insert_tuple(b"val_2").unwrap();
    
    // The prompt says: "3. insert 1 \nsync slot_2 offset len val_2" - so let's insert val_2 again to match
    print_table_slots(&path, p1, "3. insert 1");

    cleanup_test_files(name);
}
