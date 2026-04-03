# RookDB Comprehensive Test Documentation

This document explains all the tests implemented in RookDB. 

## 1. How to Run the Tests

To run all integrated and unit tests in the system, use the standard Rust testing command:
```bash
cargo test --all-targets --all-features
```

---

## 2. Integration Tests (`tests/` directory)

Integration tests check if different parts of the database (like disk storage, FSM, and memory management) communicate and work together correctly. 

### A. High-Stress and Space Allocation Tests (`test_fsm_heavy.rs`)
These tests put the database's storage system under heavy load and tricky edge cases to make sure it handles extreme scenarios gracefully.

1. **Large Speed and Volume Test (`test_large_insertions`)**
   - **Goal**: Check if the database can handle a massive amount of data quickly.
   - **What it does**: Rapidly inserts tens of thousands of rows of data into the database back-to-back.
   - **What it checks**: It measures the time taken to guarantee the system doesn't slow down over time. It proves the FSM can find space for thousands of rows instantly without freezing.

2. **Reusing Deleted Space (`test_update_delete_fsm_deallocation`)**
   - **Goal**: Ensure deleted disk space is properly recycled and not wasted.
   - **What it does**: Inserts a huge chunk of data (taking up half a physical page), then simulates deleting it (compaction) to free up the space.
   - **What it checks**: It verifies that the Free Space Manager (FSM) successfully notices the newly freed space and allows new data to be written there instead of permanently losing that disk capacity.

3. **Accurate Page Splitting (`test_allocation_accuracy`)**
   - **Goal**: Prevent data overlapping or over-stuffing a single page.
   - **What it does**: Attempts to insert two giant records (8000 bytes each) that are almost the size of a full page.
   - **What it checks**: It ensures the system is smart enough to realize both records cannot fit on the same page, forcing the database to create a second, separate page for the second record.

4. **Managing Cramped Spaces (`test_fragmentation_management`)**
   - **Goal**: See how the database handles many tiny scattered blocks of data.
   - **What it does**: Inserts small, fragmented pieces of data sequentially.
   - **What it checks**: It manually inspects the FSM tree to ensure it correctly calculates the remaining small pockets of space, preventing the database from accidentally trying to shove large data into a tiny gap.

5. **Crash Recovery (`test_persistence_fsm_recovery`)**
   - **Goal**: Prove the database can reconstruct itself if its tracking files are deleted or corrupted.
   - **What it does**: Writes records to the database, and then intentionally deletes the `.fsm` (Free Space Map) file from the hard drive, simulating a severe crash or file loss.
   - **What it checks**: It verifies that the database can automatically read the raw data file (`.dat`) and perfectly rebuild the missing `.fsm` file from scratch without losing any data mapping.

6. **Safety Against Over-sized Data (`test_boundary_violations`)**
   - **Goal**: Ensure the database rejects data that is simply too large to store.
   - **What it does**: Tries to insert a single record that exceeds the absolute maximum hardware page size (e.g., 9000 bytes).
   - **What it checks**: It ensures the database outright rejects the insert with a clear Error, rather than trying to write it and corrupting the surrounding memory or crashing the program.

---

### B. Core Storage Tests (`test_heap_manager.rs` & `test_fsm_page_allocation.rs`)
These tests focus on the standard day-to-day operations of the database storage engine.

1. **Basic File Operations (`test_heap_create`)**
   - **Goal**: Ensure we can create new database files and open existing ones safely without accidentally overwriting old data.

2. **Inserting and Scanning Data (`test_heap_insert_single` / `test_heap_scan`)**
   - **Goal**: Ensure data written can actually be read back in the correct order.
   - **What it does**: Inserts a sequence of rows and then uses the database's sequential "Scanner" to iterate through them.
   - **What it checks**: Ensures every single row is retrieved exactly as it was written, proving the storage logic is perfectly aligned.

3. **Targeted Data Retrieval (`test_heap_get_tuple`)**
   - **Goal**: Ensure we can randomly fetch a specific row.
   - **What it checks**: Proves that by providing an exact "Coordinate" (Page Number + Slot Number), the database returns exactly that specific row in constant O(1) time.

4. **Multi-Page Spanning (`test_heap_multiple_pages`)**
   - **Goal**: Test the transition when the database outgrows a single physical page (usually 8192 bytes).
   - **What it checks**: Confirms that when page 1 fills up, the database correctly establishes a link to page 2, and the scanner can seamlessly read rows stretching across both pages.

5. **Optimal Packing (`test_fsm_page_allocation`)**
   - **Goal**: Ensure the database packs data tightly to save disk space.
   - **What it checks**: Inserts thousands of rows from a CSV file. It checks that the database densely packs the rows into 2-3 full pages, rather than creating hundreds of mostly-empty pages.

---

### C. Low-Level Disk Tests (`test_create_page.rs`, `test_read_page.rs`, etc.)
These validate the absolute lowest level of the hardware map.
- **Verification**: They ensure that when a page of exactly 8192 bytes is requested, exactly 8192 bytes are written to the disk. They guarantee headers don't accidentally leak into the user's data area.

---

## 3. Unit Tests (`src/` directory)

Unit tests live directly alongside the database source code and test specific, isolated functions (like math calculations and data type casting).

### A. Memory Protection (`src/backend/page/mod.rs`)
1. **Detecting Corrupted Data (`test_page_free_space_detects_corrupted_pointers`)**
   - **Checks**: If disk corruption makes a memory pointer point to a negative number or a location outside the page, the code aggressively throws a safe Error rather than causing a fatal system crash or "kernel panic".
2. **Preventing Array Out-of-Bounds (`test_get_slot_entry_detects_out_of_bounds_tuple`)**
   - **Checks**: If the user asks for "Row 99" but only 10 rows exist, the system safely rejects the request instead of reading random garbage memory.

### B. Data Type Validation (`src/backend/types_validator.rs`)
1. **Case-Insensitive Types**
   - **Checks**: Ensures `INT`, `int`, and `InT` are all recognized correctly as the integer data type by the database parser.
2. **Unsupported Types**
   - **Checks**: Ensures types the database doesn't support yet (like `FLOAT` or `VARCHAR`) are caught early and gracefully rejected with a helpful message to the user.
