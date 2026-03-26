# Fixing Prblems : 

Implemented read_all_pages API: Added a new function in disk_manager.rs that reads all pages (header + data) from a file on disk into memory.
Updated load_table_from_disk: Modified BufferManager::load_table_from_disk in buffer_manager.rs to use this new API, simplifying the logic.
Removed Unused Code: Confirmed that load_csv_into_pages and load_csv_to_buffer in the buffer manager were redundant legacy code (unused by the active frontend) and removed them. The active bulk loading logic resides in load_csv.rs, which is correctly used by the frontend commands.

1. there is a problem with the catalog file , like if it gets corrupted or deleted , then a new catalog file will be created with no databases or tables,but instead of creating a new catalog file it should check existing database and tables and load them into the catalog struct in memory, so that we can continue using the existing databases and tables without losing any data.


test to conduct :

1. large csv load test with 500 rows to see if FSM is allocating pages correctly and if the heap file is being updated with the new FSM information. We can check the FSM fork file after loading to confirm that it has been created/updated.
2. Insert tuple test to see if after inserting a tuple, the FSM fork file is updated correctly. We can check the FSM fork file before and after the insert to confirm that it has been updated.