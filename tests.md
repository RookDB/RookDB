1. Large Insertions & Timing (test_large_insertions)
Concept Testing: Point 1 (Large insertions) and Point 9 (Time analysis).
What it does: Inserts 50,000 records (50-byte tuples) into the system consecutively as fast as possible.
Verification: Benchmarks the total elapsed CPU time checking if the insertions take an acceptable length (< 30 seconds) and ensuring the tree does not choke recursively as leaves fill up.
2. Updation and Deallocation Integrity (test_update_delete_fsm_deallocation)
Concept Testing: Points 2 (Updates/deletions mapped minimal behavior) and 4 (Deallocation).
What it does: Inserts a heavily sized tuple (4000 bytes). Since an ~8184 byte page is nearly half-full, requesting more space returns a different page or blocks the first. We trigger an FSM update manually utilizing fsm_vacuum_update(...) (to reclaim ~8000 bytes) simulating an UPDATE or DELETE compaction event.
Verification: Asserts that after reclaiming the space, the fsm_search_avail can successfully locate our page once again.
3. Allocation Accuracy & Collision (test_allocation_accuracy)
Concept Testing: Point 3 (Allocation limits, no double tracking).
What it does: Tries to insert two distinct 8000-byte tuples, each large enough to occupy over 95% of a page boundary.
Verification: As the FSM correctly tracks space boundaries, it makes sure HeapManager puts these tuples onto different pages (page_id1 != page_id2). If a collision happens, it panics.
4. Fragmentation Management / Leaf Node Bubble Up (test_fragmentation_management)
Concept Testing: Point 5 (Fragmentation / Coalescing).
What it does: Inserts tiny fragments of data sequentially. It then opens the database file directly via FSM to search the internal max-tree explicitly using build_from_heap.
Verification: The test verifies that many small usages trigger appropriate max-tree categorizations and bubbling up limits. (Note: Currently, this execution correctly exposes a bug in your build_from_heap() logic—while it initializes FSM, it forgets to correctly serialize some of the mid-level tree internal slots! The test rightfully fails here and identifies exactly what you need to fix locally next).
5. Persistence Recovery (test_persistence_fsm_recovery)
Concept Testing: Point 6 (File System Crash/Recovery).
What it does: Intentionally writes records, drops bounds (simulating ungraceful stop without saving state buffers), then surgically deletes the .fsm side-car persistence file.
Verification: Validates whether FSM::build_from_heap successfully resuscitates and regains an identical map topology from parsing only the primary heap data file (.dat) header variables.
6. Boundary Violations (test_boundary_violations)
Concept Testing: Points 7 and 8 (Boundaries and Overflows).
What it does: Explicitly tricks the HeapManager with a 9000-byte payload that heavily exceeds a safe block maximum boundary (8192 - headers bytes).
Verification: Asserts that the FSM/Heap mechanism rightfully catches Err(_) and throws an I/O rejection instead of bleeding memory, overwriting magic headers on the next page bounds, or triggering hard kernel Out-of-Memory behavior!
7. large test cases check





---

## Test Coverage

### Implemented Tests:
1. **Data Type Validation (Case-Insensitive)** ✓
   - INT, int, InT all work
   - FLOAT and VARCHAR rejected

2. **Graceful Error Handling** ✓
   - Invalid paths detected
   - Non-existent files reported
   - Directories rejected
   - Helpful guidance provided

3. **CSV Data Validation** ✓
   - Pre-load schema validation
   - Row-by-row validation
   - Column count checking
   - Invalid value detection with line numbers

4. **TEXT Type Truncation** ✓
   - Padding for short strings
   - Truncation with warnings for long strings
   - Proper deserialization

5. **Data Type Checking** ✓
   - INT validation (32-bit range)
   - TEXT validation (string checks)
   - Extensible design

6. **Catalog Persistence** ✓
   - Result<> return type
   - Graceful error handling
   - No crashes on disk errors

7. **Page API Abstraction** ✓
   - Safe pointer access
   - Boundary checking
   - Statistics calculation

8. **Single Tuple Insertion** ✓
   - Manual data entry
   - Schema validation
   - Value validation

9. **Table Display** ✓
   - Professional formatting
   - Single header row
   - Data type information

10. **Menu System** ✓
    - New options available
    - Proper organization
    - Clear categories

---