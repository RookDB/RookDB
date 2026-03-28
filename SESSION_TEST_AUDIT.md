# RookDB Session Test Audit (Design-Doc Scope Only)

Date: 2026-03-28

This audit is restricted to responsibilities described in `design-doc.md` only (FSM + Heap File Manager + related page/header behavior).

---

## 1) Commands to run all tests in the system

From repo root:

```bash
cargo test --all-targets --all-features
cargo test --doc
```

Optional (extra static checks):

```bash
cargo clippy --all-targets --all-features
```

---

## 2) What was executed in this session

Executed and passing:

- `cargo test --all-targets --all-features`
- `cargo test --doc`

Observed result:

- All unit/integration tests passed (including FSM heavy tests and heap manager tests).
- Doc tests: 0 present, 0 failed.

---

## 3) New tests added in this session

Added to `src/backend/page/mod.rs`:

1. `test_page_free_space_detects_corrupted_pointers`
   - Verifies corrupted `lower/upper` pointers are rejected (error), not blindly used.

2. `test_get_tuple_count_detects_invalid_alignment`
   - Verifies invalid slot-directory alignment is rejected.

3. `test_get_slot_entry_detects_out_of_bounds_tuple`
   - Verifies slot entries with invalid `(offset, length)` bounds are rejected.

Why these were added:

- To harden page-level safety and prevent malformed-page metadata from causing invalid behavior during FSM/heap operations.

---

## 4) Design-doc responsibility completion check

### A. Core backend responsibilities in `design-doc.md`

- FSM components (`build_from_heap`, `fsm_search_avail`, `fsm_set_avail`, `fsm_vacuum_update`): **Implemented**
- HeapManager APIs (`open`, `insert_tuple`, `get_tuple`, `scan`, `allocate_new_page`, `flush`): **Implemented**
- Header metadata fields (`page_count`, `fsm_page_count`, `total_tuples`, `last_vacuum`): **Implemented**
- Slotted-page helpers (`get_tuple_count`, `get_slot_entry`): **Implemented**
- Header persistence helper (`update_header_page`): **Implemented**

### B. Design-doc intent vs current implementation status

- **Tree-based page selection on insert:** Completed (via `fsm_search_avail` in `HeapManager::insert_tuple`).
- **Tuple retrieval by coordinates:** Completed.
- **Sequential scan across pages:** Completed.
- **FSM sidecar fork and page-level free-space tracking:** Completed.
- **CHECK_HEAP command:** Present, but output is **partial** compared to the richer sample in design doc.

### C. Gaps (within design-doc scope)

1. `HeapManager::open` design text says it should open/rebuild FSM (`build_from_heap` path). Current flow opens FSM directly and synchronizes counts; rebuild behavior is not forced in `open`.
2. Several design-doc test cases are not implemented as explicit tests (see section 5).
3. Performance tests in design-doc test list (`bench_insert_throughput`, `bench_scan_throughput`) are not present as assertive `cargo test` tests.

---

## 5) Coverage gaps against the 24-test plan in `design-doc.md`

The codebase has strong practical coverage for FSM/heap behavior, but the **exact planned test suite** in design-doc is not fully realized as named/explicit tests.

Not fully covered as explicit tests:

- `test_fsm_tree_parent_equals_max_children`
- `test_fsm_search_root_early_exit`
- `test_fsm_search_fp_next_slot_spreads`
- `test_insert_uses_fsm_tree_search` (explicit assertion of call-path behavior)
- `test_fsm_set_avail_bubbles_up` (direct structural invariant checks)
- `test_fsm_set_avail_root_equals_global_max` (direct global-max invariant test)
- `test_fsm_set_avail_decreases_category`
- `test_fsm_full_page_category_zero`
- `test_header_fsm_page_count_updated` (explicit growth invariant test naming)
- `bench_insert_throughput` and `bench_scan_throughput` as assertion-based tests

What is already well covered in current tests:

- Large insertions / boundary checks / allocation behavior
- Persistence and recovery behavior (including FSM recovery path)
- Heap create/insert/get/scan flows
- Invalid operation handling

---

## 6) Files changed in this session (test-quality work)

- `src/backend/page/mod.rs`
  - Added metadata validation and 3 new tests listed above.
- `src/backend/executor/seq_scan.rs`
  - Added corruption checks for page header and slot bounds before tuple slicing.
- `src/backend/heap/heap_manager.rs`
  - Updated test helper setup to use realistic heap creation path.
- `tests/test_create_page.rs`
  - Added `.truncate(true)` for deterministic test-file behavior.

---

## 7) Final status (design-doc scope only)

- **Implemented responsibilities:** Mostly completed.
- **Testing completeness vs design-doc plan:** **Partially completed** (functional coverage is good, but the exact planned 24-test matrix is not fully implemented as explicit tests).

If needed, next step is to add the missing explicit tests named in section 5 so your implementation can claim full design-doc test-plan completion.
