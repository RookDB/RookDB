# RookDB Design-vs-Code Audit Findings

This file summarizes confirmed mismatches between `design-doc.md` and the current implementation in `src/`.

## 1) FSM recursive traversal uses incorrect child page mapping (High)

- **Design expectation:** traversal maps logical `(level, page_no, slot)` through the FSM hierarchy to the correct descendant page.
- **Observed code:** recursion passes `child_idx` directly as next `page_no`, ignoring the current `page_no`:
  - `src/backend/fsm/fsm.rs:530-539`
- **Risk:** on non-root branches, search can walk the wrong subtree and miss eligible pages.
- **Fix direction:** compute child page number from parent context (level/page fanout), not just local child index.

## 2) FSM file offset ignores level-aware mapping from design (High)

- **Design expectation:** physical block is derived from hierarchical mapping (see design formula for level/page mapping).
- **Observed code:** read/write offset uses only:
  - `block_offset = page_no * FSM_PAGE_SIZE`
  - `src/backend/fsm/fsm.rs:337`, `src/backend/fsm/fsm.rs:381`
- **Risk:** collisions/overwrites between levels or incorrect page retrieval when multiple levels are present.
- **Fix direction:** introduce/consistently use a level-aware block mapper (e.g., `fsm_block_for(level, page_no)`), then seek by that physical block.

## 3) `fp_next_slot` load spreading is declared but not used in search path (Medium)

- **Design expectation:** search prefers child indicated by `fp_next_slot`, then fallback; advance on visited pages.
- **Observed code:** `fp_next_slot` exists and serializes, but traversal loop scans children from index `0..num_children` and does not consult/advance `fp_next_slot` in the path:
  - `src/backend/fsm/fsm.rs:54`, `84-90`, `114-129` (field + serialization + helper)
  - `src/backend/fsm/fsm.rs:530-543` (search loop not using hint)
- **Risk:** inserts cluster and lose intended load-spreading behavior.
- **Fix direction:** make child selection start from `fp_next_slot`, fallback to alternate child(ren), and persist advanced hints for visited pages.

## 4) `HeapManager::open` does not rebuild FSM from heap as specified (Medium)

- **Design expectation:** open path rebuilds/repairs FSM hints from heap state.
- **Observed code:** open path calls `FSM::open(...)` directly:
  - `src/backend/heap/heap_manager.rs:275-277`
- **Design reference:** `design-doc.md:361` calls for `FSM::build_from_heap(file_path.clone())`.
- **Risk:** stale/corrupt FSM hints may persist across restart.
- **Fix direction:** call `build_from_heap` (or equivalent verified rebuild path) during open/recovery.

## 5) Delete metric increments wrong counter (Low)

- **Observed code:** `delete_tuple` increments `insert_tuple_calls`:
  - `src/backend/heap/heap_manager.rs:543`
- **Risk:** incorrect observability and misleading performance/usage data.
- **Fix direction:** increment a delete-specific counter (`delete_tuple_calls`) instead.

## 6) Uncontrolled `println!` calls in backend hot/IO paths (Low)

- **Observed code examples:**
  - `src/backend/page/mod.rs:116-117`
  - `src/backend/heap/types.rs:24`, `45-48`, `85-88`
- **Risk:** noisy stdout in library/runtime usage; benchmark/test output pollution.
- **Fix direction:** replace with `log::trace!`/`log::debug!` or remove debug prints.

## Notes

- A quick test run previously completed successfully (`cargo test --quiet`), so these are primarily **spec-compliance and correctness-risk** issues that may not be covered by current tests.
- No `TODO`/`unimplemented!` markers were found in `src/`; gaps are mostly behavioral mismatches, not explicit stubs.
