---
title: Heap Manager and Page Layout
sidebar_position: 2
---

# Heap Manager and Page Layout

This page covers heap internals, page format, insert behavior, and integration hooks.

## Slotted Page Layout

Each data page is 8192 bytes.

- Header holds lower and upper offsets.
- Slot directory stores tuple offsets and lengths.
- Free space is the contiguous gap between lower and upper.
- Tuple bytes are packed from end of page downward.

## Metadata and Header Integrity

Page 0 stores global table metadata:

- page_count
- fsm_page_count
- total_tuples
- last_vacuum

These fields are updated on create, append, insert, delete, and open reconciliation.

## Insert Flow

Heap insertion logic uses FSM-guided routing:

1. Compute required category from tuple size.
2. Ask FSM for candidate page.
3. Validate actual contiguous free space.
4. Insert tuple and update slot directory.
5. Update FSM with post-insert free space.
6. Persist header updates.

If insertion cannot succeed on candidate pages, the manager allocates a new page and retries.

## Deletion and Reuse

Deletion marks slot entries invalid and decrements tuple count.

Current behavior includes practical optimizations:

- Reuse dead slots for future inserts
- Roll back upper pointer when tail tuple is deleted
- Roll back lower pointer when tail slot is removed

## Scan and Retrieval

- get_tuple provides coordinate-based lookup using page id and slot id.
- scan provides lazy page-wise sequential iteration.
- Dead slots are skipped during iteration.

## Compaction Integration APIs

The heap layer exposes integration-friendly facades:

- insert_raw_tuple
- update_page_free_space
- rebuild_table_fsm

These are intended for compaction and vacuum workflows.

## Performance and Tradeoffs

Current phase prioritizes predictable insertion and page-level tracking.

Tradeoff:

- Fast page-oriented operations now
- Full in-page compaction and total-space tracking deferred to future work
