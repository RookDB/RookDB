---
title: Testing Guide
sidebar_position: 6
---

# Testing Guide

This page consolidates test coverage and execution guidance for FSM and heap manager functionality.

## Coverage Areas

The test surface covers:

- Heap create insert get delete scan lifecycle
- FSM search and update behavior under load
- Catalog initialization and persistence
- Page and disk primitive correctness
- Integration flows for schema and table isolation
- Recovery behavior for FSM sidecar reconstruction

## Integration Test Files

- tests/test_heap_manager.rs
- tests/test_fsm_heavy.rs
- tests/test_hsm_integration.rs
- tests/test_init_catalog.rs
- tests/test_create_page.rs
- tests/test_fsm_page_allocation.rs
- tests/test_init_page.rs
- tests/test_init_table.rs
- tests/test_load_catalog.rs
- tests/test_page_count.rs
- tests/test_page_free_space.rs
- tests/test_read_page.rs
- tests/test_save_catalog.rs
- tests/test_write_page.rs

## Unit Test Modules

- src/backend/heap/heap_manager.rs
- src/backend/fsm/fsm.rs
- src/backend/page/mod.rs
- src/backend/page_api.rs
- src/backend/heap/types.rs
- src/backend/types_validator.rs
- src/backend/error_handler.rs

## How to Run

Run all tests:

```bash
cargo test
```

Run key suites:

```bash
cargo test --test test_heap_manager
cargo test --test test_fsm_heavy
cargo test --test test_hsm_integration
```

Run with logging:

```bash
RUST_LOG=debug cargo test -- --nocapture
RUST_LOG=trace cargo test -- --nocapture
```

Run sequentially when filesystem artifacts may conflict:

```bash
cargo test -- --test-threads=1
```

## Validation Signals

Important pass signals include:

- Correct tuple count progression through insert and delete flows
- FSM allocation and update correctness under stress
- Multi-table isolation with interleaved inserts
- Page-boundary and oversized tuple safety checks
- Rebuild success when FSM sidecar is removed

## Troubleshooting

If tests conflict on shared files:

```bash
cargo test -- --test-threads=1
```

If stale artifacts affect runs:

```bash
rm -rf database/base/test_*
rm -f database/global/catalog.json
cargo test
```

## Conclusion

The test suite provides strong evidence for correctness, integration stability, and recovery behavior of the current FSM and heap manager implementation.
