/// Instrumentation module for counting FSM and Heap operations
///
/// This module provides atomic counters that track function calls
/// without impacting performance. Enable verbose output via env_logger
/// or use `get_stats()` for programmatic access.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Global counters for FSM operations
pub struct FSMMetrics {
    pub fsm_search_avail_calls: AtomicUsize,
    pub fsm_search_tree_calls: AtomicUsize,
    pub fsm_read_page_calls: AtomicUsize,
    pub fsm_write_page_calls: AtomicUsize,
    pub fsm_serialize_page_calls: AtomicUsize,
    pub fsm_deserialize_page_calls: AtomicUsize,
    pub fsm_set_avail_calls: AtomicUsize,
    pub fsm_vacuum_update_calls: AtomicUsize,
}

/// Global counters for Heap operations
pub struct HeapMetrics {
    pub insert_tuple_calls: AtomicUsize,
    pub get_tuple_calls: AtomicUsize,
    pub allocate_page_calls: AtomicUsize,
    pub write_page_calls: AtomicUsize,
    pub read_page_calls: AtomicUsize,
    pub page_free_space_calls: AtomicUsize,
}

pub static FSM_METRICS: FSMMetrics = FSMMetrics {
    fsm_search_avail_calls: AtomicUsize::new(0),
    fsm_search_tree_calls: AtomicUsize::new(0),
    fsm_read_page_calls: AtomicUsize::new(0),
    fsm_write_page_calls: AtomicUsize::new(0),
    fsm_serialize_page_calls: AtomicUsize::new(0),
    fsm_deserialize_page_calls: AtomicUsize::new(0),
    fsm_set_avail_calls: AtomicUsize::new(0),
    fsm_vacuum_update_calls: AtomicUsize::new(0),
};

pub static HEAP_METRICS: HeapMetrics = HeapMetrics {
    insert_tuple_calls: AtomicUsize::new(0),
    get_tuple_calls: AtomicUsize::new(0),
    allocate_page_calls: AtomicUsize::new(0),
    write_page_calls: AtomicUsize::new(0),
    read_page_calls: AtomicUsize::new(0),
    page_free_space_calls: AtomicUsize::new(0),
};

/// Statistics snapshot that can be printed or logged
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub fsm_search_avail: usize,
    pub fsm_search_tree: usize,
    pub fsm_read_page: usize,
    pub fsm_write_page: usize,
    pub fsm_serialize_page: usize,
    pub fsm_deserialize_page: usize,
    pub fsm_set_avail: usize,
    pub fsm_vacuum_update: usize,
    pub heap_insert_tuple: usize,
    pub heap_get_tuple: usize,
    pub heap_allocate_page: usize,
    pub heap_write_page: usize,
    pub heap_read_page: usize,
    pub heap_page_free_space: usize,
}

impl StatsSnapshot {
    /// Get a snapshot of current metrics
    pub fn capture() -> Self {
        StatsSnapshot {
            fsm_search_avail: FSM_METRICS.fsm_search_avail_calls.load(Ordering::Relaxed),
            fsm_search_tree: FSM_METRICS.fsm_search_tree_calls.load(Ordering::Relaxed),
            fsm_read_page: FSM_METRICS.fsm_read_page_calls.load(Ordering::Relaxed),
            fsm_write_page: FSM_METRICS.fsm_write_page_calls.load(Ordering::Relaxed),
            fsm_serialize_page: FSM_METRICS.fsm_serialize_page_calls.load(Ordering::Relaxed),
            fsm_deserialize_page: FSM_METRICS.fsm_deserialize_page_calls.load(Ordering::Relaxed),
            fsm_set_avail: FSM_METRICS.fsm_set_avail_calls.load(Ordering::Relaxed),
            fsm_vacuum_update: FSM_METRICS.fsm_vacuum_update_calls.load(Ordering::Relaxed),
            heap_insert_tuple: HEAP_METRICS.insert_tuple_calls.load(Ordering::Relaxed),
            heap_get_tuple: HEAP_METRICS.get_tuple_calls.load(Ordering::Relaxed),
            heap_allocate_page: HEAP_METRICS.allocate_page_calls.load(Ordering::Relaxed),
            heap_write_page: HEAP_METRICS.write_page_calls.load(Ordering::Relaxed),
            heap_read_page: HEAP_METRICS.read_page_calls.load(Ordering::Relaxed),
            heap_page_free_space: HEAP_METRICS.page_free_space_calls.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero
    pub fn reset_all() {
        FSM_METRICS.fsm_search_avail_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_search_tree_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_read_page_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_write_page_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_serialize_page_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_deserialize_page_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_set_avail_calls.store(0, Ordering::Relaxed);
        FSM_METRICS.fsm_vacuum_update_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.insert_tuple_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.get_tuple_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.allocate_page_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.write_page_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.read_page_calls.store(0, Ordering::Relaxed);
        HEAP_METRICS.page_free_space_calls.store(0, Ordering::Relaxed);
    }

    /// Print stats in a formatted table
    pub fn print_table(&self) {
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                    OPERATION METRICS                         ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ FSM Operations:                                              ║");
        println!("║  - fsm_search_avail:     {:8} calls                      ║", self.fsm_search_avail);
        println!("║  - fsm_search_tree:      {:8} calls                      ║", self.fsm_search_tree);
        println!("║  - fsm_read_page:        {:8} calls                      ║", self.fsm_read_page);
        println!("║  - fsm_write_page:       {:8} calls                      ║", self.fsm_write_page);
        println!("║  - fsm_serialize_page:   {:8} calls                      ║", self.fsm_serialize_page);
        println!("║  - fsm_deserialize_page: {:8} calls                      ║", self.fsm_deserialize_page);
        println!("║  - fsm_set_avail:        {:8} calls                      ║", self.fsm_set_avail);
        println!("║  - fsm_vacuum_update:    {:8} calls                      ║", self.fsm_vacuum_update);
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ Heap Operations:                                             ║");
        println!("║  - insert_tuple:         {:8} calls                      ║", self.heap_insert_tuple);
        println!("║  - get_tuple:            {:8} calls                      ║", self.heap_get_tuple);
        println!("║  - allocate_page:        {:8} calls                      ║", self.heap_allocate_page);
        println!("║  - write_page:           {:8} calls                      ║", self.heap_write_page);
        println!("║  - read_page:            {:8} calls                      ║", self.heap_read_page);
        println!("║  - page_free_space:      {:8} calls                      ║", self.heap_page_free_space);
        println!("╚══════════════════════════════════════════════════════════════╝\n");
    }
}