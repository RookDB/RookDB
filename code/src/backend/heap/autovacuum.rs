//! Concurrent background autovacuum (compaction) worker pool.
//!
//! Tables are tracked in-memory and queued in a global max-heap by priority:
//!   priority = dead_tuple_count - threshold
//!
//! Threshold formula:
//!   threshold = 50 + 0.2 * table_size

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{
    Arc,
    Condvar,
    Mutex,
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::executor::compaction_table;
use crate::table::Table;

pub const AUTOVACUUM_WORKERS: usize = 3;
pub const AUTOVACUUM_BASE_THRESHOLD: usize = 50;
pub const AUTOVACUUM_SCALE_FACTOR: f64 = 0.2;

#[derive(Clone)]
struct HeapEntry {
    priority: isize,
    key: String,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.key == other.key
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

struct VacuumManager {
    heap: Arc<Mutex<BinaryHeap<HeapEntry>>>,
    tables: Arc<Mutex<HashMap<String, Arc<Mutex<Table>>>>>,
    condvar: Arc<Condvar>,
}

impl VacuumManager {
    fn new() -> Self {
        Self {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            tables: Arc::new(Mutex::new(HashMap::new())),
            condvar: Arc::new(Condvar::new()),
        }
    }
}

static MANAGER: OnceLock<Arc<VacuumManager>> = OnceLock::new();

fn manager() -> Arc<VacuumManager> {
    MANAGER
        .get_or_init(|| Arc::new(VacuumManager::new()))
        .clone()
}

fn table_key(db_name: &str, table_name: &str) -> String {
    format!("{}::{}", db_name, table_name)
}

fn split_table_key(key: &str) -> Option<(&str, &str)> {
    key.split_once("::")
}

fn compute_threshold(table_size: usize) -> usize {
    AUTOVACUUM_BASE_THRESHOLD + (AUTOVACUUM_SCALE_FACTOR * table_size as f64) as usize
}

pub fn notify_table_write(db_name: &str, table_name: &str, delta: usize, table_size: usize) {
    if delta == 0 {
        return;
    }

    let mgr = manager();
    let key = table_key(db_name, table_name);

    let table_arc = {
        let mut tables = mgr.tables.lock().unwrap();
        tables
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Table::from_table_size(table_size))))
            .clone()
    };

    let mut should_push = false;
    let mut priority = 0isize;

    {
        let mut table = table_arc.lock().unwrap();
        table.threshold = compute_threshold(table_size);
        table.dead_tuple_count = table.dead_tuple_count.saturating_add(delta);

        if table.dead_tuple_count > table.threshold && !table.in_heap {
            table.in_heap = true;
            should_push = true;
            priority = table.dead_tuple_count as isize - table.threshold as isize;
        }
    }

    if should_push {
        {
            let mut heap = mgr.heap.lock().unwrap();
            heap.push(HeapEntry {
                priority,
                key: key.clone(),
            });
        }
        mgr.condvar.notify_one();
    }
}

pub fn start(shutdown: Arc<AtomicBool>) -> Vec<JoinHandle<()>> {
    let mgr = manager();
    let mut handles = Vec::with_capacity(AUTOVACUUM_WORKERS);

    for worker_id in 0..AUTOVACUUM_WORKERS {
        let heap = Arc::clone(&mgr.heap);
        let tables = Arc::clone(&mgr.tables);
        let condvar = Arc::clone(&mgr.condvar);
        let shutdown_flag = Arc::clone(&shutdown);

        let handle = thread::Builder::new()
            .name(format!("autovacuum-{}", worker_id + 1))
            .spawn(move || worker_loop(worker_id + 1, heap, tables, condvar, shutdown_flag))
            .expect("failed to spawn autovacuum worker");

        handles.push(handle);
    }

    handles
}

fn worker_loop(
    worker_id: usize,
    heap: Arc<Mutex<BinaryHeap<HeapEntry>>>,
    tables: Arc<Mutex<HashMap<String, Arc<Mutex<Table>>>>>,
    condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
) {
    println!("[autovacuum-{}] started", worker_id);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            println!("[autovacuum-{}] shutting down", worker_id);
            return;
        }

        let entry = {
            let mut guard = heap.lock().unwrap();
            while guard.is_empty() && !shutdown.load(Ordering::Relaxed) {
                let (new_guard, _) = condvar
                    .wait_timeout(guard, Duration::from_secs(1))
                    .unwrap();
                guard = new_guard;
            }

            if shutdown.load(Ordering::Relaxed) {
                println!("[autovacuum-{}] shutting down", worker_id);
                return;
            }

            guard.pop()
        };

        let Some(entry) = entry else {
            continue;
        };

        let table_arc = {
            let table_map = tables.lock().unwrap();
            table_map.get(&entry.key).cloned()
        };

        let Some(table_arc) = table_arc else {
            continue;
        };

        let mut table_busy = false;
        {
            let mut table = table_arc.lock().unwrap();
            if table.in_use {
                table_busy = true;
            } else {
                table.in_use = true;
            }
        }

        if table_busy {
            {
                let mut guard = heap.lock().unwrap();
                guard.push(entry);
            }
            condvar.notify_one();
            continue;
        }

        let mut compacted = false;
        if let Some((db_name, table_name)) = split_table_key(&entry.key) {
            match compaction_table(db_name, table_name) {
                Ok(_) => compacted = true,
                Err(err) => {
                    eprintln!(
                        "[autovacuum-{}] compaction failed for '{}': {}",
                        worker_id, entry.key, err
                    );
                }
            }
        }

        let mut reinsert = false;
        let mut reinsert_priority = 0isize;

        {
            let mut table = table_arc.lock().unwrap();

            if compacted {
                table.dead_tuple_count = 0;
            }

            table.in_use = false;

            if table.dead_tuple_count <= table.threshold {
                table.in_heap = false;
            } else {
                table.in_heap = true;
                reinsert = true;
                reinsert_priority = table.dead_tuple_count as isize - table.threshold as isize;
            }
        }

        if reinsert {
            {
                let mut guard = heap.lock().unwrap();
                guard.push(HeapEntry {
                    priority: reinsert_priority,
                    key: entry.key,
                });
            }
            condvar.notify_one();
        }
    }
}
