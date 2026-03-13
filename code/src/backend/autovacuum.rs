//! Autovacuum – background compaction worker.
//!
//! Mirrors PostgreSQL's autovacuum daemon: a dedicated thread wakes up every
//! `AUTOVACUUM_INTERVAL_SECS` seconds, scans every table in the catalog for
//! soft-deleted slots (SLOT_FLAG_DELETED), and physically removes them by
//! calling `compaction_table`.
//!
//! # How to start
//! ```ignore
//! let shutdown = Arc::new(AtomicBool::new(false));
//! let handle   = autovacuum::start(Arc::clone(&shutdown));
//! // … run the rest of the program …
//! shutdown.store(true, Ordering::SeqCst);
//! handle.join().ok();
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::catalog::load_catalog;
use crate::executor::compaction_table;

/// How often the autovacuum worker wakes up (seconds).
/// Change this constant to tune the compaction frequency.
pub const AUTOVACUUM_INTERVAL_SECS: u64 = 30;

/// Spawn the autovacuum background thread.
///
/// The thread runs until `shutdown` is set to `true`.
/// Shutting down happens within one second of the flag being set.
pub fn start(shutdown: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("autovacuum".into())
        .spawn(move || worker(shutdown))
        .expect("failed to spawn autovacuum thread")
}

// ---------------------------------------------------------------------------
// Internal worker loop
// ---------------------------------------------------------------------------

fn worker(shutdown: Arc<AtomicBool>) {
    println!("[autovacuum] started  (interval = {}s)", AUTOVACUUM_INTERVAL_SECS);

    loop {
        // Sleep in 1-second chunks so we react to shutdown quickly.
        for _ in 0..AUTOVACUUM_INTERVAL_SECS {
            if shutdown.load(Ordering::Relaxed) {
                println!("[autovacuum] shutting down.");
                return;
            }
            thread::sleep(Duration::from_secs(1));
        }

        if shutdown.load(Ordering::Relaxed) {
            println!("[autovacuum] shutting down.");
            return;
        }

        run_cycle();
    }
}

/// One autovacuum cycle: compact every table that has soft-deleted slots.
fn run_cycle() {
    let catalog = load_catalog();

    let mut total_pages_compacted = 0usize;
    let mut tables_vacuumed      = 0usize;

    for (db_name, db) in &catalog.databases {
        for table_name in db.tables.keys() {
            match compaction_table(db_name, table_name) {
                Ok(0) => { /* nothing to do */ }
                Ok(n) => {
                    println!(
                        "[autovacuum] vacuumed '{}.{}': {} page(s) compacted",
                        db_name, table_name, n
                    );
                    total_pages_compacted += n;
                    tables_vacuumed       += 1;
                }
                Err(e) => {
                    eprintln!(
                        "[autovacuum] error vacuuming '{}.{}': {}",
                        db_name, table_name, e
                    );
                }
            }
        }
    }

    if tables_vacuumed == 0 {
        println!("[autovacuum] cycle complete – nothing to compact.");
    } else {
        println!(
            "[autovacuum] cycle complete – {} table(s), {} page(s) compacted.",
            tables_vacuumed, total_pages_compacted
        );
    }
}
