//! Starts the program and runs the frontend menu loop.

mod frontend;

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

fn main() -> std::io::Result<()> {
    // Shared shutdown flag – set to true when the user exits the menu.
    let shutdown = Arc::new(AtomicBool::new(false));

    // Start autovacuum background worker pool.
    let av_handles = storage_manager::autovacuum::start(Arc::clone(&shutdown));

    // Run the interactive menu (blocks until the user chooses Exit).
    let result = frontend::menu::run(Arc::clone(&shutdown));

    // Signal autovacuum to stop and wait for it to finish cleanly.
    shutdown.store(true, Ordering::SeqCst);
    for handle in av_handles {
        let _ = handle.join();
    }

    result
}
