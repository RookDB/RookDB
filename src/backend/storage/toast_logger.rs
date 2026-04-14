use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref TOAST_LOG: Mutex<()> = Mutex::new(());
}

const TOAST_LOG_PATH: &str = "logs/toast.log";

/// Initialize the TOAST log file
pub fn init_toast_logger() {
    let _ = fs::create_dir_all("logs");
    // Clear previous log
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(TOAST_LOG_PATH)
    {
        let _ = writeln!(file, "=== TOAST Log Started ===\n");
    }
}

/// Log a TOAST message to the log file
pub fn log_toast(message: &str) {
    let _guard = TOAST_LOG.lock().unwrap();
    
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(TOAST_LOG_PATH)
    {
        let _ = writeln!(file, "{}", message);
    }
}

/// Log a TOAST message with formatting
#[macro_export]
macro_rules! toast_log {
    ($($arg:tt)*) => {
        $crate::backend::storage::toast_logger::log_toast(&format!($($arg)*))
    };
}
