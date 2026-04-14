use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Mutex;
use chrono::Local;

lazy_static::lazy_static! {
    static ref DATABASE_LOG: Mutex<()> = Mutex::new(());
}

const DATABASE_LOG_PATH: &str = "logs/database.log";

// ============================================================================
// LOG FILE INITIALIZATION
// ============================================================================

/// Initialize the database log file
pub fn init_database_logger() {
    let _ = fs::create_dir_all("logs");
    // Clear previous log
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(DATABASE_LOG_PATH)
    {
        let _ = writeln!(file, "================================================================================");
        let _ = writeln!(file, "RookDB - Database Operations and Data Access Log");
        let _ = writeln!(file, "================================================================================");
        let _ = writeln!(file, "Log started at: {}", Local::now().format("%Y-%m-%d %H:%M:%S%.3f"));
        let _ = writeln!(file, "================================================================================\n");
    }
}

// ============================================================================
// CORE LOGGING FUNCTION
// ============================================================================

/// Log a database operation message to the log file with timestamp
pub fn log_database(message: &str) {
    let _guard = DATABASE_LOG.lock().unwrap();
    
    // Get current timestamp
    let now = Local::now();
    let timestamp = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    
    if let Ok(mut file) = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(DATABASE_LOG_PATH)
    {
        let _ = writeln!(file, "[{}] {}", timestamp, message);
    }
}

// ============================================================================
// DATABASE OPERATIONS LOGGING
// ============================================================================

/// Log database creation
pub fn log_create_database(db_name: &str) {
    log_database(&format!(
        "[DATABASE_CREATE] name='{}', status=SUCCESS",
        db_name
    ));
}

/// Log database creation failure
pub fn log_create_database_failed(db_name: &str, reason: &str) {
    log_database(&format!(
        "[DATABASE_CREATE] name='{}', status=FAILED, reason='{}'",
        db_name, reason
    ));
}

/// Log database selection
pub fn log_select_database(db_name: &str) {
    log_database(&format!(
        "[DATABASE_SELECT] name='{}', status=SUCCESS",
        db_name
    ));
}

/// Log database selection failure
pub fn log_select_database_failed(db_name: &str, reason: &str) {
    log_database(&format!(
        "[DATABASE_SELECT] name='{}', status=FAILED, reason='{}'",
        db_name, reason
    ));
}

/// Log show databases command
pub fn log_show_databases(count: usize) {
    log_database(&format!(
        "[DATABASE_SHOW] total_databases={}, status=SUCCESS",
        count
    ));
}

// ============================================================================
// TABLE OPERATIONS LOGGING
// ============================================================================

/// Log table creation
pub fn log_create_table(db: &str, table: &str, column_count: usize) {
    log_database(&format!(
        "[TABLE_CREATE] database='{}', table='{}', columns={}, status=SUCCESS",
        db, table, column_count
    ));
}

/// Log table creation failure
pub fn log_create_table_failed(db: &str, table: &str, reason: &str) {
    log_database(&format!(
        "[TABLE_CREATE] database='{}', table='{}', status=FAILED, reason='{}'",
        db, table, reason
    ));
}

/// Log show tables command
pub fn log_show_tables(db: &str, table_count: usize) {
    log_database(&format!(
        "[TABLE_SHOW] database='{}', total_tables={}, status=SUCCESS",
        db, table_count
    ));
}

/// Log table statistics viewing
pub fn log_show_table_statistics(db: &str, table: &str, page_count: u32, tuple_count: u32) {
    log_database(&format!(
        "[TABLE_STATISTICS] database='{}', table='{}', pages={}, tuples={}, status=SUCCESS",
        db, table, page_count, tuple_count
    ));
}

// ============================================================================
// DATA MODIFICATION OPERATIONS - DML (DATA MANIPULATION LANGUAGE)
// ============================================================================

/// Log INSERT operation with detailed metrics
pub fn log_insert(db: &str, table: &str, tuple_bytes: usize, page_count: u32) {
    log_database(&format!(
        "[DML_INSERT] database='{}', table='{}', tuple_size={} bytes, total_pages={}, status=SUCCESS",
        db, table, tuple_bytes, page_count
    ));
}

/// Log INSERT operation failure
pub fn log_insert_failed(db: &str, table: &str, reason: &str) {
    log_database(&format!(
        "[DML_INSERT] database='{}', table='{}', status=FAILED, reason='{}'",
        db, table, reason
    ));
}

/// Log DELETE operation with TOAST cleanup details
pub fn log_delete(db: &str, table: &str, page_num: u32, slot_index: u32, toast_values_freed: usize) {
    if toast_values_freed > 0 {
        log_database(&format!(
            "[DML_DELETE] database='{}', table='{}', location=(page={}, slot={}), freed_toast_values={}, status=SUCCESS",
            db, table, page_num, slot_index, toast_values_freed
        ));
    } else {
        log_database(&format!(
            "[DML_DELETE] database='{}', table='{}', location=(page={}, slot={}), status=SUCCESS",
            db, table, page_num, slot_index
        ));
    }
}

/// Log DELETE operation failure
pub fn log_delete_failed(db: &str, table: &str, reason: &str) {
    log_database(&format!(
        "[DML_DELETE] database='{}', table='{}', status=FAILED, reason='{}'",
        db, table, reason
    ));
}

/// Log UPDATE operation with size change metrics
pub fn log_update(db: &str, table: &str, page_num: u32, slot_index: u32, 
                  old_tuple_size: usize, new_tuple_size: usize, toast_values_freed: usize) {
    let size_change = (new_tuple_size as i64) - (old_tuple_size as i64);
    if toast_values_freed > 0 {
        log_database(&format!(
            "[DML_UPDATE] database='{}', table='{}', location=(page={}, slot={}), old_size={} bytes, new_size={} bytes, size_delta={:+} bytes, freed_toast_values={}, status=SUCCESS",
            db, table, page_num, slot_index, old_tuple_size, new_tuple_size, size_change, toast_values_freed
        ));
    } else {
        log_database(&format!(
            "[DML_UPDATE] database='{}', table='{}', location=(page={}, slot={}), old_size={} bytes, new_size={} bytes, size_delta={:+} bytes, status=SUCCESS",
            db, table, page_num, slot_index, old_tuple_size, new_tuple_size, size_change
        ));
    }
}

/// Log UPDATE operation failure
pub fn log_update_failed(db: &str, table: &str, reason: &str) {
    log_database(&format!(
        "[DML_UPDATE] database='{}', table='{}', status=FAILED, reason='{}'",
        db, table, reason
    ));
}

// ============================================================================
// DATA QUERY OPERATIONS - DQL (DATA QUERY LANGUAGE)
// ============================================================================

/// Log SCAN operation (full table scan or selection)
pub fn log_scan_tuples(db: &str, table: &str, tuple_count: u32, page_count: u32) {
    log_database(&format!(
        "[DQL_SCAN] database='{}', table='{}', tuples_scanned={}, pages_scanned={}, status=SUCCESS",
        db, table, tuple_count, page_count
    ));
}

/// Log SCAN operation failure
pub fn log_scan_tuples_failed(db: &str, table: &str, reason: &str) {
    log_database(&format!(
        "[DQL_SCAN] database='{}', table='{}', status=FAILED, reason='{}'",
        db, table, reason
    ));
}

// ============================================================================
// CSV BULK LOAD OPERATIONS
// ============================================================================

/// Log CSV bulk load operation
pub fn log_csv_load(db: &str, table: &str, file_path: &str, row_count: u32) {
    log_database(&format!(
        "[IMPORT_CSV] database='{}', table='{}', file='{}', rows_loaded={}, status=SUCCESS",
        db, table, file_path, row_count
    ));
}

/// Log CSV load failure
pub fn log_csv_load_failed(db: &str, table: &str, file_path: &str, reason: &str) {
    log_database(&format!(
        "[IMPORT_CSV] database='{}', table='{}', file='{}', status=FAILED, reason='{}'",
        db, table, file_path, reason
    ));
}

// ============================================================================
// STORAGE LAYER DATA ACCESS LOGGING
// ============================================================================

/// Log page allocation
pub fn log_page_allocate(db: &str, table: &str, page_num: u32) {
    log_database(&format!(
        "[STORAGE_PAGE_ALLOC] database='{}', table='{}', page_num={}, status=SUCCESS",
        db, table, page_num
    ));
}

/// Log page read access
pub fn log_page_read(db: &str, table: &str, page_num: u32, bytes_read: usize) {
    log_database(&format!(
        "[STORAGE_PAGE_READ] database='{}', table='{}', page_num={}, bytes={}, status=SUCCESS",
        db, table, page_num, bytes_read
    ));
}

/// Log page write access
pub fn log_page_write(db: &str, table: &str, page_num: u32, bytes_written: usize) {
    log_database(&format!(
        "[STORAGE_PAGE_WRITE] database='{}', table='{}', page_num={}, bytes={}, status=SUCCESS",
        db, table, page_num, bytes_written
    ));
}

/// Log tuple insertion into page
pub fn log_tuple_insert_slot(db: &str, table: &str, page_num: u32, slot_index: u32, tuple_size: usize) {
    log_database(&format!(
        "[STORAGE_TUPLE_INSERT] database='{}', table='{}', location=(page={}, slot={}), size={} bytes, status=SUCCESS",
        db, table, page_num, slot_index, tuple_size
    ));
}

/// Log tuple read from page
pub fn log_tuple_read_slot(db: &str, table: &str, page_num: u32, slot_index: u32, tuple_size: usize) {
    log_database(&format!(
        "[STORAGE_TUPLE_READ] database='{}', table='{}', location=(page={}, slot={}), size={} bytes, status=SUCCESS",
        db, table, page_num, slot_index, tuple_size
    ));
}

/// Log TOAST value creation
pub fn log_toast_allocate(db: &str, table: &str, value_id: u64, chunk_count: u32, total_bytes: usize) {
    log_database(&format!(
        "[TOAST_ALLOCATE] database='{}', table='{}', value_id={}, chunks={}, total_bytes={}, status=SUCCESS",
        db, table, value_id, chunk_count, total_bytes
    ));
}

/// Log TOAST value deletion/cleanup
pub fn log_toast_delete(db: &str, table: &str, value_id: u64, chunk_count: u32) {
    log_database(&format!(
        "[TOAST_DELETE] database='{}', table='{}', value_id={}, chunks_freed={}, status=SUCCESS",
        db, table, value_id, chunk_count
    ));
}

/// Log TOAST persistence
pub fn log_toast_persist(db: &str, table: &str, total_values: u32, total_bytes: usize) {
    log_database(&format!(
        "[TOAST_PERSIST] database='{}', table='{}', total_values={}, total_bytes={}, status=SUCCESS",
        db, table, total_values, total_bytes
    ));
}

// ============================================================================
// BUFFER MANAGER OPERATIONS
// ============================================================================

/// Log buffer hit
pub fn log_buffer_hit(db: &str, table: &str, page_num: u32) {
    log_database(&format!(
        "[BUFFER_HIT] database='{}', table='{}', page_num={}, status=SUCCESS",
        db, table, page_num
    ));
}

/// Log buffer miss
pub fn log_buffer_miss(db: &str, table: &str, page_num: u32) {
    log_database(&format!(
        "[BUFFER_MISS] database='{}', table='{}', page_num={}, status=MISS",
        db, table, page_num
    ));
}

/// Log buffer eviction
pub fn log_buffer_evict(db: &str, table: &str, page_num: u32, dirty: bool) {
    let status = if dirty { "EVICT_DIRTY" } else { "EVICT_CLEAN" };
    log_database(&format!(
        "[BUFFER_EVICT] database='{}', table='{}', page_num={}, status={}",
        db, table, page_num, status
    ));
}

// ============================================================================
// CATALOG OPERATIONS
// ============================================================================

/// Log catalog load/initialization
pub fn log_catalog_init(db_count: usize, table_count: usize) {
    log_database(&format!(
        "[CATALOG_INIT] databases={}, tables={}, status=SUCCESS",
        db_count, table_count
    ));
}

/// Log catalog save to disk
pub fn log_catalog_save(db_count: usize, table_count: usize) {
    log_database(&format!(
        "[CATALOG_SAVE] databases={}, tables={}, status=SUCCESS",
        db_count, table_count
    ));
}

// ============================================================================
// ERROR AND EXCEPTION LOGGING
// ============================================================================

/// Log a general error/exception
pub fn log_error(operation: &str, error_msg: &str) {
    log_database(&format!(
        "[ERROR] operation='{}', error='{}', status=EXCEPTION",
        operation, error_msg
    ));
}

/// Log a session event (start/stop)
pub fn log_session(event: &str) {
    log_database(&format!(
        "[SESSION] event='{}', timestamp={}",
        event, Local::now().format("%Y-%m-%d %H:%M:%S%.3f")
    ));
}

// ============================================================================
// MACRO FOR CONVENIENCE LOGGING
// ============================================================================

/// Log a formatted database message
#[macro_export]
macro_rules! db_log {
    ($($arg:tt)*) => {
        $crate::backend::storage::database_logger::log_database(&format!($($arg)*))
    };
}
