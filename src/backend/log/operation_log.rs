use chrono::{SecondsFormat, Utc};
use std::fs::{OpenOptions, create_dir_all};
use std::io::{self, Write};

use serde_json::{Value, json};

pub fn current_timestamp_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn append_log(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    operation: &str,
    file_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    let dir = engine.paths().log_dir(db_name, table_name);
    create_dir_all(&dir)?;

    let path = dir.join(file_name);
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    let entry = json!({
        "timestamp": current_timestamp_iso(),
        "operation": operation,
        "details": details,
        "status": status,
    });

    // One write per entry: formatting a JSON value straight into the file
    // issues a syscall for every token.
    let mut line = entry.to_string();
    line.push('\n');
    file.write_all(line.as_bytes())?;
    Ok(())
}

pub fn log_update(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(
        engine,
        db_name,
        table_name,
        "update",
        "update.log",
        details,
        status,
    )
}

pub fn log_delete(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(
        engine,
        db_name,
        table_name,
        "delete",
        "delete.log",
        details,
        status,
    )
}

pub fn log_compaction(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(
        engine,
        db_name,
        table_name,
        "compaction",
        "compaction.log",
        details,
        status,
    )
}
