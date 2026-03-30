use std::fs::{OpenOptions, create_dir_all};
use std::io::{self, Write};
use chrono::{SecondsFormat, Utc};

use serde_json::{Value, json};

pub fn current_timestamp_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn append_log(
    db_name: &str,
    table_name: &str,
    operation: &str,
    file_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    let dir = format!("database/logs/{}/{}", db_name, table_name);
    create_dir_all(&dir)?;

    let path = format!("{}/{}", dir, file_name);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    let entry = json!({
        "timestamp": current_timestamp_iso(),
        "operation": operation,
        "details": details,
        "status": status,
    });

    writeln!(file, "{}", entry)?;
    Ok(())
}

pub fn log_update(
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(db_name, table_name, "update", "update.log", details, status)
}

pub fn log_delete(
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(db_name, table_name, "delete", "delete.log", details, status)
}

pub fn log_compaction(
    db_name: &str,
    table_name: &str,
    details: Value,
    status: &str,
) -> io::Result<()> {
    append_log(
        db_name,
        table_name,
        "compaction",
        "compaction.log",
        details,
        status,
    )
}
