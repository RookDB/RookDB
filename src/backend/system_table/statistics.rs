//! System table operations for `sys_statistics`.
//!
//! Stores and retrieves column-level statistics (histograms, HLL distinct counts,
//! and selectivity bounds) in slotted-page heap files under `database/system/statistics.dat`.

use std::io;

use crate::backend::statistics::column_stats::{ColumnHistogram, ColumnStatistics};
use crate::backend::system_table::*;
use crate::types::DataValue;

/// Persist column statistics for a table into `sys_statistics`.
///
/// Replaces any existing statistics for the table to keep the catalog clean.
pub fn save_column_statistics(
    db_name: &str,
    table_name: &str,
    stats: &[ColumnStatistics],
) -> io::Result<()> {
    if stats.is_empty() {
        return Ok(());
    }

    let (table_id, _db_id) = resolve_table_id(db_name, table_name)?;

    // 1. Delete previous statistics rows for this table
    let _ = delete_rows_by_table_id("statistics", SYS_STATISTICS_SCHEMA, table_id);

    // 2. Scan sys_statistics to compute next available stat_id
    let existing = scan_system_table("statistics", SYS_STATISTICS_SCHEMA)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut next_stat_id = existing.iter().fold(1i32, |max_id, row| {
        match row.first() {
            Some(Some(DataValue::Int(id))) => std::cmp::max(max_id, *id + 1),
            _ => max_id,
        }
    });

    // 3. Serialize statistics rows
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(stats.len());
    for s in stats {
        let stat_id = next_stat_id;
        next_stat_id += 1;

        let hist_json = serde_json::to_string(&s.histogram)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let min_str = s.min_value.as_ref().map(value_to_string);
        let max_str = s.max_value.as_ref().map(value_to_string);

        rows.push(vec![
            Some(stat_id.to_string()),
            Some(table_id.to_string()),
            Some(s.column_name.clone()),
            Some(s.total_count.to_string()),
            Some(s.null_count.to_string()),
            Some(s.distinct_count.to_string()),
            min_str,
            max_str,
            Some(hist_json),
        ]);
    }

    insert_system_rows("statistics", SYS_STATISTICS_SCHEMA, &rows)?;
    crate::backend::cache::invalidate_metadata();
    Ok(())
}

/// Load all column statistics for a table from `sys_statistics`.
pub fn load_column_statistics(
    db_name: &str,
    table_name: &str,
) -> io::Result<Vec<ColumnStatistics>> {
    let (table_id, _db_id) = resolve_table_id(db_name, table_name)?;

    let rows = scan_system_table("statistics", SYS_STATISTICS_SCHEMA)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut result = Vec::new();
    for row in rows {
        // Schema: [stat_id, table_id, col_name, total_count, null_count, distinct_count, min_val, max_val, histogram_json]
        if row.len() < 9 {
            continue;
        }

        let row_tid = match row.get(1) {
            Some(Some(DataValue::Int(tid))) => *tid,
            _ => continue,
        };
        if row_tid != table_id {
            continue;
        }

        let col_name = match row.get(2) {
            Some(Some(DataValue::Varchar(s))) | Some(Some(DataValue::Char(s))) => s.clone(),
            _ => continue,
        };

        let total_count = match row.get(3) {
            Some(Some(DataValue::BigInt(c))) => *c as u64,
            Some(Some(DataValue::Int(c))) => *c as u64,
            _ => 0,
        };

        let null_count = match row.get(4) {
            Some(Some(DataValue::BigInt(c))) => *c as u64,
            Some(Some(DataValue::Int(c))) => *c as u64,
            _ => 0,
        };

        let distinct_count = match row.get(5) {
            Some(Some(DataValue::BigInt(c))) => *c as u64,
            Some(Some(DataValue::Int(c))) => *c as u64,
            _ => 1,
        };

        let hist_json = match row.get(8) {
            Some(Some(DataValue::Varchar(s))) | Some(Some(DataValue::Char(s))) => s.as_str(),
            _ => "{}",
        };

        let histogram: ColumnHistogram = serde_json::from_str(hist_json)
            .unwrap_or_default();

        let min_value = histogram.buckets.first().map(|b| b.lower.clone());
        let max_value = histogram.buckets.last().map(|b| b.upper.clone());

        result.push(ColumnStatistics {
            table_name: table_name.to_string(),
            column_name: col_name,
            total_count,
            null_count,
            distinct_count,
            min_value,
            max_value,
            histogram,
        });
    }

    Ok(result)
}

/// Load statistics for a single column.
pub fn load_single_column_statistics(
    db_name: &str,
    table_name: &str,
    column_name: &str,
) -> io::Result<Option<ColumnStatistics>> {
    let all = load_column_statistics(db_name, table_name)?;
    Ok(all.into_iter().find(|s| s.column_name.eq_ignore_ascii_case(column_name)))
}
