//! ANALYZE — collect column-level statistics for cost-based optimization.
//!
//! Scans all tuples in a table's heap file, populates HyperLogLog sketches
//! and equi-depth histograms per column, and persists the results in
//! `sys_statistics`.

use std::path::PathBuf;

use crate::backend::error::{RookError, RookResult};
use crate::backend::heap::HeapManager;
use crate::backend::statistics::column_stats::ColumnStatisticsCollector;
use crate::backend::system_table::save_column_statistics;
use crate::catalog::Catalog;
use crate::types::{deserialize_nullable_row, DataType};

/// Summary of an ANALYZE run.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeStats {
    pub tuples_analyzed: u64,
    pub columns_analyzed: usize,
}

/// Analyze a table and store column statistics in `sys_statistics`.
pub fn analyze_table(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> RookResult<AnalyzeStats> {
    // 1. Path safety
    crate::backend::name_validation::validate_database_name(db_name)
        .map_err(|e| RookError::Internal(e.to_string()))?;
    crate::backend::name_validation::validate_table_name(table_name)
        .map_err(|e| RookError::Internal(e.to_string()))?;

    // 2. Resolve table schema from catalog
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::Internal(format!("Database '{}' not found", db_name)))?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::Internal(format!("Table '{}.{}' not found", db_name, table_name)))?;

    let schema_types: Vec<DataType> = table.columns.iter().map(|c| c.data_type.clone()).collect();
    let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();

    // 3. Open table heap file
    crate::backend::cache::checkpoint();
    let heap_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    if !heap_path.exists() {
        return Err(RookError::Internal(format!("Heap file not found: {:?}", heap_path)));
    }

    let heap_manager = HeapManager::open(heap_path)
        .map_err(|e| RookError::Internal(format!("Failed to open heap: {}", e)))?;

    // 4. Initialise a collector per column
    let mut collectors: Vec<ColumnStatisticsCollector> = col_names
        .iter()
        .map(|name| ColumnStatisticsCollector::new(table_name.to_string(), name.clone()))
        .collect();

    // 5. Scan all tuples and feed column values
    let mut tuple_count = 0u64;
    for result in heap_manager.scan() {
        let (_page_id, _slot_id, raw_bytes) = result
            .map_err(|e| RookError::Internal(format!("Scan error during ANALYZE: {}", e)))?;

        let values = deserialize_nullable_row(&schema_types, &raw_bytes)
            .map_err(|e| RookError::Internal(format!("Failed to deserialize tuple: {}", e)))?;

        for (idx, collector) in collectors.iter_mut().enumerate() {
            let val = values.get(idx).and_then(|opt| opt.as_ref());
            collector.add(val);
        }

        tuple_count += 1;
    }

    // 6. Finalize statistics
    let final_stats: Vec<_> = collectors.into_iter().map(|c| c.finalize()).collect();
    let col_count = final_stats.len();

    // 7. Persist to sys_statistics
    save_column_statistics(db_name, table_name, &final_stats)
        .map_err(|e| RookError::Internal(format!("Failed to save statistics: {}", e)))?;

    log::info!(
        "[Analyze] Analyzed table '{}.{}': {} tuples, {} columns",
        db_name, table_name, tuple_count, col_count
    );

    Ok(AnalyzeStats {
        tuples_analyzed: tuple_count,
        columns_analyzed: col_count,
    })
}
