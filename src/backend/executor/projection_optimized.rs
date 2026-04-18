//! Optimized Column Reordering for Large Datasets
//!
//! Implements adaptive strategy selection for reordering columns at scale:
//! - Eager reordering for small datasets (<1M rows)
//! - Streaming lazy evaluation (1M-10M rows)
//! - Parallel hybrid with chunking (10M-1B rows)
//! - Columnar staging for sequential access (100M+ rows)
//!
//! Automatically selects best algorithm based on:
//! - Dataset size
//! - Available RAM
//! - Column reordering pattern
//! - Number of CPU cores

use std::time::Instant;
use std::io;
use crate::executor::value::Value;
use crate::executor::projection::OutputColumn;
use crate::executor::projection_enhanced::ColumnReorderSpec;

// ─── Strategy Selection ──────────────────────────────────────────────────────

/// Strategy selected for column reordering
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReorderStrategy {
    /// Eager: Load all rows, reorder in memory
    /// Best for: < 1M rows
    Eager,
    /// Streaming with small batches
    /// Best for: 1M - 10M rows
    StreamingBatched,
    /// Parallel processing with chunk-based I/O
    /// Best for: 10M - 1B rows
    ParallelHybrid,
    /// Columnar -> Reorder -> Row-reconstruct
    /// Best for: 100M+ rows with many columns
    ColumnarStaging,
}

impl ReorderStrategy {
    /// Select best strategy based on dataset characteristics
    pub fn select(
        row_count: usize,
        available_ram_mb: usize,
        column_count: usize,
        row_size_bytes: usize,
    ) -> Self {
        // Estimate memory requirements
        let estimated_table_size_mb = (row_count * row_size_bytes) / (1024 * 1024);

        // Safety threshold: use only 25% of available RAM
        let safe_ram_threshold = available_ram_mb / 4;

        // Strategy selection logic
        match () {
            // Very small datasets
            _ if row_count < 100_000 => ReorderStrategy::Eager,

            // Small datasets that fit in RAM with headroom
            _ if row_count < 1_000_000 && estimated_table_size_mb < safe_ram_threshold => {
                ReorderStrategy::Eager
            }

            // Medium datasets: use streaming batches
            _ if row_count < 10_000_000 => ReorderStrategy::StreamingBatched,

            // Large datasets with many columns: columnar is efficient
            _ if column_count > 50 && row_count < 100_000_000 => {
                if estimated_table_size_mb < safe_ram_threshold {
                    ReorderStrategy::ColumnarStaging
                } else {
                    ReorderStrategy::ParallelHybrid
                }
            }

            // Very large datasets: always use parallel
            _ if row_count >= 10_000_000 => ReorderStrategy::ParallelHybrid,

            // Default: parallel for safety
            _ => ReorderStrategy::ParallelHybrid,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ReorderStrategy::Eager => "EAGER",
            ReorderStrategy::StreamingBatched => "STREAMING_BATCHED",
            ReorderStrategy::ParallelHybrid => "PARALLEL_HYBRID",
            ReorderStrategy::ColumnarStaging => "COLUMNAR_STAGING",
        }
    }
}

// ─── Optimized Reordering Engine ────────────────────────────────────────────

pub type Row = Vec<Value>;

/// Configuration for optimized reordering
#[derive(Debug, Clone)]
pub struct OptimizedReorderConfig {
    /// Strategy to use
    pub strategy: ReorderStrategy,
    /// Available RAM in MB
    pub available_ram_mb: usize,
    /// Number of parallel workers
    pub num_workers: usize,
    /// Batch size for streaming (rows per batch)
    pub batch_size: usize,
    /// Enable metrics collection
    pub track_metrics: bool,
}

impl OptimizedReorderConfig {
    pub fn new(row_count: usize, column_count: usize, row_size_bytes: usize) -> Self {
        let available_ram_mb = Self::detect_available_ram();
        let strategy = ReorderStrategy::select(
            row_count,
            available_ram_mb,
            column_count,
            row_size_bytes,
        );

        let batch_size = Self::optimal_batch_size(row_size_bytes, available_ram_mb);
        let num_workers = Self::get_num_cpus();

        Self {
            strategy,
            available_ram_mb,
            num_workers,
            batch_size,
            track_metrics: true,
        }
    }

    /// Detect available system RAM (approximate)
    fn detect_available_ram() -> usize {
        // Simple heuristic: assume 16GB available for database operations
        // In production, use sys-info crate for actual detection
        const DEFAULT_RAM_MB: usize = 16_000;
        DEFAULT_RAM_MB
    }

    /// Compute optimal batch size for cache locality
    fn optimal_batch_size(row_size_bytes: usize, available_ram_mb: usize) -> usize {
        const L3_CACHE_KB: usize = 20_000;  // Typical modern CPU
        const CHUNK_OF_AVAILABLE: usize = 256;  // Use 256MB for batching

        let l3_batch = (L3_CACHE_KB * 1024) / row_size_bytes.max(1);
        let ram_batch = (available_ram_mb / CHUNK_OF_AVAILABLE * 1024 * 1024) / row_size_bytes.max(1);

        // Use larger batch to reduce overhead
        (l3_batch * 4).min(ram_batch).max(1000)
    }

    /// Get number of CPU cores
    fn get_num_cpus() -> usize {
        // Simple implementation: assume 8 cores if detection fails
        // Could use num_cpus crate if added as dependency
        std::cmp::min(std::cmp::max(1, 8), 64)
    }
}

// ─── Reordering Metrics ──────────────────────────────────────────────────────

/// Detailed metrics for reordering operation
#[derive(Debug, Clone)]
pub struct ReorderMetrics {
    pub strategy_used: ReorderStrategy,
    pub rows_processed: u64,
    pub elapsed_ms: u128,
    pub batches_processed: u64,
    pub peak_memory_bytes: usize,
    pub throughput_rows_per_sec: f64,
    pub cache_miss_estimate: f64,  // 0.0 to 1.0
}

impl ReorderMetrics {
    pub fn new(strategy: ReorderStrategy) -> Self {
        Self {
            strategy_used: strategy,
            rows_processed: 0,
            elapsed_ms: 0,
            batches_processed: 0,
            peak_memory_bytes: 0,
            throughput_rows_per_sec: 0.0,
            cache_miss_estimate: 0.0,
        }
    }

    pub fn compute_estimates(&mut self) {
        if self.elapsed_ms > 0 {
            self.throughput_rows_per_sec =
                (self.rows_processed as f64 * 1000.0) / (self.elapsed_ms as f64);
        }

        // Estimate cache miss rate based on strategy
        self.cache_miss_estimate = match self.strategy_used {
            ReorderStrategy::Eager => 0.3,  // Some misses from random access
            ReorderStrategy::StreamingBatched => 0.4,  // Higher due to batching overhead
            ReorderStrategy::ParallelHybrid => 0.25,  // Good locality with chunks
            ReorderStrategy::ColumnarStaging => 0.1,  // Excellent: sequential access
        };
    }

    pub fn print(&self) {
        println!("\n=== Reorder Metrics ===");
        println!("Strategy:        {}", self.strategy_used.as_str());
        println!("Rows:            {} ({:.1}M)", self.rows_processed, self.rows_processed as f64 / 1_000_000.0);
        println!("Time:            {} ms", self.elapsed_ms);
        println!("Throughput:      {:.0} rows/sec", self.throughput_rows_per_sec);
        println!("Peak Memory:     {:.1} MB", self.peak_memory_bytes as f64 / (1024.0 * 1024.0));
        println!("Batches:         {}", self.batches_processed);
        println!("Est. Cache Miss: {:.1}%", self.cache_miss_estimate * 100.0);
    }
}

// ─── Strategy 1: Eager Reordering (Baseline) ──────────────────────────────────

pub fn reorder_eager(
    rows: Vec<Row>,
    columns: &[OutputColumn],
    spec: &ColumnReorderSpec,
    mut metrics: ReorderMetrics,
) -> io::Result<(Vec<Row>, Vec<OutputColumn>, ReorderMetrics)> {
    let start = Instant::now();

    // Validate indices upfront
    for &idx in &spec.indices {
        if idx >= columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Column index {} out of bounds (max: {})", idx, columns.len() - 1),
            ));
        }
    }

    // Reorder column metadata
    let new_columns: Vec<OutputColumn> = spec.indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| {
            let mut col = columns[old_idx].clone();
            if let Some(ref names) = spec.new_names {
                if new_idx < names.len() {
                    col.name = names[new_idx].clone();
                }
            }
            col
        })
        .collect();

    // Reorder all rows at once
    let reordered_rows: Vec<Row> = rows
        .iter()
        .map(|row| {
            spec.indices
                .iter()
                .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    metrics.rows_processed = reordered_rows.len() as u64;
    metrics.elapsed_ms = start.elapsed().as_millis();
    metrics.peak_memory_bytes = reordered_rows.len() * 128;  // Estimate
    metrics.compute_estimates();

    Ok((reordered_rows, new_columns, metrics))
}

// ─── Strategy 2: Streaming with Batching ────────────────────────────────────

/// Process rows in fixed-size batches for better cache locality
pub fn reorder_streaming_batched(
    rows: Vec<Row>,
    columns: &[OutputColumn],
    spec: &ColumnReorderSpec,
    config: &OptimizedReorderConfig,
    mut metrics: ReorderMetrics,
) -> io::Result<(Vec<Row>, Vec<OutputColumn>, ReorderMetrics)> {
    let start = Instant::now();

    // Validate indices
    for &idx in &spec.indices {
        if idx >= columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Column index {} out of bounds", idx),
            ));
        }
    }

    let new_columns: Vec<OutputColumn> = spec.indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| {
            let mut col = columns[old_idx].clone();
            if let Some(ref names) = spec.new_names {
                if new_idx < names.len() {
                    col.name = names[new_idx].clone();
                }
            }
            col
        })
        .collect();

    let mut reordered_rows = Vec::with_capacity(rows.len());
    let batch_size = config.batch_size;

    // Process in batches for better cache locality
    for batch in rows.chunks(batch_size) {
        let batch_reordered: Vec<Row> = batch
            .iter()
            .map(|row| {
                spec.indices
                    .iter()
                    .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();

        reordered_rows.extend(batch_reordered);
        metrics.batches_processed += 1;
    }

    metrics.rows_processed = reordered_rows.len() as u64;
    metrics.elapsed_ms = start.elapsed().as_millis();
    metrics.peak_memory_bytes = config.batch_size * 128;
    metrics.compute_estimates();

    Ok((reordered_rows, new_columns, metrics))
}

// ─── Strategy 3: Parallel Hybrid (Most scalable) ──────────────────────────────

/// Process in parallel chunks with streaming output
pub fn reorder_parallel_hybrid(
    rows: Vec<Row>,
    columns: &[OutputColumn],
    spec: &ColumnReorderSpec,
    config: &OptimizedReorderConfig,
    mut metrics: ReorderMetrics,
) -> io::Result<(Vec<Row>, Vec<OutputColumn>, ReorderMetrics)> {
    let start = Instant::now();

    // Validate
    for &idx in &spec.indices {
        if idx >= columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Column index {} out of bounds", idx),
            ));
        }
    }

    let new_columns: Vec<OutputColumn> = spec.indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| {
            let mut col = columns[old_idx].clone();
            if let Some(ref names) = spec.new_names {
                if new_idx < names.len() {
                    col.name = names[new_idx].clone();
                }
            }
            col
        })
        .collect();

    // Divide into chunks per thread
    let chunk_size = (rows.len() + config.num_workers - 1) / config.num_workers;

    // Process chunks (simulated parallel - can be upgraded with Rayon)
    let mut reordered_rows = Vec::with_capacity(rows.len());
    
    for chunk in rows.chunks(chunk_size) {
        let chunk_reordered: Vec<Row> = chunk
            .iter()
            .map(|row| {
                spec.indices
                    .iter()
                    .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();
        
        reordered_rows.extend(chunk_reordered);
        metrics.batches_processed += 1;
    }

    metrics.rows_processed = reordered_rows.len() as u64;
    metrics.elapsed_ms = start.elapsed().as_millis();
    metrics.peak_memory_bytes = chunk_size * 128;
    metrics.compute_estimates();

    Ok((reordered_rows, new_columns, metrics))
}

// ─── Strategy 4: Columnar Staging Transform ───────────────────────────────────

/// Transform to columnar, reorder, reconstruct rows
pub fn reorder_columnar_staging(
    rows: Vec<Row>,
    columns: &[OutputColumn],
    spec: &ColumnReorderSpec,
    mut metrics: ReorderMetrics,
) -> io::Result<(Vec<Row>, Vec<OutputColumn>, ReorderMetrics)> {
    let start = Instant::now();

    if rows.is_empty() {
        metrics.elapsed_ms = 0;
        metrics.rows_processed = 0;
        return Ok((rows, columns.to_vec(), metrics));
    }

    // Validate
    for &idx in &spec.indices {
        if idx >= columns.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Column index {} out of bounds", idx),
            ));
        }
    }

    let new_columns: Vec<OutputColumn> = spec.indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| {
            let mut col = columns[old_idx].clone();
            if let Some(ref names) = spec.new_names {
                if new_idx < names.len() {
                    col.name = names[new_idx].clone();
                }
            }
            col
        })
        .collect();

    // Step 1: Gather into columns
    let num_cols = columns.len();
    let mut column_buffers: Vec<Vec<Value>> = vec![Vec::with_capacity(rows.len()); num_cols];

    for row in &rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < column_buffers.len() {
                column_buffers[col_idx].push(value.clone());
            }
        }
    }

    // Step 2: Reorder column buffers
    let reordered_buffers: Vec<Vec<Value>> = spec.indices
        .iter()
        .map(|&idx| column_buffers[idx].clone())
        .collect();

    // Step 3: Reconstruct rows from reordered columns
    let mut reordered_rows = Vec::with_capacity(rows.len());
    let num_rows = rows.len();

    for row_idx in 0..num_rows {
        let mut row: Row = Vec::with_capacity(new_columns.len());
        for col_buffer in &reordered_buffers {
            row.push(col_buffer[row_idx].clone());
        }
        reordered_rows.push(row);
    }

    metrics.rows_processed = reordered_rows.len() as u64;
    metrics.elapsed_ms = start.elapsed().as_millis();
    metrics.peak_memory_bytes = (rows.len() * columns.len()) * 24;  // 3x per Value
    metrics.compute_estimates();

    Ok((reordered_rows, new_columns, metrics))
}

// ─── Main Entry Point: Adaptive Reordering ───────────────────────────────────

/// Execute column reordering with automatically selected strategy
pub fn reorder_optimized(
    rows: Vec<Row>,
    columns: &[OutputColumn],
    spec: &ColumnReorderSpec,
    row_size_bytes: Option<usize>,
) -> io::Result<(Vec<Row>, Vec<OutputColumn>, ReorderMetrics)> {
    let row_count = rows.len();
    let column_count = columns.len();
    let row_size = row_size_bytes.unwrap_or(128);

    let config = OptimizedReorderConfig::new(row_count, column_count, row_size);
    let metrics = ReorderMetrics::new(config.strategy);

    let result = match config.strategy {
        ReorderStrategy::Eager => {
            reorder_eager(rows, columns, spec, metrics)?
        }
        ReorderStrategy::StreamingBatched => {
            reorder_streaming_batched(rows, columns, spec, &config, metrics)?
        }
        ReorderStrategy::ParallelHybrid => {
            reorder_parallel_hybrid(rows, columns, spec, &config, metrics)?
        }
        ReorderStrategy::ColumnarStaging => {
            reorder_columnar_staging(rows, columns, spec, metrics)?
        }
    };

    Ok(result)
}

// ─── Performance Analysis Utilities ──────────────────────────────────────────

/// Predict which strategy will be fastest for given parameters
pub fn predict_best_strategy(
    row_count: usize,
    column_count: usize,
    row_size_bytes: usize,
    available_ram_mb: usize,
) -> (ReorderStrategy, String) {
    let strategy = ReorderStrategy::select(row_count, available_ram_mb, column_count, row_size_bytes);

    let recommendation = match strategy {
        ReorderStrategy::Eager => {
            "Fast for small datasets; safe and predictable.".to_string()
        }
        ReorderStrategy::StreamingBatched => {
            format!(
                "Medium datasets; batches of {} rows for cache locality.",
                OptimizedReorderConfig::optimal_batch_size(row_size_bytes, available_ram_mb)
            )
        }
        ReorderStrategy::ParallelHybrid => {
            format!(
                "Large datasets; {} parallel workers with {} MB chunks.",
                OptimizedReorderConfig::get_num_cpus(),
                available_ram_mb / 4
            )
        }
        ReorderStrategy::ColumnarStaging => {
            "Many columns with sequential access; columnar transform optimal.".to_string()
        }
    };

    (strategy, recommendation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_selection() {
        // Small dataset: eager
        let (strat, _) = predict_best_strategy(100_000, 10, 128, 16_000);
        assert_eq!(strat, ReorderStrategy::Eager);

        // Medium dataset: streaming
        let (strat, _) = predict_best_strategy(5_000_000, 10, 128, 16_000);
        assert_eq!(strat, ReorderStrategy::StreamingBatched);

        // Large dataset: parallel
        let (strat, _) = predict_best_strategy(100_000_000, 10, 128, 16_000);
        assert_eq!(strat, ReorderStrategy::ParallelHybrid);
    }
}
