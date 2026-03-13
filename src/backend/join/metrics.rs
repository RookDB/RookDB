//! JoinMetrics: runtime performance data for join operations.
use std::time::Instant;

/// Collects runtime performance data per join execution.
pub struct JoinMetrics {
    pub algorithm: String,
    pub execution_ms: u128,
    pub tuples_output: u64,
    start_time: Option<Instant>,
}

impl JoinMetrics {
    /// Start timing a join execution.
    pub fn start(algorithm: &str) -> JoinMetrics {
        JoinMetrics {
            algorithm: algorithm.to_string(),
            execution_ms: 0,
            tuples_output: 0,
            start_time: Some(Instant::now()),
        }
    }

    /// Stop timing and compute elapsed time.
    pub fn stop(&mut self) {
        if let Some(start) = self.start_time.take() {
            self.execution_ms = start.elapsed().as_millis();
        }
    }

    /// Display metrics.
    pub fn display(&self) {
        println!("--- Join Metrics ---");
        println!("Algorithm:     {}", self.algorithm);
        println!("Execution:     {} ms", self.execution_ms);
        println!("Tuples output: {}", self.tuples_output);
        println!("--------------------");
    }

    /// Display as a single row (for benchmark comparison).
    pub fn display_row(&self) {
        println!("{:<20} {:>12} {:>14}",
            self.algorithm,
            self.execution_ms,
            self.tuples_output,
        );
    }
}