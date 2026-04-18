//! Benchmarking utilities for projection operator
//! 
//! Provides:
//! - Performance measurement utilities
//! - Comparison runners (ablation study)
//! - Report generation

use std::time::Instant;
use std::collections::BTreeMap;

use crate::executor::projection_enhanced::ProjectionEngine;
use crate::executor::projection::ProjectionInput;
use std::io;

// ─── Benchmark Result ────────────────────────────────────────────────────────

/// Single benchmark execution result
#[derive(Clone, Debug)]
pub struct BenchmarkRun {
    pub iteration: usize,
    pub elapsed_ms: u128,
    pub rows_processed: u64,
    pub rows_output: u64,
    pub throughput: f64,
}

/// Complete benchmark report
#[derive(Debug)]
pub struct BenchmarkReport {
    pub runs: Vec<BenchmarkRun>,
    pub min_ms: u128,
    pub max_ms: u128,
    pub avg_ms: f64,
    pub median_ms: f64,
    pub stddev_ms: f64,
}

impl BenchmarkReport {
    pub fn from_runs(runs: Vec<BenchmarkRun>) -> Self {
        let mut times: Vec<u128> = runs.iter().map(|r| r.elapsed_ms).collect();
        times.sort();
        
        let min_ms = times.first().copied().unwrap_or(0);
        let max_ms = times.last().copied().unwrap_or(0);
        let avg_ms = times.iter().sum::<u128>() as f64 / times.len() as f64;
        
        let median_ms = if times.len() % 2 == 0 {
            let mid = times.len() / 2;
            ((times[mid - 1] + times[mid]) / 2) as f64
        } else {
            times[times.len() / 2] as f64
        };

        let variance = times.iter()
            .map(|&t| {
                let diff = t as f64 - avg_ms;
                diff * diff
            })
            .sum::<f64>() / times.len() as f64;
        let stddev_ms = variance.sqrt();

        Self {
            runs,
            min_ms,
            max_ms,
            avg_ms,
            median_ms,
            stddev_ms,
        }
    }

    pub fn print(&self) {
        println!("\n=== Benchmark Report ===");
        println!("Iterations: {}", self.runs.len());
        println!("Time (ms):");
        println!("  Min:    {:.2}", self.min_ms);
        println!("  Max:    {:.2}", self.max_ms);
        println!("  Avg:    {:.2}", self.avg_ms);
        println!("  Median: {:.2}", self.median_ms);
        println!("  StdDev: {:.2}", self.stddev_ms);
        
        if let Some(first) = self.runs.first() {
            println!("Throughput: {:.0} rows/sec", first.throughput);
        }
    }
}

// ─── Projection Benchmark ────────────────────────────────────────────────────

/// Benchmark configuration
pub struct BenchmarkConfig {
    pub iterations: usize,
    pub warmup_iterations: usize,
    pub name: String,
}

impl BenchmarkConfig {
    pub fn new(name: &str, iterations: usize) -> Self {
        Self {
            iterations,
            warmup_iterations: 2,
            name: name.to_string(),
        }
    }
}

pub struct ProjectionBenchmark;

impl ProjectionBenchmark {
    /// Run single benchmark
    pub fn run(
        config: &BenchmarkConfig,
        input: ProjectionInput,
    ) -> io::Result<BenchmarkReport> {
        // Warmup
        for _ in 0..config.warmup_iterations {
            let _ = ProjectionEngine::execute_simple(ProjectionInput {
                catalog: input.catalog,
                db_name: input.db_name,
                table_name: input.table_name,
                items: input.items.clone(),
                predicate: input.predicate.clone(),
                distinct: input.distinct,
                cte_tables: input.cte_tables.clone(),
            });
        }

        // Actual runs
        let mut runs = Vec::new();
        for iteration in 0..config.iterations {
            let start = Instant::now();
            let result = ProjectionEngine::execute_simple(ProjectionInput {
                catalog: input.catalog,
                db_name: input.db_name,
                table_name: input.table_name,
                items: input.items.clone(),
                predicate: input.predicate.clone(),
                distinct: input.distinct,
                cte_tables: input.cte_tables.clone(),
            })?;
            let elapsed = start.elapsed().as_millis();

            let throughput = if elapsed > 0 {
                (result.metrics.rows_processed as f64 * 1000.0) / (elapsed as f64)
            } else {
                0.0
            };

            runs.push(BenchmarkRun {
                iteration,
                elapsed_ms: elapsed,
                rows_processed: result.metrics.rows_processed,
                rows_output: result.metrics.rows_output,
                throughput,
            });
        }

        Ok(BenchmarkReport::from_runs(runs))
    }

    /// Compare multiple configurations (ablation study)
    pub fn compare_variants(
        variants: Vec<(&str, ProjectionInput)>,
    ) -> io::Result<ComparisonReport> {
        let mut results = BTreeMap::new();

        for (name, input) in variants {
            let config = BenchmarkConfig::new(name, 5);
            let report = Self::run(&config, input)?;
            results.insert(name.to_string(), report);
        }

        Ok(ComparisonReport {
            variants: results,
        })
    }
}

// ─── Comparison Report ───────────────────────────────────────────────────────

pub struct ComparisonReport {
    pub variants: BTreeMap<String, BenchmarkReport>,
}

impl ComparisonReport {
    pub fn print(&self) {
        println!("\n=== Comparison Report ===\n");
        
        // Print individual reports
        for (name, report) in &self.variants {
            println!("Variant: {}", name);
            println!("  Avg Time: {:.2} ms", report.avg_ms);
            println!("  Min Time: {:.2} ms", report.min_ms);
            println!("  Max Time: {:.2} ms", report.max_ms);
            println!("  StdDev:   {:.2} ms", report.stddev_ms);
            println!();
        }

        // Print comparison matrix
        if self.variants.len() > 1 {
            println!("--- Speedup vs First Variant ---");
            let variant_vec: Vec<_> = self.variants.iter().collect();
            if let Some((name1, report1)) = variant_vec.first() {
                let baseline = report1.avg_ms;
                for (name2, report2) in &variant_vec[1..] {
                    let speedup = baseline / report2.avg_ms;
                    if speedup > 1.0 {
                        println!("{} -> {}: {:.2}x faster", name1, name2, speedup);
                    } else {
                        println!("{} -> {}: {:.2}x slower", name1, name2, 1.0 / speedup);
                    }
                }
            }
        }
    }
}

// ─── Layer-by-Layer Execution Profiler ──────────────────────────────────────

/// Profile execution layer by layer
#[derive(Debug, Clone)]
pub struct LayerMetrics {
    pub layer_name: String,
    pub elapsed_ms: u128,
    pub rows_in: u64,
    pub rows_out: u64,
}

pub struct LayerProfiler {
    layers: Vec<LayerMetrics>,
}

impl LayerProfiler {
    pub fn new() -> Self {
        Self { layers: vec![] }
    }

    pub fn record(&mut self, name: &str, elapsed_ms: u128, rows_in: u64, rows_out: u64) {
        self.layers.push(LayerMetrics {
            layer_name: name.to_string(),
            elapsed_ms,
            rows_in,
            rows_out,
        });
    }

    pub fn print(&self) {
        println!("\n=== Layer-by-Layer Execution Profile ===\n");
        println!("Layer                    | Time (ms) | In      | Out     | Reduction %");
        println!("{}", "-".repeat(80));

        let mut total_time = 0u128;
        for layer in &self.layers {
            let reduction = if layer.rows_in > 0 {
                ((layer.rows_in - layer.rows_out) as f64 / layer.rows_in as f64) * 100.0
            } else {
                0.0
            };
            println!(
                "{:<24} | {:<9} | {:<7} | {:<7} | {:.1}%",
                layer.layer_name,
                layer.elapsed_ms,
                layer.rows_in,
                layer.rows_out,
                reduction
            );
            total_time += layer.elapsed_ms;
        }
        println!("{}", "-".repeat(80));
        println!("Total: {} ms", total_time);
    }
}

// ─── Scalability Test ────────────────────────────────────────────────────────

/// Scalability benchmark (test with different data sizes)
pub struct ScalabilityTest {
    pub sizes: Vec<usize>,
    pub results: Vec<ScalabilityResult>,
}

#[derive(Debug, Clone)]
pub struct ScalabilityResult {
    pub rows_count: usize,
    pub elapsed_ms: u128,
    pub throughput_rows_per_sec: f64,
}

impl ScalabilityTest {
    pub fn print(&self) {
        println!("\n=== Scalability Analysis ===\n");
        println!("Rows     | Time (ms) | Throughput (rows/sec)");
        println!("{}", "-".repeat(50));

        for result in &self.results {
            println!(
                "{:<8} | {:<9} | {:.0}",
                result.rows_count,
                result.elapsed_ms,
                result.throughput_rows_per_sec
            );
        }
    }
}
