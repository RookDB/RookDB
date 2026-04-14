//! TOAST Comparison Benchmark
//!
//! This benchmark compares serialization/deserialization performance 
//! between large-payload (TOAST) and small-payload (non-TOAST) datasets.
//!
//! Test scenarios:
//! - WITH TOAST: Variable-length text data from example2.csv (multiple large fields)
//! - WITHOUT TOAST: Compact data with fixed/small values
//!
//! Methodology:
//! - Uses `std::hint::black_box()` to prevent dead-code elimination
//! - Nanosecond-resolution timing via `Instant::elapsed().as_nanos()`
//! - 50-iteration warmup before measurement
//! - Reports: mean, std dev, median, p95, p99, min, max
//! - Full comparison of TOAST overhead

use std::hint::black_box;
use std::time::Instant;

use storage_manager::backend::catalog::data_type::{DataType, Value};
use storage_manager::backend::storage::tuple_codec::TupleCodec;
use storage_manager::backend::storage::value_codec::ValueCodec;
use storage_manager::backend::storage::toast::ToastManager;

// ═══════════════════════════════════════════════════════════════════
//  Statistical Utilities
// ═══════════════════════════════════════════════════════════════════

/// A single benchmark result with full statistical analysis.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct BenchResult {
    name: String,
    iterations: usize,
    mean_ns: f64,
    stddev_ns: f64,
    median_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    min_ns: u128,
    max_ns: u128,
}

impl BenchResult {
    fn mean_us(&self) -> f64 {
        self.mean_ns / 1_000.0
    }

    fn stddev_us(&self) -> f64 {
        self.stddev_ns / 1_000.0
    }

    fn throughput(&self) -> f64 {
        if self.mean_ns > 0.0 {
            1_000_000_000.0 / self.mean_ns
        } else {
            f64::INFINITY
        }
    }

    fn print_row(&self) {
        println!(
            "  {:<50} {:>9.2} µs  ±{:<8.2} µs  {:>10.0} ops/s",
            self.name,
            self.mean_us(),
            self.stddev_us(),
            self.throughput(),
        );
    }

    fn print_detailed_row(&self) {
        println!(
            "  {:<50}",
            self.name,
        );
        println!(
            "    Mean: {:.2} µs  |  StdDev: {:.2} µs  |  Median: {:.2} µs",
            self.mean_us(),
            self.stddev_us(),
            self.median_ns / 1_000.0,
        );
        println!(
            "    P95:  {:.2} µs  |  P99: {:.2} µs  |  Throughput: {:.0} ops/s",
            self.p95_ns / 1_000.0,
            self.p99_ns / 1_000.0,
            self.throughput(),
        );
    }
}

/// Compute percentile from a SORTED slice of u128 values.
fn percentile(sorted: &[u128], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    let idx = idx.min(sorted.len() - 1);
    sorted[idx] as f64
}

/// Run a benchmark with proper warmup and statistical analysis.
fn benchmark<F>(name: &str, iterations: usize, warmup: usize, mut f: F) -> BenchResult
where
    F: FnMut(),
{
    // Warmup phase — results discarded
    for _ in 0..warmup {
        f();
    }

    // Measurement phase
    let mut times_ns: Vec<u128> = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = Instant::now();
        f();
        let elapsed_ns = start.elapsed().as_nanos();
        times_ns.push(elapsed_ns);
    }

    // Statistical analysis
    let n = times_ns.len() as f64;
    let sum: u128 = times_ns.iter().sum();
    let mean_ns = sum as f64 / n;

    let variance = times_ns
        .iter()
        .map(|&t| {
            let diff = t as f64 - mean_ns;
            diff * diff
        })
        .sum::<f64>()
        / (n - 1.0).max(1.0);
    let stddev_ns = variance.sqrt();

    let min_ns = *times_ns.iter().min().unwrap_or(&0);
    let max_ns = *times_ns.iter().max().unwrap_or(&0);

    // Sort for percentiles
    times_ns.sort_unstable();
    let median_ns = percentile(&times_ns, 50.0);
    let p95_ns = percentile(&times_ns, 95.0);
    let p99_ns = percentile(&times_ns, 99.0);

    BenchResult {
        name: name.to_string(),
        iterations,
        mean_ns,
        stddev_ns,
        median_ns,
        p95_ns,
        p99_ns,
        min_ns,
        max_ns,
    }
}

// ═══════════════════════════════════════════════════════════════════
//  TOAST vs Non-TOAST Benchmarks
// ═══════════════════════════════════════════════════════════════════

/// Create a tuple with large text fields (TOAST scenario)
fn create_toast_tuple() -> Vec<Value> {
    // Large text data similar to output.csv: very long hash strings
    let large_text = "11dbb77a887560da40f4515c3ae6801b3b3e0f69dcc1ea57897b5a64caf4e826918c4ea8df8327b7ca2de37b7924fe2b1a73247d748867049ac3bcf296453809e6c6f53b066f99e6612e3916fcf80162191f0019ce2a99d3b83ddb382bd5091c279097e4a8939e33fc3f620619a54ec89221c2c8583e908f76dd4b0191299a58d46ec895751b7a4d29779c313f7538063f1e170b921a7f2557a6395b7a5e21a6206a309a69f254980c48a5c5ec9bfe42cc11b91b5afb5e269c4fd9ee643b52a378ac9a90fc260cbfd525df5a00b43321f4dacca46e4f44b027ecf778b179456878bfd93892a54050b49ea134425a8f038df5853edbd853e6b857da95739213e5821dbce51dc4832735ad0d1f44c9ba71385865f02f0c7d13be6ac872f96420b3897f99f92b58af6ebeda47505246c622c9746ebc9f401d2794cceb2b7a09f4c3ead6f622528ad24d029853252eeac590ba071b03d1afd8258a4d8aabbb5da4c0486a4d579f02738cf068fae6482df532c248bec9115ca54038ab226661c418674811b66dce94dd138ab1121a6549ac9a00dcde7f89e8c3f379d2bd9c75061b6c27a80b7346db2bb234a7306c3f770b0ff4b6aaf4645fa7e54c11e8984b05f7aa358ac6c079119adee45dd04915d0ea794b554480ce6d54216f895cccbb6e99436f0627a454dde9627e0fea290921dfdc4098936ca29cc14da062e41db1fcede48367806a047dc02e4d952560c5e01cce01".to_string();

    vec![
        Value::Int32(1),
        Value::Boolean(true),
        Value::Text("text_row_1".to_string()),
        Value::Text(large_text.clone()),
        Value::Text(large_text),
    ]
}

/// Create a tuple with small text fields (non-TOAST scenario)
fn create_non_toast_tuple() -> Vec<Value> {
    vec![
        Value::Int32(1),
        Value::Boolean(true),
        Value::Text("row".to_string()),
        Value::Text("abc".to_string()),
        Value::Text("def".to_string()),
    ]
}

/// Schema for TOAST test tuples (5 columns)
fn toast_schema() -> Vec<(String, DataType)> {
    vec![
        ("id".to_string(), DataType::Int32),
        ("data1".to_string(), DataType::Boolean),
        ("data2".to_string(), DataType::Text),
        ("data3".to_string(), DataType::Text),
        ("data4".to_string(), DataType::Text),
    ]
}

/// Encode individual values (TOAST scenario)
fn bench_toast_value_encoding(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║        TOAST Scenario: Large Text Fields (Multi-row dataset)                  ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let large_text = "11dbb77a887560da40f4515c3ae6801b3b3e0f69dcc1ea57897b5a64caf4e826918c4ea8df8327b7ca2de37b7924fe2b1a73247d748867049ac3bcf296453809e6c6f53b066f99e6612e3916fcf80162191f0019ce2a99d3b83ddb382bd5091c279097e4a8939e33fc3f620619a54ec89221c2c8583e908f76dd4b0191299a58d46ec895751b7a4d29779c313f7538063f1e170b921a7f2557a6395b7a5e21a6206a309a69f254980c48a5c5ec9bfe42cc11b91b5afb5e269c4fd9ee643b52a378ac9a90fc260cbfd525df5a00b43321f4dacca46e4f44b027ecf778b179456878bfd93892a54050b49ea134425a8f038df5853edbd853e6b857da95739213e5821dbce51dc4832735ad0d1f44c9ba71385865f02f0c7d13be6ac872f96420b3897f99f92b58af6ebeda47505246c622c9746ebc9f401d2794cceb2b7a09f4c3ead6f622528ad24d029853252eeac590ba071b03d1afd8258a4d8aabbb5da4c0486a4d579f02738cf068fae6482df532c248bec9115ca54038ab226661c418674811b66dce94dd138ab1121a6549ac9a00dcde7f89e8c3f379d2bd9c75061b6c27a80b7346db2bb234a7306c3f770b0ff4b6aaf4645fa7e54c11e8984b05f7aa358ac6c079119adee45dd04915d0ea794b554480ce6d54216f895cccbb6e99436f0627a454dde9627e0fea290921dfdc4098936ca29cc14da062e41db1fcede48367806a047dc02e4d952560c5e01cce01".to_string();

    let value = Value::Text(large_text.clone());
    let encoded = ValueCodec::encode(&value, &DataType::Text).unwrap();
    let size_bytes = encoded.len();

    println!("\n  --- TOAST Value Operations ---");
    println!("    Field Size: {} bytes (exceeds TOAST threshold)", size_bytes);

    // Encoding
    let r = benchmark(
        "TOAST Text Encode (large field)",
        1000,
        50,
        || {
            let temp_val = Value::Text(large_text.clone());
            let enc = ValueCodec::encode(black_box(&temp_val), black_box(&DataType::Text));
            let _ = black_box(enc);
        },
    );
    r.print_row();
    results.push(r);

    // Decoding
    let r = benchmark(
        "TOAST Text Decode (large field)",
        1000,
        50,
        || {
            let dec = ValueCodec::decode(black_box(&encoded), black_box(&DataType::Text));
            let _ = black_box(dec);
        },
    );
    r.print_row();
    results.push(r);
}

/// Encode individual values (non-TOAST scenario)
fn bench_non_toast_value_encoding(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║        Non-TOAST Scenario: Small Text Fields (Compact data)                   ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let small_text = "abc".to_string();
    let value = Value::Text(small_text.clone());
    let encoded = ValueCodec::encode(&value, &DataType::Text).unwrap();
    let size_bytes = encoded.len();

    println!("\n  --- Non-TOAST Value Operations ---");
    println!("    Field Size: {} bytes (below TOAST threshold)", size_bytes);

    // Encoding
    let r = benchmark(
        "Non-TOAST Text Encode (small field)",
        10000,
        100,
        || {
            let temp_val = Value::Text(small_text.clone());
            let enc = ValueCodec::encode(black_box(&temp_val), black_box(&DataType::Text));
            let _ = black_box(enc);
        },
    );
    r.print_row();
    results.push(r);

    // Decoding
    let r = benchmark(
        "Non-TOAST Text Decode (small field)",
        10000,
        100,
        || {
            let dec = ValueCodec::decode(black_box(&encoded), black_box(&DataType::Text));
            let _ = black_box(dec);
        },
    );
    r.print_row();
    results.push(r);
}

/// Benchmark tuple serialization (TOAST scenario)
fn bench_toast_tuple_operations(results: &mut Vec<BenchResult>) {
    println!("\n  --- TOAST Tuple Operations ---");

    let tuple = create_toast_tuple();
    let schema = toast_schema();

    // Tuple encoding
    let r = benchmark(
        "TOAST Tuple Encode (5-field row with large values)",
        500,
        50,
        || {
            let mut toast_mgr = ToastManager::new();
            let t = black_box(&tuple);
            let s = black_box(&schema);
            let enc = TupleCodec::encode_tuple(t, s, &mut toast_mgr);
            let _ = black_box(enc);
        },
    );
    r.print_row();
    results.push(r);

    // Tuple decoding
    let mut toast_mgr = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&tuple, &schema, &mut toast_mgr).unwrap();
    let r = benchmark(
        "TOAST Tuple Decode (5-field row with large values)",
        500,
        50,
        || {
            let enc = black_box(&encoded);
            let s = black_box(&schema);
            let dec = TupleCodec::decode_tuple(enc, s);
            let _ = black_box(dec);
        },
    );
    r.print_row();
    results.push(r);
}

/// Benchmark tuple serialization (non-TOAST scenario)
fn bench_non_toast_tuple_operations(results: &mut Vec<BenchResult>) {
    println!("\n  --- Non-TOAST Tuple Operations ---");

    let tuple = create_non_toast_tuple();
    let schema = toast_schema();

    // Tuple encoding
    let r = benchmark(
        "Non-TOAST Tuple Encode (5-field row with small values)",
        5000,
        100,
        || {
            let mut toast_mgr = ToastManager::new();
            let t = black_box(&tuple);
            let s = black_box(&schema);
            let enc = TupleCodec::encode_tuple(t, s, &mut toast_mgr);
            let _ = black_box(enc);
        },
    );
    r.print_row();
    results.push(r);

    // Tuple decoding
    let mut toast_mgr = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&tuple, &schema, &mut toast_mgr).unwrap();
    let r = benchmark(
        "Non-TOAST Tuple Decode (5-field row with small values)",
        5000,
        100,
        || {
            let enc = black_box(&encoded);
            let s = black_box(&schema);
            let dec = TupleCodec::decode_tuple(enc, s);
            let _ = black_box(dec);
        },
    );
    r.print_row();
    results.push(r);
}

/// Compare results and calculate overhead
fn print_comparison_summary(results: &[BenchResult]) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                        TOAST Overhead Analysis                                ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝\n");

    // Find comparable benchmarks
    for (toast_name, non_toast_name) in vec![
        ("TOAST Text Encode (large field)", "Non-TOAST Text Encode (small field)"),
        ("TOAST Text Decode (large field)", "Non-TOAST Text Decode (small field)"),
        ("TOAST Tuple Encode (5-field row with large values)", "Non-TOAST Tuple Encode (5-field row with small values)"),
        ("TOAST Tuple Decode (5-field row with large values)", "Non-TOAST Tuple Decode (5-field row with small values)"),
    ] {
        let toast = results.iter().find(|r| r.name == toast_name);
        let non_toast = results.iter().find(|r| r.name == non_toast_name);

        if let (Some(t), Some(nt)) = (toast, non_toast) {
            let overhead_pct = ((t.mean_ns - nt.mean_ns) / nt.mean_ns) * 100.0;
            let ratio = t.mean_ns / nt.mean_ns;

            println!("  {}", toast_name);
            println!("    Non-TOAST baseline: {:.2} µs", nt.mean_us());
            println!("    TOAST scenario:     {:.2} µs", t.mean_us());
            println!("    Overhead:           {:.1}% slower  ({:.2}x slower)", overhead_pct, ratio);
            println!();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Main Benchmark Runner
// ═══════════════════════════════════════════════════════════════════

fn main() {
    let mut results = Vec::new();

    println!("\n");
    println!("╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                  RookDB TOAST Performance Comparison Benchmark                ║");
    println!("║  Comparing serialization/deserialization with large vs. small data payloads  ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    // Test TOAST scenario (large payloads)
    bench_toast_value_encoding(&mut results);
    bench_toast_tuple_operations(&mut results);

    // Test non-TOAST scenario (small payloads)
    bench_non_toast_value_encoding(&mut results);
    bench_non_toast_tuple_operations(&mut results);

    // Print comparison summary
    print_comparison_summary(&results);

    // Summary statistics
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                            Full Results Summary                              ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝\n");

    for result in &results {
        result.print_detailed_row();
        println!();
    }

    println!("\n✓ Benchmark completed successfully");
    println!("  Total benchmarks run: {}", results.len());
}
