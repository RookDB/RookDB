//! TOAST Comparison Benchmark
//!
//! Benchmarks tuple encode/decode for two representative dataset profiles:
//!
//! **Small BLOB (example2.csv style)**
//!   - data3: BLOB of ~16 bytes (tiny hex digests like "11dbb77a8")
//!   - data4: ARRAY<BLOB> of 4 × 16-byte BLOBs
//!   - All fields fully inline — TOAST threshold (8,192 bytes) never reached
//!
//! **Large BLOB (output.csv style)**
//!   - data3: BLOB of 12,000 bytes (hex hash, ~75 KB in actual dataset)
//!   - data4: ARRAY<BLOB> of 4 × 12,000-byte BLOBs (total 48 KB)
//!   - Both data3 and data4 exceed threshold → TOAST activation on each field
//!
//! Schema: (id INT32, data1 BOOL, data2 TEXT, data3 BLOB, data4 ARRAY<BLOB>)
//!
//! Methodology:
//!   - black_box() to prevent dead-code elimination
//!   - ns-resolution timing, 50-iter warmup
//!   - Mean, StdDev, p50, p95, p99 reported

use std::hint::black_box;
use std::time::Instant;

use storage_manager::backend::catalog::data_type::{DataType, Value};
use storage_manager::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};
use storage_manager::backend::storage::tuple_codec::TupleCodec;

// ═══════════════════════════════════════════════════════════════════
//  Statistical Utilities (same methodology as blob_array_bench)
// ═══════════════════════════════════════════════════════════════════

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
    fn mean_us(&self) -> f64 { self.mean_ns / 1_000.0 }
    fn stddev_us(&self) -> f64 { self.stddev_ns / 1_000.0 }
    fn throughput(&self) -> f64 {
        if self.mean_ns > 0.0 { 1_000_000_000.0 / self.mean_ns } else { f64::INFINITY }
    }
    fn print_row(&self) {
        println!(
            "  {:<55} {:>9.2} µs  ±{:<8.2} µs  {:>10.2} ops/s  (p50={:.2} p95={:.2} p99={:.2} µs)",
            self.name,
            self.mean_us(),
            self.stddev_us(),
            self.throughput(),
            self.median_ns / 1_000.0,
            self.p95_ns / 1_000.0,
            self.p99_ns / 1_000.0,
        );
    }
}

fn percentile(sorted: &[u128], pct: f64) -> f64 {
    if sorted.is_empty() { return 0.0; }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)] as f64
}

fn benchmark<F>(name: &str, iterations: usize, warmup: usize, mut f: F) -> BenchResult
where F: FnMut(),
{
    for _ in 0..warmup { f(); }

    let mut times_ns: Vec<u128> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        times_ns.push(start.elapsed().as_nanos());
    }

    let n = times_ns.len() as f64;
    let sum: u128 = times_ns.iter().sum();
    let mean_ns = sum as f64 / n;
    let variance = times_ns.iter()
        .map(|&t| { let d = t as f64 - mean_ns; d * d })
        .sum::<f64>() / (n - 1.0).max(1.0);
    let stddev_ns = variance.sqrt();
    let min_ns = *times_ns.iter().min().unwrap_or(&0);
    let max_ns = *times_ns.iter().max().unwrap_or(&0);
    times_ns.sort_unstable();
    let median_ns = percentile(&times_ns, 50.0);
    let p95_ns = percentile(&times_ns, 95.0);
    let p99_ns = percentile(&times_ns, 99.0);

    BenchResult { name: name.to_string(), iterations, mean_ns, stddev_ns,
                  median_ns, p95_ns, p99_ns, min_ns, max_ns }
}

// ═══════════════════════════════════════════════════════════════════
//  Schema & Data Constructors
// ═══════════════════════════════════════════════════════════════════

/// Shared schema: (INT32, BOOL, TEXT, BLOB, ARRAY<BLOB>)
fn schema() -> Vec<(String, DataType)> {
    vec![
        ("id".to_string(),    DataType::Int32),
        ("data1".to_string(), DataType::Boolean),
        ("data2".to_string(), DataType::Text),
        ("data3".to_string(), DataType::Blob),
        ("data4".to_string(), DataType::Array { element_type: Box::new(DataType::Blob) }),
    ]
}

/// Small BLOB tuple — representative of example2.csv (tiny hex digests).
/// data3 = 16-byte BLOB,  data4 = ARRAY of 4 × 16-byte BLOBs.
/// Total BLOB bytes: 80 — well below TOAST_THRESHOLD (8,192 bytes).
fn small_tuple() -> Vec<Value> {
    let blob = vec![0xABu8; 16];
    vec![
        Value::Int32(1),
        Value::Boolean(true),
        Value::Text("text_row_1".to_string()),
        Value::Blob(blob.clone()),
        Value::Array(vec![
            Value::Blob(blob.clone()),
            Value::Blob(blob.clone()),
            Value::Blob(blob.clone()),
            Value::Blob(blob),
        ]),
    ]
}

/// Large BLOB tuple — representative of output.csv (75 KB hex hashes).
/// data3 = 12,000-byte BLOB,  data4 = ARRAY of 4 × 12,000-byte BLOBs.
/// Both fields individually exceed TOAST_THRESHOLD → TOAST activated per field.
fn large_tuple() -> Vec<Value> {
    let blob = vec![0xABu8; 12_000]; // > 8,192 TOAST threshold
    vec![
        Value::Int32(1),
        Value::Boolean(true),
        Value::Text("text_row_1".to_string()),
        Value::Blob(blob.clone()),
        Value::Array(vec![
            Value::Blob(blob.clone()),
            Value::Blob(blob.clone()),
            Value::Blob(blob.clone()),
            Value::Blob(blob),
        ]),
    ]
}

// ═══════════════════════════════════════════════════════════════════
//  Benchmark Sections
// ═══════════════════════════════════════════════════════════════════

fn bench_small_blob(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  Small BLOB (example2.csv style) — Inline, No TOAST                           ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  data3: BLOB(16 B)  |  data4: ARRAY<BLOB>[4×16 B]  |  Total BLOBs: 80 B      ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let tuple  = small_tuple();
    let schema = schema();

    // Encode
    let r = benchmark("Small Encode (INT+BOOL+TEXT+BLOB(16B)+ARR<BLOB>[4×16B])", 5000, 100, || {
        let mut mgr = ToastManager::new();
        let enc = TupleCodec::encode_tuple(black_box(&tuple), black_box(&schema), &mut mgr);
        let _ = black_box(enc);
    });
    r.print_row();
    results.push(r);

    // Decode (inline — no TOAST manager needed)
    let mut mgr = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&tuple, &schema, &mut mgr).unwrap();
    let r = benchmark("Small Decode (INT+BOOL+TEXT+BLOB(16B)+ARR<BLOB>[4×16B])", 5000, 100, || {
        let dec = TupleCodec::decode_tuple(black_box(&encoded), black_box(&schema));
        let _ = black_box(dec);
    });
    r.print_row();
    results.push(r);
}

fn bench_large_blob(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  Large BLOB (output.csv style) — TOAST Activated on data3 & data4             ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  data3: BLOB(12KB) | data4: ARRAY<BLOB>[4×12KB]  | Each field → TOAST         ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let tuple  = large_tuple();
    let schema = schema();

    // Encode (both data3 and data4 trigger TOAST)
    let r = benchmark("Large Encode (INT+BOOL+TEXT+BLOB(12KB)+ARR<BLOB>[4×12KB])", 100, 10, || {
        let mut mgr = ToastManager::new();
        let enc = TupleCodec::encode_tuple(black_box(&tuple), black_box(&schema), &mut mgr);
        let _ = black_box(enc);
    });
    r.print_row();
    results.push(r);

    // Decode (TOAST reassembly for both fields)
    let mut mgr = ToastManager::new();
    let encoded = TupleCodec::encode_tuple(&tuple, &schema, &mut mgr).unwrap();
    let r = benchmark("Large Decode (INT+BOOL+TEXT+BLOB(12KB)+ARR<BLOB>[4×12KB])", 100, 10, || {
        let dec = TupleCodec::decode_tuple_with_toast(
            black_box(&encoded), black_box(&schema), black_box(&mgr));
        let _ = black_box(dec);
    });
    r.print_row();
    results.push(r);
}

// ═══════════════════════════════════════════════════════════════════
//  TOAST Overhead Summary
// ═══════════════════════════════════════════════════════════════════

fn print_overhead_summary(results: &[BenchResult]) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    TOAST Overhead Analysis (CSV Dataset Comparison)           ║");
    println!("╠══════════════════════╦════════════╦════════════╦══════════╦═══════════════════╣");
    println!("║ Operation            ║ Small (µs) ║ Large (µs) ║ Overhead ║ Multiplier        ║");
    println!("╠══════════════════════╬════════════╬════════════╬══════════╬═══════════════════╣");

    let pairs = [("Encode", "Small Encode", "Large Encode"), ("Decode", "Small Decode", "Large Decode")];
    for (label, small_prefix, large_prefix) in &pairs {
        let small = results.iter().find(|r| r.name.starts_with(small_prefix));
        let large = results.iter().find(|r| r.name.starts_with(large_prefix));
        if let (Some(s), Some(l)) = (small, large) {
            let overhead_pct = ((l.mean_ns - s.mean_ns) / s.mean_ns) * 100.0;
            let ratio = l.mean_ns / s.mean_ns;
            println!("║ {:<20} ║ {:>10.2} ║ {:>10.2} ║ {:>+7.1}% ║ {:.1}× slower        ║",
                label, s.mean_us(), l.mean_us(), overhead_pct, ratio);
        }
    }
    println!("╚══════════════════════╩════════════╩════════════╩══════════╩═══════════════════╝");

    println!("\n  TOAST threshold: {} bytes  |  Blob size per field: 12,000 bytes (large), 16 bytes (small)",
        TOAST_THRESHOLD);
    println!("  Methodology: black_box() DCE prevention, ns-resolution, warmup-then-measure");
}

// ═══════════════════════════════════════════════════════════════════
//  Multi-row Dataset Simulation (10 rows, matching CSV row count)
// ═══════════════════════════════════════════════════════════════════

fn bench_dataset_simulation(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║             Dataset Simulation — 10-Row Batch (matches CSV row count)         ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let schema = schema();
    let small_rows: Vec<Vec<Value>> = (0..10).map(|_| small_tuple()).collect();
    let large_rows: Vec<Vec<Value>> = (0..10).map(|_| large_tuple()).collect();

    // Small dataset: encode all 10 rows
    let r = benchmark("Small Dataset Encode (10 rows × small BLOBs)", 500, 50, || {
        for row in black_box(&small_rows) {
            let mut mgr = ToastManager::new();
            let enc = TupleCodec::encode_tuple(row, black_box(&schema), &mut mgr);
            let _ = black_box(enc);
        }
    });
    r.print_row();
    results.push(r);

    // Large dataset: encode all 10 rows
    let r = benchmark("Large Dataset Encode (10 rows × large BLOBs)", 20, 3, || {
        for row in black_box(&large_rows) {
            let mut mgr = ToastManager::new();
            let enc = TupleCodec::encode_tuple(row, black_box(&schema), &mut mgr);
            let _ = black_box(enc);
        }
    });
    r.print_row();
    results.push(r);

    // Small dataset: decode all 10 rows
    let encoded_small: Vec<Vec<u8>> = small_rows.iter().map(|row| {
        let mut mgr = ToastManager::new();
        TupleCodec::encode_tuple(row, &schema, &mut mgr).unwrap()
    }).collect();

    let r = benchmark("Small Dataset Decode (10 rows × small BLOBs)", 500, 50, || {
        for enc in black_box(&encoded_small) {
            let dec = TupleCodec::decode_tuple(black_box(enc), black_box(&schema));
            let _ = black_box(dec);
        }
    });
    r.print_row();
    results.push(r);

    // Large dataset: decode all 10 rows
    let mut toast_mgrs: Vec<ToastManager> = Vec::new();
    let encoded_large: Vec<Vec<u8>> = large_rows.iter().map(|row| {
        let mut mgr = ToastManager::new();
        let enc = TupleCodec::encode_tuple(row, &schema, &mut mgr).unwrap();
        toast_mgrs.push(mgr);
        enc
    }).collect();

    let r = benchmark("Large Dataset Decode (10 rows × large BLOBs)", 20, 3, || {
        for (enc, mgr) in black_box(&encoded_large).iter().zip(black_box(&toast_mgrs).iter()) {
            let dec = TupleCodec::decode_tuple_with_toast(black_box(enc), black_box(&schema), black_box(mgr));
            let _ = black_box(dec);
        }
    });
    r.print_row();
    results.push(r);
}

// ═══════════════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════════════

fn main() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║          RookDB TOAST Comparison Benchmark — BLOB & ARRAY<BLOB>               ║");
    println!("║  Comparing: example2.csv (small BLOBs) vs output.csv (large BLOBs, TOAST)    ║");
    println!("║  Schema: (INT32, BOOL, TEXT, BLOB, ARRAY<BLOB>)                               ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    println!("\n  TOAST threshold: {} bytes", TOAST_THRESHOLD);
    println!("  Small BLOB size: 16 bytes (example2.csv: short hex digests like '11dbb77a8')");
    println!("  Large BLOB size: 12,000 bytes (output.csv: 75 KB hex hashes, fully TOAST-ed)");

    let mut results: Vec<BenchResult> = Vec::new();

    bench_small_blob(&mut results);
    bench_large_blob(&mut results);
    bench_dataset_simulation(&mut results);
    print_overhead_summary(&results);

    println!("\n  ✓ Benchmark complete. {} benchmarks run.", results.len());
}
