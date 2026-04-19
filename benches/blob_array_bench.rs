//! Credible Benchmarking Suite for BLOB and ARRAY support in RookDB
//!
//! This module provides statistically rigorous performance benchmarking for:
//! - BLOB encoding/decoding at varying sizes
//! - ARRAY encoding/decoding with varying element counts and types
//! - TOAST management operations for large BLOBs
//! - Tuple operations containing BLOB and ARRAY fields
//! - Memory efficiency analysis
//!
//! Methodology:
//! - Uses `std::hint::black_box()` to prevent dead-code elimination
//! - Nanosecond-resolution timing via `Instant::elapsed().as_nanos()`
//! - 100-iteration warmup before measurement
//! - Reports: mean, std dev, median, p95, p99, min, max
//! - All results collected and summarized in a final table

use std::hint::black_box;
use std::time::Instant;

use storage_manager::backend::catalog::data_type::{DataType, Value};
use storage_manager::backend::storage::row_layout::*;
use storage_manager::backend::storage::toast::{ToastManager, TOAST_CHUNK_SIZE, TOAST_THRESHOLD};
use storage_manager::backend::storage::tuple_codec::TupleCodec;
use storage_manager::backend::storage::value_codec::ValueCodec;

const LARGE_DATA_SIZES: [usize; 3] = [1_000_000, 10_000_000, 100_000_000];

fn byte_label(size: usize) -> String {
    if size % 1_000_000 == 0 {
        format!("{}MB", size / 1_000_000)
    } else if size % 1_000 == 0 {
        format!("{}KB", size / 1_000)
    } else {
        format!("{}B", size)
    }
}

fn byte_bench_iters(size: usize) -> usize {
    if size >= 100_000_000 {
        3
    } else if size >= 10_000_000 {
        5
    } else if size >= 1_000_000 {
        10
    } else if size > 50_000 {
        100
    } else if size > 5_000 {
        500
    } else {
        2_000
    }
}

fn byte_bench_warmup(size: usize) -> usize {
    if size >= 100_000_000 {
        1
    } else if size >= 10_000_000 {
        3
    } else if size >= 1_000_000 {
        10
    } else {
        100
    }
}

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
            "  {:<45} {:>9.2} µs  ±{:<8.2} µs  {:>10.0} ops/s  (p50={:.2} p95={:.2} p99={:.2} µs)",
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
//  BLOB Benchmarks
// ═══════════════════════════════════════════════════════════════════

fn bench_blob_encoding(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    BLOB Encoding / Decoding Benchmarks                       ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let sizes: Vec<usize> = vec![
        10, 
        100, 
        1_000, 
        10_000, 
        TOAST_THRESHOLD + 1000, 
        102_400,
        1_000_000,        // 1 MB
        10_000_000,       // 10 MB
        100_000_000,      // 100 MB
    ];

    // Encoding
    println!("\n  --- BLOB Encoding (Fixed-Length Data Type) ---");
    for &size in &sizes {
        let blob = vec![0xAB_u8; size];
        let value = Value::Blob(blob);
        let iters = byte_bench_iters(size);
        let warmup = byte_bench_warmup(size);

        let r = benchmark(
            &format!("BLOB Encode (fixed-length: {})", byte_label(size)),
            iters,
            warmup,
            || {
                let encoded = ValueCodec::encode(black_box(&value), black_box(&DataType::Blob));
                let _ = black_box(encoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Decoding
    println!("\n  --- BLOB Decoding (Fixed-Length Data Type) ---");
    for &size in &sizes {
        let blob = vec![0xAB_u8; size];
        let encoded = ValueCodec::encode(&Value::Blob(blob), &DataType::Blob).unwrap();
        let iters = byte_bench_iters(size);
        let warmup = byte_bench_warmup(size);

        let r = benchmark(
            &format!("BLOB Decode (fixed-length: {})", byte_label(size)),
            iters,
            warmup,
            || {
                let decoded = ValueCodec::decode(black_box(&encoded), black_box(&DataType::Blob));
                let _ = black_box(decoded);
            },
        );
        r.print_row();
        results.push(r);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  ARRAY Benchmarks
// ═══════════════════════════════════════════════════════════════════

fn nested_int_array_type() -> DataType {
    DataType::Array {
        element_type: Box::new(DataType::Array {
            element_type: Box::new(DataType::Int32),
        }),
    }
}

fn make_nested_int_array(outer_count: usize, inner_count: usize) -> Value {
    Value::Array(
        (0..outer_count)
            .map(|outer| {
                Value::Array(
                    (0..inner_count)
                        .map(|inner| Value::Int32((outer * inner_count + inner) as i32))
                        .collect(),
                )
            })
            .collect(),
    )
}

fn bench_array_encoding(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║           ARRAY Encoding / Decoding Benchmarks (with Data Type Labels)       ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let element_counts: Vec<usize> = vec![10, 100, 1_000, 10_000, 100_000];

    // ─────────────────────────────────────────────────────────────
    // FIXED-LENGTH: ARRAY<INT32>
    // ─────────────────────────────────────────────────────────────
    
    // INT Array Encoding
    println!("\n  --- ARRAY<INT32> Encoding (fixed-length element type) ---");
    for &count in &element_counts {
        let values: Vec<Value> = (0..count).map(|i| Value::Int32(i as i32)).collect();
        let array_val = Value::Array(values);
        let array_type = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };
        let iters = if count >= 100_000 { 
            20 
        } else if count >= 10_000 { 
            100 
        } else { 
            500 
        };

        let r = benchmark(
            &format!("ARRAY<INT32> Encode [fixed-length] ({}elem)", count),
            iters,
            100,
            || {
                let encoded = ValueCodec::encode(black_box(&array_val), black_box(&array_type));
                let _ = black_box(encoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // INT Array Decoding
    println!("\n  --- ARRAY<INT32> Decoding (fixed-length element type) ---");
    for &count in &element_counts {
        let values: Vec<Value> = (0..count).map(|i| Value::Int32(i as i32)).collect();
        let array_val = Value::Array(values);
        let array_type = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };
        let encoded = ValueCodec::encode(&array_val, &array_type).unwrap();
        let iters = if count >= 100_000 { 
            20 
        } else if count >= 10_000 { 
            100 
        } else { 
            500 
        };

        let r = benchmark(
            &format!("ARRAY<INT32> Decode [fixed-length] ({}elem)", count),
            iters,
            100,
            || {
                let decoded = ValueCodec::decode(black_box(&encoded), black_box(&array_type));
                let _ = black_box(decoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // ─────────────────────────────────────────────────────────────
    // VARIABLE-LENGTH: ARRAY<TEXT>
    // ─────────────────────────────────────────────────────────────

    // TEXT Array Encoding
    println!("\n  --- ARRAY<TEXT> Encoding (variable-length element type) ---");
    for &count in &[10_usize, 100, 1_000, 10_000] {
        let values: Vec<Value> = (0..count)
            .map(|i| Value::Text(format!("item_{:06}", i)))
            .collect();
        let array_val = Value::Array(values);
        let array_type = DataType::Array {
            element_type: Box::new(DataType::Text),
        };
        let iters = if count >= 10_000 { 
            50 
        } else if count >= 1_000 { 
            100 
        } else { 
            500 
        };

        let r = benchmark(
            &format!("ARRAY<TEXT> Encode [variable-length] ({}elem)", count),
            iters,
            100,
            || {
                let encoded = ValueCodec::encode(black_box(&array_val), black_box(&array_type));
                let _ = black_box(encoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // TEXT Array Decoding
    println!("\n  --- ARRAY<TEXT> Decoding (variable-length element type) ---");
    for &count in &[10_usize, 100, 1_000, 10_000] {
        let values: Vec<Value> = (0..count)
            .map(|i| Value::Text(format!("item_{:06}", i)))
            .collect();
        let array_val = Value::Array(values);
        let array_type = DataType::Array {
            element_type: Box::new(DataType::Text),
        };
        let encoded = ValueCodec::encode(&array_val, &array_type).unwrap();
        let iters = if count >= 10_000 { 
            50 
        } else if count >= 1_000 { 
            100 
        } else { 
            500 
        };

        let r = benchmark(
            &format!("ARRAY<TEXT> Decode [variable-length] ({}elem)", count),
            iters,
            100,
            || {
                let decoded = ValueCodec::decode(black_box(&encoded), black_box(&array_type));
                let _ = black_box(decoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // ─────────────────────────────────────────────────────────────
    // Nested ARRAY benchmarks with data type labels
    // ─────────────────────────────────────────────────────────────

    // Nested INT Array Encoding
    println!("\n  --- ARRAY<ARRAY<INT32>> Encoding (nested fixed-length) ---");
    for &(outer_count, inner_count) in &[(10_usize, 4_usize), (100, 4), (1_000, 4), (10_000, 4)] {
        let array_val = make_nested_int_array(outer_count, inner_count);
        let array_type = nested_int_array_type();
        let iters = if outer_count >= 10_000 { 
            50 
        } else if outer_count >= 1_000 { 
            100 
        } else { 
            300 
        };

        let r = benchmark(
            &format!("ARRAY<ARRAY<INT32>> Encode [nested fixed-length] ({}x{})", outer_count, inner_count),
            iters,
            100,
            || {
                let encoded = ValueCodec::encode(black_box(&array_val), black_box(&array_type));
                let _ = black_box(encoded);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Nested INT Array Decoding
    println!("\n  --- ARRAY<ARRAY<INT32>> Decoding (nested fixed-length) ---");
    for &(outer_count, inner_count) in &[(10_usize, 4_usize), (100, 4), (1_000, 4), (10_000, 4)] {
        let array_val = make_nested_int_array(outer_count, inner_count);
        let array_type = nested_int_array_type();
        let encoded = ValueCodec::encode(&array_val, &array_type).unwrap();
        let iters = if outer_count >= 10_000 { 
            50 
        } else if outer_count >= 1_000 { 
            100 
        } else { 
            300 
        };

        let r = benchmark(
            &format!("ARRAY<ARRAY<INT32>> Decode [nested fixed-length] ({}x{})", outer_count, inner_count),
            iters,
            100,
            || {
                let decoded = ValueCodec::decode(black_box(&encoded), black_box(&array_type));
                let _ = black_box(decoded);
            },
        );
        r.print_row();
        results.push(r);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  TOAST Benchmarks
// ═══════════════════════════════════════════════════════════════════

fn bench_toast_operations(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                        TOAST Manager Benchmarks                              ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    // TOAST Pointer serialization / deserialization across larger payload sizes
    for &total_bytes in &[100_000_usize, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]] {
        let chunk_count = total_bytes.div_ceil(TOAST_CHUNK_SIZE) as u32;
        let ptr = ToastPointer {
            value_id: 12345,
            total_bytes: total_bytes as u32,
            chunk_count,
        };
        let bytes = ptr.to_bytes();
        let iters = if total_bytes >= 100_000_000 {
            500
        } else if total_bytes >= 10_000_000 {
            1_000
        } else {
            5_000
        };
        let warmup = byte_bench_warmup(total_bytes);

        let r = benchmark(
            &format!("TOAST Pointer to_bytes ({})", byte_label(total_bytes)),
            iters,
            warmup,
            || {
                black_box(ptr.to_bytes());
            },
        );
        r.print_row();
        results.push(r);

        let r = benchmark(
            &format!("TOAST Pointer from_bytes ({})", byte_label(total_bytes)),
            iters,
            warmup,
            || {
                let p = ToastPointer::from_bytes(black_box(&bytes));
                let _ = black_box(p);
            },
        );
        r.print_row();
        results.push(r);
    }

    // TOAST threshold check at representative payload sizes
    for &size in &[16_384_usize, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]] {
        let r = benchmark(
            &format!("TOAST should_use_toast check ({})", byte_label(size)),
            10_000,
            100,
            || {
                black_box(ToastManager::should_use_toast(black_box(size)));
            },
        );
        r.print_row();
        results.push(r);
    }

    // Store large values at increasing payload sizes
    for &size in &[TOAST_THRESHOLD + 5_000, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]] {
        let large_payload = vec![0xAB_u8; size];
        let iters = if size >= 100_000_000 {
            2
        } else if size >= 10_000_000 {
            5
        } else if size >= 1_000_000 {
            10
        } else {
            500
        };
        let warmup = byte_bench_warmup(size);
        let r = benchmark(
            &format!("TOAST store_large_value ({})", byte_label(size)),
            iters,
            warmup.min(50),
            || {
                let mut manager = ToastManager::new();
                let ptr = manager.store_large_value(black_box(&large_payload));
                let _ = black_box(ptr);
            },
        );
        r.print_row();
        results.push(r);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Tuple Benchmarks (BLOB & ARRAY focused)
// ═══════════════════════════════════════════════════════════════════

fn bench_tuple_operations(results: &mut Vec<BenchResult>) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║              Tuple Encoding/Decoding with BLOB & ARRAY                       ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    // Tuple with BLOB
    let schema_blob = vec![
        ("id".to_string(), DataType::Int32),
        ("data".to_string(), DataType::Blob),
    ];

    for &blob_size in &[100_usize, 1024, 4096, 10_240, 102_400, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]] {
        let values = vec![
            Value::Int32(42),
            Value::Blob(vec![0xAB; blob_size]),
        ];
        let iters = byte_bench_iters(blob_size);
        let warmup = byte_bench_warmup(blob_size);
        let r = benchmark(
            &format!("Tuple Encode (INT + {} BLOB)", byte_label(blob_size)),
            iters,
            warmup,
            || {
                let mut toast_mgr = ToastManager::new();
                let _enc = TupleCodec::encode_tuple(
                    black_box(&values),
                    black_box(&schema_blob),
                    &mut toast_mgr,
                );
                let _ = black_box(_enc);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with BLOB — Decoding
    println!();
    for &blob_size in &[100_usize, 1024, 4096, 10_240, 102_400, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]] {
        let values = vec![
            Value::Int32(42),
            Value::Blob(vec![0xAB; blob_size]),
        ];
        let mut toast_mgr = ToastManager::new();
        let encoded =
            TupleCodec::encode_tuple(&values, &schema_blob, &mut toast_mgr).unwrap();
        let iters = byte_bench_iters(blob_size);
        let warmup = byte_bench_warmup(blob_size);

        let r = benchmark(
            &format!("Tuple Decode (INT + {} BLOB)", byte_label(blob_size)),
            iters,
            warmup,
            || {
                let dec = TupleCodec::decode_tuple_with_toast(
                    black_box(&encoded),
                    black_box(&schema_blob),
                    black_box(&toast_mgr),
                );
                let _ = black_box(dec);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with ARRAY
    println!();
    let schema_array = vec![
        ("id".to_string(), DataType::Int32),
        (
            "scores".to_string(),
            DataType::Array {
                element_type: Box::new(DataType::Int32),
            },
        ),
    ];

    // ARRAY<INT32> - Fixed-length elements
    for &arr_size in &[10_usize, 100, 1000, 10_000] {
        let values = vec![
            Value::Int32(1),
            Value::Array((0..arr_size).map(|i| Value::Int32(i as i32)).collect()),
        ];
        let iters = if arr_size >= 10_000 { 100 } else { 500 };
        let r = benchmark(
            &format!("Tuple Encode [INT32 fixed-length] (INT + {}elem ARRAY<INT32>)", arr_size),
            iters,
            100,
            || {
                let mut toast_mgr = ToastManager::new();
                let _enc = TupleCodec::encode_tuple(
                    black_box(&values),
                    black_box(&schema_array),
                    &mut toast_mgr,
                );
                let _ = black_box(_enc);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with ARRAY<INT32> — Decoding
    println!();
    for &arr_size in &[10_usize, 100, 1000, 10_000] {
        let values = vec![
            Value::Int32(1),
            Value::Array((0..arr_size).map(|i| Value::Int32(i as i32)).collect()),
        ];
        let mut toast_mgr = ToastManager::new();
        let encoded =
            TupleCodec::encode_tuple(&values, &schema_array, &mut toast_mgr).unwrap();
        let iters = if arr_size >= 10_000 { 100 } else { 500 };

        let r = benchmark(
            &format!("Tuple Decode [INT32 fixed-length] (INT + {}elem ARRAY<INT32>)", arr_size),
            iters,
            100,
            || {
                let dec = TupleCodec::decode_tuple(black_box(&encoded), black_box(&schema_array));
                let _ = black_box(dec);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with ARRAY<TEXT> - Variable-length elements
    println!();
    let schema_array_text = vec![
        ("id".to_string(), DataType::Int32),
        (
            "tags".to_string(),
            DataType::Array {
                element_type: Box::new(DataType::Text),
            },
        ),
    ];

    for &arr_size in &[10_usize, 100, 1000, 10_000] {
        let values = vec![
            Value::Int32(1),
            Value::Array((0..arr_size).map(|i| Value::Text(format!("tag_{:06}", i))).collect()),
        ];
        let iters = if arr_size >= 10_000 { 100 } else { 500 };
        let r = benchmark(
            &format!("Tuple Encode [TEXT variable-length] (INT + {}elem ARRAY<TEXT>)", arr_size),
            iters,
            100,
            || {
                let mut toast_mgr = ToastManager::new();
                let _enc = TupleCodec::encode_tuple(
                    black_box(&values),
                    black_box(&schema_array_text),
                    &mut toast_mgr,
                );
                let _ = black_box(_enc);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with ARRAY<TEXT> — Decoding
    println!();
    for &arr_size in &[10_usize, 100, 1000, 10_000] {
        let values = vec![
            Value::Int32(1),
            Value::Array((0..arr_size).map(|i| Value::Text(format!("tag_{:06}", i))).collect()),
        ];
        let mut toast_mgr = ToastManager::new();
        let encoded =
            TupleCodec::encode_tuple(&values, &schema_array_text, &mut toast_mgr).unwrap();
        let iters = if arr_size >= 10_000 { 100 } else { 500 };

        let r = benchmark(
            &format!("Tuple Decode [TEXT variable-length] (INT + {}elem ARRAY<TEXT>)", arr_size),
            iters,
            100,
            || {
                let dec = TupleCodec::decode_tuple(black_box(&encoded), black_box(&schema_array_text));
                let _ = black_box(dec);
            },
        );
        r.print_row();
        results.push(r);
    }

    // Tuple with nested ARRAY
    println!();
    let schema_nested_array = vec![
        ("id".to_string(), DataType::Int32),
        ("matrix".to_string(), nested_int_array_type()),
    ];

    // Keep nested tuple payloads below the TOAST threshold so decode benchmarks
    // exercise the actual nested-array path rather than TOAST pointer handling.
    for &outer_count in &[10_usize, 100, 300, 1000] {
        let values = vec![
            Value::Int32(1),
            make_nested_int_array(outer_count, 4),
        ];
        let iters = if outer_count >= 1000 { 100 } else { 300 };
        let r = benchmark(
            &format!("Tuple Encode [nested INT32 fixed-length] (INT + {}x4 ARRAY<ARRAY<INT32>>)", outer_count),
            iters,
            100,
            || {
                let mut toast_mgr = ToastManager::new();
                let enc = TupleCodec::encode_tuple(
                    black_box(&values),
                    black_box(&schema_nested_array),
                    &mut toast_mgr,
                );
                let _ = black_box(enc);
            },
        );
        r.print_row();
        results.push(r);
    }

    println!();
    for &outer_count in &[10_usize, 100, 300, 1000] {
        let values = vec![
            Value::Int32(1),
            make_nested_int_array(outer_count, 4),
        ];
        let mut toast_mgr = ToastManager::new();
        let encoded =
            TupleCodec::encode_tuple(&values, &schema_nested_array, &mut toast_mgr).unwrap();
        let iters = if outer_count >= 1000 { 100 } else { 300 };

        let r = benchmark(
            &format!("Tuple Decode [nested INT32 fixed-length] (INT + {}x4 ARRAY<ARRAY<INT32>>)", outer_count),
            iters,
            100,
            || {
                let dec =
                    TupleCodec::decode_tuple(black_box(&encoded), black_box(&schema_nested_array));
                let _ = black_box(dec);
            },
        );
        r.print_row();
        results.push(r);
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Memory Efficiency Analysis
// ═══════════════════════════════════════════════════════════════════

fn bench_memory_efficiency() {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                       Memory Efficiency Analysis                             ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    println!("\n  Structure Sizes (in-memory):");
    println!("    TupleHeader:   {} bytes", std::mem::size_of::<TupleHeader>());
    println!("    VarFieldEntry: {} bytes", std::mem::size_of::<VarFieldEntry>());
    println!("    ToastPointer:  {} bytes", std::mem::size_of::<ToastPointer>());
    println!(
        "    ToastChunk:    {} bytes (base, excluding data payload)",
        std::mem::size_of::<ToastChunk>()
    );

    println!("\n  Encoded BLOB Sizes:");
    let blob_sizes: Vec<usize> = vec![10, 100, 1024, 10_240, LARGE_DATA_SIZES[0], LARGE_DATA_SIZES[1], LARGE_DATA_SIZES[2]];
    for size in &blob_sizes {
        let blob = Value::Blob(vec![0xAB; *size]);
        let encoded = ValueCodec::encode(&blob, &DataType::Blob).unwrap();
        let overhead = encoded.len() - size;
        let efficiency = (*size as f64 / encoded.len() as f64) * 100.0;
        println!(
            "    BLOB ({}):  encoded={}B  overhead={}B  efficiency={:.1}%",
            byte_label(*size),
            encoded.len(),
            overhead,
            efficiency,
        );
    }

    println!("\n  Encoded ARRAY Sizes:");
    let arr_counts = vec![10, 100, 1000];
    for count in &arr_counts {
        let arr = Value::Array((0..*count).map(|i| Value::Int32(i as i32)).collect());
        let arr_type = DataType::Array {
            element_type: Box::new(DataType::Int32),
        };
        let encoded = ValueCodec::encode(&arr, &arr_type).unwrap();
        let data_bytes = count * 4; // each INT32 = 4 bytes
        let overhead = encoded.len() - data_bytes;
        let efficiency = (data_bytes as f64 / encoded.len() as f64) * 100.0;
        println!(
            "    ARRAY<INT> ({}elem):  encoded={}B  data={}B  overhead={}B  efficiency={:.1}%",
            count,
            encoded.len(),
            data_bytes,
            overhead,
            efficiency,
        );
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Summary Table
// ═══════════════════════════════════════════════════════════════════

fn print_summary(results: &[BenchResult]) {
    println!("\n╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                          BENCHMARK SUMMARY TABLE                             ║");
    println!("╠═══════════════════════════════════════════════════╦══════════╦════════╦════════╣");
    println!("║ Operation                                        ║ Mean(µs) ║ ±StdDv ║ Ops/s  ║");
    println!("╠═══════════════════════════════════════════════════╬══════════╬════════╬════════╣");

    for r in results {
        let name = if r.name.len() > 49 {
            format!("{}…", &r.name[..48])
        } else {
            r.name.clone()
        };
        println!(
            "║ {:<49} ║ {:>8.2} ║ {:>6.2} ║ {:>6.0} ║",
            name,
            r.mean_us(),
            r.stddev_us(),
            r.throughput() / 1000.0, // in K ops/s
        );
    }
    println!("╚═══════════════════════════════════════════════════╩══════════╩════════╩════════╝");
    println!("  Note: Ops/s column is in thousands (K ops/s)");
    println!("  Methodology: black_box() DCE prevention, ns-resolution, 100-iter warmup");
}

// ═══════════════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════════════

fn main() {
    println!("\n");
    println!("╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║          RookDB BLOB & ARRAY Performance Benchmarking Suite                   ║");
    println!("║          Rust Edition 2024 | storage_manager v0.1.0                           ║");
    println!("║          Methodology: black_box + ns timing + statistical analysis            ║");
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");

    let mut results: Vec<BenchResult> = Vec::new();

    bench_blob_encoding(&mut results);
    bench_array_encoding(&mut results);
    bench_toast_operations(&mut results);
    bench_tuple_operations(&mut results);
    bench_memory_efficiency();
    print_summary(&results);

    println!("\n  Benchmarking complete.\n");
}

#[cfg(test)]
mod bench_tests {
    #[test]
    #[ignore]
    fn run_benchmarks() {
        super::main();
    }
}
