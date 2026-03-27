use std::time::Instant;

use storage_manager::types::{
    // Original imports
    Comparable, DataType, DataValue, NumericValue, OrderedF64, abs, ceiling,
    deserialize_nullable_row, floor, round, serialize_nullable_row,
    // New string functions
    trim, upper, lower, substring, length
};

fn run_row_roundtrip_workload(rows: usize) -> (f64, f64) {
    let schema = vec![
        DataType::Int,
        DataType::Varchar(32),
        DataType::Numeric {
            precision: 10,
            scale: 2,
        },
        DataType::Bool,
        DataType::Date,
        DataType::Time,
        DataType::Timestamp,
        DataType::Bit(8),
    ];

    let start = Instant::now();
    let mut checksum = 0usize;
    for i in 0..rows {
        let id = format!("{}", i as i32);
        let name = format!("user_{}", i);
        let amount = format!("{}.{}", i % 10_000, i % 100);
        let active = if i % 2 == 0 { "true" } else { "false" };
        let day = 1 + (i % 28);
        let date = format!("2026-03-{day:02}");
        let sec = i % 60;
        let time = format!("13:14:{sec:02}.123456");
        let ts = format!("2026-03-{day:02} 13:14:{sec:02}.654321");

        let bytes = serialize_nullable_row(
            &schema,
            &[
                Some(&id),
                Some(&name),
                Some(&amount),
                Some(active),
                Some(&date),
                Some(&time),
                Some(&ts),
                Some("B'10101010'"),
            ],
        )
        .unwrap();

        let values = deserialize_nullable_row(&schema, &bytes).unwrap();
        checksum += values.len();
    }

    assert_eq!(checksum, rows * schema.len());

    let secs = start.elapsed().as_secs_f64();
    let throughput = rows as f64 / secs.max(f64::EPSILON);
    (secs, throughput)
}

fn run_numeric_function_workload(iterations: usize) -> (f64, f64) {
    let start = Instant::now();
    let mut checksum = 0f64;

    for i in 0..iterations {
        let value = DataValue::DoublePrecision(OrderedF64(-12345.6789 + (i as f64 * 0.0001)));
        let a = abs(&value).unwrap();
        let r = round(&a, 3).unwrap();
        let f = floor(&r).unwrap();
        let c = ceiling(&r).unwrap();

        if let DataValue::DoublePrecision(v) = c {
            checksum += v.0;
        }
        if let DataValue::DoublePrecision(v) = f {
            checksum -= v.0;
        }
    }

    assert!(checksum.is_finite());

    let secs = start.elapsed().as_secs_f64();
    let throughput = iterations as f64 / secs.max(f64::EPSILON);
    (secs, throughput)
}

fn run_numeric_compare_workload(iterations: usize) -> (f64, f64) {
    let start = Instant::now();
    let mut less_count = 0usize;

    for i in 0..iterations {
        let left = DataValue::Numeric(NumericValue {
            unscaled: (i as i128) * 100 + 25,
            scale: 2,
        });
        let right = DataValue::Numeric(NumericValue {
            unscaled: (i as i128) * 100 + 75,
            scale: 2,
        });

        if left.compare(&right).unwrap().is_lt() {
            less_count += 1;
        }
    }

    assert_eq!(less_count, iterations);

    let secs = start.elapsed().as_secs_f64();
    let throughput = iterations as f64 / secs.max(f64::EPSILON);
    (secs, throughput)
}

#[test]
fn benchmark_rows_small_medium_large() {
    let workloads = [(2_000usize, "small"), (20_000usize, "medium"), (100_000usize, "large")];

    println!("\n=== Row Roundtrip Benchmark ===");
    println!("rows,size,seconds,rows_per_sec");
    for (rows, label) in workloads {
        let (secs, throughput) = run_row_roundtrip_workload(rows);
        println!("{rows},{label},{secs:.6},{throughput:.2}");
    }
}

#[test]
fn benchmark_numeric_functions_small_medium_large() {
    let workloads = [
        (20_000usize, "small"),
        (200_000usize, "medium"),
        (1_000_000usize, "large"),
    ];

    println!("\n=== Numeric Function Benchmark ===");
    println!("ops,size,seconds,ops_per_sec");
    for (ops, label) in workloads {
        let (secs, throughput) = run_numeric_function_workload(ops);
        println!("{ops},{label},{secs:.6},{throughput:.2}");
    }
}

#[test]
fn benchmark_numeric_comparison_small_medium_large() {
    let workloads = [
        (100_000usize, "small"),
        (1_000_000usize, "medium"),
        (5_000_000usize, "large"),
    ];

    println!("\n=== Numeric Comparison Benchmark ===");
    println!("ops,size,seconds,ops_per_sec");
    for (ops, label) in workloads {
        let (secs, throughput) = run_numeric_compare_workload(ops);
        println!("{ops},{label},{secs:.6},{throughput:.2}");
    }
}
fn run_string_function_workload(iterations: usize) -> (f64, f64) {
    let start = std::time::Instant::now();
    let mut checksum = 0usize;

    for i in 0..iterations {
        let val_str = format!("  RookDB_User_{}  ", i % 1000);
        let value = DataValue::Varchar(val_str);

        let trimmed = trim(&value).unwrap();
        let upper_cased = upper(&trimmed).unwrap();
        let lower_cased = lower(&upper_cased).unwrap();
        let sub = substring(&lower_cased, 1, 6).unwrap();
        checksum += length(&sub).unwrap();
    }

    assert!(checksum > 0);
    let secs = start.elapsed().as_secs_f64();
    let throughput = iterations as f64 / secs.max(f64::EPSILON);
    (secs, throughput)
}

#[test]
fn benchmark_string_functions_small_medium_large() {
    let workloads = [
        (20_000usize, "small"),
        (200_000usize, "medium"),
        (1_000_000usize, "large"),
    ];

    println!("\n=== String Function Benchmark ===");
    println!("ops,size,seconds,ops_per_sec");
    for (ops, label) in workloads {
        let (secs, throughput) = run_string_function_workload(ops);
        println!("{ops},{label},{secs:.6},{throughput:.2}");
    }
}
