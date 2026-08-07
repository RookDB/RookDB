//! Timing harness for the join algorithms.
//!
//! Deliberately not a criterion benchmark. `[dev-dependencies]` is empty, and
//! criterion pulls around forty transitive crates into every `cargo test` run
//! for a project whose CI has no benchmark step. This uses `std::time::Instant`
//! and follows the convention already set by `benchmark_fsm_heap`.
//!
//! It has two jobs: comparing algorithms on the same data, and producing the
//! measurements the cost model's coefficients are calibrated from. See
//! `docs/join/cost-model.md` for how to turn one into the other.
//!
//! Usage:
//!   cargo run --release --bin benchmark_joins [--json] [--rows N]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use storage_manager::catalog::Column;
use storage_manager::executor::selection::{ColumnReference, ComparisonOp, Expr, Predicate};
use storage_manager::heap::HeapManager;
use storage_manager::join::{
    JoinAlgorithm, JoinBuilder, JoinConfig, JoinType, TableRef, analyze_table, save_stats,
};
use storage_manager::types::row::serialize_nullable_typed_row;
use storage_manager::types::{DataType, DataValue};

/// Runs per measurement, after a warm-up run that is discarded.
const ITERATIONS: usize = 5;

struct Measurement {
    algorithm: JoinAlgorithm,
    rows_out: u64,
    median: Duration,
    p95: Duration,
}

fn main() {
    let arguments: Vec<String> = std::env::args().collect();
    let json = arguments.iter().any(|a| a == "--json");
    let rows: i64 = arguments
        .windows(2)
        .find(|pair| pair[0] == "--rows")
        .and_then(|pair| pair[1].parse().ok())
        .unwrap_or(20_000);

    let root = std::env::temp_dir().join(format!("rookdb-join-bench-{}", std::process::id()));
    if let Err(e) = std::fs::create_dir_all(&root) {
        eprintln!("cannot create {}: {e}", root.display());
        return;
    }

    if !json {
        println!("RookDB join benchmark");
        println!("  scratch: {}", root.display());
        println!("  rows:    {rows} per relation\n");
    }

    let scenarios: Vec<(&str, i64, i64, u64)> = vec![
        // name, left rows, right rows, work memory
        ("resident, selective key", rows, rows / 4, 64 * 1024 * 1024),
        (
            "resident, many duplicates",
            rows,
            rows / 4,
            64 * 1024 * 1024,
        ),
        ("spilling (1 MiB budget)", rows, rows / 4, 1024 * 1024),
        ("small outer, large inner", 200, rows, 64 * 1024 * 1024),
    ];

    let mut all = Vec::new();

    for (index, (name, left_rows, right_rows, memory)) in scenarios.iter().enumerate() {
        let duplicates = if name.contains("duplicates") { 20 } else { 0 };
        let Some((left, right)) = build_tables(&root, index, *left_rows, *right_rows, duplicates)
        else {
            eprintln!("could not build tables for '{name}'");
            continue;
        };

        // Measured statistics, so the planner's own choice is meaningful.
        for table in [&left, &right] {
            if let Ok(stats) = analyze_table(table) {
                let _ = save_stats(table, &stats);
            }
        }

        if !json {
            println!("── {name} ({left_rows} × {right_rows} rows, {memory} byte budget)");
        }

        let mut measurements = Vec::new();
        for algorithm in [
            JoinAlgorithm::BlockNestedLoop,
            JoinAlgorithm::SortMerge,
            JoinAlgorithm::Hash,
            JoinAlgorithm::SymmetricHash,
            JoinAlgorithm::Adaptive,
        ] {
            // A nested loop over tens of thousands of rows on both sides is
            // quadratic; skip it rather than spend minutes confirming that.
            if algorithm == JoinAlgorithm::BlockNestedLoop && *left_rows * *right_rows > 20_000_000
            {
                continue;
            }

            if let Some(measurement) = measure(&left, &right, algorithm, *memory) {
                if !json {
                    println!(
                        "   {:<20} {:>9.2?} median  {:>9.2?} p95   {} rows",
                        algorithm.name(),
                        measurement.median,
                        measurement.p95,
                        measurement.rows_out
                    );
                }
                measurements.push(measurement);
            }
        }

        if !json {
            if let Some(plan) = plan_choice(&left, &right, *memory) {
                println!("   planner chose: {}\n", plan.name());
            } else {
                println!();
            }
        }

        all.push((name.to_string(), measurements));
    }

    if json {
        print_json(&all);
    }

    let _ = std::fs::remove_dir_all(&root);
}

/// Two relations joined on their first column.
fn build_tables(
    root: &PathBuf,
    scenario: usize,
    left_rows: i64,
    right_rows: i64,
    duplicates: i64,
) -> Option<(TableRef, TableRef)> {
    let left_columns = vec![
        Column::new("k".to_string(), DataType::Int),
        Column::new("payload".to_string(), DataType::Varchar(48)),
    ];
    let right_columns = left_columns.clone();

    let left_keys = if duplicates > 0 {
        (left_rows / duplicates).max(1)
    } else {
        left_rows.max(1)
    };
    let right_keys = if duplicates > 0 {
        (right_rows / duplicates).max(1)
    } else {
        right_rows.max(1)
    };

    let left = populate(
        root,
        &format!("left{scenario}"),
        left_columns,
        left_rows,
        left_keys,
    )?;
    let right = populate(
        root,
        &format!("right{scenario}"),
        right_columns,
        right_rows,
        right_keys,
    )?;
    Some((left, right))
}

fn populate(
    root: &PathBuf,
    name: &str,
    columns: Vec<Column>,
    rows: i64,
    distinct: i64,
) -> Option<TableRef> {
    let path = root.join(format!("{name}.dat"));
    let _ = std::fs::remove_file(&path);
    let mut manager = HeapManager::create(path.clone()).ok()?;

    let types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
    for i in 0..rows {
        let values = vec![
            Some(DataValue::Int((i % distinct.max(1)) as i32)),
            Some(DataValue::Varchar(format!("{name}-row-{i}-padding"))),
        ];
        let bytes = serialize_nullable_typed_row(&types, &values).ok()?;
        manager.insert_tuple(&bytes).ok()?;
    }
    manager.flush().ok()?;

    Some(TableRef::new(name, path, columns))
}

fn condition(left: &TableRef, right: &TableRef) -> Predicate {
    let column =
        |table: &TableRef| Expr::Column(ColumnReference::new(format!("{}.k", table.alias)));
    Predicate::Compare(
        Box::new(column(left)),
        ComparisonOp::Equals,
        Box::new(column(right)),
    )
}

fn measure(
    left: &TableRef,
    right: &TableRef,
    algorithm: JoinAlgorithm,
    memory: u64,
) -> Option<Measurement> {
    let run = || -> Option<(Duration, u64)> {
        let config = JoinConfig::with_work_memory(memory);
        let builder = JoinBuilder::new(left.clone(), right.clone(), JoinType::Inner)
            .with_algorithm(algorithm)
            .with_condition(condition(left, right))
            .with_config(config);

        let started = Instant::now();
        let mut stream = builder.execute().ok()?;
        let mut produced = 0u64;
        while let Some(row) = stream.next() {
            row.ok()?;
            produced += 1;
        }
        Some((started.elapsed(), produced))
    };

    // Warm-up, discarded: the first run pays for page cache misses the rest
    // do not.
    run()?;

    let mut timings = Vec::with_capacity(ITERATIONS);
    let mut rows_out = 0;
    for _ in 0..ITERATIONS {
        let (elapsed, produced) = run()?;
        timings.push(elapsed);
        rows_out = produced;
    }
    timings.sort();

    Some(Measurement {
        algorithm,
        rows_out,
        median: timings[timings.len() / 2],
        p95: timings[(timings.len() * 95 / 100).min(timings.len() - 1)],
    })
}

fn plan_choice(left: &TableRef, right: &TableRef, memory: u64) -> Option<JoinAlgorithm> {
    JoinBuilder::new(left.clone(), right.clone(), JoinType::Inner)
        .with_condition(condition(left, right))
        .with_config(JoinConfig::with_work_memory(memory))
        .plan()
        .ok()
        .map(|plan| plan.algorithm)
}

fn print_json(all: &[(String, Vec<Measurement>)]) {
    println!("{{");
    println!("  \"scenarios\": [");
    for (index, (name, measurements)) in all.iter().enumerate() {
        println!("    {{");
        println!("      \"name\": {name:?},");
        println!("      \"results\": [");
        for (position, measurement) in measurements.iter().enumerate() {
            let comma = if position + 1 == measurements.len() {
                ""
            } else {
                ","
            };
            println!(
                "        {{ \"algorithm\": {:?}, \"rows\": {}, \"median_ms\": {:.4}, \"p95_ms\": {:.4} }}{comma}",
                measurement.algorithm.name(),
                measurement.rows_out,
                measurement.median.as_secs_f64() * 1000.0,
                measurement.p95.as_secs_f64() * 1000.0
            );
        }
        println!("      ]");
        println!("    }}{}", if index + 1 == all.len() { "" } else { "," });
    }
    println!("  ]");
    println!("}}");
}
