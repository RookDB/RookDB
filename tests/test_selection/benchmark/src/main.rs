// Comprehensive benchmark suite for the Selection Operator
// Run with: cargo run  (from tests/test_selection/benchmark/)

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;
use rand::Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

// Tuple builder

/// Build a tuple in the storage format.
/// Header is 8 bytes: [0-3] length, [4] version, [5] flags, [6-7] column count
/// Offsets are relative to where field data starts.
fn build_tuple(columns: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    let num_columns = columns.len();
    let null_bitmap_size = (num_columns + 7) / 8;
    let offset_array_size = (num_columns + 1) * 4; // +1 for sentinel

    let header_size = 8;
    let null_bitmap_start = header_size;
    let offset_array_start = null_bitmap_start + null_bitmap_size;
    let field_data_start = offset_array_start + offset_array_size;

    // Build NULL bitmap
    let mut null_bitmap = vec![0u8; null_bitmap_size];
    for (i, col) in columns.iter().enumerate() {
        if col.is_none() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            null_bitmap[byte_idx] |= 1 << bit_idx;
        }
    }

    // Build field data and offset array (offsets are relative to field_data_start)
    let mut field_data = Vec::new();
    let mut offsets = Vec::new();

    for col in columns.iter() {
        let relative_offset = field_data.len();
        offsets.push(relative_offset as u32);

        if let Some(data) = col {
            field_data.extend_from_slice(data);
        }
    }

    offsets.push(field_data.len() as u32);

    let total_length = field_data_start + field_data.len();

    let mut tuple = Vec::new();
    tuple.extend_from_slice(&(total_length as u32).to_le_bytes());
    tuple.push(1u8);
    tuple.push(0u8);
    tuple.extend_from_slice(&(num_columns as u16).to_le_bytes());
    tuple.extend_from_slice(&null_bitmap);

    for offset in offsets {
        tuple.extend_from_slice(&offset.to_le_bytes());
    }

    tuple.extend_from_slice(&field_data);
    tuple
}

// Schema definition

fn create_benchmark_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(), data_type: "INT".to_string() },
            Column { name: "amount".to_string(), data_type: "FLOAT".to_string() },
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
            Column { name: "date".to_string(), data_type: "DATE".to_string() },
        ],
    }
}

// Tuple generation

fn generate_tuples(count: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let names = vec!["Alice", "Bob", "Charlie", "Diana", "Frank"];
    let dates = vec!["2024-01-15", "2024-02-20", "2024-03-10", "2024-04-05", "2024-05-25"];

    let mut tuples = Vec::with_capacity(count);

    for _ in 0..count {
        let id: i32 = rng.gen_range(1..=1000);
        let amount: f64 = rng.gen_range(0.0..1000.0);
        let name = names[rng.gen_range(0..names.len())];
        let date = dates[rng.gen_range(0..dates.len())];

        // 10% chance of NULL for non-id columns
        let id_val = Some(id.to_le_bytes().to_vec());
        let amount_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(amount.to_le_bytes().to_vec())
        };
        let name_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(name.as_bytes().to_vec())
        };
        let date_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(date.as_bytes().to_vec())
        };

        tuples.push(build_tuple(vec![id_val, amount_val, name_val, date_val]));
    }

    tuples
}

// Benchmark helpers

fn log_line(writer: &mut BufWriter<File>, line: &str) {
    println!("{}", line);
    writeln!(writer, "{}", line).unwrap();
}

fn benchmark<F: Fn() -> usize>(name: &str, rows: usize, f: F) -> (f64, usize, String) {
    let iterations = 5;
    let mut total_time = 0.0;
    let mut output_size = 0;

    for _ in 0..iterations {
        let start = Instant::now();
        output_size = f();
        let duration = start.elapsed();
        total_time += duration.as_secs_f64() * 1000.0; // Convert to ms
    }

    let avg_ms = total_time / iterations as f64;
    let per_tuple = avg_ms * 1_000_000.0 / rows as f64;

    let line = format!(
        "{:<30} | {:<8} | {:<10.3} | {:<10.2} ns | {}",
        name, rows, avg_ms, per_tuple, output_size
    );

    println!("{}", line);

    (avg_ms, output_size, line)
}

fn run_and_log<F: Fn() -> usize>(
    writer: &mut BufWriter<File>,
    name: &str,
    rows: usize,
    f: F,
) {
    let (_, _, line) = benchmark(name, rows, f);
    writeln!(writer, "{}", line).unwrap();
}

fn make_executor(predicate: Predicate) -> SelectionExecutor {
    let schema = create_benchmark_schema();
    SelectionExecutor::new(predicate, schema).unwrap()
}

// Main benchmark entry point

fn benchmark_selection_operator() {
    let file = File::create("benchmark_output.txt").unwrap();
    let mut writer = BufWriter::new(file);

    log_line(&mut writer, &format!("\n{}", "=".repeat(80)));
    log_line(&mut writer, "SELECTION OPERATOR BENCHMARK SUITE");
    log_line(&mut writer, &"=".repeat(80));

    let dataset_sizes = vec![1_000, 10_000, 100_000, 1_000_000];

    for &size in &dataset_sizes {
        log_line(&mut writer, &format!("\n{}", "=".repeat(80)));
        log_line(&mut writer, &format!("Dataset Size: {} tuples", size));
        log_line(&mut writer, &"=".repeat(80));

        log_line(&mut writer, &format!("\nGenerating {} tuples...", size));
        let tuples = generate_tuples(size);
        log_line(&mut writer, "Generation complete.\n");

        log_line(&mut writer, &format!("{:<30} | {:<8} | {:<10} | {:<13} | {}", "## Test Name", "Rows", "Time(ms)", "ns/tuple", "Output"));
        log_line(&mut writer, &"-".repeat(90));

        // Full scan baseline
        run_and_log(&mut writer, "Full Scan", size, || {
            let mut count = 0;
            for t in &tuples {
                std::hint::black_box(t);
                count += 1;
            }
            count
        });

        // Basic predicates

        // id > 500
        let pred_id_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred_id_gt_500);
        run_and_log(&mut writer, "id > 500", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // amount > 500
        let pred_amount_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Float(500.0))),
        );
        let executor = make_executor(pred_amount_gt_500);
        run_and_log(&mut writer, "amount > 500", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // Selectivity tests

        // id > 10 (high selectivity - most pass)
        let pred_id_gt_10 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(10))),
        );
        let executor = make_executor(pred_id_gt_10);
        run_and_log(&mut writer, "id > 10 (high select)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // id > 500 (medium selectivity)
        let pred_id_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred_id_gt_500);
        run_and_log(&mut writer, "id > 500 (med select)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // id > 900 (low selectivity - few pass)
        let pred_id_gt_900 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(900))),
        );
        let executor = make_executor(pred_id_gt_900);
        run_and_log(&mut writer, "id > 900 (low select)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // AND / OR logic

        // id > 500 AND amount < 300
        let pred_and = Predicate::And(
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Int(500))),
            )),
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                ComparisonOp::LessThan,
                Box::new(Expr::Constant(Constant::Float(300.0))),
            )),
        );
        let executor = make_executor(pred_and);
        run_and_log(&mut writer, "id>500 AND amount<300", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // id > 500 OR amount < 300
        let pred_or = Predicate::Or(
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Int(500))),
            )),
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                ComparisonOp::LessThan,
                Box::new(Expr::Constant(Constant::Float(300.0))),
            )),
        );
        let executor = make_executor(pred_or);
        run_and_log(&mut writer, "id>500 OR amount<300", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // Short-circuit tests

        // id < 0 AND amount > 100 (false short-circuit)
        let pred_false_and = Predicate::And(
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                ComparisonOp::LessThan,
                Box::new(Expr::Constant(Constant::Int(0))),
            )),
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Float(100.0))),
            )),
        );
        let executor = make_executor(pred_false_and);
        run_and_log(&mut writer, "id<0 AND amt>100 (short)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // id > 0 OR amount > 100 (true short-circuit)
        let pred_true_or = Predicate::Or(
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Int(0))),
            )),
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Float(100.0))),
            )),
        );
        let executor = make_executor(pred_true_or);
        run_and_log(&mut writer, "id>0 OR amt>100 (short)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // Expressions

        // id + 10 > 500
        let pred_expr_add = Predicate::Compare(
            Box::new(Expr::Add(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                Box::new(Expr::Constant(Constant::Int(10))),
            )),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred_expr_add);
        run_and_log(&mut writer, "id + 10 > 500", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // amount * 2 > 1000
        let pred_expr_mul = Predicate::Compare(
            Box::new(Expr::Mul(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                Box::new(Expr::Constant(Constant::Float(2.0))),
            )),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Float(1000.0))),
        );
        let executor = make_executor(pred_expr_mul);
        run_and_log(&mut writer, "amount * 2 > 1000", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // NULL handling

        // name IS NULL
        let pred_is_null = Predicate::IsNull(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        );
        let executor = make_executor(pred_is_null);
        run_and_log(&mut writer, "name IS NULL", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // name IS NOT NULL
        let pred_is_not_null = Predicate::IsNotNull(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        );
        let executor = make_executor(pred_is_not_null);
        run_and_log(&mut writer, "name IS NOT NULL", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // name = "Alice"
        let pred_name_eq = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            ComparisonOp::Equals,
            Box::new(Expr::Constant(Constant::Text("Alice".to_string()))),
        );
        let executor = make_executor(pred_name_eq);
        run_and_log(&mut writer, "name = 'Alice'", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // LIKE operator

        // name LIKE "A%"
        let pred_like_a = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "A%".to_string(),
            None,
        );
        let executor = make_executor(pred_like_a);
        run_and_log(&mut writer, "name LIKE 'A%'", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // name LIKE "%li%"
        let pred_like_li = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "%li%".to_string(),
            None,
        );
        let executor = make_executor(pred_like_li);
        run_and_log(&mut writer, "name LIKE '%li%'", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // name LIKE "Bob"
        let pred_like_bob = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "Bob".to_string(),
            None,
        );
        let executor = make_executor(pred_like_bob);
        run_and_log(&mut writer, "name LIKE 'Bob'", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // IN operator

        // id IN (100, 200, 300)
        let pred_in_id = Predicate::In(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            vec![
                Expr::Constant(Constant::Int(100)),
                Expr::Constant(Constant::Int(200)),
                Expr::Constant(Constant::Int(300)),
            ],
        );
        let executor = make_executor(pred_in_id);
        run_and_log(&mut writer, "id IN (100,200,300)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // name IN ("Alice", "Bob")
        let pred_in_name = Predicate::In(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            vec![
                Expr::Constant(Constant::Text("Alice".to_string())),
                Expr::Constant(Constant::Text("Bob".to_string())),
            ],
        );
        let executor = make_executor(pred_in_name);
        run_and_log(&mut writer, "name IN ('Alice','Bob')", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // BETWEEN operator

        // id BETWEEN 200 AND 800
        let pred_between = Predicate::Between(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(200))),
            Box::new(Expr::Constant(Constant::Int(800))),
        );
        let executor = make_executor(pred_between);
        run_and_log(&mut writer, "id BETWEEN 200 AND 800", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // Type coercion tests removed - type validation now enforces strict type matching
        // at planning time to prevent type errors

        // Execution modes

        // Predicate: id > 500
        let pred = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred);

        // filter_tuples
        run_and_log(&mut writer, "EXEC: materialize", size, || {
            filter_tuples(&executor, &tuples).unwrap().len()
        });

        // filter_tuples_streaming (zero-copy: iter().clone() only yields borrowed bytes)
        run_and_log(&mut writer, "EXEC: streaming", size, || {
            let stream = tuples.iter().cloned().map(Ok);
            let mut count = 0;
            filter_tuples_streaming(&executor, stream, |_| count += 1).unwrap();
            count
        });

        // count_matching_tuples
        run_and_log(&mut writer, "EXEC: compute_only", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });

        // Worst-case predicate (both sides always evaluated, no short-circuit)
        let pred_worst = Predicate::And(
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Int(0))),
            )),
            Box::new(Predicate::Compare(
                Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
                ComparisonOp::GreaterThan,
                Box::new(Expr::Constant(Constant::Float(0.0))),
            )),
        );

        let executor = make_executor(pred_worst);

        run_and_log(&mut writer, "worst-case (no short-circuit)", size, || {
            count_matching_tuples(&executor, &tuples).unwrap()
        });
    }

    log_line(&mut writer, &format!("\n{}", "=".repeat(80)));
    log_line(&mut writer, "BENCHMARK SUITE COMPLETE");
    log_line(&mut writer, &"=".repeat(80));
}

fn main() {
    benchmark_selection_operator();
}
