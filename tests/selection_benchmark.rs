// Comprehensive benchmark suite for the Selection Operator
// Run with: cargo test --test test_selection_benchmark -- --nocapture

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;
use rand::Rng;
use std::time::Instant;

// ===== TUPLE BUILDER =====

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

// ===== SCHEMA =====

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

// ===== TUPLE GENERATION =====

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

// ===== BENCHMARK HELPERS =====

fn benchmark<F: Fn() -> usize>(name: &str, rows: usize, f: F) -> (f64, usize) {
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
    println!("{:<30} | {:<8} | {:<10.3} | {}", name, rows, avg_ms, output_size);

    (avg_ms, output_size)
}

fn make_executor(predicate:    Predicate) -> SelectionExecutor {
    let schema = create_benchmark_schema();
    SelectionExecutor::new(predicate, schema).unwrap()
}

// ===== STREAMING HELPER =====

struct TupleStream {
    tuples: Vec<Vec<u8>>,
    index: usize,
}

impl TupleStream {
    fn new(tuples: Vec<Vec<u8>>) -> Self {
        TupleStream { tuples, index: 0 }
    }
}

impl Iterator for TupleStream {
    type Item = Result<Vec<u8>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.tuples.len() {
            let tuple = self.tuples[self.index].clone();
            self.index += 1;
            Some(Ok(tuple))
        } else {
            None
        }
    }
}

// ===== MAIN BENCHMARK TEST =====

#[test]
fn benchmark_selection_operator() {
    println!("\n{}", "=".repeat(80));
    println!("SELECTION OPERATOR BENCHMARK SUITE");
    println!("{}", "=".repeat(80));

    let dataset_sizes = vec![1_000, 10_000, 100_000];

    for &size in &dataset_sizes {
        println!("\n{}", "=".repeat(80));
        println!("Dataset Size: {} tuples", size);
        println!("{}", "=".repeat(80));

        println!("\nGenerating {} tuples...", size);
        let tuples = generate_tuples(size);
        println!("Generation complete.\n");

        println!("## Test Name                     | Rows     | Time(ms)   | Output");
        println!("{}", "-".repeat(80));

        // ===== A. FULL SCAN =====
        benchmark("Full Scan", size, || tuples.len());

        // ===== B. BASIC PREDICATES =====

        // id > 500
        let pred_id_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred_id_gt_500);
        benchmark("id > 500", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // amount > 500
        let pred_amount_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("amount".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Float(500.0))),
        );
        let executor = make_executor(pred_amount_gt_500);
        benchmark("amount > 500", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== C. SELECTIVITY TESTS =====

        // id > 10 (high selectivity - most pass)
        let pred_id_gt_10 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(10))),
        );
        let executor = make_executor(pred_id_gt_10);
        benchmark("id > 10 (high select)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // id > 500 (medium selectivity)
        let pred_id_gt_500 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred_id_gt_500);
        benchmark("id > 500 (med select)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // id > 900 (low selectivity - few pass)
        let pred_id_gt_900 = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(900))),
        );
        let executor = make_executor(pred_id_gt_900);
        benchmark("id > 900 (low select)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== D. AND / OR LOGIC =====

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
        benchmark("id>500 AND amount<300", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
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
        benchmark("id>500 OR amount<300", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== SHORT-CIRCUIT TESTS =====

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
        benchmark("id<0 AND amt>100 (short)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
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
        benchmark("id>0 OR amt>100 (short)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== E. EXPRESSIONS =====

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
        benchmark("id + 10 > 500", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
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
        benchmark("amount * 2 > 1000", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== F. NULL HANDLING =====

        // name IS NULL
        let pred_is_null = Predicate::IsNull(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        );
        let executor = make_executor(pred_is_null);
        benchmark("name IS NULL", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // name IS NOT NULL
        let pred_is_not_null = Predicate::IsNotNull(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        );
        let executor = make_executor(pred_is_not_null);
        benchmark("name IS NOT NULL", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // name = "Alice"
        let pred_name_eq = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            ComparisonOp::Equals,
            Box::new(Expr::Constant(Constant::Text("Alice".to_string()))),
        );
        let executor = make_executor(pred_name_eq);
        benchmark("name = 'Alice'", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== G. LIKE =====

        // name LIKE "A%"
        let pred_like_a = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "A%".to_string(),
            None,
        );
        let executor = make_executor(pred_like_a);
        benchmark("name LIKE 'A%'", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // name LIKE "%li%"
        let pred_like_li = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "%li%".to_string(),
            None,
        );
        let executor = make_executor(pred_like_li);
        benchmark("name LIKE '%li%'", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // name LIKE "Bob"
        let pred_like_bob = Predicate::Like(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            "Bob".to_string(),
            None,
        );
        let executor = make_executor(pred_like_bob);
        benchmark("name LIKE 'Bob'", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== H. IN =====

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
        benchmark("id IN (100,200,300)", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
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
        benchmark("name IN ('Alice','Bob')", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== I. BETWEEN =====

        // id BETWEEN 200 AND 800
        let pred_between = Predicate::Between(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(200))),
            Box::new(Expr::Constant(Constant::Int(800))),
        );
        let executor = make_executor(pred_between);
        benchmark("id BETWEEN 200 AND 800", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // ===== J. TYPE COERCION =====
        // NOTE: Type coercion tests removed - type validation now enforces strict type matching
        // at planning time to prevent type errors

        // ===== K. EXECUTION MODES =====

        // Predicate: id > 500
        let pred = Predicate::Compare(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            ComparisonOp::GreaterThan,
            Box::new(Expr::Constant(Constant::Int(500))),
        );
        let executor = make_executor(pred);

        // 1. filter_tuples
        benchmark("EXEC: filter_tuples", size, || {
            filter_tuples(&executor, tuples.clone()).unwrap().len()
        });

        // 2. filter_tuples_streaming
        benchmark("EXEC: streaming", size, || {
            let stream = TupleStream::new(tuples.clone());
            let mut count = 0;
            filter_tuples_streaming(&executor, stream, |_| count += 1).unwrap();
            count
        });

        // 3. count_matching_tuples
        benchmark("EXEC: count_only", size, || {
            count_matching_tuples(&executor, tuples.clone()).unwrap()
        });
    }

    println!("\n{}", "=".repeat(80));
    println!("BENCHMARK SUITE COMPLETE");
    println!("{}", "=".repeat(80));
}
