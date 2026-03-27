//! Benchmarks for the eight cases specified in the spec.
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use storage_manager::executor::expr::Expr;
use storage_manager::executor::projection::{
    apply_distinct, eval_projection_list, filter_rows, ProjectionItem,
};
use storage_manager::executor::set_ops::{except, intersect, union};
use storage_manager::executor::value::Value;
use storage_manager::executor::projection::{OutputColumn, ResultTable};
use storage_manager::catalog::types::{Column, DataType};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn make_int_text_rows(n: usize) -> Vec<Vec<Value>> {
    (0..n)
        .map(|i| vec![Value::Int(i as i64), Value::Text(format!("name_{}", i))])
        .collect()
}

fn int_text_schema() -> Vec<Column> {
    vec![
        Column { name: "id".to_string(), data_type: "INT".to_string() },
        Column { name: "name".to_string(), data_type: "TEXT".to_string() },
    ]
}

fn make_result_table(rows: Vec<Vec<Value>>) -> ResultTable {
    ResultTable {
        columns: vec![
            OutputColumn { name: "id".to_string(), data_type: DataType::Int },
            OutputColumn { name: "name".to_string(), data_type: DataType::Text },
        ],
        rows,
    }
}

// ─── 1. Full Table Scan (iterate all rows, no filter) ──────────────────────

fn bench_full_scan(c: &mut Criterion) {
    let rows = make_int_text_rows(10_000);
    let schema = int_text_schema();
    let items = vec![ProjectionItem::Star];

    c.bench_function("full_table_scan_10k", |b| {
        b.iter(|| {
            let (_, out) = eval_projection_list(
                black_box(rows.clone()),
                black_box(&items),
                black_box(&schema),
            )
            .unwrap();
            black_box(out);
        })
    });
}

// ─── 2. Predicate Filtering ─────────────────────────────────────────────────

fn bench_predicate_filter(c: &mut Criterion) {
    let rows = make_int_text_rows(10_000);

    // Low selectivity: id > 9900 (matches ~100 rows)
    let pred_low = Expr::gt(Expr::col(0), Expr::int(9900));
    // High selectivity: id > 1 (matches ~9999 rows)
    let pred_high = Expr::gt(Expr::col(0), Expr::int(1));

    c.bench_function("predicate_low_selectivity_10k", |b| {
        b.iter(|| {
            let result = filter_rows(black_box(rows.clone()), black_box(&Some(pred_low.clone()))).unwrap();
            black_box(result);
        })
    });

    c.bench_function("predicate_high_selectivity_10k", |b| {
        b.iter(|| {
            let result = filter_rows(black_box(rows.clone()), black_box(&Some(pred_high.clone()))).unwrap();
            black_box(result);
        })
    });
}

// ─── 3. Multi-Predicate Queries ─────────────────────────────────────────────

fn bench_multi_predicate(c: &mut Criterion) {
    let rows = make_int_text_rows(10_000);
    // id > 1000 AND id < 5000 AND id != 3000
    let pred = Expr::and(
        Expr::and(
            Expr::gt(Expr::col(0), Expr::int(1000)),
            Expr::lt(Expr::col(0), Expr::int(5000)),
        ),
        Expr::ne(Expr::col(0), Expr::int(3000)),
    );

    c.bench_function("multi_predicate_and_chain_10k", |b| {
        b.iter(|| {
            let result = filter_rows(black_box(rows.clone()), black_box(&Some(pred.clone()))).unwrap();
            black_box(result);
        })
    });
}

// ─── 4. Subquery / CTE Execution ────────────────────────────────────────────

fn bench_cte_subquery(c: &mut Criterion) {
    // Simulate a CTE that produces 5000 rows, outer query filters further.
    let inner_rows = make_int_text_rows(5_000);
    let outer_pred = Expr::gt(Expr::col(0), Expr::int(2500));

    c.bench_function("cte_filter_5k", |b| {
        b.iter(|| {
            let result = filter_rows(black_box(inner_rows.clone()), black_box(&Some(outer_pred.clone()))).unwrap();
            black_box(result);
        })
    });
}

// ─── 5. DISTINCT Performance ────────────────────────────────────────────────

fn bench_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct");

    for (label, rows) in &[
        ("low_cardinality", {
            // 10k rows but only 10 distinct values
            (0..10_000).map(|i| vec![Value::Int((i % 10) as i64)]).collect::<Vec<_>>()
        }),
        ("high_cardinality", {
            // 10k rows all distinct
            (0..10_000).map(|i| vec![Value::Int(i as i64)]).collect::<Vec<_>>()
        }),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(label), rows, |b, rows| {
            b.iter(|| {
                let result = apply_distinct(black_box(rows.clone()));
                black_box(result);
            })
        });
    }
    group.finish();
}

// ─── 6. Set Operations ──────────────────────────────────────────────────────

fn bench_set_ops(c: &mut Criterion) {
    let rows_a = make_int_text_rows(5_000);
    let rows_b: Vec<Vec<Value>> = (2_500..7_500)
        .map(|i| vec![Value::Int(i as i64), Value::Text(format!("name_{}", i))])
        .collect();

    let a = || make_result_table(rows_a.clone());
    let b = || make_result_table(rows_b.clone());

    c.bench_function("union_5k_each", |bench| {
        bench.iter(|| black_box(union(a(), b(), false).unwrap()))
    });
    c.bench_function("intersect_5k_each", |bench| {
        bench.iter(|| black_box(intersect(a(), b(), false).unwrap()))
    });
    c.bench_function("except_5k_each", |bench| {
        bench.iter(|| black_box(except(a(), b(), false).unwrap()))
    });
}

// ─── 7. Expression Evaluation ────────────────────────────────────────────────

fn bench_expr_eval(c: &mut Criterion) {
    use storage_manager::executor::expr::eval_expr;

    let rows: Vec<Vec<Value>> = (0..10_000)
        .map(|i| vec![Value::Int(i as i64), Value::Int((i * 2) as i64), Value::Int(3)])
        .collect();

    // (a + b) * c > 1000
    let expr = Expr::gt(
        Expr::mul(
            Expr::add(Expr::col(0), Expr::col(1)),
            Expr::col(2),
        ),
        Expr::int(1000),
    );

    c.bench_function("expr_eval_arithmetic_10k", |b| {
        b.iter(|| {
            for row in &rows {
                black_box(eval_expr(black_box(&expr), black_box(row)).unwrap());
            }
        })
    });
}

// ─── 8. Empty Table Handling ─────────────────────────────────────────────────

fn bench_empty_table(c: &mut Criterion) {
    let schema = int_text_schema();
    let items = vec![ProjectionItem::Star];
    let pred = Expr::gt(Expr::col(0), Expr::int(0));

    c.bench_function("project_empty_table", |b| {
        b.iter(|| {
            let filtered = filter_rows(vec![], black_box(&Some(pred.clone()))).unwrap();
            let (cols, rows) = eval_projection_list(
                black_box(filtered),
                black_box(&items),
                black_box(&schema),
            ).unwrap();
            black_box((cols, rows));
        })
    });
}

criterion_group!(
    benches,
    bench_full_scan,
    bench_predicate_filter,
    bench_multi_predicate,
    bench_cte_subquery,
    bench_distinct,
    bench_set_ops,
    bench_expr_eval,
    bench_empty_table,
);
criterion_main!(benches);
