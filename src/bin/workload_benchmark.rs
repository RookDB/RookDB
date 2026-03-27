use std::env;
use std::fs;
use std::io;
use std::time::Instant;
use std::collections::HashMap;

use serde::Serialize;
use storage_manager::catalog::types::IndexAlgorithm;
use storage_manager::index::{AnyIndex, IndexKey, RecordId};

#[derive(Serialize)]
struct LatencyStats {
    min_us: f64,
    max_us: f64,
    avg_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    throughput_ops_s: f64,
}

#[derive(Serialize)]
struct AlgoMetrics {
    algorithm: String,
    insert_latency: LatencyStats,
    read_latency: LatencyStats,
    update_latency: LatencyStats,
    overall_latency: LatencyStats,
    total_ops: usize,
    successful_reads: usize,
    failed_reads: usize,
}

#[derive(Serialize)]
struct BenchmarkOutput {
    input_csv: String,
    total_operations: usize,
    algorithms: Vec<AlgoMetrics>,
}

enum Op {
    Insert(i64),
    Read(i64),
    Update(i64),
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[idx]
}

fn to_stats(samples: &mut Vec<f64>) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            min_us: 0.0,
            max_us: 0.0,
            avg_us: 0.0,
            p50_us: 0.0,
            p95_us: 0.0,
            p99_us: 0.0,
            throughput_ops_s: 0.0,
        };
    }

    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sum: f64 = samples.iter().sum();
    let avg = sum / samples.len() as f64;
    let ops_s = if avg > 0.0 { 1_000_000.0 / avg } else { 0.0 };

    LatencyStats {
        min_us: samples[0],
        max_us: samples[samples.len() - 1],
        avg_us: avg,
        p50_us: percentile(samples, 0.50),
        p95_us: percentile(samples, 0.95),
        p99_us: percentile(samples, 0.99),
        throughput_ops_s: ops_s,
    }
}

fn read_workload(csv_path: &str) -> io::Result<Vec<Op>> {
    let payload = fs::read_to_string(csv_path)?;
    let mut ops = Vec::new();

    for (i, line) in payload.lines().enumerate() {
        if i == 0 || line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 {
            continue;
        }
        let op_type = parts[0].trim();
        let key: i64 = parts[1].trim().parse().unwrap_or(0);
        
        match op_type {            "I" => ops.push(Op::Insert(key)),            "R" => ops.push(Op::Read(key)),            "U" => ops.push(Op::Update(key)),            _ => {}        }    }    Ok(ops)}
fn algorithms() -> [IndexAlgorithm; 9] {
    [
        IndexAlgorithm::StaticHash,
        IndexAlgorithm::ChainedHash,
        IndexAlgorithm::ExtendibleHash,
        IndexAlgorithm::LinearHash,
        IndexAlgorithm::BTree,
        IndexAlgorithm::BPlusTree,
        IndexAlgorithm::RadixTree,
        IndexAlgorithm::SkipList,
        IndexAlgorithm::LsmTree,
    ]
}

fn parse_args() -> (String, String) {
    let mut input = "Benchmarking/data/read_heavy_test.csv".to_string();
    let mut output = "Benchmarking/results/workload_metrics.json".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {            "--input" => {                if let Some(v) = args.next() {                    input = v;                }            }            "--output" => {                if let Some(v) = args.next() {                    output = v;                }            }            _ => {}        }    }
    (input, output)
}

fn run_for_algorithm(algo: &IndexAlgorithm, ops: &[Op]) -> io::Result<AlgoMetrics> {
    let mut idx = AnyIndex::new_empty(algo);
    let mut ground_truth: HashMap<i64, RecordId> = HashMap::new();
    
    let mut insert_latencies = Vec::new();
    let mut read_latencies = Vec::new();
    let mut update_latencies = Vec::new();
    let mut overall_latencies = Vec::new();

    let mut success_reads = 0;
    let mut fail_reads = 0;
    
    // Pseudo-random record id generator logic
    let mut record_counter: u32 = 0;

    for op in ops {
        match op {            Op::Insert(key) => {                record_counter += 1;                let rid = RecordId { page_no: record_counter, item_id: (record_counter % 100)  };                                let start = Instant::now();                let _ = idx.insert(IndexKey::Int(*key), rid.clone());                let dur = start.elapsed().as_secs_f64() * 1_000_000.0;                                insert_latencies.push(dur);                overall_latencies.push(dur);                ground_truth.insert(*key, rid);            }            Op::Read(key) => {                let start = Instant::now();                let res = idx.search(&IndexKey::Int(*key));                let dur = start.elapsed().as_secs_f64() * 1_000_000.0;                                read_latencies.push(dur);                overall_latencies.push(dur);                                if let Ok(results) = res {                    if !results.is_empty() {                        success_reads += 1;                    } else {                        fail_reads += 1;                    }                } else {                    fail_reads += 1;                }            }            Op::Update(key) => {                record_counter += 1;                let new_rid = RecordId { page_no: record_counter, item_id: (record_counter % 100)  };                                let start = Instant::now();                if let Some(old_rid) = ground_truth.get(key) {                    let _ = idx.delete(&IndexKey::Int(*key), old_rid);                }                let _ = idx.insert(IndexKey::Int(*key), new_rid.clone());                let dur = start.elapsed().as_secs_f64() * 1_000_000.0;                                update_latencies.push(dur);                overall_latencies.push(dur);                ground_truth.insert(*key, new_rid);            }        }    }
    Ok(AlgoMetrics {
        algorithm: format!("{:?}", algo),
        insert_latency: to_stats(&mut insert_latencies),
        read_latency: to_stats(&mut read_latencies),
        update_latency: to_stats(&mut update_latencies),
        overall_latency: to_stats(&mut overall_latencies),
        total_ops: ops.len(),
        successful_reads: success_reads,
        failed_reads: fail_reads,
    })
}

fn main() -> io::Result<()> {
    let (input, output) = parse_args();
    println!("Loading workload from {}...", input);
    
    let ops = read_workload(&input)?;
    println!("Loaded {} operations.", ops.len());

    let mut metrics_list = Vec::new();
    
    for algo in algorithms() {
        print!("Running workload on {:?}...", algo);
        if let Ok(metrics) = run_for_algorithm(&algo, &ops) {
            println!(" Done!");
            metrics_list.push(metrics);
        } else {
            println!(" Failed or Skipped.");
        }
    }

    let out_obj = BenchmarkOutput {
        input_csv: input,
        total_operations: ops.len(),
        algorithms: metrics_list,
    };

    let json = serde_json::to_string_pretty(&out_obj)?;
    if let Some(parent) = std::path::Path::new(&output).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, json)?;
    println!("Wrote results to {}", output);

    Ok(())
}
