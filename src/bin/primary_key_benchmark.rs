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
}

#[derive(Serialize)]
struct AlgoMetrics {
    algorithm: String,
    insert_latency: LatencyStats,
    search_latency: LatencyStats,
    total_keys: usize,
    missing_keys: usize,
    duplicate_hits: usize,
    wrong_record_ids: usize,
    unexpected_keys: usize,
    all_entries_mismatches: usize,
    range_scan_mismatches: usize,
    miss_checks_total: usize,
    miss_checks_failed: usize,
    structure_valid: bool,
    all_entries_ok: bool,
    range_scan_ok: bool,
    miss_checks_ok: bool,
    correctness_ok: bool,
}

#[derive(Serialize)]
struct BenchmarkOutput {
    input_csv: String,
    total_rows: usize,
    primary_key_unique_count: usize,
    algorithms: Vec<AlgoMetrics>,
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
        };
    }

    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sum: f64 = samples.iter().sum();

    LatencyStats {
        min_us: samples[0],
        max_us: samples[samples.len() - 1],
        avg_us: sum / samples.len() as f64,
        p50_us: percentile(samples, 0.50),
        p95_us: percentile(samples, 0.95),
        p99_us: percentile(samples, 0.99),
    }
}

fn read_primary_keys(csv_path: &str) -> io::Result<Vec<i64>> {
    let payload = fs::read_to_string(csv_path)?;
    let mut keys = Vec::new();

    for (i, line) in payload.lines().enumerate() {
        if i == 0 {
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        let first = line
            .split(',')
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed CSV row"))?;
        let id = first.trim().parse::<i64>().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid primary key at row {}: {}", i + 1, e),
            )
        })?;
        keys.push(id);
    }

    Ok(keys)
}

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
    let mut input = "Benchmarking/data/synthetic_orders.csv".to_string();
    let mut output = "Benchmarking/results/rookdb_primary_key_metrics.json".to_string();

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                if let Some(v) = args.next() {
                    input = v;
                }
            }
            "--output" => {
                if let Some(v) = args.next() {
                    output = v;
                }
            }
            _ => {}
        }
    }

    (input, output)
}

fn run_for_algorithm(algo: &IndexAlgorithm, keys: &[i64]) -> io::Result<AlgoMetrics> {
    let mut idx = AnyIndex::new_empty(algo);
    let mut expected: HashMap<i64, RecordId> = HashMap::with_capacity(keys.len());

    let mut insert_lat = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let rid = RecordId::new((i as u32 / 128) + 1, (i as u32 % 128) + 1);
        expected.insert(*key, rid.clone());
        let t0 = Instant::now();
        idx.insert(IndexKey::Int(*key), rid)?;
        insert_lat.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }

    let mut search_lat = Vec::with_capacity(keys.len());
    let mut missing = 0usize;
    let mut duplicate_hits = 0usize;
    let mut wrong_record_ids = 0usize;
    let mut unexpected_keys = 0usize;

    for key in keys {
        let t0 = Instant::now();
        let hits = idx.search(&IndexKey::Int(*key))?;
        search_lat.push(t0.elapsed().as_secs_f64() * 1_000_000.0);

        if hits.is_empty() {
            missing += 1;
        }
        if hits.len() > 1 {
            duplicate_hits += 1;
        }

        if let Some(exp) = expected.get(key) {
            if hits.len() == 1 && hits[0] != *exp {
                wrong_record_ids += 1;
            }
            if hits.len() == 1 && hits[0] == *exp {
                // valid exact match
            } else if !hits.is_empty() {
                // Non-empty but not exactly one expected RID is logically wrong.
                if hits.len() != 1 {
                    unexpected_keys += 1;
                }
            }
        }
    }

    let miss_checks_total = 1000usize;
    let mut miss_checks_failed = 0usize;
    for i in 0..miss_checks_total as i64 {
        let k = -10_000_000 - i;
        let hits = idx.search(&IndexKey::Int(k))?;
        if !hits.is_empty() {
            miss_checks_failed += 1;
        }
    }
    let miss_checks_ok = miss_checks_failed == 0;

    let structure_valid = idx.validate_structure().is_ok();

    let mut all_entries_mismatches = 0usize;
    let mut all_entries_ok = true;
    let entries = idx.all_entries()?;
    if entries.len() != expected.len() {
        all_entries_ok = false;
        all_entries_mismatches += entries.len().abs_diff(expected.len());
    }
    for (k, rid) in &entries {
        match k {
            IndexKey::Int(v) => match expected.get(v) {
                Some(exp_rid) if exp_rid == rid => {}
                Some(_) => {
                    all_entries_ok = false;
                    all_entries_mismatches += 1;
                }
                None => {
                    all_entries_ok = false;
                    all_entries_mismatches += 1;
                    unexpected_keys += 1;
                }
            },
            _ => {
                all_entries_ok = false;
                all_entries_mismatches += 1;
                unexpected_keys += 1;
            }
        }
    }

    let mut range_scan_mismatches = 0usize;
    let mut range_scan_ok = true;
    if idx.supports_range_scan() && !keys.is_empty() {
        let mut sorted_keys = keys.to_vec();
        sorted_keys.sort_unstable();
        let start = sorted_keys[0];
        let end = sorted_keys[sorted_keys.len() - 1];
        let got = idx.range_scan(&IndexKey::Int(start), &IndexKey::Int(end))?;

        let mut expected_rids = Vec::with_capacity(sorted_keys.len());
        for k in sorted_keys {
            if let Some(r) = expected.get(&k) {
                expected_rids.push(r.clone());
            }
        }

        if got.len() != expected_rids.len() {
            range_scan_ok = false;
            range_scan_mismatches += got.len().abs_diff(expected_rids.len());
        }

        let cmp_len = got.len().min(expected_rids.len());
        for i in 0..cmp_len {
            if got[i] != expected_rids[i] {
                range_scan_ok = false;
                range_scan_mismatches += 1;
            }
        }
    }

    let correctness_ok = missing == 0
        && duplicate_hits == 0
        && wrong_record_ids == 0
        && unexpected_keys == 0
        && miss_checks_ok
        && structure_valid
        && all_entries_ok
        && range_scan_ok;

    Ok(AlgoMetrics {
        algorithm: algo.display_name().to_string(),
        insert_latency: to_stats(&mut insert_lat),
        search_latency: to_stats(&mut search_lat),
        total_keys: keys.len(),
        missing_keys: missing,
        duplicate_hits,
        wrong_record_ids,
        unexpected_keys,
        all_entries_mismatches,
        range_scan_mismatches,
        miss_checks_total,
        miss_checks_failed,
        structure_valid,
        all_entries_ok,
        range_scan_ok,
        miss_checks_ok,
        correctness_ok,
    })
}

fn main() -> io::Result<()> {
    let (input, output) = parse_args();

    let keys = read_primary_keys(&input)?;
    let unique_count = {
        let mut dedup = keys.clone();
        dedup.sort_unstable();
        dedup.dedup();
        dedup.len()
    };

    let mut rows = Vec::new();
    for algo in algorithms() {
        rows.push(run_for_algorithm(&algo, &keys)?);
    }

    let payload = BenchmarkOutput {
        input_csv: input,
        total_rows: keys.len(),
        primary_key_unique_count: unique_count,
        algorithms: rows,
    };

    let out_text = serde_json::to_string_pretty(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize failed: {e}")))?;

    if let Some(parent) = std::path::Path::new(&output).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, out_text)?;

    println!("Wrote RookDB primary-key benchmark report to {}", output);
    Ok(())
}
