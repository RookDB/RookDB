use std::env;
use std::fs;
use std::io;
use std::time::Instant;
use std::collections::HashMap;
use std::path::Path;

use serde::Serialize;
use storage_manager::catalog::types::IndexAlgorithm;
use storage_manager::index::paged_store;
use storage_manager::index::tree::BTree;
use storage_manager::index::{AnyIndex, IndexKey, RecordId};
use storage_manager::page::PAGE_SIZE;

#[derive(Debug, Clone)]
struct BenchmarkArgs {
    input: String,
    output: String,
    algorithms: Vec<IndexAlgorithm>,
    btree_min_degree: Option<usize>,
    index_page_size: Option<usize>,
    persist_index_dir: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct RunOptions {
    btree_min_degree: Option<usize>,
    index_page_size: Option<usize>,
}

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
struct IndexFileMetrics {
    path: String,
    page_size_bytes: u64,
    page_count: u64,
    file_size_bytes: u64,
}

#[derive(Serialize)]
struct AlgoMetrics {
    algorithm: String,
    insert_latency: LatencyStats,
    search_latency: LatencyStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_file: Option<IndexFileMetrics>,
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

fn default_algorithms() -> Vec<IndexAlgorithm> {
    vec![
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

fn parse_algorithms(raw: &str) -> io::Result<Vec<IndexAlgorithm>> {
    let mut out = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let algo = IndexAlgorithm::from_str(trimmed).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown algorithm '{}' in --algorithms", trimmed),
            )
        })?;
        out.push(algo);
    }

    if out.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--algorithms must contain at least one valid algorithm",
        ));
    }

    Ok(out)
}

fn parse_usize_arg(value: &str, flag: &str) -> io::Result<usize> {
    value.parse::<usize>().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid value for {}: {}", flag, e),
        )
    })
}

fn parse_args() -> io::Result<BenchmarkArgs> {
    let mut input = "Benchmarking/data/synthetic_orders.csv".to_string();
    let mut output = "Benchmarking/results/rookdb_primary_key_metrics.json".to_string();
    let mut algorithms = default_algorithms();
    let mut btree_min_degree = None;
    let mut index_page_size = None;
    let mut persist_index_dir = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                if let Some(v) = args.next() {
                    input = v;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --input",
                    ));
                }
            }
            "--output" => {
                if let Some(v) = args.next() {
                    output = v;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --output",
                    ));
                }
            }
            "--algorithms" => {
                if let Some(v) = args.next() {
                    algorithms = parse_algorithms(&v)?;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --algorithms",
                    ));
                }
            }
            "--btree-min-degree" => {
                if let Some(v) = args.next() {
                    let parsed = parse_usize_arg(&v, "--btree-min-degree")?;
                    if parsed < 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--btree-min-degree must be >= 2",
                        ));
                    }
                    btree_min_degree = Some(parsed);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --btree-min-degree",
                    ));
                }
            }
            "--index-page-size" => {
                if let Some(v) = args.next() {
                    let parsed = parse_usize_arg(&v, "--index-page-size")?;
                    index_page_size = Some(parsed);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --index-page-size",
                    ));
                }
            }
            "--persist-index-dir" => {
                if let Some(v) = args.next() {
                    persist_index_dir = Some(v);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "missing value for --persist-index-dir",
                    ));
                }
            }
            _ => {}
        }
    }

    Ok(BenchmarkArgs {
        input,
        output,
        algorithms,
        btree_min_degree,
        index_page_size,
        persist_index_dir,
    })
}

fn algorithm_slug(algo: &IndexAlgorithm) -> &'static str {
    match algo {
        IndexAlgorithm::StaticHash => "static_hash",
        IndexAlgorithm::ChainedHash => "chained_hash",
        IndexAlgorithm::ExtendibleHash => "extendible_hash",
        IndexAlgorithm::LinearHash => "linear_hash",
        IndexAlgorithm::BTree => "btree",
        IndexAlgorithm::BPlusTree => "bplus_tree",
        IndexAlgorithm::RadixTree => "radix_tree",
        IndexAlgorithm::SkipList => "skip_list",
        IndexAlgorithm::LsmTree => "lsm_tree",
    }
}

fn persist_index_file(
    idx: &AnyIndex,
    path: &Path,
    page_size_override: Option<usize>,
) -> io::Result<IndexFileMetrics> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let path_str = path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "index output path contains non-UTF8 characters",
        )
    })?;

    if let Some(page_size) = page_size_override {
        let entries = idx.all_entries()?;
        paged_store::save_entries_with_page_size(path_str, entries.into_iter(), page_size)?;
    } else {
        idx.save(path_str)?;
    }

    let file_size_bytes = fs::metadata(path)?.len();
    let page_size_bytes = page_size_override.unwrap_or(PAGE_SIZE) as u64;
    let page_count = if page_size_bytes == 0 {
        0
    } else {
        (file_size_bytes + page_size_bytes - 1) / page_size_bytes
    };

    Ok(IndexFileMetrics {
        path: path.to_string_lossy().to_string(),
        page_size_bytes,
        page_count,
        file_size_bytes,
    })
}

fn run_for_algorithm(
    algo: &IndexAlgorithm,
    keys: &[i64],
    opts: RunOptions,
    persist_index_path: Option<&Path>,
) -> io::Result<AlgoMetrics> {
    let mut idx = match (algo, opts.btree_min_degree) {
        (IndexAlgorithm::BTree, Some(t)) => AnyIndex::BTree(BTree::new(t)),
        _ => AnyIndex::new_empty(algo),
    };
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

    let index_file = if let Some(path) = persist_index_path {
        Some(persist_index_file(&idx, path, opts.index_page_size)?)
    } else {
        None
    };

    Ok(AlgoMetrics {
        algorithm: algo.display_name().to_string(),
        insert_latency: to_stats(&mut insert_lat),
        search_latency: to_stats(&mut search_lat),
        index_file,
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
    let args = parse_args()?;

    let keys = read_primary_keys(&args.input)?;
    let unique_count = {
        let mut dedup = keys.clone();
        dedup.sort_unstable();
        dedup.dedup();
        dedup.len()
    };

    let mut rows = Vec::new();
    let run_options = RunOptions {
        btree_min_degree: args.btree_min_degree,
        index_page_size: args.index_page_size,
    };

    for algo in &args.algorithms {
        let persist_path = args.persist_index_dir.as_ref().map(|dir| {
            Path::new(dir).join(format!(
                "{}_d{}_p{}.idx",
                algorithm_slug(algo),
                run_options.btree_min_degree.unwrap_or(0),
                run_options.index_page_size.unwrap_or(PAGE_SIZE),
            ))
        });
        rows.push(run_for_algorithm(
            algo,
            &keys,
            run_options,
            persist_path.as_deref(),
        )?);
    }

    let payload = BenchmarkOutput {
        input_csv: args.input,
        total_rows: keys.len(),
        primary_key_unique_count: unique_count,
        algorithms: rows,
    };

    let out_text = serde_json::to_string_pretty(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize failed: {e}")))?;

    if let Some(parent) = std::path::Path::new(&args.output).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.output, out_text)?;

    println!("Wrote RookDB primary-key benchmark report to {}", args.output);
    Ok(())
}
