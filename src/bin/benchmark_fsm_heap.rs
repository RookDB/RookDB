use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use storage_manager::backend::disk::{read_all_pages, read_header_page};
use storage_manager::backend::fsm::FSM;
use storage_manager::backend::heap::HeapManager;
use storage_manager::backend::page::{get_tuple_count, page_free_space};
use sysinfo::System;

#[derive(Debug, Clone)]
struct Config {
    small_tuples: u32,
    small_tuple_size: usize,
    large_tuples: u32,
    large_tuple_size: usize,
    lookup_samples: u32,
    output: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            small_tuples: 20_000,
            small_tuple_size: 50,
            large_tuples: 1_000,
            large_tuple_size: 1_000,
            lookup_samples: 1_000,
            output: PathBuf::from("benchmark_runs/latest_fsm_heap_benchmark.json"),
        }
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    run_id_unix: u64,
    system: SystemInfo,
    config: RunConfig,
    correctness: CorrectnessSummary,
    robustness: RobustnessSummary,
    performance: PerformanceSummary,
    scalability: ScalabilitySummary,
    notes: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct BenchmarkHistoryRow {
    run_id_unix: u64,
    output_file: String,
    small_tuples: u32,
    large_tuples: u32,
    lookup_samples: u32,
    inserted_total: u32,
    small_insert_tps: f64,
    large_insert_tps: f64,
    lookup_ops: f64,
    scan_tps: f64,
    fsm_rebuild_seconds: f64,
    scan_matches_insert_count: bool,
    oversized_tuple_rejected: bool,
    fsm_rebuild_search_found_page: bool,
}

#[derive(Serialize)]
struct SystemInfo {
    os_name: String,
    os_version: String,
    kernel_version: String,
    architecture: String,
    cpu_brand: String,
    logical_cores: usize,
    total_memory_bytes_sysinfo: u64,
}

#[derive(Serialize)]
struct RunConfig {
    small_tuples: u32,
    small_tuple_size: usize,
    large_tuples: u32,
    large_tuple_size: usize,
    lookup_samples: u32,
}

#[derive(Serialize)]
struct CorrectnessSummary {
    inserted_total: u32,
    scanned_total: u32,
    point_lookup_samples: u32,
    point_lookup_passed: u32,
    scan_matches_insert_count: bool,
}

#[derive(Serialize)]
struct RobustnessSummary {
    oversized_tuple_rejected: bool,
    fsm_rebuild_search_found_page: bool,
}

#[derive(Serialize)]
struct PerformanceSummary {
    small_insert_seconds: f64,
    small_insert_tuples_per_sec: f64,
    large_insert_seconds: f64,
    large_insert_tuples_per_sec: f64,
    point_lookup_seconds: f64,
    point_lookup_ops_per_sec: f64,
    seq_scan_seconds: f64,
    seq_scan_tuples_per_sec: f64,
    fsm_rebuild_seconds: f64,
}

#[derive(Serialize)]
struct ScalabilitySummary {
    heap_page_count: u32,
    fsm_page_count: u32,
    pages_used_with_tuples: u32,
    avg_tuples_per_used_page: f64,
    avg_free_bytes_on_used_pages: f64,
}

fn parse_config() -> io::Result<Config> {
    let mut config = Config::default();
    let args: Vec<String> = env::args().skip(1).collect();

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--small-tuples" => {
                index += 1;
                config.small_tuples = parse_u32_arg(&args, index, "--small-tuples")?;
            }
            "--small-size" => {
                index += 1;
                config.small_tuple_size = parse_usize_arg(&args, index, "--small-size")?;
            }
            "--large-tuples" => {
                index += 1;
                config.large_tuples = parse_u32_arg(&args, index, "--large-tuples")?;
            }
            "--large-size" => {
                index += 1;
                config.large_tuple_size = parse_usize_arg(&args, index, "--large-size")?;
            }
            "--lookup-samples" => {
                index += 1;
                config.lookup_samples = parse_u32_arg(&args, index, "--lookup-samples")?;
            }
            "--output" => {
                index += 1;
                if index >= args.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Missing value for --output",
                    ));
                }
                config.output = PathBuf::from(&args[index]);
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Unknown argument: {}", unknown),
                ));
            }
        }
        index += 1;
    }

    if config.small_tuple_size == 0 || config.large_tuple_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Tuple sizes must be > 0",
        ));
    }

    Ok(config)
}

fn parse_u32_arg(args: &[String], index: usize, flag: &str) -> io::Result<u32> {
    if index >= args.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Missing value for {}", flag),
        ));
    }
    args[index].parse::<u32>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid numeric value for {}", flag),
        )
    })
}

fn parse_usize_arg(args: &[String], index: usize, flag: &str) -> io::Result<usize> {
    if index >= args.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Missing value for {}", flag),
        ));
    }
    args[index].parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid numeric value for {}", flag),
        )
    })
}

fn print_help() {
    println!("FSM/Heap Benchmark Runner");
    println!("Usage: cargo run --bin benchmark_fsm_heap -- [options]\n");
    println!("Options:");
    println!("  --small-tuples <n>     Number of small tuple inserts (default: 20000)");
    println!("  --small-size <bytes>   Small tuple size in bytes (default: 50)");
    println!("  --large-tuples <n>     Number of large tuple inserts (default: 1000)");
    println!("  --large-size <bytes>   Large tuple size in bytes (default: 1000)");
    println!("  --lookup-samples <n>   Number of point lookups (default: 1000)");
    println!("  --output <path>        JSON report output path");
    println!("  -h, --help             Show this help\n");
}

fn make_tuple(size: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    if size == 0 {
        return bytes;
    }

    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = ((seed as usize + index) % 251) as u8;
    }

    let seed_bytes = seed.to_le_bytes();
    let copy_len = seed_bytes.len().min(size);
    bytes[0..copy_len].copy_from_slice(&seed_bytes[0..copy_len]);
    bytes
}

fn collect_system_info() -> SystemInfo {
    let mut system = System::new_all();
    system.refresh_all();

    let os_name = System::name().unwrap_or_else(|| "unknown".to_string());
    let os_version = System::os_version().unwrap_or_else(|| "unknown".to_string());
    let kernel_version = System::kernel_version().unwrap_or_else(|| "unknown".to_string());
    let architecture = std::env::consts::ARCH.to_string();
    let cpu_brand = system
        .cpus()
        .first()
        .map(|cpu| cpu.brand().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    SystemInfo {
        os_name,
        os_version,
        kernel_version,
        architecture,
        cpu_brand,
        logical_cores: system.cpus().len(),
        total_memory_bytes_sysinfo: system.total_memory(),
    }
}

fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn append_history_jsonl(path: &Path, row: &BenchmarkHistoryRow) -> io::Result<()> {
    ensure_parent_dir(path)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(row)?;
    writeln!(file, "{}", line)?;
    Ok(())
}

fn append_history_csv(path: &Path, row: &BenchmarkHistoryRow) -> io::Result<()> {
    ensure_parent_dir(path)?;
    let file_exists = path.exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !file_exists {
        writeln!(
            file,
            "run_id_unix,output_file,small_tuples,large_tuples,lookup_samples,inserted_total,small_insert_tps,large_insert_tps,lookup_ops,scan_tps,fsm_rebuild_seconds,scan_matches_insert_count,oversized_tuple_rejected,fsm_rebuild_search_found_page"
        )?;
    }

    writeln!(
        file,
        "{},{},{},{},{},{},{:.4},{:.4},{:.4},{:.4},{:.6},{},{},{}",
        row.run_id_unix,
        row.output_file,
        row.small_tuples,
        row.large_tuples,
        row.lookup_samples,
        row.inserted_total,
        row.small_insert_tps,
        row.large_insert_tps,
        row.lookup_ops,
        row.scan_tps,
        row.fsm_rebuild_seconds,
        row.scan_matches_insert_count,
        row.oversized_tuple_rejected,
        row.fsm_rebuild_search_found_page
    )?;
    Ok(())
}

fn refresh_docs_benchmark_log(
    docs_file: &Path,
    latest: &BenchmarkHistoryRow,
    history_csv: &Path,
    latest_json: &Path,
) -> io::Result<()> {
    ensure_parent_dir(docs_file)?;

    let start_marker = "<!-- BENCHMARK_RUN_LOG_START -->";
    let end_marker = "<!-- BENCHMARK_RUN_LOG_END -->";

    let generated = format!(
        "{start}\n\
### Auto-updated Benchmark Run Log\n\
\n\
Latest run is injected automatically by `cargo run --bin benchmark_fsm_heap ...`.\n\
\n\
- Latest run id: `{run_id}`\n\
- Latest JSON report: `{latest_json}`\n\
- History CSV: `{history_csv}`\n\
\n\
| Run ID | Small TPS | Large TPS | Lookup OPS | Scan TPS | Rebuild sec | Correctness | Oversize Reject |\n\
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :---: |\n\
| `{run_id}` | {small_tps:.2} | {large_tps:.2} | {lookup_ops:.2} | {scan_tps:.2} | {rebuild:.6} | {correct} | {oversized} |\n\
\n\
> Re-run the benchmark command to refresh this section and append to history files.\n\
{end}",
        start = start_marker,
        end = end_marker,
        run_id = latest.run_id_unix,
        latest_json = latest_json.display(),
        history_csv = history_csv.display(),
        small_tps = latest.small_insert_tps,
        large_tps = latest.large_insert_tps,
        lookup_ops = latest.lookup_ops,
        scan_tps = latest.scan_tps,
        rebuild = latest.fsm_rebuild_seconds,
        correct = if latest.scan_matches_insert_count { "✅" } else { "❌" },
        oversized = if latest.oversized_tuple_rejected { "✅" } else { "❌" },
    );

    let mut content = if docs_file.exists() {
        fs::read_to_string(docs_file)?
    } else {
        String::new()
    };

    if let (Some(start), Some(end)) = (content.find(start_marker), content.find(end_marker)) {
        let end_idx = end + end_marker.len();
        content.replace_range(start..end_idx, &generated);
    } else {
        if !content.ends_with('\n') {
            content.push('\n');
        }
        content.push('\n');
        content.push_str(&generated);
        content.push('\n');
    }

    fs::write(docs_file, content)?;
    Ok(())
}

fn main() -> io::Result<()> {
    let config = parse_config()?;
    ensure_parent_dir(&config.output)?;

    let run_id_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let heap_path = PathBuf::from(format!(
        "benchmark_runs/fsm_heap_bench_{}.dat",
        run_id_unix
    ));
    let fsm_path = heap_path.with_extension("dat.fsm");

    if heap_path.exists() {
        fs::remove_file(&heap_path)?;
    }
    if fsm_path.exists() {
        fs::remove_file(&fsm_path)?;
    }

    fs::create_dir_all("benchmark_runs")?;

    let system = collect_system_info();

    println!("[BENCH] Starting benchmark run: {}", run_id_unix);
    println!(
        "[BENCH] Config: small={}x{}, large={}x{}, lookups={}",
        config.small_tuples,
        config.small_tuple_size,
        config.large_tuples,
        config.large_tuple_size,
        config.lookup_samples
    );

    let mut manager = HeapManager::create(heap_path.clone())?;
    let mut coordinates: Vec<(u32, u32)> = Vec::new();

    let lookup_stride = (config.small_tuples / config.lookup_samples.max(1)).max(1);

    let small_start = Instant::now();
    for index in 0..config.small_tuples {
        let tuple = make_tuple(config.small_tuple_size, index as u64);
        let coord = manager.insert_tuple(&tuple)?;
        if index % lookup_stride == 0 {
            coordinates.push(coord);
        }
    }
    let small_elapsed = small_start.elapsed().as_secs_f64();

    let large_start = Instant::now();
    for index in 0..config.large_tuples {
        let tuple = make_tuple(config.large_tuple_size, (index as u64) + 1_000_000);
        manager.insert_tuple(&tuple)?;
    }
    let large_elapsed = large_start.elapsed().as_secs_f64();

    // Correctness + robustness check: oversized tuple should fail
    let oversized = vec![0xEE; 9000];
    let oversized_tuple_rejected = manager.insert_tuple(&oversized).is_err();

    let lookup_count = coordinates.len() as u32;
    let lookup_start = Instant::now();
    let mut lookup_passed = 0u32;
    for (page_id, slot_id) in coordinates.iter().copied() {
        if manager.get_tuple(page_id, slot_id).is_ok() {
            lookup_passed += 1;
        }
    }
    let lookup_elapsed = lookup_start.elapsed().as_secs_f64();

    let scan_start = Instant::now();
    let mut scanned_total = 0u32;
    for item in manager.scan() {
        if item.is_ok() {
            scanned_total += 1;
        }
    }
    let scan_elapsed = scan_start.elapsed().as_secs_f64();

    manager.flush()?;

    let mut heap_file_for_header = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&heap_path)?;
    let header = read_header_page(&mut heap_file_for_header)?;

    let mut heap_file_for_pages = OpenOptions::new().read(true).open(&heap_path)?;
    let all_pages = read_all_pages(&mut heap_file_for_pages)?;

    let mut pages_used_with_tuples = 0u32;
    let mut total_free_bytes_used_pages = 0u64;
    let mut total_tuples_in_used_pages = 0u64;

    for page in all_pages.iter().skip(1) {
        let tuple_count = get_tuple_count(page).unwrap_or(0);
        if tuple_count > 0 {
            pages_used_with_tuples += 1;
            total_tuples_in_used_pages += tuple_count as u64;
            total_free_bytes_used_pages += page_free_space(page).unwrap_or(0) as u64;
        }
    }

    let mut heap_file_for_rebuild = File::open(&heap_path)?;
    let rebuild_start = Instant::now();
    let mut rebuilt_fsm = FSM::build_from_heap(&mut heap_file_for_rebuild, fsm_path.clone())?;
    let rebuild_elapsed = rebuild_start.elapsed().as_secs_f64();

    let fsm_rebuild_search_found_page = rebuilt_fsm.fsm_search_avail(1)?.is_some();

    let inserted_total = config.small_tuples + config.large_tuples;

    let performance = PerformanceSummary {
        small_insert_seconds: small_elapsed,
        small_insert_tuples_per_sec: config.small_tuples as f64 / small_elapsed.max(0.000_001),
        large_insert_seconds: large_elapsed,
        large_insert_tuples_per_sec: config.large_tuples as f64 / large_elapsed.max(0.000_001),
        point_lookup_seconds: lookup_elapsed,
        point_lookup_ops_per_sec: lookup_count as f64 / lookup_elapsed.max(0.000_001),
        seq_scan_seconds: scan_elapsed,
        seq_scan_tuples_per_sec: scanned_total as f64 / scan_elapsed.max(0.000_001),
        fsm_rebuild_seconds: rebuild_elapsed,
    };

    let scalability = ScalabilitySummary {
        heap_page_count: header.page_count,
        fsm_page_count: header.fsm_page_count,
        pages_used_with_tuples,
        avg_tuples_per_used_page: if pages_used_with_tuples > 0 {
            total_tuples_in_used_pages as f64 / pages_used_with_tuples as f64
        } else {
            0.0
        },
        avg_free_bytes_on_used_pages: if pages_used_with_tuples > 0 {
            total_free_bytes_used_pages as f64 / pages_used_with_tuples as f64
        } else {
            0.0
        },
    };

    let correctness = CorrectnessSummary {
        inserted_total,
        scanned_total,
        point_lookup_samples: lookup_count,
        point_lookup_passed: lookup_passed,
        scan_matches_insert_count: scanned_total == inserted_total,
    };

    let robustness = RobustnessSummary {
        oversized_tuple_rejected,
        fsm_rebuild_search_found_page,
    };

    let report = BenchmarkReport {
        run_id_unix,
        system,
        config: RunConfig {
            small_tuples: config.small_tuples,
            small_tuple_size: config.small_tuple_size,
            large_tuples: config.large_tuples,
            large_tuple_size: config.large_tuple_size,
            lookup_samples: config.lookup_samples,
        },
        correctness,
        robustness,
        performance,
        scalability,
        notes: vec![
            "This benchmark is single-process and focuses on FSM/Heap manager behavior.".to_string(),
            "Use repeated runs and median values for stable reporting.".to_string(),
            "sysinfo memory unit depends on crate behavior/version; record as raw value in report.".to_string(),
        ],
    };

    let json = serde_json::to_string_pretty(&report)?;
    let mut out = File::create(&config.output)?;
    out.write_all(json.as_bytes())?;

    let history_row = BenchmarkHistoryRow {
        run_id_unix,
        output_file: config.output.display().to_string(),
        small_tuples: config.small_tuples,
        large_tuples: config.large_tuples,
        lookup_samples: config.lookup_samples,
        inserted_total,
        small_insert_tps: report.performance.small_insert_tuples_per_sec,
        large_insert_tps: report.performance.large_insert_tuples_per_sec,
        lookup_ops: report.performance.point_lookup_ops_per_sec,
        scan_tps: report.performance.seq_scan_tuples_per_sec,
        fsm_rebuild_seconds: report.performance.fsm_rebuild_seconds,
        scan_matches_insert_count: report.correctness.scan_matches_insert_count,
        oversized_tuple_rejected: report.robustness.oversized_tuple_rejected,
        fsm_rebuild_search_found_page: report.robustness.fsm_rebuild_search_found_page,
    };

    let history_jsonl = PathBuf::from("benchmark_runs/benchmark_history.jsonl");
    let history_csv = PathBuf::from("benchmark_runs/benchmark_history.csv");
    append_history_jsonl(&history_jsonl, &history_row)?;
    append_history_csv(&history_csv, &history_row)?;

    let docs_report = PathBuf::from("docs/content/projects/fsm-heap-manager.md");
    refresh_docs_benchmark_log(&docs_report, &history_row, &history_csv, &config.output)?;

    println!("[BENCH] Report saved to {}", config.output.display());
    println!("[BENCH] History JSONL: {}", history_jsonl.display());
    println!("[BENCH] History CSV: {}", history_csv.display());
    println!("[BENCH] Docs benchmark log refreshed: {}", docs_report.display());
    println!("[BENCH] Inserted total: {}", inserted_total);
    println!(
        "[BENCH] Small insert TPS: {:.2}, Large insert TPS: {:.2}",
        report.performance.small_insert_tuples_per_sec,
        report.performance.large_insert_tuples_per_sec
    );
    println!(
        "[BENCH] Lookup OPS: {:.2}, Scan TPS: {:.2}",
        report.performance.point_lookup_ops_per_sec,
        report.performance.seq_scan_tuples_per_sec
    );

    Ok(())
}
