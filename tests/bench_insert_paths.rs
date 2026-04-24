use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use storage_manager::catalog::{
    create_table, save_catalog, Catalog, Column, Database, SortDirection, SortKey,
};
use storage_manager::disk::read_page;
use storage_manager::heap::insert_tuple;
use storage_manager::ordered::{
    append_delta_tuple, merge_if_needed, ordered_scan, scan_all_delta_tuples, sorted_insert,
};
use storage_manager::page::{Page, ITEM_ID_SIZE, PAGE_HEADER_SIZE};
use storage_manager::sorting::comparator::TupleComparator;
use storage_manager::sorting::external_sort::external_sort;
use storage_manager::sorting::in_memory_sort::in_memory_sort;
use storage_manager::table::page_count;

#[derive(Clone, Copy)]
enum DataPattern {
    Ascending,
    Descending,
    Random,
    Duplicates,
    RandomDuplicates,
}

impl DataPattern {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ascending => "ascending",
            Self::Descending => "descending",
            Self::Random => "random",
            Self::Duplicates => "duplicates",
            Self::RandomDuplicates => "random_duplicates",
        }
    }
}

struct BenchResult {
    operation: String,
    pattern: String,
    rows: usize,
    millis: u128,
    rows_per_sec: f64,
    note: String,
}

static DB_COUNTER: AtomicUsize = AtomicUsize::new(1);
const CATALOG_PATH: &str = "database/global/catalog.json";

struct CatalogBackup {
    existed: bool,
    contents: Vec<u8>,
}

impl CatalogBackup {
    fn capture() -> Self {
        let path = Path::new(CATALOG_PATH);
        match fs::read(path) {
            Ok(bytes) => Self {
                existed: true,
                contents: bytes,
            },
            Err(_) => Self {
                existed: false,
                contents: Vec::new(),
            },
        }
    }
}

impl Drop for CatalogBackup {
    fn drop(&mut self) {
        let path = Path::new(CATALOG_PATH);
        if self.existed {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::write(path, &self.contents);
        } else if path.exists() {
            let _ = fs::remove_file(path);
        }
    }
}

fn benchmark_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

fn env_pattern(key: &str, default: DataPattern) -> DataPattern {
    match std::env::var(key) {
        Ok(v) => match v.to_lowercase().as_str() {
            "asc" | "ascending" => DataPattern::Ascending,
            "desc" | "descending" => DataPattern::Descending,
            "random" => DataPattern::Random,
            "dup" | "duplicate" | "duplicates" => DataPattern::Duplicates,
            "random_dup" | "random_dups" | "random_duplicates" | "randdup" | "randdups" => {
                DataPattern::RandomDuplicates
            }
            _ => default,
        },
        Err(_) => default,
    }
}

fn unique_db(prefix: &str) -> String {
    let id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}", prefix, id)
}

fn schema() -> Vec<Column> {
    vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ]
}

fn sort_keys_id_asc() -> Vec<SortKey> {
    vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }]
}

fn make_tuple(id: i32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = format!("v{:09}", id).into_bytes();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

fn deterministic_shuffle(values: &mut [i32]) {
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..values.len()).rev() {
        seed ^= seed << 7;
        seed ^= seed >> 9;
        seed ^= seed << 8;
        let j = (seed as usize) % (i + 1);
        values.swap(i, j);
    }
}

fn generate_ids(rows: usize, pattern: DataPattern) -> Vec<i32> {
    assert!(rows <= i32::MAX as usize, "rows must fit into i32");
    match pattern {
        DataPattern::Ascending => (1..=rows as i32).collect(),
        DataPattern::Descending => (1..=rows as i32).rev().collect(),
        DataPattern::Random => {
            let mut ids: Vec<i32> = (1..=rows as i32).collect();
            deterministic_shuffle(&mut ids);
            ids
        }
        DataPattern::Duplicates => {
            let distinct = rows.clamp(1, 200);
            let mut ids = Vec::with_capacity(rows);
            for i in 0..rows {
                ids.push((i % distinct + 1) as i32);
            }
            ids
        }
        DataPattern::RandomDuplicates => {
            let distinct = rows.clamp(1, 200);
            let mut ids = Vec::with_capacity(rows);
            for i in 0..rows {
                ids.push((i % distinct + 1) as i32);
            }
            deterministic_shuffle(&mut ids);
            ids
        }
    }
}

fn setup_catalog(db_name: &str) -> Catalog {
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
    fs::create_dir_all("database/global").unwrap();
    fs::create_dir_all(format!("database/base/{}", db_name)).unwrap();

    let mut catalog = Catalog {
        databases: HashMap::new(),
    };
    catalog.databases.insert(
        db_name.to_string(),
        Database {
            tables: HashMap::new(),
        },
    );
    save_catalog(&catalog);
    catalog
}

fn table_path(db_name: &str, table_name: &str) -> String {
    format!("database/base/{}/{}.dat", db_name, table_name)
}

fn cleanup_db(db_name: &str) {
    let _ = fs::remove_dir_all(format!("database/base/{}", db_name));
}

fn table_data_pages(file_path: &str) -> u32 {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .unwrap();
    let total = page_count(&mut file).unwrap();
    total.saturating_sub(1)
}

fn delta_total_pages(db_name: &str, table_name: &str) -> u32 {
    let path = format!("database/base/{}/{}.delta", db_name, table_name);
    if !Path::new(&path).exists() {
        return 0;
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    page_count(&mut file).unwrap()
}

fn ordered_page_count_from_header(file_path: &str) -> u32 {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .unwrap();
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).unwrap();
    u32::from_le_bytes(buf)
}

fn ordered_page_count_in_file(file: &mut std::fs::File) -> u32 {
    let mut buf = [0u8; 4];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut buf).unwrap();
    u32::from_le_bytes(buf)
}

fn read_all_tuples(file_path: &str) -> Vec<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)
        .unwrap();

    let total = page_count(&mut file).unwrap();
    let mut tuples = Vec::new();

    for page_num in 1..total {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num).unwrap();
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            tuples.push(page.data[offset..offset + length].to_vec());
        }
    }

    tuples
}

fn measure<F>(f: F) -> Duration
where
    F: FnOnce(),
{
    let start = Instant::now();
    f();
    start.elapsed()
}

fn build_result(
    operation: &str,
    pattern: DataPattern,
    rows: usize,
    duration: Duration,
    note: impl Into<String>,
) -> BenchResult {
    BenchResult {
        operation: operation.to_string(),
        pattern: pattern.as_str().to_string(),
        rows,
        millis: duration.as_millis(),
        rows_per_sec: rows as f64 / duration.as_secs_f64().max(1e-9),
        note: note.into(),
    }
}

fn bench_heap_insert(ids: &[i32], pattern: DataPattern) -> BenchResult {
    let db = unique_db("bench_heap_insert");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), None);

    let path = table_path(&db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();

    let duration = measure(|| {
        for id in ids {
            let tuple = make_tuple(*id);
            insert_tuple(&mut file, &tuple).unwrap();
        }
    });

    let tuples = read_all_tuples(&path);
    assert_eq!(tuples.len(), ids.len());
    let pages = table_data_pages(&path);

    cleanup_db(&db);
    build_result(
        "heap_insert_tuple",
        pattern,
        ids.len(),
        duration,
        format!("heap append path, data_pages={}", pages),
    )
}

fn bench_ordered_sorted_insert(
    ids: &[i32],
    pattern: DataPattern,
    track_splits: bool,
) -> BenchResult {
    let db = unique_db("bench_sorted_insert");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    let sort_keys = sort_keys_id_asc();
    let columns = schema();
    create_table(
        &mut catalog,
        &db,
        table,
        columns.clone(),
        Some(sort_keys.clone()),
    );

    let comparator = TupleComparator::new(columns, sort_keys);
    let path = table_path(&db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();

    let mut measured_splits = 0u32;
    let duration = measure(|| {
        let mut previous_pages = if track_splits {
            ordered_page_count_in_file(&mut file)
        } else {
            0
        };

        for id in ids {
            let tuple = make_tuple(*id);
            sorted_insert(&mut file, &tuple, &comparator).unwrap();
            if track_splits {
                let current_pages = ordered_page_count_in_file(&mut file);
                if current_pages > previous_pages {
                    measured_splits += current_pages - previous_pages;
                }
                previous_pages = current_pages;
            }
        }
    });

    let rows = ordered_scan(&mut file, &catalog, &db, table).unwrap();
    assert_eq!(rows.len(), ids.len());
    for w in rows.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }
    let total_pages = ordered_page_count_from_header(&path);
    let split_note = if track_splits {
        format!("measured_splits_by_header_growth={}", measured_splits)
    } else {
        "measured_splits_by_header_growth=disabled".to_string()
    };

    cleanup_db(&db);
    build_result(
        "ordered_sorted_insert",
        pattern,
        ids.len(),
        duration,
        format!(
            "direct ordered insertion, {}, data_pages={}",
            split_note,
            total_pages.saturating_sub(1)
        ),
    )
}

fn bench_ordered_delta_append_only(ids: &[i32], pattern: DataPattern) -> BenchResult {
    let db = unique_db("bench_delta_append");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), Some(sort_keys_id_asc()));

    let duration = measure(|| {
        for id in ids {
            let tuple = make_tuple(*id);
            append_delta_tuple(&db, table, &tuple).unwrap();
        }
    });

    let delta_rows = scan_all_delta_tuples(&db, table).unwrap();
    assert_eq!(delta_rows.len(), ids.len());
    let delta_pages = delta_total_pages(&db, table);

    cleanup_db(&db);
    build_result(
        "ordered_delta_append_only",
        pattern,
        ids.len(),
        duration,
        format!("sidecar append, no merge, delta_pages={}", delta_pages),
    )
}

fn bench_ordered_delta_append_and_merge(ids: &[i32], pattern: DataPattern) -> [BenchResult; 2] {
    let db = unique_db("bench_delta_merge");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), Some(sort_keys_id_asc()));

    let append_duration = measure(|| {
        for id in ids {
            let tuple = make_tuple(*id);
            append_delta_tuple(&db, table, &tuple).unwrap();
        }
    });
    let delta_pages_before_merge = delta_total_pages(&db, table);

    {
        let tbl = catalog
            .databases
            .get_mut(&db)
            .unwrap()
            .tables
            .get_mut(table)
            .unwrap();
        tbl.delta_enabled = Some(true);
        tbl.delta_current_tuples = Some(ids.len() as u64);
        tbl.delta_merge_threshold_tuples = Some(ids.len() as u64);
    }
    save_catalog(&catalog);

    let merge_duration = measure(|| {
        let merged = merge_if_needed(&mut catalog, &db, table).unwrap();
        assert!(merged);
    });

    let delta_rows = scan_all_delta_tuples(&db, table).unwrap();
    assert_eq!(delta_rows.len(), 0);
    let delta_pages_after = delta_total_pages(&db, table);

    let path = table_path(&db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let base_rows = ordered_scan(&mut file, &catalog, &db, table).unwrap();
    assert_eq!(base_rows.len(), ids.len());
    let base_pages = table_data_pages(&path);

    cleanup_db(&db);

    [
        build_result(
            "ordered_delta_append_for_merge",
            pattern,
            ids.len(),
            append_duration,
            format!(
                "sidecar append before merge, delta_rows={}, delta_pages_before_merge={}",
                ids.len(),
                delta_pages_before_merge
            ),
        ),
        build_result(
            "ordered_delta_merge",
            pattern,
            ids.len(),
            merge_duration,
            format!(
                "base + delta merge, merges=1, delta_pages_before_merge={}, delta_pages_after_merge={}, base_pages={}",
                delta_pages_before_merge, delta_pages_after, base_pages
            ),
        ),
    ]
}

fn bench_in_memory_sort(ids: &[i32], pattern: DataPattern) -> BenchResult {
    let db = unique_db("bench_in_memory_sort");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), None);

    let path = table_path(&db, table);
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        for id in ids {
            let tuple = make_tuple(*id);
            insert_tuple(&mut file, &tuple).unwrap();
        }
    }

    let sort_keys = sort_keys_id_asc();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let duration = measure(|| {
        in_memory_sort(&mut catalog, &db, table, sort_keys, &mut file).unwrap();
    });

    let tuples = read_all_tuples(&path);
    assert_eq!(tuples.len(), ids.len());
    for w in tuples.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }
    let pages = table_data_pages(&path);

    cleanup_db(&db);
    build_result(
        "heap_to_ordered_in_memory_sort",
        pattern,
        ids.len(),
        duration,
        format!("sort-only time, output_data_pages={}", pages),
    )
}

fn bench_external_sort(ids: &[i32], pattern: DataPattern, pool_size: usize) -> BenchResult {
    let db = unique_db("bench_external_sort");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), None);

    let path = table_path(&db, table);
    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        for id in ids {
            let tuple = make_tuple(*id);
            insert_tuple(&mut file, &tuple).unwrap();
        }
    }

    let sort_keys = sort_keys_id_asc();
    let duration = measure(|| {
        external_sort(&mut catalog, &db, table, sort_keys, pool_size).unwrap();
    });

    let tuples = read_all_tuples(&path);
    assert_eq!(tuples.len(), ids.len());
    for w in tuples.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }
    let pages = table_data_pages(&path);

    let db_dir = format!("database/base/{}", db);
    if let Ok(entries) = fs::read_dir(&db_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.starts_with(".sort_tmp_"),
                "temp file {} was not cleaned up",
                name
            );
        }
    }

    cleanup_db(&db);
    build_result(
        "heap_to_ordered_external_sort",
        pattern,
        ids.len(),
        duration,
        format!(
            "sort-only time, pool_size={}, output_data_pages={}",
            pool_size, pages
        ),
    )
}

fn bench_heap_load_plus_in_memory_sort(ids: &[i32], pattern: DataPattern) -> BenchResult {
    let db = unique_db("bench_e2e_in_memory");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), None);

    let path = table_path(&db, table);
    let sort_keys = sort_keys_id_asc();

    let duration = measure(|| {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        for id in ids {
            let tuple = make_tuple(*id);
            insert_tuple(&mut file, &tuple).unwrap();
        }

        in_memory_sort(&mut catalog, &db, table, sort_keys, &mut file).unwrap();
    });

    let tuples = read_all_tuples(&path);
    assert_eq!(tuples.len(), ids.len());
    for w in tuples.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }
    let pages = table_data_pages(&path);

    cleanup_db(&db);
    build_result(
        "e2e_heap_load_plus_in_memory_sort",
        pattern,
        ids.len(),
        duration,
        format!("load + sort total, output_data_pages={}", pages),
    )
}

fn bench_heap_load_plus_external_sort(
    ids: &[i32],
    pattern: DataPattern,
    pool_size: usize,
) -> BenchResult {
    let db = unique_db("bench_e2e_external");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), None);

    let path = table_path(&db, table);
    let sort_keys = sort_keys_id_asc();

    let duration = measure(|| {
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            for id in ids {
                let tuple = make_tuple(*id);
                insert_tuple(&mut file, &tuple).unwrap();
            }
        }

        external_sort(&mut catalog, &db, table, sort_keys, pool_size).unwrap();
    });

    let tuples = read_all_tuples(&path);
    assert_eq!(tuples.len(), ids.len());
    for w in tuples.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }
    let pages = table_data_pages(&path);

    cleanup_db(&db);
    build_result(
        "e2e_heap_load_plus_external_sort",
        pattern,
        ids.len(),
        duration,
        format!(
            "load + sort total, pool_size={}, output_data_pages={}",
            pool_size, pages
        ),
    )
}

fn bench_ordered_delta_end_to_end(ids: &[i32], pattern: DataPattern) -> BenchResult {
    let db = unique_db("bench_e2e_delta");
    let table = "t";
    let mut catalog = setup_catalog(&db);
    create_table(&mut catalog, &db, table, schema(), Some(sort_keys_id_asc()));

    let mut delta_pages_before_merge = 0u32;
    let duration = measure(|| {
        for id in ids {
            let tuple = make_tuple(*id);
            append_delta_tuple(&db, table, &tuple).unwrap();
        }
        delta_pages_before_merge = delta_total_pages(&db, table);

        {
            let tbl = catalog
                .databases
                .get_mut(&db)
                .unwrap()
                .tables
                .get_mut(table)
                .unwrap();
            tbl.delta_enabled = Some(true);
            tbl.delta_current_tuples = Some(ids.len() as u64);
            tbl.delta_merge_threshold_tuples = Some(ids.len() as u64);
        }
        save_catalog(&catalog);

        let merged = merge_if_needed(&mut catalog, &db, table).unwrap();
        assert!(merged);
    });

    let path = table_path(&db, table);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let rows = ordered_scan(&mut file, &catalog, &db, table).unwrap();
    assert_eq!(rows.len(), ids.len());
    for w in rows.windows(2) {
        assert!(extract_int(&w[0]) <= extract_int(&w[1]));
    }

    let delta_pages_after = delta_total_pages(&db, table);
    let base_pages = table_data_pages(&path);
    cleanup_db(&db);

    build_result(
        "e2e_ordered_delta_append_plus_merge",
        pattern,
        ids.len(),
        duration,
        format!(
            "append + merge total, merges=1, delta_pages_before_merge={}, delta_pages_after_merge={}, base_pages={}",
            delta_pages_before_merge, delta_pages_after, base_pages
        ),
    )
}

fn print_results(title: &str, results: &[BenchResult]) {
    println!("\n{}", title);
    println!("{:-<115}", "");
    println!(
        "{:<36} {:<11} {:>8} {:>10} {:>16}  {}",
        "operation", "pattern", "rows", "time_ms", "rows_per_sec", "note"
    );
    println!("{:-<115}", "");

    for r in results {
        println!(
            "{:<36} {:<11} {:>8} {:>10} {:>16.2}  {}",
            r.operation, r.pattern, r.rows, r.millis, r.rows_per_sec, r.note
        );
    }

    println!("{:-<115}\n", "");
}

#[test]
#[ignore = "manual benchmark; run with --ignored --nocapture"]
fn bench_insertion_and_sort_paths() {
    let _guard = benchmark_lock();
    let _catalog_backup = CatalogBackup::capture();

    let rows = env_usize("ROOKDB_BENCH_ROWS", 5000);
    let external_rows = env_usize("ROOKDB_BENCH_EXTERNAL_ROWS", rows * 4);
    let pool_size = env_usize("ROOKDB_BENCH_EXTERNAL_POOL", 4);
    let pattern = env_pattern("ROOKDB_BENCH_PATTERN", DataPattern::Random);
    let track_splits = env_bool("ROOKDB_BENCH_TRACK_SPLITS", false);

    println!(
        "Running benchmark with rows={}, external_rows={}, pattern={}, external_pool={}...",
        rows,
        external_rows,
        pattern.as_str(),
        pool_size
    );

    let ids = generate_ids(rows, pattern);
    let external_ids = generate_ids(external_rows, pattern);

    let mut micro_results = vec![
        bench_heap_insert(&ids, pattern),
        bench_ordered_sorted_insert(&ids, pattern, track_splits),
        bench_ordered_delta_append_only(&ids, pattern),
    ];
    micro_results.extend(bench_ordered_delta_append_and_merge(&ids, pattern));
    micro_results.push(bench_in_memory_sort(&ids, pattern));
    micro_results.push(bench_external_sort(&external_ids, pattern, pool_size));

    let end_to_end_results = vec![
        bench_heap_load_plus_in_memory_sort(&ids, pattern),
        bench_ordered_delta_end_to_end(&ids, pattern),
        bench_heap_load_plus_external_sort(&ids, pattern, pool_size),
    ];

    println!("\nNOTE: microbench results are component-level timings; end-to-end includes full load + maintenance costs.");
    println!(
        "NOTE: all end-to-end rows are aligned at {} for direct strategy comparison.",
        rows
    );
    print_results("RookDB Microbenchmarks (Component Timings)", &micro_results);
    print_results("RookDB End-to-End Strategy Benchmarks", &end_to_end_results);
}

#[test]
#[ignore = "manual benchmark; run with --ignored --nocapture"]
fn bench_sorted_insert_pattern_sweep() {
    let _guard = benchmark_lock();
    let _catalog_backup = CatalogBackup::capture();

    let rows = env_usize("ROOKDB_BENCH_SWEEP_ROWS", 4000);
    let track_splits = env_bool("ROOKDB_BENCH_TRACK_SPLITS", false);
    let patterns = [
        DataPattern::Ascending,
        DataPattern::Descending,
        DataPattern::Random,
        DataPattern::Duplicates,
        DataPattern::RandomDuplicates,
    ];

    println!("Running sorted_insert pattern sweep with rows={}...", rows);

    let mut results = Vec::new();
    for pattern in patterns {
        let ids = generate_ids(rows, pattern);
        results.push(bench_ordered_sorted_insert(&ids, pattern, track_splits));
        results.push(bench_ordered_delta_append_only(&ids, pattern));
    }

    print_results("RookDB Insertion Pattern Sweep", &results);
}
