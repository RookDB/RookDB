use std::collections::HashMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;
use std::time::Instant;

use postgres::{Client, NoTls};
use serde::Serialize;

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{
    compaction_table, delete_tuples, parse_set_clause, parse_where_clause, update_tuples,
};
use storage_manager::heap::{init_table, insert_tuple};

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    rows: usize,
    iterations: usize,
    db_name: String,
    table_name: String,
    pg_url: String,
    pg_schema: String,
    rook_only: bool,
}

#[derive(Debug, Clone, Serialize)]
struct TimingSummary {
    samples_ms: Vec<f64>,
    fastest_ms: f64,
    slowest_ms: f64,
    total_ms: f64,
    average_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct EngineReport {
    update: TimingSummary,
    delete: TimingSummary,
    compaction: TimingSummary,
}

#[derive(Debug, Clone, Serialize)]
struct ComparisonReport {
    config: ReportConfig,
    rookdb: EngineReport,
    postgres: EngineReport,
    ratios: RatioReport,
}

#[derive(Debug, Clone, Serialize)]
struct RookOnlyReport {
    config: ReportConfig,
    rookdb: EngineReport,
}

#[derive(Debug, Clone, Serialize)]
struct ReportConfig {
    rows: usize,
    iterations: usize,
    db_name: String,
    table_name: String,
    pg_schema: String,
    rook_only: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RatioReport {
    update_avg_rook_over_postgres: f64,
    delete_avg_rook_over_postgres: f64,
    compaction_avg_rook_over_postgres: f64,
}

fn main() -> io::Result<()> {
    let cfg = parse_args();
    println!("Running benchmark with rows={} iterations={}...", cfg.rows, cfg.iterations);

    let base_dir = format!("database/base/{}", cfg.db_name);
    fs::create_dir_all(&base_dir)?;

    let rook_seed_path = format!("{}/{}_seed.dat", base_dir, cfg.table_name);
    let rook_active_path = format!("{}/{}.dat", base_dir, cfg.table_name);

    create_rook_seed(&rook_seed_path, cfg.rows)?;
    if !cfg.rook_only {
        create_postgres_seed(&cfg)?;
    }

    let catalog = make_catalog(&cfg.db_name, &cfg.table_name);

    let update_groups = parse_where_clause("id <= 50000").unwrap_or_default();
    let update_set = parse_set_clause("score = score + 10").unwrap_or_default();
    let delete_groups = parse_where_clause("id <= 20000").unwrap_or_default();

    let rook_update = bench_rook_update(
        &catalog,
        &cfg,
        &rook_seed_path,
        &rook_active_path,
        &update_set,
        &update_groups,
    )?;

    let rook_delete = bench_rook_delete(
        &catalog,
        &cfg,
        &rook_seed_path,
        &rook_active_path,
        &delete_groups,
    )?;

    let rook_compaction = bench_rook_compaction(
        &catalog,
        &cfg,
        &rook_seed_path,
        &rook_active_path,
        &delete_groups,
    )?;

    let rook_report = EngineReport {
        update: summarize(rook_update),
        delete: summarize(rook_delete),
        compaction: summarize(rook_compaction),
    };

    if cfg.rook_only {
        let report = RookOnlyReport {
            config: ReportConfig {
                rows: cfg.rows,
                iterations: cfg.iterations,
                db_name: cfg.db_name.clone(),
                table_name: cfg.table_name.clone(),
                pg_schema: cfg.pg_schema.clone(),
                rook_only: true,
            },
            rookdb: rook_report,
        };

        print_rook_only_report(&report);

        let out_dir = "benchmark_results";
        fs::create_dir_all(out_dir)?;
        let out_path = format!(
            "{}/bench_report_rook_only_{}_{}.json",
            out_dir, cfg.db_name, cfg.table_name
        );
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        fs::write(&out_path, json)?;
        println!("\nSaved RookDB-only report to {}", out_path);
        return Ok(());
    }

    let postgres_update = bench_postgres_update(&cfg)?;
    let postgres_delete = bench_postgres_delete(&cfg)?;
    let postgres_compaction = bench_postgres_vacuum_full(&cfg)?;

    let postgres_report = EngineReport {
        update: summarize(postgres_update),
        delete: summarize(postgres_delete),
        compaction: summarize(postgres_compaction),
    };

    let report = ComparisonReport {
        config: ReportConfig {
            rows: cfg.rows,
            iterations: cfg.iterations,
            db_name: cfg.db_name.clone(),
            table_name: cfg.table_name.clone(),
            pg_schema: cfg.pg_schema.clone(),
            rook_only: false,
        },
        ratios: RatioReport {
            update_avg_rook_over_postgres: ratio(
                rook_report.update.average_ms,
                postgres_report.update.average_ms,
            ),
            delete_avg_rook_over_postgres: ratio(
                rook_report.delete.average_ms,
                postgres_report.delete.average_ms,
            ),
            compaction_avg_rook_over_postgres: ratio(
                rook_report.compaction.average_ms,
                postgres_report.compaction.average_ms,
            ),
        },
        rookdb: rook_report,
        postgres: postgres_report,
    };

    print_report(&report);

    let out_dir = "benchmark_results";
    fs::create_dir_all(out_dir)?;
    let out_path = format!("{}/bench_report_{}_{}.json", out_dir, cfg.db_name, cfg.table_name);
    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    fs::write(&out_path, json)?;
    println!("\nSaved full report to {}", out_path);

    Ok(())
}

fn parse_args() -> BenchmarkConfig {
    let mut rows = 100_000usize;
    let mut iterations = 5usize;
    let mut db_name = "bench_db".to_string();
    let mut table_name = "bench_table".to_string();
    let mut pg_url = "postgresql://postgres:postgres@localhost:5432/postgres".to_string();
    let mut pg_schema = "public".to_string();
    let mut rook_only = false;

    let args: Vec<String> = env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" if i + 1 < args.len() => {
                if let Ok(v) = args[i + 1].parse::<usize>() {
                    rows = v;
                }
                i += 2;
            }
            "--iterations" if i + 1 < args.len() => {
                if let Ok(v) = args[i + 1].parse::<usize>() {
                    iterations = v;
                }
                i += 2;
            }
            "--db" if i + 1 < args.len() => {
                db_name = args[i + 1].clone();
                i += 2;
            }
            "--table" if i + 1 < args.len() => {
                table_name = args[i + 1].clone();
                i += 2;
            }
            "--pg-url" if i + 1 < args.len() => {
                pg_url = args[i + 1].clone();
                i += 2;
            }
            "--pg-schema" if i + 1 < args.len() => {
                pg_schema = args[i + 1].clone();
                i += 2;
            }
            "--rook-only" => {
                rook_only = true;
                i += 1;
            }
            _ => i += 1,
        }
    }

    BenchmarkConfig {
        rows,
        iterations,
        db_name,
        table_name,
        pg_url,
        pg_schema,
        rook_only,
    }
}

fn ratio(a: f64, b: f64) -> f64 {
    if b <= f64::EPSILON {
        0.0
    } else {
        a / b
    }
}

fn summarize(samples_ms: Vec<f64>) -> TimingSummary {
    let mut sorted = samples_ms.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let total_ms: f64 = samples_ms.iter().sum();
    let average_ms = if samples_ms.is_empty() {
        0.0
    } else {
        total_ms / samples_ms.len() as f64
    };

    let fastest_ms = sorted.first().copied().unwrap_or(0.0);
    let slowest_ms = sorted.last().copied().unwrap_or(0.0);

    TimingSummary {
        samples_ms,
        fastest_ms,
        slowest_ms,
        total_ms,
        average_ms,
    }
}

fn print_summary_row(name: &str, rook: &TimingSummary, postgres: &TimingSummary) {
    println!(
        "{:<12} | Rook avg {:>9.3} ms (min {:>9.3}, max {:>9.3}, total {:>10.3}) | PostgreSQL avg {:>9.3} ms (min {:>9.3}, max {:>9.3}, total {:>10.3})",
        name,
        rook.average_ms,
        rook.fastest_ms,
        rook.slowest_ms,
        rook.total_ms,
        postgres.average_ms,
        postgres.fastest_ms,
        postgres.slowest_ms,
        postgres.total_ms,
    );
}

fn print_report(report: &ComparisonReport) {
    println!("\n=== Benchmark Summary ===");
    println!(
        "Rows: {} | Iterations: {} | Rook table: database/base/{}/{}.dat | PG schema: {}",
        report.config.rows,
        report.config.iterations,
        report.config.db_name,
        report.config.table_name,
        report.config.pg_schema,
    );
    print_summary_row("UPDATE", &report.rookdb.update, &report.postgres.update);
    print_summary_row("DELETE", &report.rookdb.delete, &report.postgres.delete);
    print_summary_row("COMPACTION", &report.rookdb.compaction, &report.postgres.compaction);
    println!(
        "\nAvg ratio (Rook/PostgreSQL): update={:.3}x delete={:.3}x compaction={:.3}x",
        report.ratios.update_avg_rook_over_postgres,
        report.ratios.delete_avg_rook_over_postgres,
        report.ratios.compaction_avg_rook_over_postgres
    );
}

fn print_rook_only_report(report: &RookOnlyReport) {
    println!("\n=== RookDB Benchmark Summary (Rook-only) ===");
    println!(
        "Rows: {} | Iterations: {} | Rook table: database/base/{}/{}.dat",
        report.config.rows,
        report.config.iterations,
        report.config.db_name,
        report.config.table_name,
    );
    println!(
        "UPDATE     | avg {:>9.3} ms (min {:>9.3}, max {:>9.3}, total {:>10.3})",
        report.rookdb.update.average_ms,
        report.rookdb.update.fastest_ms,
        report.rookdb.update.slowest_ms,
        report.rookdb.update.total_ms,
    );
    println!(
        "DELETE     | avg {:>9.3} ms (min {:>9.3}, max {:>9.3}, total {:>10.3})",
        report.rookdb.delete.average_ms,
        report.rookdb.delete.fastest_ms,
        report.rookdb.delete.slowest_ms,
        report.rookdb.delete.total_ms,
    );
    println!(
        "COMPACTION | avg {:>9.3} ms (min {:>9.3}, max {:>9.3}, total {:>10.3})",
        report.rookdb.compaction.average_ms,
        report.rookdb.compaction.fastest_ms,
        report.rookdb.compaction.slowest_ms,
        report.rookdb.compaction.total_ms,
    );
}

fn make_catalog(db: &str, table: &str) -> Catalog {
    let mut databases = HashMap::new();
    let mut tables = HashMap::new();
    tables.insert(
        table.to_string(),
        Table {
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: "INT".into(),
                },
                Column {
                    name: "age".into(),
                    data_type: "INT".into(),
                },
                Column {
                    name: "score".into(),
                    data_type: "INT".into(),
                },
                Column {
                    name: "name".into(),
                    data_type: "TEXT".into(),
                },
            ],
        },
    );
    databases.insert(db.to_string(), Database { tables });
    Catalog { databases }
}

fn make_rook_row(id: i32) -> Vec<u8> {
    let age = 18 + (id % 50);
    let score = 100 + (id % 1000);
    let name = format!("u{:09}", id);

    let mut bytes = Vec::with_capacity(22);
    bytes.extend_from_slice(&id.to_le_bytes());
    bytes.extend_from_slice(&age.to_le_bytes());
    bytes.extend_from_slice(&score.to_le_bytes());

    let mut text = name.as_bytes().to_vec();
    text.resize(10, b' ');
    text.truncate(10);
    bytes.extend_from_slice(&text);
    bytes
}

fn create_rook_seed(path: &str, rows: usize) -> io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    if Path::new(path).exists() {
        fs::remove_file(path)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(path)?;

    init_table(&mut file)?;
    for id in 1..=rows {
        let row = make_rook_row(id as i32);
        insert_tuple(&mut file, &row)?;
    }
    Ok(())
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn pg_seed_table_name(cfg: &BenchmarkConfig) -> String {
    format!("{}_seed", cfg.table_name)
}

fn pg_active_table_name(cfg: &BenchmarkConfig) -> String {
    format!("{}_active", cfg.table_name)
}

fn pg_qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

fn pg_connect(cfg: &BenchmarkConfig) -> io::Result<Client> {
    Client::connect(&cfg.pg_url, NoTls)
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL connect error: {:?}", e)))
}

fn create_postgres_seed(cfg: &BenchmarkConfig) -> io::Result<()> {
    let mut client = pg_connect(cfg)?;
    let schema = quote_ident(&cfg.pg_schema);
    let seed_table = pg_qualified_table(&cfg.pg_schema, &pg_seed_table_name(cfg));

    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};\nDROP TABLE IF EXISTS {seed_table};\nCREATE TABLE {seed_table} (id INTEGER PRIMARY KEY, age INTEGER NOT NULL, score INTEGER NOT NULL, name VARCHAR(10) NOT NULL);"
        ))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL seed DDL error: {}", e)))?;

    let mut tx = client
        .transaction()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL transaction error: {}", e)))?;
    let insert_sql = format!(
        "INSERT INTO {} (id, age, score, name) VALUES ($1, $2, $3, $4)",
        seed_table
    );
    for id in 1..=cfg.rows {
        let i = id as i32;
        let age = 18 + (i % 50);
        let score = 100 + (i % 1000);
        let name = format!("u{:09}", i);
        tx.execute(&insert_sql, &[&i, &age, &score, &name])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL seed insert error: {}", e)))?;
    }
    tx.commit()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL commit error: {}", e)))?;

    Ok(())
}

fn reset_postgres_active_table(client: &mut Client, cfg: &BenchmarkConfig) -> io::Result<()> {
    let active = pg_qualified_table(&cfg.pg_schema, &pg_active_table_name(cfg));
    let seed = pg_qualified_table(&cfg.pg_schema, &pg_seed_table_name(cfg));

    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {active};\nCREATE TABLE {active} AS TABLE {seed};\nALTER TABLE {active} ADD PRIMARY KEY (id);"
        ))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL active reset error: {}", e)))?;

    Ok(())
}

fn bench_rook_update(
    catalog: &Catalog,
    cfg: &BenchmarkConfig,
    seed_path: &str,
    active_path: &str,
    set_assignments: &[storage_manager::executor::SetAssignment],
    condition_groups: &[Vec<storage_manager::executor::Condition>],
) -> io::Result<Vec<f64>> {
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        fs::copy(seed_path, active_path)?;
        let mut file = OpenOptions::new().read(true).write(true).open(active_path)?;

        let start = Instant::now();
        let _ = update_tuples(
            catalog,
            &cfg.db_name,
            &cfg.table_name,
            &mut file,
            set_assignments,
            condition_groups,
            false,
        )?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}

fn bench_rook_delete(
    catalog: &Catalog,
    cfg: &BenchmarkConfig,
    seed_path: &str,
    active_path: &str,
    condition_groups: &[Vec<storage_manager::executor::Condition>],
) -> io::Result<Vec<f64>> {
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        fs::copy(seed_path, active_path)?;
        let mut file = OpenOptions::new().read(true).write(true).open(active_path)?;

        let start = Instant::now();
        let _ = delete_tuples(
            catalog,
            &cfg.db_name,
            &cfg.table_name,
            &mut file,
            condition_groups,
            false,
        )?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}

fn bench_rook_compaction(
    catalog: &Catalog,
    cfg: &BenchmarkConfig,
    seed_path: &str,
    active_path: &str,
    delete_groups: &[Vec<storage_manager::executor::Condition>],
) -> io::Result<Vec<f64>> {
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        fs::copy(seed_path, active_path)?;

        let mut file = OpenOptions::new().read(true).write(true).open(active_path)?;
        let _ = delete_tuples(
            catalog,
            &cfg.db_name,
            &cfg.table_name,
            &mut file,
            delete_groups,
            false,
        )?;
        drop(file);

        let start = Instant::now();
        let _ = compaction_table(&cfg.db_name, &cfg.table_name)?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}

fn bench_postgres_update(cfg: &BenchmarkConfig) -> io::Result<Vec<f64>> {
    let mut client = pg_connect(cfg)?;
    let active = pg_qualified_table(&cfg.pg_schema, &pg_active_table_name(cfg));
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        reset_postgres_active_table(&mut client, cfg)?;

        let start = Instant::now();
        client
            .execute(&format!("UPDATE {active} SET score = score + 10 WHERE id <= 50000"), &[])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL UPDATE error: {}", e)))?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}

fn bench_postgres_delete(cfg: &BenchmarkConfig) -> io::Result<Vec<f64>> {
    let mut client = pg_connect(cfg)?;
    let active = pg_qualified_table(&cfg.pg_schema, &pg_active_table_name(cfg));
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        reset_postgres_active_table(&mut client, cfg)?;

        let start = Instant::now();
        client
            .execute(&format!("DELETE FROM {active} WHERE id <= 20000"), &[])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL DELETE error: {}", e)))?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}

fn bench_postgres_vacuum_full(cfg: &BenchmarkConfig) -> io::Result<Vec<f64>> {
    let mut client = pg_connect(cfg)?;
    let active = pg_qualified_table(&cfg.pg_schema, &pg_active_table_name(cfg));
    let mut samples = Vec::with_capacity(cfg.iterations);
    for _ in 0..cfg.iterations {
        reset_postgres_active_table(&mut client, cfg)?;
        client
            .execute(&format!("DELETE FROM {active} WHERE id <= 20000"), &[])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL pre-vacuum DELETE error: {}", e)))?;

        let start = Instant::now();
        client
            .batch_execute(&format!("VACUUM FULL {active};"))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("PostgreSQL VACUUM FULL error: {}", e)))?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(samples)
}
