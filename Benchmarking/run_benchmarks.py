#!/usr/bin/env python3
"""Primary-key benchmark pipeline for RookDB with SQLite, DuckDB, and In-Memory baselines.

Research-aligned evaluation metrics in this script follow common benchmark
practice (latency percentiles and throughput), as used in YCSB-style studies:
- Cooper et al. (2010), Benchmarking Cloud Serving Systems with YCSB.

Pipeline:
1) Generate controlled synthetic data (deterministic, real-world-like distributions).
2) Load into SQLite, DuckDB, Python Dict, and measure primary-key lookup latency.
3) Run Rust RookDB index benchmark across all index algorithms.
4) Cross-verify correctness between outputs.
5) Run scalability sweeps across multiple dataset sizes.
6) Emit metrics, plots, and a documentation-ready report.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import duckdb
import subprocess
import time
from pathlib import Path
from statistics import mean
from typing import Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RookDB primary-key benchmark")
    parser.add_argument("--rows", type=int, default=50000, help="number of synthetic rows for single-run artifact")
    parser.add_argument(
        "--scales",
        default="10000,30000,50000",
        help="comma-separated row counts for scalability evaluation",
    )
    parser.add_argument("--seed", type=int, default=42, help="deterministic RNG seed")
    parser.add_argument(
        "--data-csv",
        default="Benchmarking/data/synthetic_orders.csv",
        help="synthetic data CSV path",
    )
    parser.add_argument(
        "--sqlite-db",
        default="Benchmarking/results/sqlite_baseline.db",
        help="SQLite baseline DB path",
    )
    parser.add_argument(
        "--duckdb-db",
        default="Benchmarking/results/duckdb_baseline.db",
        help="DuckDB baseline DB path",
    )
    parser.add_argument(
        "--rookdb-json",
        default="Benchmarking/results/rookdb_primary_key_metrics.json",
        help="RookDB benchmark output JSON path",
    )
    parser.add_argument(
        "--comparison-csv",
        default="Benchmarking/results/latency_comparison.csv",
        help="comparison CSV path",
    )
    parser.add_argument(
        "--verification-json",
        default="Benchmarking/results/correctness_verification.json",
        help="verification JSON path",
    )
    parser.add_argument(
        "--report-md",
        default="Benchmarking/results/benchmark_report.md",
        help="benchmark report markdown path",
    )
    parser.add_argument(
        "--scalability-csv",
        default="Benchmarking/results/scalability_summary.csv",
        help="scalability summary CSV path",
    )
    parser.add_argument(
        "--charts-dir",
        default="Benchmarking/results/charts",
        help="directory for matplotlib chart outputs",
    )
    return parser.parse_args()


def _latency_stats(samples_us: List[float]) -> Dict[str, float]:
    if not samples_us:
        return {
            "min_us": 0.0,
            "max_us": 0.0,
            "avg_us": 0.0,
            "p50_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "throughput_ops_s": 0.0,
        }
    arr = sorted(samples_us)

    def p(q: float) -> float:
        idx = int(round((len(arr) - 1) * q))
        return float(arr[idx])

    return {
        "min_us": float(arr[0]),
        "max_us": float(arr[-1]),
        "avg_us": float(mean(arr)),
        "p50_us": p(0.50),
        "p95_us": p(0.95),
        "p99_us": p(0.99),
        "throughput_ops_s": float(1_000_000.0 / mean(arr)) if mean(arr) > 0 else 0.0,
    }


def generate_synthetic_orders(csv_path: Path, rows: int, seed: int) -> Dict[str, int]:
    rng = random.Random(seed)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    regions = ["NA", "EU", "APAC", "LATAM", "MEA"]
    region_weights = [0.37, 0.27, 0.22, 0.09, 0.05]
    devices = ["web", "ios", "android"]
    device_weights = [0.46, 0.24, 0.30]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "customer_id", "region", "device", "amount_cents", "event_ts"])

        base_ts = 1_700_000_000
        for i in range(1, rows + 1):
            order_id = i
            customer_id = min(250_000, int(rng.paretovariate(1.45) * 1000) + 1)
            amount_cents = max(99, int(rng.lognormvariate(4.2, 0.9) * 100))
            region = rng.choices(regions, weights=region_weights, k=1)[0]
            device = rng.choices(devices, weights=device_weights, k=1)[0]
            event_ts = base_ts + i * 13 + rng.randint(0, 3600)
            writer.writerow([order_id, customer_id, region, device, amount_cents, event_ts])

    return {"rows": rows, "seed": seed}

def python_dict_baseline(csv_path: Path) -> Dict:
    ids: List[int] = []
    store: Dict[int, tuple] = {}
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            k = int(row["id"])
            ids.append(k)
            store[k] = (
                k,
                int(row["customer_id"]),
                row["region"],
                row["device"],
                int(row["amount_cents"]),
                int(row["event_ts"]),
            )
            
    total_rows = len(store)
    unique_pk = len(store)
    
    lookup_lat_us: List[float] = []
    for key in ids:
        t0 = time.perf_counter()
        hit = store.get(key)
        lookup_lat_us.append((time.perf_counter() - t0) * 1_000_000.0)
        if hit is None:
            raise RuntimeError(f"Dict missing primary key {key}")

    miss_ok = True
    for k in range(-100, 0):
        if store.get(k) is not None:
            miss_ok = False
            break

    return {
        "total_rows": total_rows,
        "unique_primary_keys": unique_pk,
        "lookup_latency": _latency_stats(lookup_lat_us),
        "miss_checks_ok": miss_ok,
    }


def sqlite_baseline(csv_path: Path, db_path: Path) -> Dict:
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            region TEXT NOT NULL,
            device TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            event_ts INTEGER NOT NULL
        )
        """
    )

    ids: List[int] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            ids.append(int(row["id"]))
            rows.append(
                (
                    int(row["id"]),
                    int(row["customer_id"]),
                    row["region"],
                    row["device"],
                    int(row["amount_cents"]),
                    int(row["event_ts"]),
                )
            )

    cur.executemany(
        "INSERT INTO orders (id, customer_id, region, device, amount_cents, event_ts) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM orders")
    total_rows = int(cur.fetchone()[0])

    cur.execute("SELECT COUNT(DISTINCT id) FROM orders")
    unique_pk = int(cur.fetchone()[0])

    lookup_lat_us: List[float] = []
    for key in ids:
        t0 = time.perf_counter()
        cur.execute("SELECT id FROM orders WHERE id = ?", (key,))
        hit = cur.fetchone()
        lookup_lat_us.append((time.perf_counter() - t0) * 1_000_000.0)
        if hit is None:
            raise RuntimeError(f"SQLite missing primary key {key}")

    miss_ok = True
    for k in range(-100, 0):
        cur.execute("SELECT id FROM orders WHERE id = ?", (k,))
        if cur.fetchone() is not None:
            miss_ok = False
            break

    conn.close()

    return {
        "total_rows": total_rows,
        "unique_primary_keys": unique_pk,
        "lookup_latency": _latency_stats(lookup_lat_us),
        "miss_checks_ok": miss_ok,
    }

def duckdb_baseline(csv_path: Path, db_path: Path) -> Dict:
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(db_path))
    
    conn.execute(f"CREATE TABLE orders AS SELECT * FROM read_csv_auto('{csv_path}')")
    
    ids: List[int] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.append(int(row["id"]))
            
    total_rows = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    unique_pk = conn.execute("SELECT COUNT(DISTINCT id) FROM orders").fetchone()[0]

    lookup_lat_us: List[float] = []
    for key in ids:
        t0 = time.perf_counter()
        conn.execute("SELECT id FROM orders WHERE id = ?", (key,))
        hit = conn.fetchone()
        lookup_lat_us.append((time.perf_counter() - t0) * 1_000_000.0)
        if hit is None:
            raise RuntimeError(f"DuckDB missing primary key {key}")

    miss_ok = True
    for k in range(-100, 0):
        conn.execute("SELECT id FROM orders WHERE id = ?", (k,))
        if conn.fetchone() is not None:
            miss_ok = False
            break

    conn.close()

    return {
        "total_rows": total_rows,
        "unique_primary_keys": unique_pk,
        "lookup_latency": _latency_stats(lookup_lat_us),
        "miss_checks_ok": miss_ok,
    }

def run_rookdb_benchmark(csv_path: Path, output_json: Path) -> Dict:
    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "primary_key_benchmark",
        "--",
        "--input",
        str(csv_path),
        "--output",
        str(output_json),
    ]
    subprocess.run(cmd, check=True)
    return json.loads(output_json.read_text(encoding="utf-8"))


def verify_correctness(sqlite_info: Dict, duckdb_info: Dict, dict_info: Dict, rookdb_info: Dict) -> Dict:
    total_rows = sqlite_info["total_rows"]
    unique_pk = sqlite_info["unique_primary_keys"]

    algo_checks = []
    all_ok = True
    for algo in rookdb_info.get("algorithms", []):
        algo_ok = (
            bool(algo.get("correctness_ok"))
            and int(algo.get("total_keys", -1)) == total_rows
            and int(rookdb_info.get("primary_key_unique_count", -1)) == unique_pk
        )
        if not algo_ok:
            all_ok = False
        algo_checks.append(
            {
                "algorithm": algo.get("algorithm"),
                "correctness_ok": bool(algo.get("correctness_ok")),
                "total_keys_match_sqlite": int(algo.get("total_keys", -1)) == total_rows,
                "primary_key_unique_match_sqlite": int(rookdb_info.get("primary_key_unique_count", -1)) == unique_pk,
            }
        )

    return {
        "overall_ok": all_ok and sqlite_info["miss_checks_ok"] and dict_info["miss_checks_ok"] and duckdb_info["miss_checks_ok"],
        "sqlite_total_rows": total_rows,
        "sqlite_unique_primary_keys": unique_pk,
        "rookdb_total_rows": int(rookdb_info.get("total_rows", -1)),
        "rookdb_unique_primary_keys": int(rookdb_info.get("primary_key_unique_count", -1)),
        "sqlite_miss_checks_ok": bool(sqlite_info.get("miss_checks_ok")),
        "duckdb_miss_checks_ok": bool(duckdb_info.get("miss_checks_ok")),
        "dict_miss_checks_ok": bool(dict_info.get("miss_checks_ok")),
        "algorithm_checks": algo_checks,
    }


def parse_scales(scales_raw: str) -> List[int]:
    out = []
    for tok in scales_raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(int(tok))
    if not out:
        raise ValueError("no valid scales provided")
    return sorted(set(out))


def plot_search_latency_bar(latency_csv: Path, out_png: Path) -> None:
    systems = []
    p95_vals = []

    with latency_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = f"{row['system']}:{row['algorithm']}"
            if row["search_p95_us"]:
                systems.append(name)
                p95_vals.append(float(row["search_p95_us"]))

    plt.figure(figsize=(14, 6))
    plt.bar(range(len(systems)), p95_vals)
    plt.xticks(range(len(systems)), systems, rotation=50, ha="right")
    plt.ylabel("Search p95 latency (us)")
    plt.title("Primary-key Search p95: SQLite vs DuckDB vs Dict vs RookDB Indexes")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=180)
    plt.close()


def plot_insert_latency_rookdb(latency_csv: Path, out_png: Path) -> None:
    algos = []
    vals = []

    with latency_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["system"] == "rookdb" and row["insert_p95_us"]:
                algos.append(row["algorithm"])
                vals.append(float(row["insert_p95_us"]))

    plt.figure(figsize=(12, 5))
    plt.bar(range(len(algos)), vals)
    plt.xticks(range(len(algos)), algos, rotation=35, ha="right")
    plt.ylabel("Insert p95 latency (us)")
    plt.title("RookDB Primary-key Insert p95 by Index")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=180)
    plt.close()


def plot_scalability(scalability_csv: Path, out_png: Path) -> None:
    rows = []
    sqlite_p95 = []
    duckdb_p95 = []
    dict_p95 = []
    rookdb_best_p95 = []

    per_rows_rook: Dict[int, List[float]] = {}
    per_rows_sqlite: Dict[int, float] = {}
    per_rows_duckdb: Dict[int, float] = {}
    per_rows_dict: Dict[int, float] = {}

    with scalability_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            n = int(row["rows"])
            p95 = float(row["search_p95_us"])
            if row["system"] == "sqlite":
                per_rows_sqlite[n] = p95
            elif row["system"] == "duckdb":
                per_rows_duckdb[n] = p95
            elif row["system"] == "python_dict":
                per_rows_dict[n] = p95
            else:
                per_rows_rook.setdefault(n, []).append(p95)

    rows = sorted(set(list(per_rows_sqlite.keys()) + list(per_rows_rook.keys())))
    for n in rows:
        sqlite_p95.append(per_rows_sqlite.get(n, float("nan")))
        duckdb_p95.append(per_rows_duckdb.get(n, float("nan")))
        dict_p95.append(per_rows_dict.get(n, float("nan")))
        best = min(per_rows_rook.get(n, [float("nan")]))
        rookdb_best_p95.append(best)

    plt.figure(figsize=(10, 5))
    plt.plot(rows, sqlite_p95, marker="o", label="SQLite p95")
    plt.plot(rows, duckdb_p95, marker="v", label="DuckDB p95")
    plt.plot(rows, dict_p95, marker="x", label="Python Dict p95")
    plt.plot(rows, rookdb_best_p95, marker="s", label="Best RookDB index p95")
    plt.xlabel("Rows")
    plt.ylabel("Search p95 latency (us)")
    plt.title("Scalability: Search p95 vs Dataset Size")
    plt.legend()
    plt.grid(True, alpha=0.25)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=180)
    plt.close()


def write_comparison_csv(path: Path, sqlite_info: Dict, duckdb_info: Dict, dict_info: Dict, rookdb_info: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "system",
            "algorithm",
            "search_p95_us",
            "search_avg_us",
            "search_p99_us",
            "search_throughput_ops_s",
            "insert_p95_us",
            "insert_avg_us",
            "insert_p99_us",
            "insert_throughput_ops_s",
            "correctness_ok",
        ])

        writer.writerow([
            "sqlite",
            "primary_key_index",
            f"{sqlite_info['lookup_latency']['p95_us']:.6f}",
            f"{sqlite_info['lookup_latency']['avg_us']:.6f}",
            f"{sqlite_info['lookup_latency']['p99_us']:.6f}",
            f"{sqlite_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])
        
        writer.writerow([
            "duckdb",
            "primary_key_index",
            f"{duckdb_info['lookup_latency']['p95_us']:.6f}",
            f"{duckdb_info['lookup_latency']['avg_us']:.6f}",
            f"{duckdb_info['lookup_latency']['p99_us']:.6f}",
            f"{duckdb_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])
        
        writer.writerow([
            "python_dict",
            "in_memory_hash",
            f"{dict_info['lookup_latency']['p95_us']:.6f}",
            f"{dict_info['lookup_latency']['avg_us']:.6f}",
            f"{dict_info['lookup_latency']['p99_us']:.6f}",
            f"{dict_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])

        for algo in rookdb_info.get("algorithms", []):
            writer.writerow([
                "rookdb",
                algo.get("algorithm"),
                f"{float(algo['search_latency']['p95_us']):.6f}",
                f"{float(algo['search_latency']['avg_us']):.6f}",
                f"{float(algo['search_latency']['p99_us']):.6f}",
                f"{(1_000_000.0 / float(algo['search_latency']['avg_us'])) if float(algo['search_latency']['avg_us']) > 0 else 0.0:.6f}",
                f"{float(algo['insert_latency']['p95_us']):.6f}",
                f"{float(algo['insert_latency']['avg_us']):.6f}",
                f"{float(algo['insert_latency']['p99_us']):.6f}",
                f"{(1_000_000.0 / float(algo['insert_latency']['avg_us'])) if float(algo['insert_latency']['avg_us']) > 0 else 0.0:.6f}",
                str(bool(algo.get("correctness_ok"))).lower(),
            ])


def write_scalability_csv(path: Path, scale_rows: List[List[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rows",
            "system",
            "algorithm",
            "search_p95_us",
            "search_avg_us",
            "search_p99_us",
            "search_throughput_ops_s",
            "insert_p95_us",
            "insert_avg_us",
            "insert_p99_us",
            "insert_throughput_ops_s",
            "correctness_ok",
        ])
        for r in scale_rows:
            writer.writerow(r)


def write_report(
    path: Path,
    rows: int,
    seed: int,
    sqlite_info: Dict,
    duckdb_info: Dict,
    dict_info: Dict,
    verification: Dict,
) -> None:
    lines = [
        "# RookDB Primary-Key Benchmark (SQLite, DuckDB & Dict Baselines)",
        "",
        "## Dataset",
        f"- Rows: {rows}",
        f"- Seed: {seed}",
        "- Data: controlled synthetic orders with heavy-tail customers, skewed categories, and bursty timestamps",
        "",
        "## DuckDB Baseline (Measured)",
        f"- Total rows: {duckdb_info['total_rows']}",
        f"- Unique primary keys: {duckdb_info['unique_primary_keys']}",
        f"- Search p95 latency: {duckdb_info['lookup_latency']['p95_us']:.6f} us",
        f"- Search p99 latency: {duckdb_info['lookup_latency']['p99_us']:.6f} us",
        f"- Search avg latency: {duckdb_info['lookup_latency']['avg_us']:.6f} us",
        f"- Search throughput: {duckdb_info['lookup_latency']['throughput_ops_s']:.2f} ops/s",
        "",
        "## SQLite Baseline (Measured)",
        f"- Total rows: {sqlite_info['total_rows']}",
        f"- Unique primary keys: {sqlite_info['unique_primary_keys']}",
        f"- Search p95 latency: {sqlite_info['lookup_latency']['p95_us']:.6f} us",
        f"- Search p99 latency: {sqlite_info['lookup_latency']['p99_us']:.6f} us",
        f"- Search avg latency: {sqlite_info['lookup_latency']['avg_us']:.6f} us",
        f"- Search throughput: {sqlite_info['lookup_latency']['throughput_ops_s']:.2f} ops/s",
        "",
        "## Python Dict Baseline (Speed of Light limit)",
        f"- Unique primary keys: {dict_info['unique_primary_keys']}",
        f"- Search p95 latency: {dict_info['lookup_latency']['p95_us']:.6f} us",
        f"- Search p99 latency: {dict_info['lookup_latency']['p99_us']:.6f} us",
        f"- Search avg latency: {dict_info['lookup_latency']['avg_us']:.6f} us",
        f"- Search throughput: {dict_info['lookup_latency']['throughput_ops_s']:.2f} ops/s",
        "",
        "## Correctness Cross-Verification",
        f"- Overall status: {'PASS' if verification['overall_ok'] else 'FAIL'}",
        f"- SQLite miss checks: {'PASS' if verification['sqlite_miss_checks_ok'] else 'FAIL'}",
        f"- DuckDB miss checks: {'PASS' if verification['duckdb_miss_checks_ok'] else 'FAIL'}",
        f"- Dict miss checks: {'PASS' if verification['dict_miss_checks_ok'] else 'FAIL'}",
        "- RookDB algorithms tested on primary key: 9",
        "",
        "## Artifacts",
        "- Benchmarking/results/rookdb_primary_key_metrics.json",
        "- Benchmarking/results/latency_comparison.csv",
        "- Benchmarking/results/scalability_summary.csv",
        "- Benchmarking/results/correctness_verification.json",
        "- Benchmarking/results/charts/search_p95_comparison.png",
        "- Benchmarking/results/charts/rookdb_insert_p95.png",
        "- Benchmarking/results/charts/scalability_search_p95.png",
        "",
        "## Reference Metrics Context",
        "- Latency percentiles (p50/p95/p99) and throughput are standard service-benchmark metrics.",
        "- Reference: Cooper et al., 2010, YCSB (SoCC).",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()

    root = Path(__file__).resolve().parents[1]
    csv_path = root / args.data_csv
    sqlite_db = root / args.sqlite_db
    duckdb_db = root / args.duckdb_db
    rookdb_json = root / args.rookdb_json
    comparison_csv = root / args.comparison_csv
    verification_json = root / args.verification_json
    report_md = root / args.report_md
    scalability_csv = root / args.scalability_csv
    charts_dir = root / args.charts_dir

    scales = parse_scales(args.scales)

    generate_synthetic_orders(csv_path, args.rows, args.seed)
    
    # Run Baselines
    sqlite_info = sqlite_baseline(csv_path, sqlite_db)
    duckdb_info = duckdb_baseline(csv_path, duckdb_db)
    dict_info = python_dict_baseline(csv_path)

    rookdb_json.parent.mkdir(parents=True, exist_ok=True)
    rookdb_info = run_rookdb_benchmark(csv_path, rookdb_json)

    verification = verify_correctness(sqlite_info, duckdb_info, dict_info, rookdb_info)
    verification_json.write_text(json.dumps(verification, indent=2), encoding="utf-8")

    write_comparison_csv(comparison_csv, sqlite_info, duckdb_info, dict_info, rookdb_info)

    # Scalability sweep.
    scale_rows: List[List[str]] = []
    for n in scales:
        scale_csv = root / f"Benchmarking/data/synthetic_orders_{n}.csv"
        scale_db = root / f"Benchmarking/results/sqlite_baseline_{n}.db"
        scale_duckdb = root / f"Benchmarking/results/duckdb_baseline_{n}.db"
        scale_rook = root / f"Benchmarking/results/rookdb_primary_key_metrics_{n}.json"

        generate_synthetic_orders(scale_csv, n, args.seed + n)
        s_info = sqlite_baseline(scale_csv, scale_db)
        dduck_info = duckdb_baseline(scale_csv, scale_duckdb)
        d_info = python_dict_baseline(scale_csv)
        r_info = run_rookdb_benchmark(scale_csv, scale_rook)

        scale_rows.append([
            str(n),
            "sqlite",
            "primary_key_index",
            f"{s_info['lookup_latency']['p95_us']:.6f}",
            f"{s_info['lookup_latency']['avg_us']:.6f}",
            f"{s_info['lookup_latency']['p99_us']:.6f}",
            f"{s_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])
        
        scale_rows.append([
            str(n),
            "duckdb",
            "primary_key_index",
            f"{dduck_info['lookup_latency']['p95_us']:.6f}",
            f"{dduck_info['lookup_latency']['avg_us']:.6f}",
            f"{dduck_info['lookup_latency']['p99_us']:.6f}",
            f"{dduck_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])
        
        scale_rows.append([
            str(n),
            "python_dict",
            "in_memory_hash",
            f"{d_info['lookup_latency']['p95_us']:.6f}",
            f"{d_info['lookup_latency']['avg_us']:.6f}",
            f"{d_info['lookup_latency']['p99_us']:.6f}",
            f"{d_info['lookup_latency']['throughput_ops_s']:.6f}",
            "",
            "",
            "",
            "",
            "true",
        ])

        for algo in r_info.get("algorithms", []):
            scale_rows.append([
                str(n),
                "rookdb",
                algo.get("algorithm"),
                f"{float(algo['search_latency']['p95_us']):.6f}",
                f"{float(algo['search_latency']['avg_us']):.6f}",
                f"{float(algo['search_latency']['p99_us']):.6f}",
                f"{(1_000_000.0 / float(algo['search_latency']['avg_us'])) if float(algo['search_latency']['avg_us']) > 0 else 0.0:.6f}",
                f"{float(algo['insert_latency']['p95_us']):.6f}",
                f"{float(algo['insert_latency']['avg_us']):.6f}",
                f"{float(algo['insert_latency']['p99_us']):.6f}",
                f"{(1_000_000.0 / float(algo['insert_latency']['avg_us'])) if float(algo['insert_latency']['avg_us']) > 0 else 0.0:.6f}",
                str(bool(algo.get("correctness_ok"))).lower(),
            ])

    write_scalability_csv(scalability_csv, scale_rows)

    # Charts.
    plot_search_latency_bar(comparison_csv, charts_dir / "search_p95_comparison.png")
    plot_insert_latency_rookdb(comparison_csv, charts_dir / "rookdb_insert_p95.png")
    plot_scalability(scalability_csv, charts_dir / "scalability_search_p95.png")
    write_report(report_md, args.rows, args.seed, sqlite_info, duckdb_info, dict_info, verification)
    
    print("Benchmark completed. Report generated at", report_md)
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
