#!/usr/bin/env python3
"""Run end-to-end index benchmarks for RookDB and generate charts/report.

This script:
1) Executes the Rust benchmark runner for all index algorithms and workloads.
2) Converts raw JSON output into summary CSV tables.
3) Produces charts for latency, build time, index size, and logical I/O count.
4) Produces a Markdown analysis report for documentation.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Tuple

from standards_compare_engine import StandardsComparator


WORKLOAD_ORDER = ["insert_heavy", "read_heavy", "mixed", "range_query"]


@dataclass
class BenchmarkPaths:
    root: Path
    results_dir: Path
    charts_dir: Path
    raw_json: Path
    summary_csv: Path
    workload_csv: Path
    report_md: Path
    standards_csv: Path
    standards_report_md: Path
    standards_chart_svg: Path
    standards_raw_chart_svg: Path
    dat_validation_json: Path
    index_validation_json: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RookDB benchmark driver")
    parser.add_argument("--preload", type=int, default=20000, help="preloaded rows per benchmark case")
    parser.add_argument("--ops", type=int, default=8000, help="operations per workload")
    parser.add_argument("--range-width", type=int, default=64, help="range width for range scans")
    parser.add_argument("--seed", type=int, default=7, help="base RNG seed")
    parser.add_argument("--repeats", type=int, default=5, help="number of repeated benchmark runs")
    parser.add_argument(
        "--cargo-profile",
        choices=["release", "debug"],
        default="release",
        help="cargo profile used for benchmark binary",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="skip executing the Rust benchmark and only post-process existing raw JSON",
    )
    parser.add_argument(
        "--validate-dat",
        action="store_true",
        help="Run independent .dat correctness validation after benchmarking",
    )
    parser.add_argument(
        "--validate-index",
        action="store_true",
        help="Run read/write/search/range index validation plus corruption checks",
    )
    return parser.parse_args()


def resolve_paths() -> BenchmarkPaths:
    root = Path(__file__).resolve().parents[1]
    results_dir = root / "Benchmarking" / "results"
    charts_dir = results_dir / "charts"
    return BenchmarkPaths(
        root=root,
        results_dir=results_dir,
        charts_dir=charts_dir,
        raw_json=results_dir / "raw_results.json",
        summary_csv=results_dir / "summary_by_index.csv",
        workload_csv=results_dir / "summary_by_workload.csv",
        report_md=results_dir / "analysis_report.md",
        standards_csv=results_dir / "standards_comparison.csv",
        standards_report_md=results_dir / "standards_comparison.md",
        standards_chart_svg=charts_dir / "standards_latency_baseline_compare.svg",
        standards_raw_chart_svg=charts_dir / "standards_raw_p95_by_workload.svg",
        dat_validation_json=results_dir / "dat_validation_report.json",
        index_validation_json=results_dir / "index_validation_report.json",
    )


def run_rust_benchmark(args: argparse.Namespace, paths: BenchmarkPaths) -> None:
    all_rows: List[Dict] = []
    first_meta: Dict | None = None

    for run_id in range(args.repeats):
        run_output = paths.results_dir / f"raw_results_run_{run_id + 1}.json"
        cmd = [
            "cargo",
            "run",
            "--bin",
            "index_benchmark",
            "--",
            "--output",
            str(run_output),
            "--index-dir",
            str(paths.results_dir / "index_files"),
            "--preload",
            str(args.preload),
            "--ops",
            str(args.ops),
            "--range-width",
            str(args.range_width),
            "--seed",
            str(args.seed + run_id * 100_000),
        ]

        if args.cargo_profile == "release":
            cmd.insert(2, "--release")

        print("Running:", " ".join(cmd))
        subprocess.run(cmd, cwd=paths.root, check=True)

        run_payload = json.loads(run_output.read_text(encoding="utf-8"))
        if first_meta is None:
            first_meta = run_payload.get("metadata", {})
        for row in run_payload.get("results", []):
            row_copy = dict(row)
            row_copy["run_id"] = run_id + 1
            all_rows.append(row_copy)

    merged = {
        "metadata": {
            **(first_meta or {}),
            "repeats": args.repeats,
        },
        "results": all_rows,
    }
    paths.raw_json.write_text(json.dumps(merged, indent=2), encoding="utf-8")


def load_results(raw_path: Path) -> Dict:
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw benchmark output: {raw_path}")
    with raw_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def by_index(results: List[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = {}
    for row in results:
        grouped.setdefault(row["index_algorithm"], []).append(row)
    return grouped


def by_workload(results: List[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = {}
    for row in results:
        grouped.setdefault(row["workload"], []).append(row)
    return grouped


def safe_mean(values: Iterable[float]) -> float:
    vals = list(values)
    if not vals:
        return 0.0
    return float(mean(vals))


def safe_std(values: Iterable[float]) -> float:
    vals = [float(v) for v in values]
    if len(vals) <= 1:
        return 0.0
    return float(pstdev(vals))


def write_csv_summaries(results: List[Dict], paths: BenchmarkPaths) -> None:
    grouped_index = by_index(results)
    with paths.summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "index_algorithm",
            "avg_build_time_ms",
            "std_build_time_ms",
            "avg_index_size_bytes",
            "std_index_size_bytes",
            "avg_latency_p95_us",
            "std_latency_p95_us",
            "avg_latency_p99_us",
            "std_latency_p99_us",
            "avg_io_ops",
            "std_io_ops",
        ])

        for algo in sorted(grouped_index.keys()):
            rows = grouped_index[algo]
            writer.writerow([
                algo,
                f"{safe_mean(r['build_time_ms'] for r in rows):.3f}",
                f"{safe_std(r['build_time_ms'] for r in rows):.3f}",
                f"{safe_mean(r['index_size_bytes'] for r in rows):.1f}",
                f"{safe_std(r['index_size_bytes'] for r in rows):.3f}",
                f"{safe_mean(r['latency_us']['p95'] for r in rows):.3f}",
                f"{safe_std(r['latency_us']['p95'] for r in rows):.3f}",
                f"{safe_mean(r['latency_us']['p99'] for r in rows):.3f}",
                f"{safe_std(r['latency_us']['p99'] for r in rows):.3f}",
                f"{safe_mean(r['io_operations_count'] for r in rows):.1f}",
                f"{safe_std(r['io_operations_count'] for r in rows):.3f}",
            ])

    grouped_workload = by_workload(results)
    with paths.workload_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "workload",
            "avg_latency_p95_us",
            "std_latency_p95_us",
            "avg_latency_p99_us",
            "std_latency_p99_us",
            "avg_io_ops",
            "std_io_ops",
            "cases",
        ])

        for workload in WORKLOAD_ORDER:
            rows = grouped_workload.get(workload, [])
            if workload == "range_query":
                rows = [r for r in rows if not r.get("range_workload_skipped", False)]
            writer.writerow([
                workload,
                f"{safe_mean(r['latency_us']['p95'] for r in rows):.3f}",
                f"{safe_std(r['latency_us']['p95'] for r in rows):.3f}",
                f"{safe_mean(r['latency_us']['p99'] for r in rows):.3f}",
                f"{safe_std(r['latency_us']['p99'] for r in rows):.3f}",
                f"{safe_mean(r['io_operations_count'] for r in rows):.1f}",
                f"{safe_std(r['io_operations_count'] for r in rows):.3f}",
                len(rows),
            ])


def write_bar_chart_svg(
    out: Path,
    labels: List[str],
    values: List[float],
    title: str,
    y_label: str,
) -> None:
    width = 1200
    height = 680
    margin_left = 80
    margin_right = 30
    margin_top = 60
    margin_bottom = 140
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    max_v = max(values) if values else 1.0
    max_v = max(max_v, 1.0)
    bar_w = plot_w / max(len(labels), 1)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<style>text{font-family:Arial,sans-serif} .axis{stroke:#333;stroke-width:1} .grid{stroke:#ddd;stroke-width:1}</style>',
        f'<text x="{width/2}" y="28" text-anchor="middle" font-size="20">{title}</text>',
        f'<text x="20" y="{height/2}" transform="rotate(-90 20 {height/2})" text-anchor="middle" font-size="14">{y_label}</text>',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}"/>',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}"/>',
    ]

    ticks = 5
    for i in range(ticks + 1):
        frac = i / ticks
        y = margin_top + plot_h - frac * plot_h
        val = frac * max_v
        parts.append(f'<line class="grid" x1="{margin_left}" y1="{y:.1f}" x2="{margin_left + plot_w}" y2="{y:.1f}"/>')
        parts.append(f'<text x="{margin_left - 8}" y="{y + 4:.1f}" text-anchor="end" font-size="11">{val:.1f}</text>')

    for i, (label, value) in enumerate(zip(labels, values)):
        x = margin_left + i * bar_w + bar_w * 0.15
        w = bar_w * 0.7
        h = (value / max_v) * plot_h
        y = margin_top + plot_h - h
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" fill="#2E6F95"/>')
        parts.append(f'<text x="{x + w/2:.1f}" y="{margin_top + plot_h + 18}" text-anchor="middle" font-size="10" transform="rotate(25 {x + w/2:.1f} {margin_top + plot_h + 18})">{label}</text>')
        parts.append(f'<text x="{x + w/2:.1f}" y="{max(y - 5, margin_top + 12):.1f}" text-anchor="middle" font-size="9">{value:.1f}</text>')

    parts.append("</svg>")
    out.write_text("\n".join(parts), encoding="utf-8")


def write_grouped_bar_chart_svg(
    out: Path,
    group_labels: List[str],
    series_labels: List[str],
    values_matrix: List[List[float]],
    title: str,
    y_label: str,
) -> None:
    width = 1400
    height = 760
    margin_left = 90
    margin_right = 40
    margin_top = 70
    margin_bottom = 170
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    flat_vals = [v for row in values_matrix for v in row]
    max_v = max(flat_vals) if flat_vals else 1.0
    max_v = max(max_v, 1.0)

    group_w = plot_w / max(len(group_labels), 1)
    bar_w = group_w / max(len(series_labels) + 2, 2)

    palette = [
        "#2E6F95", "#6A994E", "#BC4749", "#7B2CBF", "#FF7F11",
        "#0081A7", "#3A86FF", "#8338EC", "#FF006E", "#6D6875"
    ]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<style>text{font-family:Arial,sans-serif} .axis{stroke:#333;stroke-width:1} .grid{stroke:#ddd;stroke-width:1}</style>',
        f'<text x="{width/2}" y="30" text-anchor="middle" font-size="21">{title}</text>',
        f'<text x="22" y="{height/2}" transform="rotate(-90 22 {height/2})" text-anchor="middle" font-size="14">{y_label}</text>',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}"/>',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}"/>',
    ]

    for i in range(6):
        frac = i / 5
        y = margin_top + plot_h - frac * plot_h
        val = frac * max_v
        parts.append(f'<line class="grid" x1="{margin_left}" y1="{y:.1f}" x2="{margin_left + plot_w}" y2="{y:.1f}"/>')
        parts.append(f'<text x="{margin_left - 8}" y="{y + 4:.1f}" text-anchor="end" font-size="11">{val:.1f}</text>')

    for g_idx, group in enumerate(group_labels):
        gx = margin_left + g_idx * group_w
        parts.append(f'<text x="{gx + group_w/2:.1f}" y="{margin_top + plot_h + 24}" text-anchor="middle" font-size="12">{group}</text>')
        for s_idx, series in enumerate(series_labels):
            value = values_matrix[s_idx][g_idx]
            x = gx + (s_idx + 1) * bar_w
            h = (value / max_v) * plot_h
            y = margin_top + plot_h - h
            color = palette[s_idx % len(palette)]
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w * 0.8:.1f}" height="{h:.1f}" fill="{color}"/>')

    legend_x = margin_left
    legend_y = height - 85
    for i, series in enumerate(series_labels):
        color = palette[i % len(palette)]
        x = legend_x + i * 130
        parts.append(f'<rect x="{x}" y="{legend_y}" width="12" height="12" fill="{color}"/>')
        parts.append(f'<text x="{x + 18}" y="{legend_y + 10}" font-size="11">{series}</text>')

    parts.append("</svg>")
    out.write_text("\n".join(parts), encoding="utf-8")

def chart_latency_p95(results: List[Dict], paths: BenchmarkPaths) -> Path:
    out = paths.charts_dir / "latency_p95_by_workload.svg"
    algorithms = sorted({r["index_algorithm"] for r in results})
    workload_to_idx = {w: i for i, w in enumerate(WORKLOAD_ORDER)}

    values_matrix: List[List[float]] = []
    for algo in algorithms:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for row in [r for r in results if r["index_algorithm"] == algo]:
            if row.get("workload") == "range_query" and row.get("range_workload_skipped", False):
                continue
            buckets[row["workload"]].append(float(row["latency_us"]["p95"]))
        vals = [safe_mean(buckets.get(w, [])) for w in WORKLOAD_ORDER]
        values_matrix.append(vals)

    write_grouped_bar_chart_svg(
        out=out,
        group_labels=WORKLOAD_ORDER,
        series_labels=algorithms,
        values_matrix=values_matrix,
        title="P95 Query Latency by Workload and Index",
        y_label="Latency (microseconds)",
    )
    return out


def chart_build_time(results: List[Dict], paths: BenchmarkPaths) -> Path:
    out = paths.charts_dir / "build_time_ms_by_index.svg"
    grouped = by_index(results)
    algorithms = sorted(grouped.keys())
    values = [safe_mean(r["build_time_ms"] for r in grouped[a]) for a in algorithms]

    write_bar_chart_svg(
        out=out,
        labels=algorithms,
        values=values,
        title="Average Index Build Time",
        y_label="Build Time (ms)",
    )
    return out


def chart_index_size(results: List[Dict], paths: BenchmarkPaths) -> Path:
    out = paths.charts_dir / "index_size_bytes_by_index.svg"
    grouped = by_index(results)
    algorithms = sorted(grouped.keys())
    values = [safe_mean(r["index_size_bytes"] for r in grouped[a]) for a in algorithms]

    write_bar_chart_svg(
        out=out,
        labels=algorithms,
        values=values,
        title="Average Persisted Index Size",
        y_label="Size (bytes)",
    )
    return out


def chart_io_count(results: List[Dict], paths: BenchmarkPaths) -> Path:
    out = paths.charts_dir / "logical_io_ops_by_workload.svg"
    grouped = by_workload(results)
    labels = WORKLOAD_ORDER
    values = []
    for w in labels:
        rows = grouped.get(w, [])
        if w == "range_query":
            rows = [r for r in rows if not r.get("range_workload_skipped", False)]
        values.append(safe_mean(r["io_operations_count"] for r in rows))

    write_bar_chart_svg(
        out=out,
        labels=labels,
        values=values,
        title="Average Logical Operations (Internal) by Workload",
        y_label="Logical operations (internal metric)",
    )
    return out


def chart_raw_p95_by_workload(
    paths: BenchmarkPaths,
    series_labels: List[str],
    values_matrix: List[List[float]],
) -> Path:
    out = paths.standards_raw_chart_svg
    write_grouped_bar_chart_svg(
        out=out,
        group_labels=WORKLOAD_ORDER,
        series_labels=series_labels,
        values_matrix=values_matrix,
        title="Raw P95 Latency by Workload (Measured Systems)",
        y_label="P95 latency (microseconds)",
    )
    return out


def find_best(results: List[Dict], workload: str, metric: str) -> Tuple[str, float]:
    candidates = [r for r in results if r["workload"] == workload]
    if not candidates:
        return ("n/a", 0.0)
    best = min(candidates, key=lambda r: r["latency_us"][metric])
    return best["index_algorithm"], best["latency_us"][metric]


def write_standards_comparison(results: List[Dict], paths: BenchmarkPaths, metadata: Dict) -> None:
    comparator = StandardsComparator()
    outcome = comparator.compare(
        results,
        preload=int(metadata.get("preload_rows", 12_000)),
        ops=int(metadata.get("operations_per_workload", 5_000)),
        range_width=int(metadata.get("range_width", 64)),
        seed=int(metadata.get("seed", 131)) + 991,
        repeats=max(1, min(5, int(metadata.get("repeats", 1)))),
    )

    with paths.standards_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "workload",
            "rookdb_normalized_latency_index",
            "profile",
            "profile_latency_index",
            "absolute_delta",
        ])
        for row in outcome.csv_rows:
            writer.writerow([
                row["workload"],
                f"{float(row['rookdb_normalized_latency_index']):.6f}",
                row["profile"],
                f"{float(row['profile_latency_index']):.6f}",
                f"{float(row['absolute_delta']):.6f}",
            ])

    write_grouped_bar_chart_svg(
        out=paths.standards_chart_svg,
        group_labels=WORKLOAD_ORDER,
        series_labels=outcome.series_labels,
        values_matrix=outcome.values_matrix,
        title="RookDB vs Measured Reference Systems (Latency Index)",
        y_label="Normalized latency index (lower is better)",
    )

    chart_raw_p95_by_workload(paths, outcome.raw_series_labels, outcome.raw_values_matrix)

    paths.standards_report_md.write_text(outcome.markdown, encoding="utf-8")


def write_report(raw: Dict, results: List[Dict], paths: BenchmarkPaths) -> None:
    metadata = raw.get("metadata", {})

    best_read, best_read_p95 = find_best(results, "read_heavy", "p95")
    best_insert, best_insert_p95 = find_best(results, "insert_heavy", "p95")
    best_mixed, best_mixed_p95 = find_best(results, "mixed", "p95")

    range_rows = [r for r in results if r["workload"] == "range_query" and not r["range_workload_skipped"]]
    best_range_algo = "n/a"
    best_range_val = 0.0
    if range_rows:
        best_range = min(range_rows, key=lambda r: r["latency_us"]["p95"])
        best_range_algo = best_range["index_algorithm"]
        best_range_val = best_range["latency_us"]["p95"]

    skipped_range = [r["index_algorithm"] for r in results if r["workload"] == "range_query" and r["range_workload_skipped"]]

    report = f"""# RookDB Benchmarking Initial Results

## Run Configuration
- Seed: {metadata.get('seed', 'n/a')}
- Preload rows per case: {metadata.get('preload_rows', 'n/a')}
- Operations per workload: {metadata.get('operations_per_workload', 'n/a')}
- Range width: {metadata.get('range_width', 'n/a')}
- Repeats: {metadata.get('repeats', 1)}
- Total benchmark scenarios: {len(results)}

## Workloads Implemented
- Insert-heavy workload
- Read-heavy workload
- Mixed workload
- Range query workload

## Metrics Implemented
- Query latency: min, max, avg, p50, p95, p99
- Logical operations count (internal metric)
- Persisted index size on disk
- Index build time measurement

## Initial Findings
- Best p95 for insert-heavy: **{best_insert}** at **{best_insert_p95:.3f} us**
- Best p95 for read-heavy: **{best_read}** at **{best_read_p95:.3f} us**
- Best p95 for mixed: **{best_mixed}** at **{best_mixed_p95:.3f} us**
- Best p95 for range-query: **{best_range_algo}** at **{best_range_val:.3f} us**

Range query was skipped for these hash indexes (expected): {', '.join(sorted(set(skipped_range))) if skipped_range else 'none'}.

## Generated Artifacts
- Raw benchmark data: Benchmarking/results/raw_results.json
- Summary by index: Benchmarking/results/summary_by_index.csv
- Summary by workload: Benchmarking/results/summary_by_workload.csv
- Charts:
    - Benchmarking/results/charts/latency_p95_by_workload.svg
    - Benchmarking/results/charts/build_time_ms_by_index.svg
    - Benchmarking/results/charts/index_size_bytes_by_index.svg
    - Benchmarking/results/charts/logical_io_ops_by_workload.svg

## Notes and Assumptions
- Logical operations count is an internal benchmark metric: number of benchmarked index operations plus save/load operations per scenario.
- Hash indexes do not support ordered range scans and are marked as skipped for range workload.
- This phase provides initial results; larger-scale runs can be produced by increasing --preload and --ops.
"""

    paths.report_md.write_text(report, encoding="utf-8")


def ensure_dirs(paths: BenchmarkPaths) -> None:
    paths.results_dir.mkdir(parents=True, exist_ok=True)
    paths.charts_dir.mkdir(parents=True, exist_ok=True)


def run_dat_validation(paths: BenchmarkPaths) -> None:
    cmd = [
        sys.executable,
        str(paths.root / "Benchmarking" / "validate_dat_files.py"),
        "--root",
        "database",
        "--output",
        str(paths.dat_validation_json),
    ]
    subprocess.run(cmd, cwd=paths.root, check=True)


def run_index_validation(paths: BenchmarkPaths) -> None:
    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "index_validation",
    ]
    subprocess.run(cmd, cwd=paths.root, check=True)


def main() -> int:
    args = parse_args()
    paths = resolve_paths()
    ensure_dirs(paths)

    if not args.skip_run:
        run_rust_benchmark(args, paths)

    raw = load_results(paths.raw_json)
    results = raw.get("results", [])
    if not results:
        print("No results found in raw benchmark JSON.", file=sys.stderr)
        return 1

    write_csv_summaries(results, paths)
    chart_latency_p95(results, paths)
    chart_build_time(results, paths)
    chart_index_size(results, paths)
    chart_io_count(results, paths)

    write_report(raw, results, paths)
    write_standards_comparison(results, paths, raw.get("metadata", {}))

    if args.validate_dat:
        run_dat_validation(paths)

    if args.validate_index:
        run_index_validation(paths)

    print("Benchmark processing complete.")
    print(f"Raw data: {paths.raw_json}")
    print(f"Summary: {paths.summary_csv}")
    print(f"Report: {paths.report_md}")
    print(f"Standards comparison: {paths.standards_report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
