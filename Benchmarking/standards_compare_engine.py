#!/usr/bin/env python3
"""Measured-reference comparison engine for benchmark standards analysis.

This engine compares RookDB workload behavior against *measured* reference
systems, not hardcoded vectors:
- SQLite (B-tree index)
- SortedContainers SortedList (tree-like ordered structure)
- Python dict (hash-like point access reference)
"""

from __future__ import annotations

import random
import sqlite3
import time
from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import Dict, List

import numpy as np
from sortedcontainers import SortedList


WORKLOAD_ORDER = ["insert_heavy", "read_heavy", "mixed", "range_query"]


@dataclass
class ComparisonOutcome:
    series_labels: List[str]
    values_matrix: List[List[float]]
    raw_series_labels: List[str]
    raw_values_matrix: List[List[float]]
    csv_rows: List[Dict[str, float | str]]
    markdown: str


class StandardsComparator:
    def __init__(self) -> None:
        self.references = [
            "Cooper et al. (2010), Benchmarking Cloud Serving Systems with YCSB, SoCC.",
            "YCSB project documentation: https://github.com/brianfrankcooper/YCSB",
            "SQLite documentation: https://www.sqlite.org/docs.html",
            "SortedContainers documentation: https://grantjenks.com/docs/sortedcontainers/",
        ]

    @staticmethod
    def _workload_mix(workload: str) -> Dict[str, float]:
        if workload == "insert_heavy":
            return {"insert": 0.8, "read": 0.2, "delete": 0.0, "range": 0.0}
        if workload == "read_heavy":
            return {"insert": 0.1, "read": 0.9, "delete": 0.0, "range": 0.0}
        if workload == "mixed":
            return {"insert": 0.45, "read": 0.45, "delete": 0.10, "range": 0.0}
        return {"insert": 0.0, "read": 0.0, "delete": 0.0, "range": 1.0}

    @staticmethod
    def _p95(samples_us: List[float]) -> float:
        if not samples_us:
            return float("nan")
        arr = sorted(samples_us)
        rank = int(round(0.95 * (len(arr) - 1)))
        return float(arr[rank])

    @staticmethod
    def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        denom = float(np.linalg.norm(a) * np.linalg.norm(b))
        if denom == 0.0:
            return 0.0
        return float(np.dot(a, b) / denom)

    @staticmethod
    def _profile_distance(rook_vec: np.ndarray, ref_vec: np.ndarray) -> Dict[str, float]:
        mask = np.isfinite(rook_vec) & np.isfinite(ref_vec)
        if not np.any(mask):
            return {"cosine_similarity": 0.0, "mae": float("nan"), "rmse": float("nan")}
        rv = rook_vec[mask]
        pv = ref_vec[mask]
        diff = rv - pv
        return {
            "cosine_similarity": StandardsComparator._cosine_similarity(rv, pv),
            "mae": float(np.mean(np.abs(diff))),
            "rmse": float(sqrt(float(np.mean(diff * diff)))),
        }

    def _aggregate_rookdb_raw(self, results: List[Dict]) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        for workload in WORKLOAD_ORDER:
            vals = [
                float(r["latency_us"]["p95"])
                for r in results
                if r.get("workload") == workload
                and not (workload == "range_query" and r.get("range_workload_skipped", False))
            ]
            out[workload] = {
                "mean": float(mean(vals)) if vals else float("nan"),
                "std": float(pstdev(vals)) if len(vals) > 1 else 0.0,
            }
        return out

    def _run_sqlite_reference(
        self, preload: int, ops: int, range_width: int, seed: int
    ) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        rng = random.Random(seed)
        base_keys = [rng.randint(-1_000_000, 1_000_000) for _ in range(preload)]

        for workload in WORKLOAD_ORDER:
            conn = sqlite3.connect(":memory:")
            cur = conn.cursor()
            cur.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v INTEGER)")
            cur.executemany("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", [(k, k) for k in base_keys])
            conn.commit()

            latencies: List[float] = []
            keys = list(base_keys)
            mix = self._workload_mix(workload)

            for _ in range(ops):
                p = rng.random()
                if p < mix["insert"]:
                    k = rng.randint(-1_000_000, 1_000_000)
                    t0 = time.perf_counter()
                    cur.execute("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", (k, k))
                    conn.commit()
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                    keys.append(k)
                elif p < mix["insert"] + mix["read"]:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    cur.execute("SELECT v FROM kv WHERE k = ?", (k,))
                    cur.fetchone()
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                elif p < mix["insert"] + mix["read"] + mix["delete"]:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    cur.execute("DELETE FROM kv WHERE k = ?", (k,))
                    conn.commit()
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                else:
                    anchor = keys[rng.randrange(len(keys))] if keys else 0
                    lo = anchor - range_width // 2
                    hi = anchor + range_width // 2
                    t0 = time.perf_counter()
                    cur.execute("SELECT COUNT(*) FROM kv WHERE k BETWEEN ? AND ?", (lo, hi))
                    cur.fetchone()
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)

            conn.close()
            out[workload] = {"p95": self._p95(latencies)}

        return out

    def _run_sorted_reference(
        self, preload: int, ops: int, range_width: int, seed: int
    ) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        rng = random.Random(seed)
        base_keys = [rng.randint(-1_000_000, 1_000_000) for _ in range(preload)]

        for workload in WORKLOAD_ORDER:
            tree = SortedList(base_keys)
            latencies: List[float] = []
            keys = list(base_keys)
            mix = self._workload_mix(workload)

            for _ in range(ops):
                p = rng.random()
                if p < mix["insert"]:
                    k = rng.randint(-1_000_000, 1_000_000)
                    t0 = time.perf_counter()
                    tree.add(k)
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                    keys.append(k)
                elif p < mix["insert"] + mix["read"]:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    _ = k in tree
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                elif p < mix["insert"] + mix["read"] + mix["delete"]:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    idx = tree.bisect_left(k)
                    if idx < len(tree) and tree[idx] == k:
                        tree.pop(idx)
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                else:
                    anchor = keys[rng.randrange(len(keys))] if keys else 0
                    lo = anchor - range_width // 2
                    hi = anchor + range_width // 2
                    t0 = time.perf_counter()
                    _ = tree.bisect_right(hi) - tree.bisect_left(lo)
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)

            out[workload] = {"p95": self._p95(latencies)}

        return out

    def _run_hash_reference(self, preload: int, ops: int, seed: int) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        rng = random.Random(seed)
        base_keys = [rng.randint(-1_000_000, 1_000_000) for _ in range(preload)]

        for workload in WORKLOAD_ORDER:
            h: Dict[int, int] = {k: 1 for k in base_keys}
            latencies: List[float] = []
            keys = list(base_keys)
            mix = self._workload_mix(workload)

            if workload == "range_query":
                out[workload] = {"p95": float("nan")}
                continue

            for _ in range(ops):
                p = rng.random()
                if p < mix["insert"]:
                    k = rng.randint(-1_000_000, 1_000_000)
                    t0 = time.perf_counter()
                    h[k] = h.get(k, 0) + 1
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                    keys.append(k)
                elif p < mix["insert"] + mix["read"]:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    _ = h.get(k)
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)
                else:
                    k = keys[rng.randrange(len(keys))] if keys else 0
                    t0 = time.perf_counter()
                    h.pop(k, None)
                    latencies.append((time.perf_counter() - t0) * 1_000_000.0)

            out[workload] = {"p95": self._p95(latencies)}

        return out

    def compare(
        self,
        results: List[Dict],
        *,
        preload: int,
        ops: int,
        range_width: int,
        seed: int,
        repeats: int,
    ) -> ComparisonOutcome:
        rookdb = self._aggregate_rookdb_raw(results)

        ref_labels = [
            "SQLite (B-tree)",
            "SortedContainers (tree-like)",
            "Python dict (hash-like)",
        ]
        ref_runs: Dict[str, List[Dict[str, Dict[str, float]]]] = {label: [] for label in ref_labels}

        for i in range(max(1, repeats)):
            ref_seed = seed + i * 1000
            ref_runs["SQLite (B-tree)"].append(self._run_sqlite_reference(preload, ops, range_width, ref_seed))
            ref_runs["SortedContainers (tree-like)"].append(
                self._run_sorted_reference(preload, ops, range_width, ref_seed + 7)
            )
            ref_runs["Python dict (hash-like)"].append(self._run_hash_reference(preload, ops, ref_seed + 13))

        ref_agg: Dict[str, Dict[str, float]] = {}
        for label, runs in ref_runs.items():
            ref_agg[label] = {}
            for workload in WORKLOAD_ORDER:
                vals = [float(r[workload]["p95"]) for r in runs if np.isfinite(r[workload]["p95"])]
                ref_agg[label][workload] = float(mean(vals)) if vals else float("nan")

        raw_series_labels = ["RookDB"] + ref_labels
        raw_values_matrix: List[List[float]] = []
        raw_values_matrix.append([rookdb[w]["mean"] for w in WORKLOAD_ORDER])
        for label in ref_labels:
            raw_values_matrix.append([ref_agg[label][w] for w in WORKLOAD_ORDER])

        finite_values = [v for row in raw_values_matrix for v in row if np.isfinite(v) and v > 0]
        global_min = min(finite_values) if finite_values else 1.0
        norm_values_matrix: List[List[float]] = []
        for row in raw_values_matrix:
            norm_values_matrix.append([
                (float(v) / global_min) if np.isfinite(v) and v > 0 else float("nan")
                for v in row
            ])

        rook_vec = np.array(norm_values_matrix[0], dtype=float)
        profile_metrics = []
        csv_rows: List[Dict[str, float | str]] = []
        for idx, label in enumerate(ref_labels, start=1):
            ref_vec = np.array(norm_values_matrix[idx], dtype=float)
            metrics = self._profile_distance(rook_vec, ref_vec)
            profile_metrics.append({"profile": label, **metrics})
            for workload, rv, pv in zip(WORKLOAD_ORDER, rook_vec, ref_vec):
                csv_rows.append(
                    {
                        "workload": workload,
                        "rookdb_normalized_latency_index": float(rv) if np.isfinite(rv) else float("nan"),
                        "profile": label,
                        "profile_latency_index": float(pv) if np.isfinite(pv) else float("nan"),
                        "absolute_delta": float(abs(rv - pv)) if np.isfinite(rv) and np.isfinite(pv) else float("nan"),
                    }
                )

        profile_metrics_sorted = sorted(profile_metrics, key=lambda x: (-x["cosine_similarity"], x["rmse"]))
        metric_lines = "\n".join(
            [
                f"- {m['profile']}: cosine={m['cosine_similarity']:.4f}, rmse={m['rmse']:.4f}, mae={m['mae']:.4f}"
                for m in profile_metrics_sorted
            ]
        )

        rook_lines = "\n".join(
            [
                f"- {w}: avg p95={rookdb[w]['mean']:.3f} us, std={rookdb[w]['std']:.3f} us"
                for w in WORKLOAD_ORDER
            ]
        )
        ref_lines = "\n".join([f"- {r}" for r in self.references])

        markdown = f"""# Reference Systems Comparison

## Method
- Comparison engine: `Benchmarking/standards_compare_engine.py`
- Reference systems are measured, not hardcoded:
  - SQLite (B-tree)
  - SortedContainers (tree-like ordered structure)
  - Python dict (hash-like structure)
- Similarity metrics are computed on consistently normalized vectors.
- Raw p95 values are also reported to preserve absolute performance meaning.

## Caveat
- This is still a lightweight proxy benchmark, not a full cross-DBMS publication-grade study.

## RookDB Workload Summary
{rook_lines}

## Similarity Scores (Normalized)
{metric_lines}

## Artifacts
- CSV: Benchmarking/results/standards_comparison.csv
- Normalized graph: Benchmarking/results/charts/standards_latency_baseline_compare.svg
- Raw graph: Benchmarking/results/charts/standards_raw_p95_by_workload.svg

## References
{ref_lines}
"""

        return ComparisonOutcome(
            series_labels=raw_series_labels,
            values_matrix=norm_values_matrix,
            raw_series_labels=raw_series_labels,
            raw_values_matrix=raw_values_matrix,
            csv_rows=csv_rows,
            markdown=markdown,
        )
