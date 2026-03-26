#!/usr/bin/env python3
"""Validate all .dat files and run dummy-data correctness checks."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

from dat_validator import (
    PAGE_HEADER_SIZE,
    PAGE_SIZE,
    TABLE_HEADER_SIZE,
    ITEM_ID_SIZE,
    validate_all_dat_files,
)


def _u32(n: int) -> bytes:
    return int(n).to_bytes(4, "little", signed=False)


def create_dummy_valid_file(path: Path) -> Dict:
    """Create a valid dummy .dat file with deterministic tuples and return expected metadata."""
    header = bytearray(TABLE_HEADER_SIZE)
    page = bytearray(PAGE_SIZE)

    tuples = [b"ABCD", b"EFGH", b"IJKL"]

    lower = PAGE_HEADER_SIZE
    upper = PAGE_SIZE

    for i, t in enumerate(tuples):
        upper -= len(t)
        page[upper : upper + len(t)] = t
        page[lower : lower + 4] = _u32(upper)
        page[lower + 4 : lower + 8] = _u32(len(t))
        lower += ITEM_ID_SIZE

    page[0:4] = _u32(lower)
    page[4:8] = _u32(upper)

    # header page_count includes metadata page + 1 data page
    header[0:4] = _u32(2)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header) + bytes(page))

    return {
        "expected_live_slots": len(tuples),
        "expected_page_count": 2,
        "expected_file_size": TABLE_HEADER_SIZE + PAGE_SIZE,
    }


def create_dummy_corrupt_file(path: Path) -> None:
    """Create a corrupt .dat file to verify validator catches corruption."""
    payload = bytearray(TABLE_HEADER_SIZE + PAGE_SIZE)
    payload[0:4] = _u32(99)  # wrong page count on purpose

    base = TABLE_HEADER_SIZE
    payload[base + 0 : base + 4] = _u32(400)  # lower
    payload[base + 4 : base + 8] = _u32(300)  # upper lower>upper invalid

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def run_dummy_validation(dummy_dir: Path) -> Dict:
    valid_path = dummy_dir / "dummy_valid.dat"
    corrupt_path = dummy_dir / "dummy_corrupt.dat"

    expected = create_dummy_valid_file(valid_path)
    create_dummy_corrupt_file(corrupt_path)

    results = validate_all_dat_files(dummy_dir)
    by_name = {Path(r.file_path).name: r for r in results}

    valid = by_name.get("dummy_valid.dat")
    corrupt = by_name.get("dummy_corrupt.dat")

    assertions: List[str] = []
    ok = True

    if valid is None or corrupt is None:
        return {
            "ok": False,
            "assertions": ["missing dummy validation files in results"],
            "results": [asdict(r) for r in results],
        }

    if not valid.ok:
        ok = False
        assertions.append("dummy_valid.dat failed validation")
    if valid.total_live_slots != expected["expected_live_slots"]:
        ok = False
        assertions.append(
            f"dummy_valid.dat slot mismatch: got {valid.total_live_slots}, expected {expected['expected_live_slots']}"
        )
    if valid.page_count_header != expected["expected_page_count"]:
        ok = False
        assertions.append(
            f"dummy_valid.dat page_count mismatch: got {valid.page_count_header}, expected {expected['expected_page_count']}"
        )

    if corrupt.ok:
        ok = False
        assertions.append("dummy_corrupt.dat unexpectedly passed validation")

    if ok:
        assertions.append("dummy_valid.dat passed expected structural checks")
        assertions.append("dummy_corrupt.dat correctly failed structural checks")

    return {
        "ok": ok,
        "assertions": assertions,
        "results": [asdict(r) for r in results],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate RookDB .dat files")
    parser.add_argument(
        "--root",
        default="database",
        help="Root directory under which .dat files are validated",
    )
    parser.add_argument(
        "--output",
        default="Benchmarking/results/dat_validation_report.json",
        help="Path to write JSON validation report",
    )
    parser.add_argument(
        "--dummy-dir",
        default="Benchmarking/tmp/dat_dummy_validation",
        help="Directory for generated dummy validation files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    output = Path(args.output)
    dummy_dir = Path(args.dummy_dir)

    db_results = validate_all_dat_files(root) if root.exists() else []
    dummy_report = run_dummy_validation(dummy_dir)

    summary = {
        "database_root": str(root),
        "validated_dat_files_count": len(db_results),
        "database_files_ok_count": sum(1 for r in db_results if r.ok),
        "database_files_failed_count": sum(1 for r in db_results if not r.ok),
        "dummy_validation_ok": dummy_report["ok"],
    }

    payload = {
        "summary": summary,
        "database_file_results": [asdict(r) for r in db_results],
        "dummy_validation": dummy_report,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote .dat validation report to {output}")
    print(json.dumps(summary, indent=2))
    return 0 if summary["database_files_failed_count"] == 0 and summary["dummy_validation_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
