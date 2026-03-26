#!/usr/bin/env python3
"""Reusable APIs to validate RookDB .dat files and slotted pages."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

PAGE_SIZE = 8192
TABLE_HEADER_SIZE = 8192
PAGE_HEADER_SIZE = 8
ITEM_ID_SIZE = 8


@dataclass
class SlotInfo:
    item_id: int
    offset: int
    length: int
    tombstone: bool


@dataclass
class PageValidation:
    page_no: int
    ok: bool
    lower: int
    upper: int
    slot_count: int
    live_slot_count: int
    errors: List[str]


@dataclass
class DatValidationResult:
    file_path: str
    ok: bool
    file_size: int
    page_count_header: int
    actual_pages: int
    total_slots: int
    total_live_slots: int
    errors: List[str]
    page_results: List[PageValidation]


def _u32_le(buf: bytes, offset: int) -> int:
    return int.from_bytes(buf[offset : offset + 4], "little", signed=False)


def parse_page_slots(page: bytes) -> Tuple[int, int, List[SlotInfo], List[str]]:
    errors: List[str] = []
    lower = _u32_le(page, 0)
    upper = _u32_le(page, 4)

    if not (PAGE_HEADER_SIZE <= lower <= PAGE_SIZE):
        errors.append(f"invalid lower pointer: {lower}")
    if not (PAGE_HEADER_SIZE <= upper <= PAGE_SIZE):
        errors.append(f"invalid upper pointer: {upper}")
    if lower > upper:
        errors.append(f"lower > upper ({lower} > {upper})")

    if (lower - PAGE_HEADER_SIZE) % ITEM_ID_SIZE != 0:
        errors.append("slot directory size is not multiple of ITEM_ID_SIZE")

    slot_count = max(0, (lower - PAGE_HEADER_SIZE) // ITEM_ID_SIZE)
    slots: List[SlotInfo] = []

    for i in range(slot_count):
        slot_off = PAGE_HEADER_SIZE + i * ITEM_ID_SIZE
        off = _u32_le(page, slot_off)
        length = _u32_le(page, slot_off + 4)
        tombstone = length == 0

        if not tombstone:
            if off < upper:
                errors.append(f"slot {i} points before upper boundary: off={off}, upper={upper}")
            if off >= PAGE_SIZE:
                errors.append(f"slot {i} offset out of bounds: {off}")
            if off + length > PAGE_SIZE:
                errors.append(f"slot {i} tuple overflow: off={off}, len={length}")

        slots.append(SlotInfo(item_id=i, offset=off, length=length, tombstone=tombstone))

    return lower, upper, slots, errors


def validate_dat_file(dat_path: Path) -> DatValidationResult:
    payload = dat_path.read_bytes()
    size = len(payload)
    errors: List[str] = []

    if size < TABLE_HEADER_SIZE:
        errors.append("file smaller than table header")

    if size % PAGE_SIZE != 0:
        errors.append("file size is not page-aligned")

    actual_pages = size // PAGE_SIZE if size >= PAGE_SIZE else 0
    page_count_header = _u32_le(payload, 0) if size >= 4 else 0

    if actual_pages != page_count_header:
        errors.append(
            f"header page_count mismatch: header={page_count_header}, actual_pages={actual_pages}"
        )

    page_results: List[PageValidation] = []
    total_slots = 0
    total_live_slots = 0

    for page_no in range(1, actual_pages):
        base = page_no * PAGE_SIZE
        page = payload[base : base + PAGE_SIZE]
        lower, upper, slots, page_errors = parse_page_slots(page)
        live_slots = sum(1 for s in slots if not s.tombstone)
        total_slots += len(slots)
        total_live_slots += live_slots

        page_results.append(
            PageValidation(
                page_no=page_no,
                ok=not page_errors,
                lower=lower,
                upper=upper,
                slot_count=len(slots),
                live_slot_count=live_slots,
                errors=page_errors,
            )
        )

        errors.extend([f"page {page_no}: {e}" for e in page_errors])

    return DatValidationResult(
        file_path=str(dat_path),
        ok=not errors,
        file_size=size,
        page_count_header=page_count_header,
        actual_pages=actual_pages,
        total_slots=total_slots,
        total_live_slots=total_live_slots,
        errors=errors,
        page_results=page_results,
    )


def validate_all_dat_files(root_dir: Path) -> List[DatValidationResult]:
    files = sorted(root_dir.rglob("*.dat"))
    return [validate_dat_file(path) for path in files]


def result_to_dict(result: DatValidationResult) -> Dict:
    out = asdict(result)
    return out
