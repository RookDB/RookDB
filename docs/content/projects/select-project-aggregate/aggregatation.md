---
title: Aggregation
sidebar_position: 3
---

# Aggregation

## Overview

This page documents the current aggregation work completed so far in RookDB. The work follows the Volcano iterator model and is backend-only.

## Supported Aggregates (Phase 1)

- `COUNT(*)`
- `COUNT(col)`
- `MIN(col)`
- `MAX(col)`

## Null Handling

- `COUNT(*)` counts all rows.
- `COUNT(col)` skips NULL values.
- `MIN` and `MAX` ignore NULL values.
- If all values are NULL, the result is `NULL` for `MIN`/`MAX`.

## Implementation Summary (Current)

- Core executor types are defined: `Value`, `AggFunc`, `AggReq`, `Executor`, and `Tuple`.
- `SeqScan` iterator decodes tuples using a NULL bitmap.
- `AggregationState` maintains one state slot per requested aggregate and updates per tuple.