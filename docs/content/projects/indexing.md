---
title: Indexing
sidebar_position: 1
---

# Indexing

## Overview

RookDB supports secondary indexes to accelerate point lookups and ordered range
queries. Indexes live alongside table files and store `(page_no, item_id)`
record IDs that point to the table heap.

## Supported Algorithms

- Static Hash
- Chained Hash
- Extendible Hash
- Linear Hash
- B-Tree
- B+ Tree
- Radix Tree
- Skip List
- LSM Tree

## Index Files

Index files are stored next to their table file at:

```
database/base/{db}/{table}_{index}.idx
```

Indexes are serialized to JSON using `serde` for easy persistence.

## Operations

### Build From Table

Bulk build scans every tuple in the table and inserts keys into a new index.
This is used after CSV ingestion and during rebuilds.

### Load Index

`AnyIndex::load` deserializes an index file based on the catalog algorithm
metadata and returns a ready-to-use in-memory index.

### Search (Point Lookup)

Point lookup returns a list of `RecordId` values for an exact key match.

### Range Scan (Tree Indexes)

Tree-based indexes expose ordered range scan. Hash-based indexes return an
error for this operation.

### Index Scan (Fetch Tuples)

Index scan uses a point lookup to fetch the matching tuples from disk. Each
`RecordId` is translated into `(page_no, item_id)` access on the table file.

### Clustered Index

Clustered index creation reorders table tuples by the indexed column and then
rebuilds all table indexes so record IDs remain consistent.

Only one clustered index is allowed per table.

### Index Validation

Index validation checks heap/index consistency by verifying every live tuple in
the heap has a corresponding `(key, RecordId)` entry in the chosen index.

## Maintenance

- **Insert**: adds `(key, RecordId)` to every index on the table.
- **Delete**: removes `(key, RecordId)` from every index on the table.
- **Bulk Rebuild**: re-scans the table and rewrites all indexes.
