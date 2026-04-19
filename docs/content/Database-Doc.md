---
id: database-doc
title: "Database Doc"
---

# Codebase Structure
- The codebase is organized into two main directories: **frontend** and **backend**.
- All data generated for databases, tables, and tuples is stored in the **database** directory, which is automatically created when the program is executed.
- The **frontend** directory contains all code related to command-line interface (CLI) inputs and outputs.
- The **backend** directory contains the core implementation of the storage manager.

## Overall Layout of the data
All persistent data used and created by the system is stored inside the `code/database/` directory. This directory serves as the root location for both metadata and table data. The folder structure and path constants defined in `code/src/backend/layout` specify how databases and tables are organized as directories and files within this location.

### Database Directory Layout
```bash
database/
  ├── global/
  │   └── catalog.json
  └── base/
      ├── db1/
      │   └── {table}.dat
      ├── db2/
      │   └── {table}.dat
```

### Directory Descriptions

- **database/**  
  Root directory for all persistent data used and created by the system.

- **global/**  
  Contains system-wide metadata required to interpret database structure.

- **catalog.json**  
  Stores metadata for all databases, tables, and their column definitions.

- **base/**  
  Contains one subdirectory per database.

- **`{database}/`**  
  Represents a single database and holds all table files belonging to it.

- **`{table}.dat`**  
  Physical file corresponding to a table, containing both table metadata and tuple data.


## Catalog File Structure

The catalog is stored as a JSON file at `database/global/catalog.json`.

### Catalog JSON Structure

```json
{
  "databases": {
    "<database_name>": {
      "tables": {
        "<table_name>": {
          "columns": [
            {
              "name": "<column_name>",
              "data_type": "<data_type>"
            }
          ]
        }
      }
    }
  }
}
```

#### Example Catalog.json file
```json
{
  "databases": {
    "users": {
      "tables": {
        "students": {
          "columns": [
            {
              "name": "id",
              "data_type": "INT"
            },
            {
              "name": "name",
              "data_type": "TEXT"
            }
          ]
        },
        "teachers": {
          "columns": [
            {
              "name": "id",
              "data_type": "INT"
            },
            {
              "name": "name",
              "data_type": "TEXT"
            }
          ]
        }
      }
    }
  }
}
```



## Table File Structure

Each `{table}.dat` file stores table data as a contiguous sequence of bytes and is divided into fixed-size pages. Each page is 8 KB in size.

The first page of the file is reserved as the **Table Header**. Within this page, only the first 4 bytes are used to store the total number of pages that contain tuple data. The remaining bytes in the header page are currently unused.

All subsequent pages are data pages used to store tuples.

### Page Structure

The page structure is based on the PostgreSQL slotted-page layout, with only the minimum required metadata implemented.

For reference, the PostgreSQL page layout is described at:  
https://www.postgresql.org/docs/current/storage-page-layout.html#STORAGE-PAGE-LAYOUT-FIGURE

Each data page is divided into:
- A **page header**, which stores the `lower` and `upper` offsets
- An **Item ID array**, growing forward from the page header
- **Tuple data**, appended from the end of the page backward

The page-related implementation is located in `src/backend/page/mod.rs`.

## Newly Introduced Database Files

The FSM + Heap integration adds a sidecar file per table and benchmark outputs for evaluation.

### Runtime Files

- `{table}.dat`: Main heap file containing table header + slotted data pages.
- `{table}.dat.fsm`: Free Space Map sidecar file storing tree pages for free-space search.
- `database/global/catalog.json`: Logical metadata for databases, tables, and schema.

### Intermediate / Benchmark-Generated Files

These are generated while running benchmark scripts and are not part of the core catalog/heap storage:

- `benchmark_runs/benchmark_comparison.csv`: Unified cross-engine summary.
- `benchmark_runs/benchmark_history.csv`: RookDB benchmark run history.
- `benchmark_runs/benchmark_history.jsonl`: Per-run JSONL history for scripting.
- `benchmark_runs/latest_fsm_heap_benchmark.json`: Latest internal benchmark snapshot.
- `benchmark_runs/postgres_fsm_summary.txt`: PostgreSQL FSM comparison summary.
- `benchmark_runs/postgres_fsm_metrics.csv`: PostgreSQL free-space metrics export.
- `benchmark_runs/sqlite_benchmark.txt`, `benchmark_runs/mysql_benchmark.txt`, `benchmark_runs/pgbench_results.txt`: Engine-specific benchmark outputs.

## Database Structure Modifications

Compared with the earlier single-file table layout, the storage model now uses:

1. Main table heap file (`.dat`) for tuples.
2. FSM sidecar fork (`.dat.fsm`) for free-space indexing.

This keeps heap tuple storage and free-space metadata decoupled, and allows `.fsm` to be rebuilt from heap state when needed.

## Page and Tuple Layout Updates

- Page size remains 8 KB.
- Data pages use slotted-page organization with:
  - `lower` pointer (item-id growth direction)
  - `upper` pointer (tuple payload growth direction)
- Tuple payload remains variable-length bytes inserted into the region bounded by `upper` and `lower`.
- FSM stores free-space summaries externally and does not alter tuple payload encoding in the heap page.

## Algorithms Used

- **FSM max-tree search:** top-down free-space candidate selection from root to leaf.
- **Bubble-up update:** leaf free-space updates propagate via `max(left, right)` to parents.
- **Heap insert with fallback:** attempt page from FSM; if no fit, allocate new page and update FSM.
- **FSM rebuild from heap:** full scan of heap pages and reconstruction of leaf/internal FSM pages.

## Data Structures Added / Used

- `FSMPage`: fixed-size page representation for FSM nodes and metadata.
- `HeaderMetadata`: persistent heap metadata including page counters/statistics.
- `HeapScanIterator`: streaming sequential iterator for tuple scanning.
- Slot entries (`offset`, `length`) in page item-id region for tuple addressing.