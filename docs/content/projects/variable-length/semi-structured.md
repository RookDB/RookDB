---
title: Semi-Structured Data Types
sidebar_position: 1
---

# Semi-Structured Data Types

RookDB supports four variable-length data types for semi-structured and composite data: **JSON**, **JSONB**, **XML**, and **User-Defined Types (UDT)**.

## Overview

| Type | Storage Model | Description |
|------|--------------|-------------|
| **JSON** | Variable-length UTF-8 text | Stores JSON as validated text; preserves whitespace and key order |
| **JSONB** | Variable-length binary | Stores JSON in a parsed binary format; normalizes key order and removes whitespace |
| **XML** | Variable-length UTF-8 text | Stores well-formed XML documents as validated text |
| **UDT** | Variable-length composite | Stores a struct-like grouping of primitive types defined by the user |

All four types use **length-prefixed encoding**: each value is stored as a 4-byte (u32) length header followed by the data bytes.

```
┌──────────────────────────────────────┐
│ [4B length prefix][variable data]    │
└──────────────────────────────────────┘
```

---

## JSON

JSON columns store validated JSON text. The original formatting, whitespace, and key ordering are preserved.

**Validation**: At insertion time, each JSON value is validated using `serde_json`. Invalid JSON is rejected with an error.

**Storage**: The raw UTF-8 bytes of the JSON string are stored with a 4-byte length prefix.

**Example CSV value**:
```
"{""name"": ""Alice"", ""age"": 30}"
```

**Source**: `src/backend/executor/json_utils.rs`

---

## JSONB

JSONB columns store JSON in a compact binary format. Unlike JSON, JSONB normalizes the data by sorting object keys lexicographically and stripping whitespace.

### Binary Format

JSONB uses a recursive tagged encoding:

```
┌───────────┬─────────────────────────────────────────┐
│ Tag (1B)  │ Payload (variable, depends on tag)       │
└───────────┴─────────────────────────────────────────┘

Tag values:
  0x00 = null       → no payload
  0x01 = false      → no payload
  0x02 = true       → no payload
  0x03 = number     → 8 bytes (f64 little-endian)
  0x04 = string     → [4B length][UTF-8 bytes]
  0x05 = array      → [4B element count][element₁][element₂]...
  0x06 = object     → [4B pair count][key₁][value₁][key₂][value₂]...
                       (keys sorted lexicographically)
```

**Source**: `src/backend/executor/jsonb.rs`

---

## XML

XML columns store well-formed XML documents as validated UTF-8 text.

### Validation

The `XmlValidator` checks well-formedness at insertion time:

- Matching open/close tags
- Proper nesting
- Valid attributes
- CDATA sections, comments, and XML declarations are allowed

Invalid or malformed XML is rejected with a descriptive error.

**Example CSV value**:
```
"<person><name>Alice</name><age>30</age></person>"
```

**Source**: `src/backend/executor/xml_utils.rs`

---

## User-Defined Types (UDT)

UDTs let users define composite types made up of primitive fields (INT, TEXT, BOOLEAN). A UDT is registered in the catalog at the database level and can then be used as a column type in tables.

### Defining a UDT

UDTs are created via the interactive CLI (menu option 9). Each field has a name and a primitive type:

```
Enter type name: address
Enter fields in the format:- name:type (INT, TEXT, BOOLEAN)
Press Enter on an empty line to finish
Enter field (name:type): street:TEXT
Enter field (name:type): city:TEXT
Enter field (name:type): zip:INT
Enter field (name:type):
```

### Using a UDT in a Table

When creating a table, reference the UDT with the `UDT:` prefix:

```
Enter column (name:type): location:UDT:address
```

The catalog validates that the referenced UDT exists in the current database before allowing table creation.

### Serialization

UDT values are serialized field-by-field according to the type definition:

| Field Type | Serialized Size |
|-----------|----------------|
| INT | 4 bytes (little-endian) |
| TEXT | 10 bytes (padded) |
| BOOLEAN | 1 byte |

The total serialized bytes are then stored with a 4-byte length prefix, like all variable-length types.

### CSV Format for UDT Values

In CSV files, UDT field values are comma-separated within the column:

```csv
id,name,location
1,Alice,"Main St,Springfield,62704"
```

The fields are parsed in order according to the UDT definition.

**Source**: `src/backend/executor/udt.rs`

### Catalog Storage

UDT definitions are stored in the `types` map within each database:

```json
{
  "databases": {
    "mydb": {
      "tables": { ... },
      "types": {
        "address": {
          "fields": [
            { "name": "street", "data_type": "TEXT" },
            { "name": "city", "data_type": "TEXT" },
            { "name": "zip", "data_type": "INT" }
          ]
        }
      }
    }
  }
}
```

**Source**: `src/backend/catalog/types.rs`, `src/backend/catalog/catalog.rs`

---

## Variable-Length Encoding

All four types share the same encoding strategy within tuples:

```
┌──────────┬──────────────────────────┬───────────┐
│ col1     │ col2                     │ col3      │
│ (fixed)  │ [4B len][variable data]  │ (fixed)   │
└──────────┴──────────────────────────┴───────────┘
```

During sequential scan, the deserializer checks each column's type from the catalog:
- **Fixed types** (INT, TEXT, BOOLEAN): advance by the known size
- **Variable types** (JSON, JSONB, XML, UDT): read the 4-byte length prefix, then read exactly that many bytes

### Tuple Size Limit

Without TOAST, a single tuple must fit within one page: `PAGE_SIZE - PAGE_HEADER_SIZE - ITEM_ID_SIZE = 8192 - 8 - 8 = 8176 bytes`. Values that exceed this limit are automatically handled by the TOAST system described below.

---

## TOAST (The Oversized-Attribute Storage Technique)

When a variable-length value exceeds **2048 bytes** (`TOAST_THRESHOLD`), RookDB stores it out-of-line in a separate **toast table** rather than inline in the heap tuple. The original value is replaced by an 18-byte **TOAST pointer** that references the external data.

### TOAST Pointer

The pointer is stored inline in place of the original value and has the following layout:

```
┌───────────┬─────────────┬────────────────┬───────────────┬─────────────┬─────────────┐
│ tag (1B)  │ compress(1B)│ value_id (4B)  │ orig_size(4B) │ stored(4B)  │ chunks (4B) │
└───────────┴─────────────┴────────────────┴───────────────┴─────────────┴─────────────┘
Total: 18 bytes (TOAST_POINTER_SIZE)
```

| Field | Size | Description |
|-------|------|-------------|
| `tag` | 1 byte | `0x01` — identifies this as a TOAST pointer (vs. `0x00` for inline data) |
| `compression` | 1 byte | `0x00` = uncompressed, `0x01` = LZ4 compressed |
| `toast_value_id` | 4 bytes (u32 LE) | Unique identifier for the value in the toast table |
| `original_size` | 4 bytes (u32 LE) | Original uncompressed byte count |
| `stored_size` | 4 bytes (u32 LE) | Byte count as stored (after compression, if applied) |
| `num_chunks` | 4 bytes (u32 LE) | Number of chunks the value was split into |

### Compression

TOAST uses **LZ4 block compression** (via the `lz4_flex` crate). When compression is enabled, the entire value is compressed before chunking. The compressed payload includes a prepended size header so the decompressor knows the original length.

The compression flag is recorded in the TOAST pointer so the reader knows whether to decompress after reassembly.

### Storing a TOASTed Value

1. **Allocate a unique ID** — read the next available `toast_value_id` from the toast table header (offset 4), increment, and write back.
2. **Compress (optional)** — if compression is enabled, the full value is LZ4-compressed.
3. **Chunk** — the (possibly compressed) data is split into chunks of up to **2000 bytes** (`TOAST_CHUNK_SIZE`).
4. **Write chunks** — each chunk is stored as a tuple in the toast table with the following layout:

```
┌──────────────────┬──────────────┬──────────────────┬──────────────────┐
│ value_id (4B LE) │ chunk_seq(4B)│ chunk_len (4B LE)│ chunk_data (var) │
└──────────────────┴──────────────┴──────────────────┴──────────────────┘
```

5. **Replace inline** — the original value in the heap tuple is replaced with the 18-byte TOAST pointer.

The toast table is stored at: `database/base/{database}/{table}_toast.dat`

### Retrieving a TOASTed Value (Detoasting)

1. **Detect pointer** — during a sequential scan, if a variable-length column's inline data starts with tag `0x01`, it is recognized as a TOAST pointer.
2. **Scan the toast table** — all data pages of the toast table are scanned, collecting chunks whose `toast_value_id` matches the pointer's `value_id`.
3. **Sort by sequence** — collected chunks are sorted by `chunk_seq` to restore original ordering.
4. **Reassemble** — chunk data is concatenated in sequence order.
5. **Decompress (if needed)** — if the pointer's compression flag is `0x01`, the reassembled data is decompressed using LZ4.
6. **Return** — the original value bytes are returned to the caller for display or further processing.

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `TOAST_THRESHOLD` | 2048 bytes | Values larger than this are TOASTed |
| `TOAST_CHUNK_SIZE` | 2000 bytes | Maximum data bytes per chunk |
| `TOAST_INLINE_TAG` | `0x00` | Tag for inline (non-TOASTed) data |
| `TOAST_POINTER_TAG` | `0x01` | Tag for a TOAST pointer |
| `TOAST_POINTER_SIZE` | 18 bytes | Total size of a TOAST pointer |

**Source**: `src/backend/toast/mod.rs`, `src/backend/toast/toast_writer.rs`, `src/backend/toast/toast_reader.rs`, `src/backend/toast/compression.rs`

---

## Display Format

When tuples are displayed via `Show Tuples`, each type is formatted as follows:

| Type | Display Format |
|------|---------------|
| JSON | Original JSON text as stored |
| JSONB | Reconstructed JSON with sorted keys, no extra whitespace |
| XML | Original XML text as stored |
| UDT | `(field1=value1, field2=value2, ...)` |
