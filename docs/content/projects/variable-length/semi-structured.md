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

A single tuple must fit within one page: `PAGE_SIZE - PAGE_HEADER_SIZE - ITEM_ID_SIZE = 8192 - 8 - 8 = 8176 bytes`. Values that cause the tuple to exceed this limit are rejected at insertion time.

---

## Display Format

When tuples are displayed via `Show Tuples`, each type is formatted as follows:

| Type | Display Format |
|------|---------------|
| JSON | Original JSON text as stored |
| JSONB | Reconstructed JSON with sorted keys, no extra whitespace |
| XML | Original XML text as stored |
| UDT | `(field1=value1, field2=value2, ...)` |
