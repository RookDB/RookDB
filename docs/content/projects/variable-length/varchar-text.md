---
title: VarChar and Text
sidebar_position: 1
---

# VarChar and Text

RookDB supports variable-length string types **TEXT** and **VARCHAR**.

## Storage format

- **TEXT** and **VARCHAR** are stored with a 2-byte little-endian length prefix (u16) followed by the raw UTF-8 bytes. No padding; row size varies with content.
- **VARCHAR(n)** (e.g. `VARCHAR(255)`) enforces a maximum length at write time: values longer than `n` bytes are truncated.

## Supported column types

| Type        | Description                          |
| ----------- | ------------------------------------ |
| `INT`       | Fixed 4-byte signed integer.         |
| `TEXT`      | Variable-length string (no max).     |
| `VARCHAR`   | Variable-length string (no max).     |
| `VARCHAR(n)`| Variable-length string, max `n` bytes. |

## Example

```
id:INT
name:TEXT
description:VARCHAR(500)
```

When loading CSV data, TEXT and VARCHAR columns accept strings of any length; VARCHAR(n) truncates to `n` bytes if needed.