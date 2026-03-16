# User Guide

## Show Databases

Displays all databases available in the catalog.

---

## Create Database

Creates a new database and updates the catalog.

Steps:
1. Enter a database name when prompted

Example:
```
users
```

---

## Select Database

Sets the active database for performing operations.

Steps:
1. Enter a database name from the displayed list

---

## Show Tables

Displays all tables in the selected database.

---

## Create Table

Creates a new table with a schema.

Steps:
1. Enter table name
2. Enter columns using format:

```
column_name:data_type
```

3. Press Enter on an empty line to finish

Supported Types:
- **INT** – fixed-length 4-byte integer
- **TEXT** – variable-length string (stored as length + bytes)
- **VARCHAR** – variable-length string, no max
- **VARCHAR(n)** – variable-length string with max length (e.g. `VARCHAR(255)`)

Example:
```
id:INT
name:TEXT
title:VARCHAR(100)
```

---

## Load CSV

Loads CSV data into an existing table.

Steps:
1. Enter table name
2. Enter CSV file path

Example:
```
examples/example.csv
```

---

## Show Tuples

Displays tuples stored in table pages along with page metadata such as pointers and tuple count.

---

## Show Table Statistics

Displays storage statistics like total number of pages.

---

## Exit

Exit from RookDB.