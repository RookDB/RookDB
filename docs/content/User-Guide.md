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
- INT
- TEXT

Example:
```
id:INT
name:TEXT
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

## Create Index

Creates a secondary index on a table column.

Steps:
1. Enter table name
2. Enter column name
3. Enter index name
4. Choose an algorithm

Example algorithms:
```
static_hash
extendible_hash
linear_hash
btree
bplus_tree
radix_tree
```

---

## Drop Index

Removes a secondary index from a table.

Steps:
1. Enter table name
2. Enter index name

---

## List Indexes

Lists all secondary indexes defined on a table.

Steps:
1. Enter table name

---

## Search by Index

Finds records by searching an index with a single key.

Steps:
1. Enter table name
2. Enter index name
3. Enter search value

---

## Range Scan (tree indexes only)

Runs an ordered range scan using a tree-based index.

Steps:
1. Enter table name
2. Enter index name
3. Enter start value (inclusive)
4. Enter end value (inclusive)

---

## Exit

Exit from RookDB.