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
- BOOLEAN
- JSON
- JSONB
- XML
- UDT:{type_name} (e.g., `UDT:address`)

Example:
```
id:INT
name:TEXT
payload:JSON
metadata:JSONB
config:XML
location:UDT:address
```

> **Note**: When using a UDT column type, the referenced type must already exist in the current database. Use **Create Type** first.

---

## Load CSV

Loads CSV data into an existing table.

Steps:
1. Enter table name
2. Enter CSV file path

Each value is validated at load time based on its column type:
- **JSON**: Must be valid JSON
- **JSONB**: Must be valid JSON (converted to binary format on storage)
- **XML**: Must be well-formed XML
- **UDT**: Field values are comma-separated and must match the type definition

Example CSV with variable-length types:
```csv
id,name,payload,metadata,config,location
1,Alice,"{""key"": ""value""}","{""score"": 42}","<settings><mode>dark</mode></settings>","Main St,Springfield,62704"
```

> **Note**: JSON values in CSV must have internal double quotes escaped by doubling them (`""`). The maximum tuple size is 8176 bytes.

---

## Show Tuples

Displays tuples stored in table pages along with page metadata such as pointers and tuple count.

---

## Show Table Statistics

Displays storage statistics like total number of pages.

---

## Create Type

Creates a new User-Defined Type (UDT) in the selected database.

Steps:
1. Enter a type name
2. Enter fields using the format `name:type`
3. Press Enter on an empty line to finish

Supported field types: INT, TEXT, BOOLEAN

Example:
```
Enter type name: address
Enter field (name:type): street:TEXT
Enter field (name:type): city:TEXT
Enter field (name:type): zip:INT
Enter field (name:type):
```

Once created, the type can be used as a column type in tables with the `UDT:` prefix (e.g., `UDT:address`).

---

## Show Types

Displays all User-Defined Types registered in the selected database, including their field names and types.

---

## Exit

Exit from RookDB.