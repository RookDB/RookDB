#!/bin/bash

# Test script to verify TOAST persistence with large blobs

cd "$(dirname "$0")" || exit 1

DATABASE="test_db"
TABLE="big_blob_table"
CSV_PATH="examples/big_blob.csv"

# Clean up old test database
echo "=== Cleaning up old test database ==="
rm -rf "database/base/${DATABASE}"
mkdir -p "database/base/${DATABASE}"

# Build the project
echo ""
echo "=== Building project ==="
cargo build 2>&1 | grep -E "(Compiling|Finished|error|warning)" || cargo build

# Create test database and table
echo ""
echo "=== Creating test database and table ==="
./target/debug/storage_manager <<EOF
create_db $DATABASE
create_table $DATABASE $TABLE id,INT data,BLOB
EOF

# Load the big blob CSV
echo ""
echo "=== Loading big_blob.csv (68KB) ==="
echo "Expected: Large BLOB should be chunked via TOAST and persisted to disk"
./target/debug/storage_manager <<EOF
use_db $DATABASE
load_csv $TABLE $CSV_PATH
show_tuples $TABLE
EOF

# Check if TOAST file was created
echo ""
echo "=== Checking for persisted TOAST file ==="
TOAST_FILE="database/base/${DATABASE}/${TABLE}.toast"
if [ -f "$TOAST_FILE" ]; then
    ls -lh "$TOAST_FILE"
    echo "✓ TOAST file successfully created"
else
    echo "✗ TOAST file NOT found (expected at $TOAST_FILE)"
fi

# Check the data file
echo ""
echo "=== Checking table data file ==="
DATA_FILE="database/base/${DATABASE}/${TABLE}.dat"
if [ -f "$DATA_FILE" ]; then
    ls -lh "$DATA_FILE"
    echo "✓ Table data file exists"
else
    echo "✗ Table data file NOT found"
fi

# Try loading again to verify TOAST is correctly loaded from disk
echo ""
echo "=== Verifying TOAST persistence (reload and display) ==="
./target/debug/storage_manager <<EOF
use_db $DATABASE
show_tuples $TABLE
EOF

echo ""
echo "=== Test Complete ==="
