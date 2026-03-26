---
title: Update and Delete
sidebar_position: 2
---

# Update and Delete

## Delete

Tuple deletion in the heap marks the slot length as zero. The tuple payload is
left in place, which keeps page layout stable and makes deletes cheap.

### Index Maintenance

When a tuple is deleted through the index-aware path, the system:

1. Reads the tuple bytes from the page.
2. Removes `(key, RecordId)` from every index on the table.

This keeps secondary indexes synchronized with the heap without requiring a
full rebuild after every delete.

## Update

RookDB treats updates as delete + insert:

1. Delete the old tuple and remove its index entries.
2. Insert the new tuple and add its index entries.

This keeps index maintenance logic centralized and consistent.
