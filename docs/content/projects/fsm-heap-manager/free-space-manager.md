---
title: Free Space Manager Deep Dive
sidebar_position: 3
---

# Free Space Manager Deep Dive

This page documents FSM tree structure, quantization, search/update logic, and operational guarantees.

## FSM Tree Structure

RookDB uses a PostgreSQL-style binary max-tree in a sidecar file.

Conceptually:

- Leaf level tracks heap page free-space categories.
- Internal levels store max values of descendants.
- Root supports early rejection when no page can satisfy request.

## Quantization Model

FSM stores free space in category range 0 to 255.

General idea:

- Lower category means less free space.
- Higher category means more free space.
- One byte per tracked heap page keeps structure compact.

## Search Behavior

fsm_search_avail:

1. Check root against minimum category.
2. Traverse toward qualifying child nodes.
3. Resolve leaf slot to heap page id.
4. Return candidate page or none.

This avoids linear heap scans for every insert.

## Update Behavior

fsm_set_avail:

1. Compute new category from free bytes.
2. Update leaf entry for target heap page.
3. Bubble max values up to parent nodes.
4. Persist modified pages.

Vacuum or reclaim paths call fsm_vacuum_update, which delegates to the same update mechanism.

## Rebuild and Fault Tolerance

build_from_heap supports sidecar reconstruction:

- Reads heap pages.
- Recomputes categories.
- Rebuilds FSM pages and parent maxima.
- Writes and syncs sidecar.

This allows recovery when the sidecar is missing or stale.

## Insertion Routing Guarantees

The FSM route selection enables:

- Efficient page discovery
- Dense packing behavior
- Predictable fallback to page allocation

Combined with heap retries, this gives robust insertion progress even under fragmentation.
