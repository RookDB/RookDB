//! `KeyOrder` — key comparison and in-node search for one tree.
//!
//! It borrows only the tree's key types, so a caller can keep a page borrowed
//! from the buffer pool while it searches the page.

use crate::backend::error::{RookError, RookResult};
use std::cmp::Ordering;
use std::io;

use super::codec::{KEY_LEN_SIZE, decode_key};
use super::node::{NodeView, int_from_le};
use crate::types::Comparable;
use crate::types::DataType;
use crate::types::value::DataValue;

/// Where one leaf entry sits: `(index, key offset, encoded key length)`.
pub(crate) type LeafSlot = (usize, usize, usize);

/// What a leaf holds for one `(key, heap location)` pair.
pub(crate) struct LeafMatch {
    pub(crate) num_keys: usize,
    /// The matching entry, if this leaf has it.
    pub(crate) found: Option<LeafSlot>,
    /// Offset one past the last key.
    pub(crate) used: usize,
    /// A larger key was seen, so later leaves cannot hold the entry.
    pub(crate) past_key: bool,
    pub(crate) next_leaf: u32,
}

/// Where a new entry goes in a leaf.
pub(crate) struct LeafInsertSlot {
    pub(crate) num_keys: usize,
    pub(crate) index: usize,
    /// Offset the new key is written at.
    pub(crate) key_off: usize,
    /// Offset one past the last key.
    pub(crate) used: usize,
}

/// The key ordering of a tree, defined by its key segment types.
#[derive(Clone, Copy)]
pub(crate) struct KeyOrder<'a> {
    key_types: &'a [DataType],
}

fn invalid_data(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Split one `[len][bytes]` segment off the front of `data`. Returns the
/// payload and the bytes consumed; same errors as [`decode_key`].
#[inline]
fn split_segment(data: &[u8]) -> RookResult<(&[u8], usize)> {
    if data.len() < KEY_LEN_SIZE {
        return Err(RookError::io(
            io::ErrorKind::UnexpectedEof,
            "Truncated key length",
        ));
    }
    let len = u16::from_le_bytes([data[0], data[1]]) as usize;
    if data.len() < KEY_LEN_SIZE + len {
        return Err(RookError::io(
            io::ErrorKind::UnexpectedEof,
            "Truncated key data",
        ));
    }
    Ok((&data[KEY_LEN_SIZE..KEY_LEN_SIZE + len], KEY_LEN_SIZE + len))
}

/// Reject a VARCHAR payload longer than the declared limit, as
/// `DataValue::from_bytes` does.
#[inline]
fn check_varchar_len(payload: &[u8], max_len: u16) -> RookResult<()> {
    if payload.len() > max_len as usize {
        return Err(RookError::from(invalid_data(format!(
            "VARCHAR payload length {} exceeds declared limit {}",
            payload.len(),
            max_len
        ))));
    }
    Ok(())
}

/// Compare the leading segments of `a` and `b`, both of type `ty`. Returns
/// the ordering and the bytes each segment occupies.
///
/// VARCHAR segments are compared as the bytes they are stored as: Rust orders
/// strings by their UTF-8 bytes, so this is the order `DataValue::compare`
/// gives the decoded strings, without building them. Keys are only ever
/// encoded from a `String`, so the payloads are not re-validated as UTF-8.
/// Every other type is decoded first.
#[inline]
fn cmp_segments(a: &[u8], b: &[u8], ty: &DataType) -> RookResult<(Ordering, usize, usize)> {
    if let DataType::Varchar(max_len) = ty {
        let (pa, ca) = split_segment(a)?;
        let (pb, cb) = split_segment(b)?;
        check_varchar_len(pa, *max_len)?;
        check_varchar_len(pb, *max_len)?;
        return Ok((pa.cmp(pb), ca, cb));
    }
    let (va, ca) = decode_key(a, ty)?;
    let (vb, cb) = decode_key(b, ty)?;
    let ordering = va.compare(&vb).map_err(invalid_data)?;
    Ok((ordering, ca, cb))
}

/// Compare the leading segment of `encoded`, of type `ty`, against `value`.
/// Returns the ordering and the bytes the segment occupies.
#[inline]
fn cmp_segment_vs_value(
    encoded: &[u8],
    ty: &DataType,
    value: &DataValue,
) -> RookResult<(Ordering, usize)> {
    if let (DataType::Varchar(max_len), DataValue::Varchar(text)) = (ty, value) {
        let (payload, consumed) = split_segment(encoded)?;
        check_varchar_len(payload, *max_len)?;
        return Ok((payload.cmp(text.as_bytes()), consumed));
    }
    let (decoded, consumed) = decode_key(encoded, ty)?;
    let ordering = decoded.compare(value).map_err(invalid_data)?;
    Ok((ordering, consumed))
}

impl<'a> KeyOrder<'a> {
    pub(crate) fn new(key_types: &'a [DataType]) -> Self {
        Self { key_types }
    }

    /// Compare two encoded keys segment-by-segment using the key types.
    ///
    /// Encoded keys are opaque `[len][bytes]` concatenations; comparing each
    /// segment by its declared type preserves value ordering (plain byte
    /// comparison would not — little-endian integers sort wrong as bytes).
    ///
    /// Single-column trees store bare segments (legacy format); composite
    /// trees wrap the whole key in an envelope (see `encode_key_multi`).
    pub(crate) fn cmp_encoded_keys(&self, a: &[u8], b: &[u8]) -> RookResult<Ordering> {
        use std::cmp::Ordering as O;
        if self.key_types.len() == 1 {
            return cmp_segments(a, b, &self.key_types[0]).map(|(ordering, _, _)| ordering);
        }
        // Composite: strip the outer envelope from both sides, then walk
        // inner segments in lockstep.
        if a.len() < KEY_LEN_SIZE || b.len() < KEY_LEN_SIZE {
            return Err(RookError::io(
                io::ErrorKind::InvalidData,
                "Truncated composite key envelope",
            ));
        }
        let mut ia = KEY_LEN_SIZE;
        let mut ib = KEY_LEN_SIZE;
        let mut seg = 0usize;
        loop {
            match (ia >= a.len(), ib >= b.len()) {
                (true, true) => return Ok(O::Equal),
                (true, false) => return Ok(O::Less),
                (false, true) => return Ok(O::Greater),
                (false, false) => {}
            }
            // Segments align positionally: type for the current segment index.
            let ty = &self.key_types[seg % self.key_types.len()];
            let (c, ca, cb) = cmp_segments(&a[ia..], &b[ib..], ty)?;
            if c != O::Equal {
                return Ok(c);
            }
            ia += ca;
            ib += cb;
            seg += 1;
        }
    }

    /// Compare an encoded key against a slice of typed values (one per key
    /// segment). Used by range bounds and lookups supplied as values.
    pub(crate) fn cmp_encoded_vs_values(
        &self,
        encoded: &[u8],
        values: &[DataValue],
    ) -> RookResult<Ordering> {
        use std::cmp::Ordering as O;
        debug_assert_eq!(
            values.len(),
            self.key_types.len(),
            "value/key arity mismatch"
        );
        if self.key_types.len() == 1 {
            return cmp_segment_vs_value(encoded, &self.key_types[0], &values[0])
                .map(|(ordering, _)| ordering);
        }
        if encoded.len() < KEY_LEN_SIZE {
            return Err(RookError::io(
                io::ErrorKind::InvalidData,
                "Truncated composite key envelope",
            ));
        }
        let mut off = KEY_LEN_SIZE;
        for (i, v) in values.iter().enumerate() {
            if off >= encoded.len() {
                return Ok(O::Less);
            }
            let (c, consumed) = cmp_segment_vs_value(&encoded[off..], &self.key_types[i], v)?;
            if c != O::Equal {
                return Ok(c);
            }
            off += consumed;
        }
        Ok(O::Equal)
    }

    /// Key width in bytes when this is a single-column integer index.
    fn int_key_width(&self) -> Option<usize> {
        match self.key_types {
            [DataType::SmallInt] => Some(2),
            [DataType::Int] => Some(4),
            [DataType::BigInt] => Some(8),
            _ => None,
        }
    }

    /// Key width when `view` can be binary-searched as plain integers: a
    /// single-column integer index whose keys on this page all have the
    /// declared width. Comparing them as `i64` matches `DataValue::compare`,
    /// which widens SMALLINT/INT/BIGINT before comparing.
    pub(crate) fn int_layout(&self, view: &NodeView<'_>) -> Option<usize> {
        let width = self.int_key_width()?;
        view.has_uniform_keys(width).then_some(width)
    }

    /// [`KeyOrder::int_layout`] plus the integer held by `encoded_key`, when
    /// the encoded key has exactly the declared width.
    pub(crate) fn int_fast_path(
        &self,
        view: &NodeView<'_>,
        encoded_key: &[u8],
    ) -> Option<(usize, i64)> {
        let width = self.int_layout(view)?;
        if encoded_key.len() != KEY_LEN_SIZE + width
            || u16::from_le_bytes([encoded_key[0], encoded_key[1]]) as usize != width
        {
            return None;
        }
        Some((width, int_from_le(&encoded_key[KEY_LEN_SIZE..])))
    }

    /// Pick the child of an internal node to descend into for `encoded_key`:
    /// the child left of the first separator greater than the key, or the
    /// last child when no separator is greater. Returns `(index, page_id)`.
    pub(crate) fn pick_child(
        &self,
        view: &NodeView<'_>,
        encoded_key: &[u8],
    ) -> RookResult<(usize, u32)> {
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let child_idx = view.int_partition_point(width, |k| k <= target);
            return Ok((child_idx, view.child(child_idx)));
        }

        let mut child_idx = view.num_keys();
        for (i, entry) in view.keys().enumerate() {
            let (_, key) = entry?;
            if self.cmp_encoded_keys(key, encoded_key)? == Ordering::Greater {
                child_idx = i;
                break;
            }
        }
        Ok((child_idx, view.child(child_idx)))
    }

    /// Pick the leftmost child that can hold `encoded_key`: the child left of
    /// the first separator that is not less than the key. Duplicates of a key
    /// can sit on both sides of an equal separator, so readers start here and
    /// walk right along the leaf chain.
    pub(crate) fn pick_child_lower(
        &self,
        view: &NodeView<'_>,
        encoded_key: &[u8],
    ) -> RookResult<u32> {
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let child_idx = view.int_partition_point(width, |k| k < target);
            return Ok(view.child(child_idx));
        }

        let mut child_idx = view.num_keys();
        for (i, entry) in view.keys().enumerate() {
            let (_, key) = entry?;
            if self.cmp_encoded_keys(key, encoded_key)? != Ordering::Less {
                child_idx = i;
                break;
            }
        }
        Ok(view.child(child_idx))
    }

    /// Locate the entry matching both the key and the heap location in one
    /// leaf page.
    pub(crate) fn locate_leaf_entry(
        &self,
        view: &NodeView<'_>,
        encoded_key: &[u8],
        value: (u32, u32),
    ) -> RookResult<LeafMatch> {
        let mut found: Option<LeafSlot> = None;
        let mut past_key = false;
        let mut used = view.keys_start();
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let entry = KEY_LEN_SIZE + width;
            used = view.keys_start() + view.num_keys() * entry;
            let first = view.int_partition_point(width, |k| k < target);
            for i in first..view.num_keys() {
                if view.int_key(i, width) != target {
                    past_key = true;
                    break;
                }
                if view.value(i) == value {
                    found = Some((i, view.keys_start() + i * entry, entry));
                    break;
                }
            }
        } else {
            for (i, entry) in view.keys().enumerate() {
                let (off, key) = entry?;
                used = off + key.len();
                if found.is_some() || past_key {
                    continue;
                }
                let cmp = self.cmp_encoded_keys(key, encoded_key)?;
                if cmp == Ordering::Equal && view.value(i) == value {
                    // Exact match — remove this specific entry
                    found = Some((i, off, key.len()));
                } else if cmp == Ordering::Greater {
                    // Key not in this leaf (past sorted position)
                    past_key = true;
                }
            }
        }
        Ok(LeafMatch {
            num_keys: view.num_keys(),
            found,
            used,
            past_key,
            next_leaf: view.next_leaf(),
        })
    }

    /// Find where `encoded_key` goes in a leaf and where the key area ends.
    ///
    /// For duplicate keys the position is AFTER all entries with the same
    /// key, so that the sort order is: key < == == < <
    pub(crate) fn leaf_insert_slot(
        &self,
        view: &NodeView<'_>,
        encoded_key: &[u8],
    ) -> RookResult<LeafInsertSlot> {
        let mut insert_at: Option<(usize, usize)> = None; // (index, offset)
        let mut used = view.keys_start();
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let entry = KEY_LEN_SIZE + width;
            used = view.keys_start() + view.num_keys() * entry;
            let pos = view.int_partition_point(width, |k| k <= target);
            if pos < view.num_keys() {
                insert_at = Some((pos, view.keys_start() + pos * entry));
            }
        } else {
            for (i, entry) in view.keys().enumerate() {
                let (off, key) = entry?;
                used = off + key.len();
                if insert_at.is_none()
                    && self.cmp_encoded_keys(key, encoded_key)? == Ordering::Greater
                {
                    insert_at = Some((i, off));
                }
            }
        }
        let (index, key_off) = insert_at.unwrap_or((view.num_keys(), used));
        Ok(LeafInsertSlot {
            num_keys: view.num_keys(),
            index,
            key_off,
            used,
        })
    }
}
