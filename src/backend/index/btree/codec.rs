//! B+Tree on-disk codec — split from the original single-file `btree.rs`.
//!
//! Page constants, key framing (`[u16 len][bytes]` + composite envelopes),
//! the root-pointer sidecar, and key size helpers.

use crate::backend::error::{RookError, RookResult};
use std::io;
use std::path::{Path, PathBuf};

use crate::types::DataType;
use crate::types::value::DataValue;

pub const BTREE_PAGE_SIZE: usize = 8192;

/// Page type markers.
pub(crate) const PAGE_TYPE_INTERNAL: u32 = 0;
pub(crate) const PAGE_TYPE_LEAF: u32 = 1;

/// Size of the shared page header.
pub(crate) const PAGE_HEADER: usize = 8; // page_type (4) + num_keys (4)
/// Leaf-specific header beyond the shared header.
pub(crate) const LEAF_HEADER: usize = 16; // shared(8) + next_leaf(4) + prev_leaf(4)
/// Size of a value tuple (page_id + slot_id).
pub(crate) const VALUE_SIZE: usize = 8; // page_id(4) + slot_id(4)
/// Size of a child pointer.
pub(crate) const CHILD_SIZE: usize = 4; // u32
/// Size of the key length prefix.
pub(crate) const KEY_LEN_SIZE: usize = 2; // u16

// ─── Encoded Key Helpers ────────────────────────────────────────────────────

/// Sidecar file storing the current root page id (`<idx>.root`, u32 LE).
///
/// The index file itself has no header page, so without this every
/// `BTree::open` assumed page 0 — the ORIGINAL leftmost leaf — and all
/// searches degenerated to next_leaf chain walks.
pub(crate) fn root_sidecar_path(idx_path: &Path) -> PathBuf {
    crate::backend::paths::with_suffix(idx_path, ".root")
}

/// Replace the sidecar atomically: a crash leaves the old pointer or the
/// new one, never a short file.
pub(crate) fn write_root_sidecar(idx_path: &Path, root_page_id: u32) -> RookResult<()> {
    let path = root_sidecar_path(idx_path);
    let tmp = crate::backend::paths::with_suffix(&path, ".tmp");
    let mut file = std::fs::File::create(&tmp)?;
    std::io::Write::write_all(&mut file, &root_page_id.to_le_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path).map_err(RookError::from)
}

pub(crate) fn read_root_sidecar(idx_path: &Path) -> RookResult<u32> {
    let bytes = std::fs::read(root_sidecar_path(idx_path))?;
    if bytes.len() < 4 {
        return Err(RookError::io(
            io::ErrorKind::InvalidData,
            "short root sidecar",
        ));
    }
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Encode a DataValue to the on-disk key format: `[u16 length][bytes]`.
pub(crate) fn encode_key(key: &DataValue) -> Vec<u8> {
    let bytes = key.to_bytes();
    let mut out = Vec::with_capacity(KEY_LEN_SIZE + bytes.len());
    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(&bytes);
    out
}

/// Decode a key from on-disk format. Returns `(key, bytes_consumed)`.
pub(crate) fn decode_key(data: &[u8], ty: &DataType) -> RookResult<(DataValue, usize)> {
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
    let key_bytes = &data[KEY_LEN_SIZE..KEY_LEN_SIZE + len];
    let value = DataValue::from_bytes(ty, key_bytes)?;
    Ok((value, KEY_LEN_SIZE + len))
}

/// Encode a composite key as one framed entry:
/// `[u16 total_len][seg1_len][seg1][seg2_len][seg2]…`
///
/// The outer envelope lets leaf serialisation/deserialisation treat the
/// whole multi-segment key as a single opaque `[len][bytes]` entry, exactly
/// like single-column keys.
pub(crate) fn encode_key_multi(values: &[DataValue]) -> RookResult<Vec<u8>> {
    let mut body = Vec::new();
    for v in values {
        body.extend_from_slice(&encode_key(v));
    }
    let total = body.len();
    if total > u16::MAX as usize {
        return Err(RookError::io(
            io::ErrorKind::InvalidInput,
            format!("Composite key too large: {} bytes", total),
        ));
    }
    let mut out = Vec::with_capacity(KEY_LEN_SIZE + total);
    out.extend_from_slice(&(total as u16).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}
