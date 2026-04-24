//! Visibility Map (VM) – tracks per-page "all-visible" status.
//!
//! # PostgreSQL analogy
//! PostgreSQL stores a VM fork alongside every heap file (`<rel>_vm`).
//! Each heap page has two VM bits:
//!   • **all-visible** – every tuple on the page is visible to all current and
//!     future transactions. Set by VACUUM after sweeping the page; cleared by
//!     any INSERT / UPDATE / DELETE that touches the page.
//!   • **all-frozen** – every tuple is frozen (XID wraparound safe). Not needed
//!     in RookDB (no MVCC XIDs), so we track only the all-visible bit here.
//!
//! # RookDB implementation
//! * The VM lives in `database/base/<db>/<table>_vm` (one file per table).
//! * Each byte covers 8 heap pages; bit `i % 8` of byte `i / 8` is page `i`.
//!   Bit value:
//!     1 → all-visible (page has zero dead tuples; vacuum can skip it)
//!     0 → dirty       (page may have dead tuples; vacuum must visit it)
//! * Page 0 (header page) is always treated as non-visible by convention.
//!
//! # Integration points
//! | Event            | Action                              |
//! |------------------|-------------------------------------|
//! | DELETE / UPDATE  | `vm_clear_page(db, tbl, page_id)`   |
//! | Compaction       | `vm_set_page(db, tbl, page_id)` for pages with no dead tuples |
//! | Vacuum scan      | `vm_is_visible(db, tbl, page_id)` → skip if true |
//! | Table drop/trunc | delete the `_vm` file               |

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

fn vm_path(db_name: &str, table_name: &str) -> PathBuf {
    PathBuf::from(format!("database/base/{}/{}_vm", db_name, table_name))
}

/// Byte index and bit mask within that byte for `page_id`.
#[inline]
fn byte_and_mask(page_id: u32) -> (u64, u8) {
    let byte_idx = page_id as u64 / 8;
    let bit_pos  = (page_id % 8) as u8;
    (byte_idx, 1u8 << bit_pos)
}

// ─────────────────────────────────────────────────────────────────────────
// In-memory cache (avoids a read-modify-write for every DELETE)
// ─────────────────────────────────────────────────────────────────────────

/// Per-table in-memory copy of the VM byte array.
#[derive(Default, Clone)]
struct VmCache {
    bytes: Vec<u8>,
    dirty: bool,
}

struct VmRegistry {
    tables: HashMap<String, VmCache>,
}

impl VmRegistry {
    fn new() -> Self { Self { tables: HashMap::new() } }

    fn key(db: &str, table: &str) -> String { format!("{}::{}", db, table) }

    /// Ensure the cache holds at least `needed_bytes` bytes.
    fn ensure_capacity(cache: &mut VmCache, needed_bytes: usize) {
        if cache.bytes.len() < needed_bytes {
            cache.bytes.resize(needed_bytes, 0u8);
        }
    }

    fn get_mut(&mut self, db: &str, table: &str) -> &mut VmCache {
        let key = Self::key(db, table);
        self.tables.entry(key).or_default()
    }
}

static VM_REGISTRY: OnceLock<Mutex<VmRegistry>> = OnceLock::new();

fn registry() -> &'static Mutex<VmRegistry> {
    VM_REGISTRY.get_or_init(|| Mutex::new(VmRegistry::new()))
}

// ─────────────────────────────────────────────────────────────────────────
// Disk I/O helpers
// ─────────────────────────────────────────────────────────────────────────

/// Read the current on-disk VM byte at `byte_idx`.
/// Returns 0 if the file is shorter than `byte_idx + 1`.
fn read_vm_byte(path: &PathBuf, byte_idx: u64) -> io::Result<u8> {
    if !path.exists() {
        return Ok(0);
    }
    let mut file = OpenOptions::new().read(true).open(path)?;
    let len = file.seek(SeekFrom::End(0))?;
    if byte_idx >= len {
        return Ok(0);
    }
    file.seek(SeekFrom::Start(byte_idx))?;
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;
    Ok(buf[0])
}

/// Write a single byte at `byte_idx`, extending the file if necessary.
fn write_vm_byte(path: &PathBuf, byte_idx: u64, value: u8) -> io::Result<()> {
    // Ensure parent directory exists.
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).open(path)?;

    let len = file.seek(SeekFrom::End(0))?;
    if byte_idx >= len {
        // Extend with zeros up to the target byte.
        let padding = byte_idx - len;
        file.write_all(&vec![0u8; padding as usize])?;
        file.write_all(&[value])?;
    } else {
        file.seek(SeekFrom::Start(byte_idx))?;
        file.write_all(&[value])?;
    }
    file.flush()?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────

/// Mark `page_id` as **all-visible** (compaction confirmed no dead tuples).
///
/// Called by `compaction_table` for each page that contained no dead slots
/// (or after successfully compacting a page and leaving it clean).
pub fn vm_set_page(db_name: &str, table_name: &str, page_id: u32) -> io::Result<()> {
    if page_id == 0 {
        return Ok(()); // header page – never mark visible
    }
    let (byte_idx, mask) = byte_and_mask(page_id);
    let path = vm_path(db_name, table_name);

    {
        let mut reg = registry().lock().unwrap();
        let cache = reg.get_mut(db_name, table_name);
        VmRegistry::ensure_capacity(cache, byte_idx as usize + 1);
        cache.bytes[byte_idx as usize] |= mask;
        cache.dirty = true;
    }

    // Eagerly flush to disk so vacuum survives crashes.
    let current = read_vm_byte(&path, byte_idx)?;
    write_vm_byte(&path, byte_idx, current | mask)
}

/// Mark `page_id` as **dirty** (a write touched the page).
///
/// Called by DELETE and UPDATE before modifying any slot on the page.
pub fn vm_clear_page(db_name: &str, table_name: &str, page_id: u32) -> io::Result<()> {
    if page_id == 0 {
        return Ok(()); // header page – always dirty
    }
    let (byte_idx, mask) = byte_and_mask(page_id);
    let path = vm_path(db_name, table_name);

    let already_clear = {
        let mut reg = registry().lock().unwrap();
        let cache = reg.get_mut(db_name, table_name);
        VmRegistry::ensure_capacity(cache, byte_idx as usize + 1);
        let already = cache.bytes[byte_idx as usize] & mask == 0;
        if !already {
            cache.bytes[byte_idx as usize] &= !mask;
            cache.dirty = true;
        }
        already
    };

    if !already_clear {
        let current = read_vm_byte(&path, byte_idx)?;
        write_vm_byte(&path, byte_idx, current & !mask)?;
    }
    Ok(())
}

/// Returns `true` if `page_id` is currently all-visible.
///
/// Called by vacuum/compaction to decide whether a page can be skipped.
pub fn vm_is_visible(db_name: &str, table_name: &str, page_id: u32) -> bool {
    if page_id == 0 {
        return false;
    }
    let (byte_idx, mask) = byte_and_mask(page_id);

    // Check in-memory cache first.
    {
        let mut reg = registry().lock().unwrap();
        let cache = reg.get_mut(db_name, table_name);
        if (byte_idx as usize) < cache.bytes.len() {
            return cache.bytes[byte_idx as usize] & mask != 0;
        }
    }

    // Fall back to disk read.
    let path = vm_path(db_name, table_name);
    match read_vm_byte(&path, byte_idx) {
        Ok(b) => b & mask != 0,
        Err(_) => false,
    }
}

/// Returns the count of all-visible pages from 1..=`max_page_id`.
///
/// Useful for diagnostic output (mirrors `pg_visibility_map_summary`).
pub fn vm_visible_count(db_name: &str, table_name: &str, max_page_id: u32) -> u32 {
    let mut count = 0u32;
    for page_id in 1..=max_page_id {
        if vm_is_visible(db_name, table_name, page_id) {
            count += 1;
        }
    }
    count
}

/// Invalidate the in-memory cache for a table (e.g. after a table drop).
pub fn vm_evict(db_name: &str, table_name: &str) {
    let key = VmRegistry::key(db_name, table_name);
    if let Ok(mut reg) = registry().lock() {
        reg.tables.remove(&key);
    }
}
