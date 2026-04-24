//! OID (Object Identifier) counter – persistent, monotonically increasing.
//!
//! The next available OID is stored as a little-endian u32 in the first four
//! bytes of `database/global/pg_oid_counter.dat`.  OIDs < USER_OID_START are
//! reserved for built-in objects.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::layout::{GLOBAL_DIR, OID_COUNTER_FILE, USER_OID_START};
use crate::catalog::types::CatalogError;

/// Manages the persistent OID counter for all database objects.
pub struct OidCounter {
    pub next_oid: u32,
    counter_file_path: String,
}

impl OidCounter {
    // ──────────────────────────────────────────────────────────────
    // Construction
    // ──────────────────────────────────────────────────────────────

    /// Create a new OidCounter pointing at the default counter file.
    /// Call `load()` afterwards to restore the persisted value from disk.
    pub fn new() -> Self {
        OidCounter {
            next_oid: USER_OID_START,
            counter_file_path: OID_COUNTER_FILE.to_string(),
        }
    }

    // ──────────────────────────────────────────────────────────────
    // Load / persist
    // ──────────────────────────────────────────────────────────────

    /// Read `next_oid` from the counter file.
    /// If the file does not exist the counter stays at `USER_OID_START`.
    pub fn load(&mut self) -> Result<(), CatalogError> {
        let path = Path::new(&self.counter_file_path);
        if !path.exists() {
            return Ok(());
        }

        let mut file = fs::File::open(path)?;
        let mut buf = [0u8; 4];
        file.read_exact(&mut buf)?;
        let stored = u32::from_le_bytes(buf);

        // Never let the counter go below USER_OID_START
        if stored >= USER_OID_START {
            self.next_oid = stored;
        }
        Ok(())
    }

    /// Write the current `next_oid` to disk atomically (overwrite first 4 bytes).
    pub fn persist(&self) -> Result<(), CatalogError> {
        // Ensure parent directory exists
        if let Some(parent) = Path::new(&self.counter_file_path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false) // keep any extra bytes that might follow
            .open(&self.counter_file_path)?;

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.next_oid.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Ensure the counter file and its parent directory exist.
    /// Called during bootstrap.
    pub fn initialize() -> Result<(), io::Error> {
        let global = Path::new(GLOBAL_DIR);
        if !global.exists() {
            fs::create_dir_all(global)?;
        }
        let path = Path::new(OID_COUNTER_FILE);
        if !path.exists() {
            // Write the initial value
            let mut file = fs::File::create(path)?;
            file.write_all(&USER_OID_START.to_le_bytes())?;
        }
        Ok(())
    }
}
