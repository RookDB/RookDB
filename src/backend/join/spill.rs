//! Spilling rows to disk.
//!
//! Three properties matter, and each fixes a defect in the previous
//! implementation:
//!
//! * **Length framing.** `deserialize_nullable_row` derives its last
//!   variable-length payload from the length of the slice it is given, so a
//!   row read back out of a padded buffer decodes incorrectly. Every row is
//!   therefore written with an explicit length.
//! * **Per-operator directories.** Each operator gets its own directory, named
//!   with the process id and a process-global counter, so the two sides of a
//!   self-join cannot collide. Cleanup removes only that directory and never
//!   enumerates the root, so one operator can no longer delete another's
//!   files.
//! * **Cleanup by `Drop`.** It cannot be forgotten, and it runs while the
//!   stack unwinds, so a panic mid-join leaves nothing behind.
//!
//! Rows are written as plain framed records rather than into a heap file. A
//! heap file would add a header page and a free-space-map fork per partition,
//! and charge an FSM search plus a header rewrite per row, to buy random
//! access and space reuse that a spill file never needs.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::error::JoinError;
use super::memory::{MemoryAccountant, row_footprint};

/// File magic plus a format version.
const MAGIC: [u8; 8] = *b"RKJRUN\0\x01";
/// Magic, then the schema fingerprint.
const HEADER_LEN: usize = 16;

/// Distinguishes operators within one process.
static SCOPE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A directory owned by one operator, removed when the last reference drops.
///
/// `RunHandle`s hold an `Arc` to the scope, so the directory outlives any
/// reader still in flight - a partition cannot be deleted while another is
/// still being read.
#[derive(Debug)]
pub struct SpillScope {
    dir: PathBuf,
    next_file: AtomicU64,
}

impl SpillScope {
    /// Create a fresh directory under `root`.
    pub fn create(root: &Path) -> Result<Arc<Self>, JoinError> {
        let counter = SCOPE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = root.join(format!(
            "join-{}-{}-{}",
            std::process::id(),
            epoch_seconds(),
            counter
        ));

        std::fs::create_dir_all(&dir).map_err(|e| {
            JoinError::Io(format!(
                "cannot create spill directory {}: {e}",
                dir.display()
            ))
        })?;

        Ok(Arc::new(Self {
            dir,
            next_file: AtomicU64::new(0),
        }))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// A unique path inside this scope. `label` names the role - which side,
    /// which partition - so a directory listing is readable during debugging.
    fn next_path(&self, label: &str) -> PathBuf {
        let index = self.next_file.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!("{label}-{index:05}.run"))
    }
}

impl Drop for SpillScope {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                log::warn!(
                    "[join] could not remove spill directory {}: {e}",
                    self.dir.display()
                );
            }
        }
    }
}

/// Writes framed rows into a new run file.
pub struct RunWriter {
    file: BufWriter<File>,
    path: PathBuf,
    fingerprint: u64,
    rows: u64,
    bytes: u64,
    scope: Arc<SpillScope>,
}

impl RunWriter {
    pub fn create(
        scope: &Arc<SpillScope>,
        label: &str,
        fingerprint: u64,
    ) -> Result<Self, JoinError> {
        let path = scope.next_path(label);
        let file = File::create(&path)
            .map_err(|e| JoinError::Io(format!("cannot create {}: {e}", path.display())))?;
        let mut file = BufWriter::new(file);

        file.write_all(&MAGIC)
            .and_then(|()| file.write_all(&fingerprint.to_le_bytes()))
            .map_err(|e| JoinError::Io(format!("cannot write {} header: {e}", path.display())))?;

        Ok(Self {
            file,
            path,
            fingerprint,
            rows: 0,
            bytes: 0,
            scope: Arc::clone(scope),
        })
    }

    pub fn write_row(&mut self, row: &[u8]) -> Result<(), JoinError> {
        let length = u32::try_from(row.len()).map_err(|_| {
            JoinError::Io(format!("row of {} bytes is too large to spill", row.len()))
        })?;

        self.file
            .write_all(&length.to_le_bytes())
            .and_then(|()| self.file.write_all(row))
            .map_err(|e| JoinError::Io(format!("cannot write to {}: {e}", self.path.display())))?;

        self.rows += 1;
        self.bytes += row.len() as u64;
        Ok(())
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn finish(mut self) -> Result<RunHandle, JoinError> {
        self.file
            .flush()
            .map_err(|e| JoinError::Io(format!("cannot flush {}: {e}", self.path.display())))?;

        Ok(RunHandle {
            path: self.path.clone(),
            fingerprint: self.fingerprint,
            rows: self.rows,
            bytes: self.bytes,
            _scope: Arc::clone(&self.scope),
        })
    }
}

/// A finished run file.
#[derive(Debug, Clone)]
pub struct RunHandle {
    path: PathBuf,
    fingerprint: u64,
    rows: u64,
    bytes: u64,
    /// Never read: held so the scope - and therefore this file's directory -
    /// outlives every handle to it.
    _scope: Arc<SpillScope>,
}

impl RunHandle {
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Row bytes written, excluding framing.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the run from the start. May be called any number of times.
    pub fn reader(&self) -> Result<RunReader, JoinError> {
        RunReader::open(&self.path, self.fingerprint)
    }
}

/// Reads framed rows back.
pub struct RunReader {
    file: BufReader<File>,
    path: PathBuf,
}

impl RunReader {
    fn open(path: &Path, expected_fingerprint: u64) -> Result<Self, JoinError> {
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| JoinError::Io(format!("cannot open {}: {e}", path.display())))?;
        let mut file = BufReader::new(file);

        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| JoinError::Io(format!("cannot read {} header: {e}", path.display())))?;

        if header[..8] != MAGIC {
            return Err(JoinError::Io(format!(
                "{} is not a join run file",
                path.display()
            )));
        }

        let mut fingerprint_bytes = [0u8; 8];
        fingerprint_bytes.copy_from_slice(&header[8..16]);
        let fingerprint = u64::from_le_bytes(fingerprint_bytes);

        // Reading a run under a different schema would decode every row
        // incorrectly and silently. Refusing is what makes a run file safe to
        // share between the two sides of a self-join.
        if fingerprint != expected_fingerprint {
            return Err(JoinError::Io(format!(
                "{} was written for a different schema (fingerprint {fingerprint:#x}, expected {expected_fingerprint:#x})",
                path.display()
            )));
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }
}

impl Iterator for RunReader {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut length = [0u8; 4];
        match self.file.read_exact(&mut length) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => {
                return Some(Err(JoinError::Io(format!(
                    "cannot read from {}: {e}",
                    self.path.display()
                ))));
            }
        }

        let length = u32::from_le_bytes(length) as usize;
        let mut row = vec![0u8; length];
        if let Err(e) = self.file.read_exact(&mut row) {
            return Some(Err(JoinError::Io(format!(
                "{} ends inside a row of {length} bytes: {e}",
                self.path.display()
            ))));
        }

        Some(Ok(row))
    }
}

// ── Buffers that spill when they outgrow their budget ────────────────────────

/// Accumulates rows in memory, moving to a run file once the budget is spent.
///
/// This is what stops a single hot key from being unbounded: a sort-merge
/// join's duplicate group, a hash partition, and the NULL-keyed rows a sort
/// sets aside all use it.
pub struct RowBufferBuilder {
    scope: Arc<SpillScope>,
    label: String,
    fingerprint: u64,
    memory: Vec<Vec<u8>>,
    charged: u64,
    writer: Option<RunWriter>,
    rows: u64,
}

impl RowBufferBuilder {
    pub fn new(scope: &Arc<SpillScope>, label: impl Into<String>, fingerprint: u64) -> Self {
        Self {
            scope: Arc::clone(scope),
            label: label.into(),
            fingerprint,
            memory: Vec::new(),
            charged: 0,
            writer: None,
            rows: 0,
        }
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn spilled(&self) -> bool {
        self.writer.is_some()
    }

    /// Add a row, spilling this buffer if it no longer fits.
    pub fn push(&mut self, row: &[u8], budget: &MemoryAccountant) -> Result<(), JoinError> {
        self.rows += 1;

        if let Some(writer) = &mut self.writer {
            return writer.write_row(row);
        }

        let footprint = row_footprint(row.len());
        if budget.charge(footprint).is_ok() {
            self.charged += footprint;
            self.memory.push(row.to_vec());
            return Ok(());
        }

        // Out of budget: move everything held so far to disk, then continue
        // there. The memory is released only after the write succeeds.
        let mut writer = RunWriter::create(&self.scope, &self.label, self.fingerprint)?;
        for buffered in &self.memory {
            writer.write_row(buffered)?;
        }
        writer.write_row(row)?;

        budget.release(self.charged);
        self.charged = 0;
        self.memory = Vec::new();
        self.writer = Some(writer);
        Ok(())
    }

    pub fn finish(self, budget: &MemoryAccountant) -> Result<RowBuffer, JoinError> {
        match self.writer {
            Some(writer) => Ok(RowBuffer::Disk(writer.finish()?)),
            None => {
                budget.release(self.charged);
                Ok(RowBuffer::Memory(self.memory))
            }
        }
    }
}

/// A finished buffer of rows, re-readable any number of times.
#[derive(Debug, Clone)]
pub enum RowBuffer {
    Memory(Vec<Vec<u8>>),
    Disk(RunHandle),
}

impl RowBuffer {
    pub fn len(&self) -> u64 {
        match self {
            RowBuffer::Memory(rows) => rows.len() as u64,
            RowBuffer::Disk(handle) => handle.rows(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn spilled(&self) -> bool {
        matches!(self, RowBuffer::Disk(_))
    }

    /// Iterate the rows. Memory and disk buffers present the same interface,
    /// so a consumer never branches on where the rows live.
    pub fn reader(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Vec<u8>, JoinError>> + '_>, JoinError> {
        Ok(match self {
            RowBuffer::Memory(rows) => Box::new(rows.iter().map(|row| Ok(row.clone()))),
            RowBuffer::Disk(handle) => Box::new(handle.reader()?),
        })
    }
}

// ── Orphan cleanup ───────────────────────────────────────────────────────────

/// Remove spill directories left behind by processes that are no longer
/// running.
///
/// `Drop` covers normal exits and panics; this covers the rest - a `SIGKILL`,
/// a power loss, `panic = "abort"`. It only ever removes directories whose
/// names it recognises, and only when the owning process is gone *and* the
/// directory is older than `min_age`, so it cannot race a process that has
/// just started or trip over a recycled process id.
pub fn sweep_orphans(root: &Path, min_age: Duration) -> Result<usize, JoinError> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        // Nothing has spilled yet.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(JoinError::Io(format!(
                "cannot list {}: {e}",
                root.display()
            )));
        }
    };

    let now = epoch_seconds();
    let own_pid = std::process::id();
    let mut removed = 0usize;

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some((pid, created)) = parse_scope_name(name) else {
            continue;
        };

        if pid == own_pid || process_is_alive(pid) {
            continue;
        }
        if now.saturating_sub(created) < min_age.as_secs() {
            continue;
        }

        match std::fs::remove_dir_all(&path) {
            Ok(()) => removed += 1,
            Err(e) => log::warn!("[join] could not remove {}: {e}", path.display()),
        }
    }

    Ok(removed)
}

/// `join-{pid}-{epoch}-{counter}` - anything else is not ours to delete.
fn parse_scope_name(name: &str) -> Option<(u32, u64)> {
    let rest = name.strip_prefix("join-")?;
    let mut parts = rest.split('-');
    let pid = parts.next()?.parse::<u32>().ok()?;
    let created = parts.next()?.parse::<u64>().ok()?;
    parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((pid, created))
}

#[cfg(target_os = "linux")]
fn process_is_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// Without a way to ask, assume the process is alive and let the age check be
/// the only criterion. Reclaiming space late is better than deleting a running
/// query's spill files.
#[cfg(not(target_os = "linux"))]
fn process_is_alive(_pid: u32) -> bool {
    true
}
