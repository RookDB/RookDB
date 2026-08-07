//! Tunables for the join subsystem.
//!
//! Defaults are derived once, at construction; nothing here is read from the
//! environment on a hot path. Tests set the memory budget explicitly to a few
//! kilobytes, which is what makes the spilling paths reachable in a unit test
//! rather than only under production-sized data.

use std::path::{Path, PathBuf};

/// Never budget less than this: below it, even a single row plus its hash
/// entry would not fit and the operator could make no progress.
pub const MIN_WORK_MEMORY: u64 = 64 * 1024;

/// Refuse absurd budgets rather than trusting a mistyped environment variable.
pub const MAX_WORK_MEMORY: u64 = 16 * 1024 * 1024 * 1024;

/// Bounds on the budget derived from system memory when nothing else says.
const DERIVED_FLOOR: u64 = 4 * 1024 * 1024;
const DERIVED_CEILING: u64 = 256 * 1024 * 1024;

/// Environment override, in bytes.
pub const WORK_MEMORY_ENV: &str = "ROOKDB_JOIN_WORK_MEM";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinConfig {
    /// Bytes an operator may hold in memory before it must spill or change
    /// strategy. See `docs/join/design-rationale.md` for why this is a
    /// deliberate over-estimate rather than a hard limit.
    pub work_memory_bytes: u64,
    /// Directory under which each operator creates its own spill directory.
    pub spill_root: PathBuf,
}

impl JoinConfig {
    /// Budget from `ROOKDB_JOIN_WORK_MEM` if set, otherwise a quarter of the
    /// system's available memory, clamped.
    pub fn resolve() -> Self {
        Self {
            work_memory_bytes: resolve_work_memory(),
            spill_root: default_spill_root(),
        }
    }

    /// A configuration with an explicit budget. Used by tests to force the
    /// hybrid, Grace and multi-run merge paths.
    pub fn with_work_memory(bytes: u64) -> Self {
        Self {
            work_memory_bytes: bytes.clamp(MIN_WORK_MEMORY, MAX_WORK_MEMORY),
            spill_root: default_spill_root(),
        }
    }

    pub fn spill_root(mut self, root: impl AsRef<Path>) -> Self {
        self.spill_root = root.as_ref().to_path_buf();
        self
    }

    pub fn work_memory(mut self, bytes: u64) -> Self {
        self.work_memory_bytes = bytes.clamp(MIN_WORK_MEMORY, MAX_WORK_MEMORY);
        self
    }
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self::resolve()
    }
}

fn default_spill_root() -> PathBuf {
    Path::new(crate::layout::DATA_DIR).join("tmp").join("join")
}

fn resolve_work_memory() -> u64 {
    match std::env::var(WORK_MEMORY_ENV) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(bytes) => bytes.clamp(MIN_WORK_MEMORY, MAX_WORK_MEMORY),
            Err(e) => {
                // Reported rather than swallowed: silently ignoring a
                // mistyped budget is how a tuning change appears to do
                // nothing.
                log::warn!(
                    "{WORK_MEMORY_ENV}={raw:?} is not a byte count ({e}); using the default"
                );
                derived_work_memory()
            }
        },
        Err(_) => derived_work_memory(),
    }
}

/// A quarter of currently available RAM, clamped.
///
/// `sysinfo` is refreshed exactly once here - it is expensive, and a budget
/// that drifted mid-query would make an operator's behaviour irreproducible.
fn derived_work_memory() -> u64 {
    use sysinfo::{MemoryRefreshKind, System};

    let mut system = System::new();
    system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());

    let available = system.available_memory();
    if available == 0 {
        return DERIVED_FLOOR;
    }
    (available / 4).clamp(DERIVED_FLOOR, DERIVED_CEILING)
}
