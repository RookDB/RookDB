//! Tunables for the join subsystem.
//!
//! Resolved once at construction. Every value can be overridden by a
//! `ROOKDB_JOIN_*` environment variable.

use std::path::{Path, PathBuf};

/// Smallest budget we will accept. Has to hold one row plus its hash entry.
pub const MIN_WORK_MEMORY: u64 = 4 * 1024;

/// Largest budget we will accept, so a mistyped variable cannot ask for
/// terabytes.
pub const MAX_WORK_MEMORY: u64 = 16 * 1024 * 1024 * 1024;

/// Bounds on the budget derived from system memory.
const DERIVED_FLOOR: u64 = 4 * 1024 * 1024;
const DERIVED_CEILING: u64 = 256 * 1024 * 1024;

pub const WORK_MEMORY_ENV: &str = "ROOKDB_JOIN_WORK_MEM";
pub const SPILL_ROOT_ENV: &str = "ROOKDB_JOIN_SPILL_ROOT";

/// Thresholds the operators consult.
///
/// These are policy, not correctness: any value produces the right answer, and
/// only the work done to get there changes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinTuning {
    /// Partitions a hash join creates per level.
    pub hash_fan_out: usize,
    /// How many times it will try repartitioning before giving up. Past this,
    /// a partition that will not shrink is one dominated by a single key.
    pub max_repartition_depth: u32,
    /// Outer rows a nested loop buffers per pass over the inner side.
    pub block_rows: usize,
    /// Rows the adaptive operator reads from each side to find the smaller.
    pub adaptive_sample_rows: u64,
    /// Rows between checks of real system memory.
    pub pressure_check_rows: u64,
    /// Below this fraction of memory still free, halve the budget.
    pub pressure_threshold: f64,
    /// Read-ahead assumed per run when choosing a merge fan-in.
    pub merge_buffer_bytes: u64,
    /// Above this many relations, order greedily instead of exhaustively.
    pub max_exhaustive_relations: usize,
    /// Buckets in a column histogram.
    pub histogram_buckets: usize,
    /// Values retained while sampling for histogram boundaries.
    pub histogram_sample_rows: usize,
    /// Rows the CLI prints before truncating.
    pub max_display_rows: usize,
    /// Assumed row count, page count and row width for a relation that cannot
    /// be read at all.
    pub fallback_rows: u64,
    pub fallback_pages: u32,
    pub fallback_row_bytes: f64,
}

impl Default for JoinTuning {
    fn default() -> Self {
        Self {
            hash_fan_out: 16,
            max_repartition_depth: 3,
            block_rows: 1024,
            adaptive_sample_rows: 8_192,
            pressure_check_rows: 65_536,
            pressure_threshold: 0.10,
            merge_buffer_bytes: 64 * 1024,
            max_exhaustive_relations: 8,
            histogram_buckets: 64,
            histogram_sample_rows: 20_000,
            max_display_rows: 200,
            fallback_rows: 1_000,
            fallback_pages: 10,
            fallback_row_bytes: 100.0,
        }
    }
}

impl JoinTuning {
    /// Defaults, with any `ROOKDB_JOIN_*` overrides applied.
    pub fn from_env() -> Self {
        let mut tuning = Self::default();

        // Fan-out below two would never split anything; depth zero disables
        // repartitioning, which is a legitimate choice.
        tuning.hash_fan_out = env_usize("ROOKDB_JOIN_FAN_OUT", tuning.hash_fan_out).max(2);
        tuning.max_repartition_depth = env_u64(
            "ROOKDB_JOIN_MAX_REPARTITION",
            u64::from(tuning.max_repartition_depth),
        ) as u32;
        tuning.block_rows = env_usize("ROOKDB_JOIN_BLOCK_ROWS", tuning.block_rows).max(1);
        tuning.adaptive_sample_rows =
            env_u64("ROOKDB_JOIN_SAMPLE_ROWS", tuning.adaptive_sample_rows).max(1);
        tuning.pressure_check_rows =
            env_u64("ROOKDB_JOIN_PRESSURE_ROWS", tuning.pressure_check_rows).max(1);
        tuning.pressure_threshold =
            env_f64("ROOKDB_JOIN_PRESSURE_FRACTION", tuning.pressure_threshold).clamp(0.0, 1.0);
        tuning.merge_buffer_bytes =
            env_u64("ROOKDB_JOIN_MERGE_BUFFER", tuning.merge_buffer_bytes).max(1024);
        // The search is exponential; 20 relations is already far past useful.
        tuning.max_exhaustive_relations = env_usize(
            "ROOKDB_JOIN_MAX_DP_RELATIONS",
            tuning.max_exhaustive_relations,
        )
        .clamp(2, 20);
        tuning.histogram_buckets =
            env_usize("ROOKDB_JOIN_HISTOGRAM_BUCKETS", tuning.histogram_buckets).clamp(2, 4096);
        tuning.histogram_sample_rows =
            env_usize("ROOKDB_JOIN_HISTOGRAM_SAMPLE", tuning.histogram_sample_rows)
                .max(tuning.histogram_buckets);
        tuning.max_display_rows =
            env_usize("ROOKDB_JOIN_MAX_DISPLAY_ROWS", tuning.max_display_rows);
        tuning.fallback_rows = env_u64("ROOKDB_JOIN_FALLBACK_ROWS", tuning.fallback_rows).max(1);
        tuning.fallback_pages = env_u64(
            "ROOKDB_JOIN_FALLBACK_PAGES",
            u64::from(tuning.fallback_pages),
        )
        .max(1) as u32;
        tuning.fallback_row_bytes =
            env_f64("ROOKDB_JOIN_FALLBACK_ROW_BYTES", tuning.fallback_row_bytes).max(1.0);

        tuning
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinConfig {
    /// Bytes an operator may hold before it spills or changes strategy. A
    /// budget, not a hard limit - see `docs/join/design-rationale.md`.
    pub work_memory_bytes: u64,
    /// Where each operator creates its own spill directory.
    pub spill_root: PathBuf,
    pub tuning: JoinTuning,
}

impl JoinConfig {
    /// Read the environment, falling back to a share of system memory.
    pub fn resolve() -> Self {
        Self {
            work_memory_bytes: resolve_work_memory(),
            spill_root: resolve_spill_root(),
            tuning: JoinTuning::from_env(),
        }
    }

    /// An explicit budget with default tuning. Used to force the spilling
    /// paths in tests, so it deliberately ignores the environment - a config
    /// built by hand should not be altered behind the caller's back.
    pub fn with_work_memory(bytes: u64) -> Self {
        Self {
            work_memory_bytes: bytes.clamp(MIN_WORK_MEMORY, MAX_WORK_MEMORY),
            spill_root: resolve_spill_root(),
            tuning: JoinTuning::default(),
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

    pub fn tuning(mut self, tuning: JoinTuning) -> Self {
        self.tuning = tuning;
        self
    }
}

impl Default for JoinConfig {
    fn default() -> Self {
        Self::resolve()
    }
}

fn resolve_spill_root() -> PathBuf {
    match std::env::var(SPILL_ROOT_ENV) {
        Ok(root) if !root.trim().is_empty() => PathBuf::from(root.trim()),
        _ => Path::new(crate::layout::DATA_DIR).join("tmp").join("join"),
    }
}

fn resolve_work_memory() -> u64 {
    match std::env::var(WORK_MEMORY_ENV) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(bytes) => bytes.clamp(MIN_WORK_MEMORY, MAX_WORK_MEMORY),
            Err(e) => {
                // Reported, not swallowed: a silently ignored setting looks
                // like the tuning had no effect.
                log::warn!(
                    "{WORK_MEMORY_ENV}={raw:?} is not a byte count ({e}); using the default"
                );
                derived_work_memory()
            }
        },
        Err(_) => derived_work_memory(),
    }
}

/// A quarter of available RAM, clamped. `sysinfo` is polled once - a budget
/// that drifted mid-query would make an operator irreproducible.
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

fn env_usize(name: &str, fallback: usize) -> usize {
    env_parsed(name, fallback)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env_parsed(name, fallback)
}

fn env_f64(name: &str, fallback: f64) -> f64 {
    env_parsed(name, fallback)
}

fn env_parsed<T: std::str::FromStr>(name: &str, fallback: T) -> T {
    let Ok(raw) = std::env::var(name) else {
        return fallback;
    };
    match raw.trim().parse::<T>() {
        Ok(value) => value,
        Err(_) => {
            log::warn!("{name}={raw:?} could not be parsed; using the default");
            fallback
        }
    }
}
