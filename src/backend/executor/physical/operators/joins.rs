use std::collections::HashMap;
use std::path::PathBuf;

use super::super::explain::PlanNode;
use super::super::expr::{Expr, Predicate, evaluate_predicate};
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::cache::{CachedIndex, HeapHandle};
use crate::backend::error::{RookError, RookResult};

use crate::types::DataType;
use crate::types::value::DataValue;

// ── Join Type ─────────────────────────────────────────────────────────────────

/// Join type matching SQL semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl JoinType {
    /// The join type as `EXPLAIN` shows it.
    pub fn label(&self) -> &'static str {
        match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
            JoinType::Cross => "Cross",
        }
    }
}

fn yes_no(yes: bool) -> &'static str {
    if yes { "yes" } else { "no" }
}

// ── NestedLoopJoin Operator ───────────────────────────────────────────────────

/// A nested-loop join operator that works for any join predicate.
///
/// Reads the right (inner) child into memory once, then streams the left
/// (outer) child one row at a time, pairing each outer row with every inner
/// row and evaluating the join predicate on each pair. Rows come out as they
/// are found: the outer side is read only as far as the consumer asks.
///
/// Supports: INNER, LEFT, RIGHT, FULL, CROSS join types. An outer row
/// without a partner is NULL-extended (LEFT/FULL) right after its last
/// pairing; inner rows without one (RIGHT/FULL) come out after the outer
/// side ends. For CROSS joins (no predicate), all pairs match.
pub struct NestedLoopJoinOperator {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,
    /// The join predicate (None for CROSS JOIN).
    predicate: Option<Predicate>,
    /// The join type.
    join_type: JoinType,
    /// Output schema (left columns + right columns).
    output_schema: Vec<ColumnInfo>,
    left_arity: usize,
    right_arity: usize,

    // ── Runtime state ──
    phase: NestedLoopPhase,
    /// All tuples from the right child, read on the first call.
    inner_rows: Vec<Tuple>,
    /// Which inner rows have found at least one match (RIGHT/FULL only).
    inner_matched: Vec<bool>,
    /// The outer row being paired, and whether it has matched yet.
    cur_outer: Option<Tuple>,
    cur_outer_matched: bool,
    /// Next inner row to pair with `cur_outer`; in the unmatched phase, the
    /// next inner row to check.
    inner_pos: usize,
    /// Joined row the predicate is evaluated on; handed out when it matches.
    candidate: Tuple,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NestedLoopPhase {
    /// Nothing read yet.
    Start,
    /// Streaming outer rows past the inner rows.
    Outer,
    /// Outer side done: NULL-extending unmatched inner rows (RIGHT/FULL).
    InnerUnmatched,
    Done,
}

impl NestedLoopJoinOperator {
    /// Create a new nested-loop join operator.
    ///
    /// `left` and `right` are the child operators.
    /// `predicate` is the join condition (None for CROSS JOIN).
    /// `join_type` controls outer join null-extension behaviour.
    pub fn new(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        predicate: Option<Predicate>,
        join_type: JoinType,
    ) -> Self {
        // Build output schema: left columns + right columns.
        // ColumnInfo.table is populated with actual table names by the scan operators,
        // so table-qualified references like `t1.id` vs `t2.id` resolve correctly
        // without sentinel markers. The planner also uses the original column info
        // when building the combined schema for predicate resolution.
        let mut output_schema = left.schema().to_vec();
        output_schema.extend(right.schema().iter().cloned());
        let left_arity = left.schema().len();
        let right_arity = right.schema().len();

        Self {
            left,
            right,
            predicate,
            join_type,
            output_schema,
            left_arity,
            right_arity,
            phase: NestedLoopPhase::Start,
            inner_rows: Vec::new(),
            inner_matched: Vec::new(),
            cur_outer: None,
            cur_outer_matched: false,
            inner_pos: 0,
            candidate: Tuple::empty(),
        }
    }

    fn preserve_outer(&self) -> bool {
        matches!(self.join_type, JoinType::Left | JoinType::Full)
    }

    fn preserve_inner(&self) -> bool {
        matches!(self.join_type, JoinType::Right | JoinType::Full)
    }

    /// Read the inner side and pick the first phase.
    fn start(&mut self) -> RookResult<()> {
        self.inner_rows.clear();
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.right.next_batch(&mut child_batch)? > 0 {
            self.inner_rows.append(&mut child_batch);
        }
        self.inner_matched = if self.preserve_inner() {
            vec![false; self.inner_rows.len()]
        } else {
            Vec::new()
        };
        self.cur_outer = None;
        self.inner_pos = 0;
        // With no inner row to pair with and no outer row to preserve, the
        // outer side need not be read at all.
        self.phase = if self.inner_rows.is_empty() && !self.preserve_outer() {
            NestedLoopPhase::InnerUnmatched
        } else {
            NestedLoopPhase::Outer
        };
        Ok(())
    }

    /// Pair outer rows with inner rows until one output row is ready.
    /// Returns `None` after moving on to the unmatched-inner phase.
    fn outer_step(&mut self) -> RookResult<Option<Tuple>> {
        // A CROSS join, or a join without a condition, matches every pair.
        let predicate = match self.join_type {
            JoinType::Cross => None,
            _ => self.predicate.as_ref(),
        };
        loop {
            let Some(outer) = &self.cur_outer else {
                match self.left.next()? {
                    Some(tuple) => {
                        self.cur_outer = Some(tuple);
                        self.cur_outer_matched = false;
                        self.inner_pos = 0;
                        continue;
                    }
                    None => {
                        self.phase = NestedLoopPhase::InnerUnmatched;
                        self.inner_pos = 0;
                        return Ok(None);
                    }
                }
            };
            while self.inner_pos < self.inner_rows.len() {
                let idx = self.inner_pos;
                self.inner_pos += 1;
                outer.concatenate_into(&self.inner_rows[idx], &mut self.candidate);
                if let Some(pred) = predicate
                    && evaluate_predicate(pred, &self.candidate, &self.output_schema)? != Some(true)
                {
                    continue;
                }
                self.cur_outer_matched = true;
                if let Some(matched) = self.inner_matched.get_mut(idx) {
                    *matched = true;
                }
                return Ok(Some(std::mem::replace(&mut self.candidate, Tuple::empty())));
            }
            // Every inner row seen: this outer row is finished.
            let outer = self.cur_outer.take().expect("current outer row");
            if !self.cur_outer_matched && self.preserve_outer() {
                let mut values = outer.values;
                values.extend(std::iter::repeat_n(None, self.right_arity));
                return Ok(Some(Tuple::new(values)));
            }
        }
    }

    /// Next unmatched inner row, NULL-extended. The inner rows are not needed
    /// after this phase, so each is moved out rather than cloned.
    fn inner_unmatched_step(&mut self) -> Option<Tuple> {
        while self.inner_pos < self.inner_matched.len() {
            let idx = self.inner_pos;
            self.inner_pos += 1;
            if !self.inner_matched[idx] {
                let inner = std::mem::replace(&mut self.inner_rows[idx], Tuple::empty());
                let mut values = Vec::with_capacity(self.left_arity + inner.values.len());
                values.extend(std::iter::repeat_n(None, self.left_arity));
                values.extend(inner.values);
                return Some(Tuple::new(values));
            }
        }
        None
    }
}

impl PhysicalOperator for NestedLoopJoinOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        loop {
            match self.phase {
                NestedLoopPhase::Start => self.start()?,
                NestedLoopPhase::Outer => {
                    if let Some(tuple) = self.outer_step()? {
                        return Ok(Some(tuple));
                    }
                }
                NestedLoopPhase::InnerUnmatched => {
                    if let Some(tuple) = self.inner_unmatched_step() {
                        return Ok(Some(tuple));
                    }
                    self.phase = NestedLoopPhase::Done;
                    self.inner_rows = Vec::new();
                    self.inner_matched = Vec::new();
                }
                NestedLoopPhase::Done => return Ok(None),
            }
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.left.reset()?;
        self.right.reset()?;
        self.inner_rows.clear();
        self.inner_matched.clear();
        self.cur_outer = None;
        self.inner_pos = 0;
        self.phase = NestedLoopPhase::Start;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        let (left, right) = (
            self.left.estimate_cardinality(),
            self.right.estimate_cardinality(),
        );
        match (&self.predicate, self.join_type) {
            (None, _) | (_, JoinType::Cross) => left.saturating_mul(right),
            _ => (left.saturating_mul(right) / 5).max(1),
        }
    }

    fn name(&self) -> &'static str {
        "NestedLoopJoin"
    }

    fn explain(&self) -> PlanNode {
        PlanNode::new(self.name())
            .with("type", self.join_type.label())
            .with("predicate", yes_no(self.predicate.is_some()))
            .child(self.left.explain())
            .child(self.right.explain())
    }
}

// ── HashJoin Operator ─────────────────────────────────────────────────────────

/// A hash join operator for equality joins.
///
/// Builds a hash table on one input (the build side) and streams the other
/// (the probe side) past it, so rows come out as soon as the build side has
/// been read. Supports INNER, LEFT, RIGHT and FULL joins: a probe row without
/// a partner is NULL-extended as it passes, a build row without one after the
/// probe side ends.
///
/// The build side is held in memory up to a byte budget (see
/// [`SPILL_BUDGET_BYTES`]). Past it both inputs are hash-partitioned to temp
/// files and joined one partition at a time. A partition that is itself over
/// budget is split again with a different hash seed; one that cannot be split
/// (a single hot key) is joined in budget-sized blocks of build rows, each
/// block against the whole probe partition.
///
/// For non-equality conditions use NestedLoopJoinOperator.
pub struct HashJoinOperator {
    build: Box<dyn PhysicalOperator>,
    probe: Box<dyn PhysicalOperator>,
    /// Expressions to hash on the build side.
    build_keys: Vec<Expr>,
    /// Expressions to hash on the probe side.
    probe_keys: Vec<Expr>,
    /// Any remaining predicate (non-equality conditions that must also be satisfied).
    remaining_predicate: Option<Predicate>,
    /// Output schema (left columns + right columns).
    output_schema: Vec<ColumnInfo>,
    /// Child schemas, copied once so no call clones them again.
    build_schema: Vec<ColumnInfo>,
    probe_schema: Vec<ColumnInfo>,
    /// True when the build side is the left input; decides the column order
    /// of the joined row.
    build_is_left: bool,
    /// Unmatched build rows are emitted NULL-extended.
    preserve_build: bool,
    /// Unmatched probe rows are emitted NULL-extended.
    preserve_probe: bool,
    /// The join type, for `EXPLAIN`.
    join_type: JoinType,
    /// Per-operator budget; `None` uses [`SPILL_BUDGET_BYTES`].
    budget_override: Option<usize>,
    /// Budget in force for the current run (bytes).
    budget: usize,

    // ── Runtime state ──
    phase: HashJoinPhase,
    /// Build rows currently in memory: the whole build side, one spilled
    /// partition, or one block of a partition too large to load.
    build_rows: Vec<Tuple>,
    /// Hash table: key values → positions in `build_rows`.
    hash_table: HashMap<Vec<DataValue>, Vec<u32>>,
    /// Which `build_rows` found a partner (only kept when `preserve_build`).
    build_matched: Vec<bool>,
    /// Estimated size of `build_rows` and their hash entries.
    build_bytes: usize,
    /// Largest `build_bytes` seen (diagnostics/tests).
    peak_build_bytes: usize,
    /// Probe rows fetched from the child and not yet joined (reversed, so
    /// the next row is popped from the end).
    probe_buf: Vec<Tuple>,
    /// The probe row being joined.
    cur_probe: Option<Tuple>,
    /// `build_rows` positions sharing the current probe row's key.
    cur_matches: Vec<u32>,
    /// Current position within `cur_matches`.
    match_pos: usize,
    /// Whether the current probe row has produced a joined row.
    cur_matched: bool,
    /// Scratch buffer for the probe row's key.
    key_buf: Vec<DataValue>,
    /// Next `build_rows` position to check for an unmatched build row.
    unmatched_pos: usize,

    // ── Spill state (grace hash join) ──
    /// `Some` once the build side spilled.
    spill: Option<SpillState>,
    /// True once this join has taken the spill path (stays set after the
    /// temp files are cleaned up — useful for tests and diagnostics).
    did_spill: bool,
    /// The partition being joined.
    cur_part: Option<SpillPartition>,
    /// Build rows of `cur_part` not yet loaded.
    build_reader: Option<SpillReader>,
    /// A build row read past the end of the current block.
    pending_build: Option<Tuple>,
    /// Probe rows of `cur_part`; `None` while probing straight from the child.
    probe_reader: Option<SpillReader>,
    /// First / last block of `cur_part`. Both are true when the partition's
    /// build side fits in memory, and when the join never spilled.
    block_first: bool,
    block_last: bool,
    /// For a partition joined in several blocks: which probe rows matched in
    /// any block so far, by position in the probe file.
    probe_matched: Vec<bool>,
    /// Position of the current probe row in the probe file.
    probe_ord: usize,
    /// NULL-keyed rows of a preserved side being emitted, with their side.
    null_reader: Option<(SpillReader, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashJoinPhase {
    /// Nothing read yet.
    Build,
    /// Joining probe rows against `build_rows`.
    Probe,
    /// Emitting the unmatched rows of `build_rows`.
    BuildUnmatched,
    /// Picking the next spilled partition.
    NextPartition,
    /// Emitting the NULL-keyed rows set aside while spilling.
    NullKeys,
    Done,
}

/// In-memory budget for the hash-join build side, in bytes. 0 means "unset":
/// the `ROOK_HASH_JOIN_BUDGET_BYTES` environment variable if present, else
/// [`DEFAULT_SPILL_BUDGET_BYTES`]. Tests shrink it to force the spill path.
pub static SPILL_BUDGET_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
const DEFAULT_SPILL_BUDGET_BYTES: usize = 64 * 1024 * 1024;
/// Hash joins that have spilled in this process (diagnostics/tests).
pub static SPILL_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// Hash partitions created when the build side first spills.
const SPILL_PARTITIONS: usize = 64;
/// Sub-partitions created when a spilled partition is itself over budget.
const REPARTITION_FANOUT: usize = 16;
/// How many times a partition may be split again before it is joined in
/// blocks instead.
const MAX_SPILL_DEPTH: u32 = 3;
/// Share of the hash table charged to each build row (key vector, bucket
/// and position list).
const HASH_ENTRY_OVERHEAD: usize = 96;

/// Indexes into [`SpillState::null_rows`].
const BUILD_SIDE: usize = 0;
const PROBE_SIDE: usize = 1;

fn spill_budget() -> usize {
    let configured = SPILL_BUDGET_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    if configured > 0 {
        return configured;
    }
    static ENV: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ENV.get_or_init(|| {
        std::env::var("ROOK_HASH_JOIN_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_SPILL_BUDGET_BYTES)
    })
}

/// Estimated memory held by one build row: the tuple, its string payloads
/// and its share of the hash table.
fn estimate_tuple_bytes(tuple: &Tuple) -> usize {
    let mut bytes = std::mem::size_of::<Tuple>()
        + HASH_ENTRY_OVERHEAD
        + tuple.values.len() * std::mem::size_of::<Option<DataValue>>();
    for value in tuple.values.iter().flatten() {
        if let DataValue::Char(s) | DataValue::Varchar(s) | DataValue::Bit(s) = value {
            bytes += s.len();
        }
    }
    bytes
}

fn spill_error(e: impl Into<RookError>) -> RookError {
    e.into().with_context("hash join spill")
}

/// One spilled run of tuples.
struct SpillFile {
    path: PathBuf,
    rows: usize,
    /// Sum of [`estimate_tuple_bytes`] over the rows: what loading the file
    /// as a build side would cost.
    bytes: usize,
}

impl SpillFile {
    fn remove(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct SpillWriter {
    file: SpillFile,
    out: std::io::BufWriter<std::fs::File>,
    buf: Vec<u8>,
}

impl SpillWriter {
    fn create(path: PathBuf) -> RookResult<Self> {
        let out = std::io::BufWriter::new(std::fs::File::create(&path).map_err(spill_error)?);
        Ok(Self {
            file: SpillFile {
                path,
                rows: 0,
                bytes: 0,
            },
            out,
            buf: Vec::with_capacity(128),
        })
    }

    fn write(&mut self, tuple: &Tuple) -> RookResult<()> {
        use std::io::Write;
        self.buf.clear();
        encode_tuple(tuple, &mut self.buf);
        self.out.write_all(&self.buf).map_err(spill_error)?;
        self.file.rows += 1;
        self.file.bytes += estimate_tuple_bytes(tuple);
        Ok(())
    }

    fn finish(self) -> RookResult<SpillFile> {
        self.out
            .into_inner()
            .map_err(|e| spill_error(e.into_error()))?;
        Ok(self.file)
    }
}

struct SpillReader {
    input: std::io::BufReader<std::fs::File>,
    remaining: usize,
}

impl SpillReader {
    fn open(file: &SpillFile) -> RookResult<Self> {
        let input = std::fs::File::open(&file.path).map_err(spill_error)?;
        Ok(Self {
            input: std::io::BufReader::new(input),
            remaining: file.rows,
        })
    }

    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        decode_tuple(&mut self.input).map(Some).map_err(spill_error)
    }
}

/// Both sides of one hash partition.
struct SpillPartition {
    build: SpillFile,
    probe: SpillFile,
    /// How many times these rows have been partitioned; also the hash seed
    /// that produced this partition.
    depth: u32,
    /// False when splitting the parent left the whole build side in this
    /// one partition, so another split would not shrink it.
    splittable: bool,
}

impl SpillPartition {
    fn remove(&self) {
        self.build.remove();
        self.probe.remove();
    }
}

/// Temp files of one spilled hash join.
struct SpillState {
    dir: PathBuf,
    next_file: usize,
    /// Partitions still to join.
    pending: Vec<SpillPartition>,
    /// NULL-keyed rows of a preserved side ([`BUILD_SIDE`] / [`PROBE_SIDE`]).
    /// They match nothing, so they skip partitioning and are emitted
    /// NULL-extended at the end.
    null_rows: [Option<SpillWriter>; 2],
}

impl SpillState {
    fn create() -> RookResult<Self> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "rookdb_hj_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).map_err(spill_error)?;
        Ok(Self {
            dir,
            next_file: 0,
            pending: Vec::new(),
            null_rows: [None, None],
        })
    }

    fn writer(&mut self) -> RookResult<SpillWriter> {
        let path = self.dir.join(format!("run_{}.bin", self.next_file));
        self.next_file += 1;
        SpillWriter::create(path)
    }

    fn writers(&mut self, count: usize) -> RookResult<Vec<SpillWriter>> {
        (0..count).map(|_| self.writer()).collect()
    }

    /// Send a row to the partition of its key. A NULL-keyed row goes to
    /// `null_side`'s file, or nowhere when that side is not preserved.
    fn route(
        &mut self,
        writers: &mut [SpillWriter],
        key: Option<&[DataValue]>,
        tuple: &Tuple,
        depth: u32,
        null_side: Option<usize>,
    ) -> RookResult<()> {
        match (key, null_side) {
            (Some(key), _) => writers[partition_of(key, depth, writers.len())].write(tuple),
            (None, Some(side)) => {
                if self.null_rows[side].is_none() {
                    self.null_rows[side] = Some(self.writer()?);
                }
                self.null_rows[side]
                    .as_mut()
                    .expect("writer just created")
                    .write(tuple)
            }
            (None, None) => Ok(()),
        }
    }

    /// Queue the partitions written at `depth`. `splittable` is false when
    /// the build rows all landed in one of them.
    fn push_partitions(
        &mut self,
        build: Vec<SpillWriter>,
        probe: Vec<SpillWriter>,
        depth: u32,
    ) -> RookResult<()> {
        let splittable = build.iter().filter(|w| w.file.rows > 0).count() > 1;
        for (build, probe) in build.into_iter().zip(probe) {
            self.pending.push(SpillPartition {
                build: build.finish()?,
                probe: probe.finish()?,
                depth,
                splittable,
            });
        }
        Ok(())
    }

    fn remove_dir(&self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Drop for HashJoinOperator {
    fn drop(&mut self) {
        if let Some(spill) = self.spill.take() {
            spill.remove_dir();
        }
    }
}

// Spill encoding. Each value carries a tag for its own runtime type, so a
// row reads back exactly as it was written whatever the column was declared
// as: `[u32 arity][per value: u8 tag + payload]`.
const TAG_NULL: u8 = 0;
const TAG_SMALLINT: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_BIGINT: u8 = 3;
const TAG_REAL: u8 = 4;
const TAG_DOUBLE: u8 = 5;
const TAG_NUMERIC: u8 = 6;
const TAG_BOOL: u8 = 7;
const TAG_CHAR: u8 = 8;
const TAG_VARCHAR: u8 = 9;
const TAG_DATE: u8 = 10;
const TAG_TIME: u8 = 11;
const TAG_BIT: u8 = 12;
const TAG_TIMESTAMP: u8 = 13;

fn encode_tuple(tuple: &Tuple, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(tuple.values.len() as u32).to_le_bytes());
    for value in &tuple.values {
        match value {
            None => buf.push(TAG_NULL),
            Some(dv) => encode_value(dv, buf),
        }
    }
}

fn encode_value(dv: &DataValue, buf: &mut Vec<u8>) {
    use chrono::{Datelike, Timelike};
    fn text(tag: u8, s: &str, buf: &mut Vec<u8>) {
        buf.push(tag);
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
    fn date(d: &chrono::NaiveDate, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&d.num_days_from_ce().to_le_bytes());
    }
    fn time(t: &chrono::NaiveTime, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&t.num_seconds_from_midnight().to_le_bytes());
        buf.extend_from_slice(&t.nanosecond().to_le_bytes());
    }
    match dv {
        DataValue::SmallInt(v) => {
            buf.push(TAG_SMALLINT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DataValue::Int(v) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DataValue::BigInt(v) => {
            buf.push(TAG_BIGINT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DataValue::Real(v) => {
            buf.push(TAG_REAL);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }
        DataValue::DoublePrecision(v) => {
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }
        DataValue::Numeric(v) => {
            buf.push(TAG_NUMERIC);
            buf.extend_from_slice(&v.unscaled.to_le_bytes());
            buf.push(v.scale);
        }
        DataValue::Bool(v) => {
            buf.push(TAG_BOOL);
            buf.push(u8::from(*v));
        }
        DataValue::Char(s) => text(TAG_CHAR, s, buf),
        DataValue::Varchar(s) => text(TAG_VARCHAR, s, buf),
        DataValue::Bit(s) => text(TAG_BIT, s, buf),
        DataValue::Date(d) => {
            buf.push(TAG_DATE);
            date(d, buf);
        }
        DataValue::Time(t) => {
            buf.push(TAG_TIME);
            time(t, buf);
        }
        DataValue::Timestamp(ts) => {
            buf.push(TAG_TIMESTAMP);
            date(&ts.date(), buf);
            time(&ts.time(), buf);
        }
    }
}

fn decode_tuple(r: &mut impl std::io::Read) -> RookResult<Tuple> {
    let arity = u32::from_le_bytes(read_array(r)?) as usize;
    let mut values = Vec::with_capacity(arity);
    for _ in 0..arity {
        let [tag] = read_array(r)?;
        values.push(if tag == TAG_NULL {
            None
        } else {
            Some(decode_value(tag, r)?)
        });
    }
    Ok(Tuple::new(values))
}

fn read_array<const N: usize>(r: &mut impl std::io::Read) -> RookResult<[u8; N]> {
    let mut bytes = [0u8; N];
    r.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn decode_value(tag: u8, r: &mut impl std::io::Read) -> RookResult<DataValue> {
    use crate::types::value::{NumericValue, OrderedF32, OrderedF64};
    fn invalid(what: &str) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("corrupt spill file: {}", what),
        )
    }
    fn text(r: &mut impl std::io::Read) -> RookResult<String> {
        let len = u32::from_le_bytes(read_array(r)?) as usize;
        let mut bytes = vec![0u8; len];
        r.read_exact(&mut bytes)?;
        String::from_utf8(bytes)
            .map_err(|_| invalid("text is not UTF-8"))
            .map_err(RookError::from)
    }
    fn date(r: &mut impl std::io::Read) -> RookResult<chrono::NaiveDate> {
        chrono::NaiveDate::from_num_days_from_ce_opt(i32::from_le_bytes(read_array(r)?))
            .ok_or_else(|| invalid("date out of range"))
            .map_err(RookError::from)
    }
    fn time(r: &mut impl std::io::Read) -> RookResult<chrono::NaiveTime> {
        let secs = u32::from_le_bytes(read_array(r)?);
        let nanos = u32::from_le_bytes(read_array(r)?);
        chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or_else(|| invalid("time out of range"))
            .map_err(RookError::from)
    }
    Ok(match tag {
        TAG_SMALLINT => DataValue::SmallInt(i16::from_le_bytes(read_array(r)?)),
        TAG_INT => DataValue::Int(i32::from_le_bytes(read_array(r)?)),
        TAG_BIGINT => DataValue::BigInt(i64::from_le_bytes(read_array(r)?)),
        TAG_REAL => DataValue::Real(OrderedF32(f32::from_le_bytes(read_array(r)?))),
        TAG_DOUBLE => DataValue::DoublePrecision(OrderedF64(f64::from_le_bytes(read_array(r)?))),
        TAG_NUMERIC => {
            let unscaled = i128::from_le_bytes(read_array(r)?);
            let [scale] = read_array(r)?;
            DataValue::Numeric(NumericValue { unscaled, scale })
        }
        TAG_BOOL => {
            let [v] = read_array(r)?;
            DataValue::Bool(v != 0)
        }
        TAG_CHAR => DataValue::Char(text(r)?),
        TAG_VARCHAR => DataValue::Varchar(text(r)?),
        TAG_BIT => DataValue::Bit(text(r)?),
        TAG_DATE => DataValue::Date(date(r)?),
        TAG_TIME => DataValue::Time(time(r)?),
        TAG_TIMESTAMP => {
            let date = date(r)?;
            DataValue::Timestamp(date.and_time(time(r)?))
        }
        _ => return Err(RookError::from(invalid("unknown value tag"))),
    })
}

/// Partition index for a hash key (must agree between build and probe).
/// `depth` seeds the hash, so each split of a partition spreads its keys
/// differently from the split that produced it.
fn partition_of(keys: &[DataValue], depth: u32, fanout: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    depth.hash(&mut h);
    keys.hash(&mut h);
    h.finish() as usize % fanout
}

impl HashJoinOperator {
    /// Create a new INNER hash join operator.
    ///
    /// `build` and `probe` are the child operators.
    /// `build_keys` are the expressions evaluated on the build side to form the hash key.
    /// `probe_keys` are the expressions evaluated on the probe side to form the hash key.
    /// They must have the same length and corresponding entries must be type-compatible.
    /// `remaining_predicate` is any non-equality condition that must also be satisfied.
    pub fn new(
        build: Box<dyn PhysicalOperator>,
        probe: Box<dyn PhysicalOperator>,
        build_keys: Vec<Expr>,
        probe_keys: Vec<Expr>,
        remaining_predicate: Option<Predicate>,
    ) -> Self {
        Self::with_join_type(
            build,
            probe,
            build_keys,
            probe_keys,
            remaining_predicate,
            JoinType::Inner,
        )
    }

    /// Create a hash join of the given type. The output is always the left
    /// columns followed by the right columns.
    ///
    /// A LEFT join builds on the right input and streams the left one, so
    /// left rows keep their order and an unmatched one is NULL-extended in
    /// place. Every other type builds on the left input. `Cross` has no
    /// keys to hash and is treated as `Inner`.
    pub fn with_join_type(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        remaining_predicate: Option<Predicate>,
        join_type: JoinType,
    ) -> Self {
        let mut output_schema = left.schema().to_vec();
        output_schema.extend(right.schema().iter().cloned());

        let build_is_left = join_type != JoinType::Left;
        let (build, probe, build_keys, probe_keys) = if build_is_left {
            (left, right, left_keys, right_keys)
        } else {
            (right, left, right_keys, left_keys)
        };
        let (preserve_build, preserve_probe) = match join_type {
            JoinType::Inner | JoinType::Cross => (false, false),
            // The preserved side of a LEFT or RIGHT join is the probe side.
            JoinType::Left | JoinType::Right => (false, true),
            JoinType::Full => (true, true),
        };

        Self {
            build_schema: build.schema().to_vec(),
            probe_schema: probe.schema().to_vec(),
            build,
            probe,
            build_keys,
            probe_keys,
            remaining_predicate,
            output_schema,
            build_is_left,
            preserve_build,
            preserve_probe,
            join_type,
            budget_override: None,
            budget: 0,
            phase: HashJoinPhase::Build,
            build_rows: Vec::new(),
            hash_table: HashMap::new(),
            build_matched: Vec::new(),
            build_bytes: 0,
            peak_build_bytes: 0,
            probe_buf: Vec::new(),
            cur_probe: None,
            cur_matches: Vec::new(),
            match_pos: 0,
            cur_matched: false,
            key_buf: Vec::new(),
            unmatched_pos: 0,
            spill: None,
            did_spill: false,
            cur_part: None,
            build_reader: None,
            pending_build: None,
            probe_reader: None,
            block_first: true,
            block_last: true,
            probe_matched: Vec::new(),
            probe_ord: 0,
            null_reader: None,
        }
    }

    /// Hold at most `bytes` of build rows in memory for this join, instead
    /// of the process-wide [`SPILL_BUDGET_BYTES`].
    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.budget_override = Some(bytes);
        self
    }

    /// Whether this join spilled to disk (diagnostics/tests).
    pub fn did_spill(&self) -> bool {
        self.did_spill
    }

    /// Largest estimated size the in-memory build rows reached (diagnostics/tests).
    pub fn peak_build_bytes(&self) -> usize {
        self.peak_build_bytes
    }

    /// Compute the hash key values from a tuple using a set of key expressions.
    /// Returns `None` if any key is NULL (meaning this tuple cannot match in a
    /// hash join, since SQL NULL != NULL).
    fn make_hash_key(
        keys: &[Expr],
        tuple: &Tuple,
        schema: &[ColumnInfo],
    ) -> RookResult<Option<Vec<DataValue>>> {
        let mut parts = Vec::with_capacity(keys.len());
        Ok(Self::fill_hash_key(keys, tuple, schema, &mut parts)?.then_some(parts))
    }

    /// [`make_hash_key`](Self::make_hash_key) into a reused buffer. Returns
    /// false if any key is NULL.
    fn fill_hash_key(
        keys: &[Expr],
        tuple: &Tuple,
        schema: &[ColumnInfo],
        parts: &mut Vec<DataValue>,
    ) -> RookResult<bool> {
        parts.clear();
        for expr in keys {
            match expr.evaluate(tuple, schema)? {
                Some(dv) => parts.push(Self::canonicalize_hash_key(dv)),
                None => return Ok(false), // NULL key → can never match (NULL != NULL)
            }
        }
        Ok(true)
    }

    fn canonicalize_hash_key(dv: DataValue) -> DataValue {
        super::utils::canonical_key(dv)
    }
}

impl HashJoinOperator {
    /// Read the build side into the hash table. If it outgrows the budget,
    /// partition it to disk instead, and the probe side after it.
    fn run_build(&mut self) -> RookResult<()> {
        self.budget = self.budget_override.unwrap_or_else(spill_budget);
        let null_side = self.preserve_build.then_some(BUILD_SIDE);
        let mut writers: Option<Vec<SpillWriter>> = None;
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.build.next_batch(&mut child_batch)? > 0 {
            for tuple in child_batch.drain(..) {
                let key = Self::make_hash_key(&self.build_keys, &tuple, &self.build_schema)?;
                if let (Some(writers), Some(spill)) = (&mut writers, &mut self.spill) {
                    // Already spilling: every tuple goes straight to its partition.
                    spill.route(writers, key.as_deref(), &tuple, 0, null_side)?;
                    continue;
                }
                // A NULL-keyed tuple can never match (NULL != NULL); it is
                // only kept when unmatched build rows are part of the result.
                if key.is_none() && !self.preserve_build {
                    continue;
                }
                self.push_build_row(key, tuple);
                if self.build_bytes > self.budget {
                    writers = Some(self.begin_spill()?);
                }
            }
        }

        match writers {
            None => self.start_probe(),
            Some(build_writers) => {
                let probe_writers = self.partition_probe()?;
                if let Some(spill) = &mut self.spill {
                    spill.push_partitions(build_writers, probe_writers, 0)?;
                }
                self.phase = HashJoinPhase::NextPartition;
            }
        }
        Ok(())
    }

    fn push_build_row(&mut self, key: Option<Vec<DataValue>>, tuple: Tuple) {
        if let Some(key) = key {
            self.hash_table
                .entry(key)
                .or_default()
                .push(self.build_rows.len() as u32);
        }
        self.build_bytes += estimate_tuple_bytes(&tuple);
        self.peak_build_bytes = self.peak_build_bytes.max(self.build_bytes);
        self.build_rows.push(tuple);
    }

    /// The in-memory build side exceeded the budget: hash-partition what
    /// was read so far to temp files and return the writers, so the rest of
    /// the build stream can follow.
    fn begin_spill(&mut self) -> RookResult<Vec<SpillWriter>> {
        let mut spill = SpillState::create()?;
        let mut writers = spill.writers(SPILL_PARTITIONS)?;
        let null_side = self.preserve_build.then_some(BUILD_SIDE);
        log::debug!(
            "[HashJoin] build side exceeded {} bytes — spilling to {} partitions in {:?}",
            self.budget,
            SPILL_PARTITIONS,
            spill.dir
        );
        self.did_spill = true;
        SPILL_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.hash_table.clear();
        self.build_bytes = 0;
        let rows = std::mem::take(&mut self.build_rows);
        // Own the directory before writing, so an error still cleans it up.
        let spill = self.spill.insert(spill);
        for tuple in &rows {
            let key = Self::make_hash_key(&self.build_keys, tuple, &self.build_schema)?;
            spill.route(&mut writers, key.as_deref(), tuple, 0, null_side)?;
        }
        Ok(writers)
    }

    /// Spill mode: partition the whole probe stream to disk.
    fn partition_probe(&mut self) -> RookResult<Vec<SpillWriter>> {
        let spill = self.spill.as_mut().expect("spill state");
        let mut writers = spill.writers(SPILL_PARTITIONS)?;
        let null_side = self.preserve_probe.then_some(PROBE_SIDE);
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.probe.next_batch(&mut child_batch)? > 0 {
            for tuple in child_batch.drain(..) {
                let keyed = Self::fill_hash_key(
                    &self.probe_keys,
                    &tuple,
                    &self.probe_schema,
                    &mut self.key_buf,
                )?;
                let key = keyed.then_some(self.key_buf.as_slice());
                spill.route(&mut writers, key, &tuple, 0, null_side)?;
            }
        }
        Ok(writers)
    }

    /// `build_rows` and the hash table are ready: start joining probe rows.
    fn start_probe(&mut self) {
        self.build_matched.clear();
        if self.preserve_build {
            self.build_matched.resize(self.build_rows.len(), false);
        }
        self.cur_probe = None;
        self.cur_matches.clear();
        self.match_pos = 0;
        self.probe_ord = 0;
        self.unmatched_pos = 0;
        // With nothing to match and no probe row to preserve, the probe
        // side need not be read at all.
        self.phase = if self.build_rows.is_empty() && !self.preserve_probe {
            HashJoinPhase::BuildUnmatched
        } else {
            HashJoinPhase::Probe
        };
    }

    /// Next probe row: from the current partition's file, else from the child.
    fn next_probe(&mut self) -> RookResult<Option<Tuple>> {
        if let Some(reader) = &mut self.probe_reader {
            return reader.next();
        }
        if self.probe_buf.is_empty() {
            if self.probe.next_batch(&mut self.probe_buf)? == 0 {
                return Ok(None);
            }
            self.probe_buf.reverse();
        }
        Ok(self.probe_buf.pop())
    }

    /// Joined row in output order (left columns, then right columns).
    fn join_row(&self, build: &Tuple, probe: &Tuple) -> Tuple {
        let (left, right) = if self.build_is_left {
            (build, probe)
        } else {
            (probe, build)
        };
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend(left.values.iter().cloned());
        values.extend(right.values.iter().cloned());
        Tuple::new(values)
    }

    /// One side's values with NULLs in place of the other side's columns.
    fn null_extend(&self, values: Vec<Option<DataValue>>, is_build: bool) -> Tuple {
        let other_arity = if is_build {
            self.probe_schema.len()
        } else {
            self.build_schema.len()
        };
        let nulls = std::iter::repeat_n(None, other_arity);
        let mut out = Vec::with_capacity(values.len() + other_arity);
        if is_build == self.build_is_left {
            out.extend(values);
            out.extend(nulls);
        } else {
            out.extend(nulls);
            out.extend(values);
        }
        Tuple::new(out)
    }

    /// Join probe rows against `build_rows` until one output row is ready.
    /// Returns `None` after moving on to the next phase.
    fn probe_step(&mut self) -> RookResult<Option<Tuple>> {
        loop {
            if let Some(probe) = &self.cur_probe {
                while self.match_pos < self.cur_matches.len() {
                    let idx = self.cur_matches[self.match_pos] as usize;
                    self.match_pos += 1;
                    let joined = self.join_row(&self.build_rows[idx], probe);
                    if let Some(remaining) = &self.remaining_predicate
                        && evaluate_predicate(remaining, &joined, &self.output_schema)?
                            != Some(true)
                    {
                        continue;
                    }
                    self.cur_matched = true;
                    if self.preserve_build {
                        self.build_matched[idx] = true;
                    }
                    return Ok(Some(joined));
                }
            }

            // The current probe row has met all its candidates.
            if let Some(probe) = self.cur_probe.take() {
                let ord = self.probe_ord;
                self.probe_ord += 1;
                if self.preserve_probe {
                    let unmatched = if self.block_first && self.block_last {
                        !self.cur_matched
                    } else {
                        // Joined in several blocks: a probe row is unmatched
                        // only if no block matched it, known at the last one.
                        if self.block_first {
                            self.probe_matched.push(self.cur_matched);
                        } else if self.cur_matched {
                            self.probe_matched[ord] = true;
                        }
                        self.block_last && !self.probe_matched[ord]
                    };
                    if unmatched {
                        return Ok(Some(self.null_extend(probe.values, false)));
                    }
                }
            }

            let Some(probe) = self.next_probe()? else {
                self.unmatched_pos = 0;
                self.phase = HashJoinPhase::BuildUnmatched;
                return Ok(None);
            };
            self.cur_matches.clear();
            self.match_pos = 0;
            self.cur_matched = false;
            // A NULL probe key matches nothing (NULL != NULL).
            if Self::fill_hash_key(
                &self.probe_keys,
                &probe,
                &self.probe_schema,
                &mut self.key_buf,
            )? && let Some(matches) = self.hash_table.get(self.key_buf.as_slice())
            {
                self.cur_matches.extend_from_slice(matches);
            }
            self.cur_probe = Some(probe);
        }
    }

    /// Every probe row has met `build_rows`: load the next block of the
    /// current partition, or move on to the next partition.
    fn finish_block(&mut self) -> RookResult<()> {
        if self.spill.is_none() {
            self.finish();
            return Ok(());
        }
        if !self.block_last {
            self.block_first = false;
            return self.load_block();
        }
        self.build_reader = None;
        self.probe_reader = None;
        self.probe_matched.clear();
        if let Some(part) = self.cur_part.take() {
            part.remove();
        }
        self.phase = HashJoinPhase::NextPartition;
        Ok(())
    }

    /// Start joining the next spilled partition, splitting it first when
    /// its build side is over budget.
    fn next_partition(&mut self) -> RookResult<()> {
        let Some(part) = self.spill.as_mut().and_then(|s| s.pending.pop()) else {
            self.phase = HashJoinPhase::NullKeys;
            return Ok(());
        };
        // A side with no rows only matters if the other side is preserved.
        if (part.build.rows == 0 && (part.probe.rows == 0 || !self.preserve_probe))
            || (part.probe.rows == 0 && !self.preserve_build)
        {
            part.remove();
            return Ok(());
        }
        if part.build.bytes > self.budget
            && part.probe.rows > 0
            && part.splittable
            && part.depth < MAX_SPILL_DEPTH
        {
            return self.repartition(part);
        }
        // Fits in memory, or cannot usefully be split (one hot key): the
        // build side is loaded a budget-sized block at a time.
        self.build_reader = Some(SpillReader::open(&part.build)?);
        self.pending_build = None;
        self.cur_part = Some(part);
        self.block_first = true;
        self.probe_matched.clear();
        self.load_block()
    }

    /// Split an over-budget partition with the next hash seed.
    fn repartition(&mut self, part: SpillPartition) -> RookResult<()> {
        let depth = part.depth + 1;
        log::debug!(
            "[HashJoin] partition of {} build bytes is over budget — splitting (depth {})",
            part.build.bytes,
            depth
        );
        let spill = self.spill.as_mut().expect("spill state");
        let mut sides = Vec::with_capacity(2);
        for (file, keys, schema) in [
            (&part.build, &self.build_keys, &self.build_schema),
            (&part.probe, &self.probe_keys, &self.probe_schema),
        ] {
            let mut writers = spill.writers(REPARTITION_FANOUT)?;
            let mut reader = SpillReader::open(file)?;
            while let Some(tuple) = reader.next()? {
                // NULL-keyed rows were set aside before the first partitioning.
                if Self::fill_hash_key(keys, &tuple, schema, &mut self.key_buf)? {
                    let p = partition_of(&self.key_buf, depth, REPARTITION_FANOUT);
                    writers[p].write(&tuple)?;
                }
            }
            sides.push(writers);
        }
        part.remove();
        let probe_writers = sides.pop().expect("probe side");
        let build_writers = sides.pop().expect("build side");
        spill.push_partitions(build_writers, probe_writers, depth)
    }

    /// Load the next block of the current partition's build side — as many
    /// rows as the budget allows, at least one — and rewind its probe side.
    fn load_block(&mut self) -> RookResult<()> {
        self.hash_table.clear();
        self.build_rows.clear();
        self.build_bytes = 0;

        let mut reader = self.build_reader.take().expect("build reader");
        loop {
            let tuple = match self.pending_build.take() {
                Some(tuple) => tuple,
                None => match reader.next()? {
                    Some(tuple) => tuple,
                    None => break,
                },
            };
            if !self.build_rows.is_empty()
                && self.build_bytes + estimate_tuple_bytes(&tuple) > self.budget
            {
                self.pending_build = Some(tuple);
                break;
            }
            let key = Self::make_hash_key(&self.build_keys, &tuple, &self.build_schema)?;
            self.push_build_row(key, tuple);
        }
        self.block_last = self.pending_build.is_none();
        self.build_reader = Some(reader);

        let part = self.cur_part.as_ref().expect("current partition");
        self.probe_reader = Some(SpillReader::open(&part.probe)?);
        self.start_probe();
        Ok(())
    }

    /// Emit the NULL-keyed rows set aside while spilling. Returns `None`
    /// once they are all out and the join is finished.
    fn null_key_step(&mut self) -> RookResult<Option<Tuple>> {
        loop {
            if let Some((reader, side)) = &mut self.null_reader {
                let is_build = *side == BUILD_SIDE;
                if let Some(tuple) = reader.next()? {
                    return Ok(Some(self.null_extend(tuple.values, is_build)));
                }
                self.null_reader = None;
            }
            let next = self.spill.as_mut().and_then(|spill| {
                [BUILD_SIDE, PROBE_SIDE]
                    .into_iter()
                    .find_map(|side| Some((spill.null_rows[side].take()?, side)))
            });
            let Some((writer, side)) = next else {
                self.finish();
                return Ok(None);
            };
            self.null_reader = Some((SpillReader::open(&writer.finish()?)?, side));
        }
    }

    /// All output produced: release memory and temp files.
    fn finish(&mut self) {
        self.clear_run_state();
        self.phase = HashJoinPhase::Done;
    }

    fn clear_run_state(&mut self) {
        self.build_rows = Vec::new();
        self.hash_table = HashMap::new();
        self.build_matched = Vec::new();
        self.build_bytes = 0;
        self.probe_buf.clear();
        self.cur_probe = None;
        self.cur_matches.clear();
        self.match_pos = 0;
        self.cur_matched = false;
        self.unmatched_pos = 0;
        self.cur_part = None;
        self.build_reader = None;
        self.pending_build = None;
        self.probe_reader = None;
        self.block_first = true;
        self.block_last = true;
        self.probe_matched = Vec::new();
        self.probe_ord = 0;
        self.null_reader = None;
        if let Some(spill) = self.spill.take() {
            spill.remove_dir();
        }
    }
}

impl PhysicalOperator for HashJoinOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        loop {
            match self.phase {
                HashJoinPhase::Build => self.run_build()?,
                HashJoinPhase::Probe => {
                    if let Some(joined) = self.probe_step()? {
                        return Ok(Some(joined));
                    }
                }
                HashJoinPhase::BuildUnmatched => {
                    if self.preserve_build {
                        while self.unmatched_pos < self.build_rows.len() {
                            let idx = self.unmatched_pos;
                            self.unmatched_pos += 1;
                            if !self.build_matched[idx] {
                                // Each build row is emitted here at most once.
                                let values = std::mem::take(&mut self.build_rows[idx].values);
                                return Ok(Some(self.null_extend(values, true)));
                            }
                        }
                    }
                    self.finish_block()?;
                }
                HashJoinPhase::NextPartition => self.next_partition()?,
                HashJoinPhase::NullKeys => {
                    if let Some(row) = self.null_key_step()? {
                        return Ok(Some(row));
                    }
                }
                HashJoinPhase::Done => return Ok(None),
            }
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.build.reset()?;
        self.probe.reset()?;
        self.clear_run_state();
        self.phase = HashJoinPhase::Build;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        let (build, probe) = (
            self.build.estimate_cardinality(),
            self.probe.estimate_cardinality(),
        );
        (build.saturating_mul(probe) / 5).max(1)
    }

    fn name(&self) -> &'static str {
        "HashJoin"
    }

    fn explain(&self) -> PlanNode {
        let budget = self.budget_override.unwrap_or_else(spill_budget);
        let mut node = PlanNode::new("HashJoin")
            .with("type", self.join_type.label())
            .with("equi_keys", self.build_keys.len())
            .with("residual", yes_no(self.remaining_predicate.is_some()))
            .with(
                "build_side",
                if self.build_is_left { "left" } else { "right" },
            )
            .with("memory_budget", format!("{} bytes", budget));
        if self.phase != HashJoinPhase::Build {
            node = node.with(
                "build",
                if self.did_spill {
                    "spilled to disk"
                } else {
                    "in memory"
                },
            );
        }
        // The first input is the build side, the second the probe side.
        node.child(self.build.explain()).child(self.probe.explain())
    }
}

// ── IndexNestedLoopJoin Operator ─────────────────────────────────────────────

/// An index-nested-loop join operator for equality joins where the inner side
/// has a B+ Tree index on the join key.
///
/// For each tuple from the outer child, probes the inner index using the outer
/// tuple's join key, fetches matching inner tuples directly from the heap,
/// evaluates any residual predicate, and yields the joined tuple.
///
/// Supports INNER and LEFT joins.
pub struct IndexNestedLoopJoinOperator {
    outer: Box<dyn PhysicalOperator>,
    index: CachedIndex,
    heap_manager: HeapHandle,
    outer_key_idx: usize,
    inner_schema: Vec<ColumnInfo>,
    inner_schema_types: Vec<DataType>,
    output_schema: Vec<ColumnInfo>,
    residual_predicate: Option<Predicate>,
    join_type: JoinType,
    /// The inner table and its index, for `EXPLAIN`.
    described: Option<(String, String)>,

    // Runtime state
    current_outer: Option<Tuple>,
    inner_matches: Vec<(u32, u32)>,
    match_pos: usize,
    had_match: bool,
    exhausted: bool,
}

impl IndexNestedLoopJoinOperator {
    pub fn new(
        outer: Box<dyn PhysicalOperator>,
        index: CachedIndex,
        heap_manager: HeapHandle,
        outer_key_idx: usize,
        inner_schema: Vec<ColumnInfo>,
        residual_predicate: Option<Predicate>,
        join_type: JoinType,
    ) -> Self {
        let mut output_schema = outer.schema().to_vec();
        output_schema.extend(inner_schema.iter().cloned());
        let inner_schema_types: Vec<DataType> =
            inner_schema.iter().map(|c| c.data_type.clone()).collect();

        Self {
            outer,
            index,
            heap_manager,
            outer_key_idx,
            inner_schema,
            inner_schema_types,
            output_schema,
            residual_predicate,
            join_type,
            described: None,
            current_outer: None,
            inner_matches: Vec::new(),
            match_pos: 0,
            had_match: false,
            exhausted: false,
        }
    }

    /// Name the inner table and the index probed, for `EXPLAIN`.
    pub fn described(mut self, table: &str, index: &str) -> Self {
        self.described = Some((table.to_string(), index.to_string()));
        self
    }
}

impl PhysicalOperator for IndexNestedLoopJoinOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            // If we have a current outer tuple, try to yield matching inner tuples
            if let Some(ref outer_tuple) = self.current_outer {
                while self.match_pos < self.inner_matches.len() {
                    let (page_id, slot_id) = self.inner_matches[self.match_pos];
                    self.match_pos += 1;

                    let raw_bytes = self.heap_manager.get_tuple(page_id, slot_id).map_err(|e| {
                        RookError::Internal(format!(
                            "Failed to fetch heap tuple (page={}, slot={}): {}",
                            page_id, slot_id, e
                        ))
                    })?;
                    let inner_values = crate::types::deserialize_nullable_row(
                        &self.inner_schema_types,
                        &raw_bytes,
                    )
                    .map_err(|e| {
                        RookError::Internal(format!("Failed to deserialise tuple: {}", e))
                    })?;

                    let mut joined_values = outer_tuple.values.clone();
                    joined_values.extend(inner_values);
                    let candidate = Tuple::new(joined_values);

                    if let Some(ref pred) = self.residual_predicate {
                        if let Some(true) =
                            evaluate_predicate(pred, &candidate, &self.output_schema)?
                        {
                            self.had_match = true;
                            return Ok(Some(candidate));
                        } else {
                            continue;
                        }
                    } else {
                        self.had_match = true;
                        return Ok(Some(candidate));
                    }
                }

                // If LEFT join and no inner row matched, emit outer row with NULLs
                if !self.had_match && self.join_type == JoinType::Left {
                    let mut null_values = outer_tuple.values.clone();
                    for _ in 0..self.inner_schema.len() {
                        null_values.push(None);
                    }
                    self.current_outer = None;
                    return Ok(Some(Tuple::new(null_values)));
                }

                self.current_outer = None;
            }

            // Pull next outer tuple
            match self.outer.next()? {
                Some(outer_tuple) => {
                    let key_opt = outer_tuple
                        .values
                        .get(self.outer_key_idx)
                        .and_then(|v| v.clone());
                    match key_opt {
                        Some(key_val) => {
                            let tids = self
                                .index
                                .read(|btree| btree.search_range(&key_val, &key_val))
                                .map_err(|e| {
                                    RookError::Internal(format!(
                                        "Index lookup failed in INLJ: {}",
                                        e
                                    ))
                                })?;
                            self.current_outer = Some(outer_tuple);
                            self.inner_matches = tids;
                            self.match_pos = 0;
                            self.had_match = false;
                        }
                        None => {
                            // NULL outer key never matches any inner key
                            if self.join_type == JoinType::Left {
                                let mut null_values = outer_tuple.values.clone();
                                for _ in 0..self.inner_schema.len() {
                                    null_values.push(None);
                                }
                                return Ok(Some(Tuple::new(null_values)));
                            }
                            continue;
                        }
                    }
                }
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        while batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
            match self.next()? {
                Some(tuple) => batch.push(tuple),
                None => break,
            }
        }
        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.outer.reset()?;
        self.current_outer = None;
        self.inner_matches.clear();
        self.match_pos = 0;
        self.had_match = false;
        self.exhausted = false;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        // About one match per outer row.
        self.outer.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "IndexNestedLoopJoin"
    }

    fn explain(&self) -> PlanNode {
        let mut node = PlanNode::new("IndexNestedLoopJoin").with("type", self.join_type.label());
        if let Some((table, index)) = &self.described {
            node = node.with("inner_table", table).with("index", index);
        }
        if let Some(outer_key) = self.output_schema.get(self.outer_key_idx) {
            node = node.with("outer_key", &outer_key.name);
        }
        node.with("residual", yes_no(self.residual_predicate.is_some()))
            .child(self.outer.explain())
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        self.outer.ordering()
    }
}

// ── Spill tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod spill_tests {
    use super::*;
    use crate::types::DataType;
    use std::cell::Cell;
    use std::rc::Rc;

    type Row = Vec<Option<DataValue>>;

    struct MockChild {
        rows: Vec<Row>,
        pos: usize,
        info: Vec<ColumnInfo>,
        /// Rows handed out so far, shared with the test.
        pulled: Rc<Cell<usize>>,
    }

    impl MockChild {
        /// Rows over columns `names`, all declared INT.
        fn new(names: &[&str], rows: Vec<Row>) -> Self {
            Self {
                rows,
                pos: 0,
                info: names
                    .iter()
                    .map(|name| ColumnInfo {
                        merged: false,
                        name: name.to_string(),
                        data_type: DataType::Int,
                        table: None,
                    })
                    .collect(),
                pulled: Rc::new(Cell::new(0)),
            }
        }

        fn int_rows(count: usize, name: &str) -> Self {
            Self::with_nulls(count, &[], name)
        }

        fn with_nulls(count: usize, null_at: &[usize], name: &str) -> Self {
            let rows = (0..count)
                .map(|i| vec![(!null_at.contains(&i)).then_some(DataValue::Int(i as i32))])
                .collect();
            Self::new(&[name], rows)
        }
    }

    impl PhysicalOperator for MockChild {
        fn next(&mut self) -> RookResult<Option<Tuple>> {
            if self.pos < self.rows.len() {
                let row = self.rows[self.pos].clone();
                self.pos += 1;
                self.pulled.set(self.pulled.get() + 1);
                Ok(Some(Tuple::new(row)))
            } else {
                Ok(None)
            }
        }
        fn schema(&self) -> &[ColumnInfo] {
            &self.info
        }
        fn reset(&mut self) -> RookResult<()> {
            self.pos = 0;
            Ok(())
        }
        fn name(&self) -> &'static str {
            "MockJoinChild"
        }
        fn estimate_cardinality(&self) -> usize {
            self.rows.len()
        }
    }

    fn key_expr(column: &str) -> Vec<Expr> {
        vec![Expr::Column {
            table: None,
            column: column.to_string(),
        }]
    }

    /// `left JOIN right ON lk = rk` over two-column inputs `(lk, lv)` and
    /// `(rk, rv)`, with an optional build budget in bytes.
    fn join(
        left: Vec<Row>,
        right: Vec<Row>,
        join_type: JoinType,
        budget: Option<usize>,
    ) -> HashJoinOperator {
        let op = HashJoinOperator::with_join_type(
            Box::new(MockChild::new(&["lk", "lv"], left)),
            Box::new(MockChild::new(&["rk", "rv"], right)),
            key_expr("lk"),
            key_expr("rk"),
            None,
            join_type,
        );
        match budget {
            Some(bytes) => op.with_memory_budget(bytes),
            None => op,
        }
    }

    /// Every output row, in an order that does not depend on the join path.
    fn drain(op: &mut dyn PhysicalOperator) -> Vec<Row> {
        let mut out = Vec::new();
        while let Some(t) = op.next().unwrap() {
            out.push(t.values);
        }
        out.sort_by_cached_key(|row| format!("{:?}", row));
        out
    }

    fn collect(op: &mut dyn PhysicalOperator) -> Vec<(i32, i32)> {
        drain(op)
            .into_iter()
            .map(|row| match (&row[0], &row[1]) {
                (Some(DataValue::Int(k)), Some(DataValue::Int(p))) => (*k, *p),
                other => panic!("unexpected row {:?}", other),
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The join computed pair by pair, as the reference result.
    fn expected(left: &[Row], right: &[Row], join_type: JoinType) -> Vec<Row> {
        let mut out = Vec::new();
        let mut right_matched = vec![false; right.len()];
        for l in left {
            let mut matched = false;
            for (r_idx, r) in right.iter().enumerate() {
                if l[0].is_some() && l[0] == r[0] {
                    matched = true;
                    right_matched[r_idx] = true;
                    out.push([l.clone(), r.clone()].concat());
                }
            }
            if !matched && matches!(join_type, JoinType::Left | JoinType::Full) {
                out.push([l.clone(), vec![None; 2]].concat());
            }
        }
        if matches!(join_type, JoinType::Right | JoinType::Full) {
            for (r, _) in right.iter().zip(&right_matched).filter(|(_, m)| !**m) {
                out.push([vec![None; 2], r.clone()].concat());
            }
        }
        out.sort_by_cached_key(|row| format!("{:?}", row));
        out
    }

    /// `count` rows `(key, tag)`: keys cycle through `distinct` values from
    /// `first_key`, every `null_every`-th key is NULL.
    fn keyed_rows(
        count: usize,
        first_key: i32,
        distinct: i32,
        null_every: usize,
        tag: i32,
    ) -> Vec<Row> {
        (0..count)
            .map(|i| {
                let key = (i % null_every != null_every - 1)
                    .then_some(DataValue::Int(first_key + i as i32 % distinct));
                vec![key, Some(DataValue::Int(tag + i as i32))]
            })
            .collect()
    }

    #[test]
    fn spilled_join_matches_in_memory_results() {
        // Build: unique keys 0..60. Probe: keys 0..119 — probes 60..119
        // have no build partner, probes 0..59 match exactly once. Spilling
        // scatters both sides across 64 partitions, so this also proves
        // partition assignment agrees between build and probe.
        let build = MockChild::int_rows(60, "k");
        let probe = MockChild::int_rows(120, "k");
        let mut hj = HashJoinOperator::new(
            Box::new(build),
            Box::new(probe),
            key_expr("k"),
            key_expr("k"),
            None,
        )
        .with_memory_budget(1024); // a handful of build tuples

        let out = collect(&mut hj);
        assert!(hj.did_spill(), "join should have taken the spill path");

        let expected: Vec<(i32, i32)> = (0..60i32).map(|k| (k, k)).collect();
        assert_eq!(out.len(), expected.len(), "match count mismatch");
        assert_eq!(out, expected, "spilled join produced wrong pairs");
    }

    #[test]
    fn spill_path_matches_in_memory_path_exactly() {
        // Same join run twice: once in memory, once forced to spill.
        let run = |budget: usize| {
            let build = MockChild::int_rows(30, "k");
            let probe = MockChild::int_rows(45, "k");
            let mut hj = HashJoinOperator::new(
                Box::new(build),
                Box::new(probe),
                key_expr("k"),
                key_expr("k"),
                None,
            )
            .with_memory_budget(budget);
            (collect(&mut hj), hj.did_spill())
        };
        let (in_memory, spilled_a) = run(1 << 20);
        assert!(!spilled_a);
        let (spilled, spilled_b) = run(512);
        assert!(spilled_b);
        assert_eq!(
            in_memory, spilled,
            "spill path diverged from in-memory path"
        );
    }

    #[test]
    fn spill_skips_null_keys_on_both_sides() {
        // Build: 10 rows, NULL at 3 and 7. Probe: 20 rows, NULL at 5 and 15.
        let build = MockChild::with_nulls(10, &[3, 7], "k");
        let probe = MockChild::with_nulls(20, &[5, 15], "k");
        let mut hj = HashJoinOperator::new(
            Box::new(build),
            Box::new(probe),
            key_expr("k"),
            key_expr("k"),
            None,
        )
        .with_memory_budget(512);
        let out = collect(&mut hj);
        assert!(hj.did_spill());
        // Probe k matches build k only when both exist and NEITHER key is
        // NULL: build covers 0..10 (NULL at 3 and 7); probe covers 0..20
        // (NULL at 5 and 15). Probe 5 has a build partner but its own key
        // is NULL → NULL != NULL → no match.
        let expected: Vec<(i32, i32)> = (0..10i32)
            .filter(|k| k != &3 && k != &7 && k != &5)
            .map(|k| (k, k))
            .collect();
        assert_eq!(out, expected);
    }

    #[test]
    fn every_join_type_agrees_across_memory_budgets() {
        // Duplicate keys, NULL keys and unmatched rows on both sides.
        let left = keyed_rows(230, 0, 40, 7, 1000);
        let right = keyed_rows(190, 25, 45, 5, 5000);
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
        ] {
            let want = expected(&left, &right, join_type);
            // None: in memory. 4 KiB: spilled, partitions fit. 600 bytes:
            // partitions split again. 1 byte: one build row per block.
            for budget in [None, Some(4096), Some(600), Some(1)] {
                let mut hj = join(left.clone(), right.clone(), join_type, budget);
                let got = drain(&mut hj);
                assert_eq!(hj.did_spill(), budget.is_some());
                assert_eq!(
                    got.len(),
                    want.len(),
                    "{:?} join, budget {:?}: row count",
                    join_type,
                    budget
                );
                assert_eq!(got, want, "{:?} join, budget {:?}", join_type, budget);

                // A second run after reset() gives the same rows.
                hj.reset().unwrap();
                assert_eq!(drain(&mut hj), want, "{:?} join after reset", join_type);
            }
        }
    }

    #[test]
    fn residual_predicate_decides_outer_matches_when_spilled() {
        // ON lk = rk AND lv < rv: a key match that fails the residual is
        // no match, so both rows come out NULL-extended.
        let left: Vec<Row> = (0..80)
            .map(|i| vec![Some(DataValue::Int(i % 20)), Some(DataValue::Int(i))])
            .collect();
        let right: Vec<Row> = (0..60)
            .map(|i| vec![Some(DataValue::Int(i % 30)), Some(DataValue::Int(40))])
            .collect();
        let residual = Predicate::Compare(
            Expr::Column {
                table: None,
                column: "lv".to_string(),
            },
            super::super::super::expr::ComparisonOp::LessThan,
            Expr::Column {
                table: None,
                column: "rv".to_string(),
            },
        );
        let run = |budget: usize| {
            let mut hj = HashJoinOperator::with_join_type(
                Box::new(MockChild::new(&["lk", "lv"], left.clone())),
                Box::new(MockChild::new(&["rk", "rv"], right.clone())),
                key_expr("lk"),
                key_expr("rk"),
                Some(residual.clone()),
                JoinType::Full,
            )
            .with_memory_budget(budget);
            (drain(&mut hj), hj.did_spill())
        };
        let (in_memory, spilled) = run(1 << 20);
        assert!(!spilled);
        // 40 left rows (lv 0..39) pass against 2 right rows each; the other
        // 40 left rows and the 20 right rows with keys 20..29 are unmatched.
        assert_eq!(in_memory.len(), 40 * 2 + 40 + 20);
        for budget in [2048, 1] {
            let (rows, spilled) = run(budget);
            assert!(spilled);
            assert_eq!(rows, in_memory, "budget {}", budget);
        }
    }

    #[test]
    fn probe_side_is_streamed_not_buffered() {
        let build = MockChild::int_rows(10, "k");
        let probe = MockChild::int_rows(20_000, "k");
        let pulled = probe.pulled.clone();
        let mut hj = HashJoinOperator::new(
            Box::new(build),
            Box::new(probe),
            key_expr("k"),
            key_expr("k"),
            None,
        );
        assert!(hj.next().unwrap().is_some());
        assert!(
            pulled.get() <= super::super::trait_::DEFAULT_BATCH_SIZE,
            "first row needed {} probe rows",
            pulled.get()
        );
        let mut rows = 1;
        while hj.next().unwrap().is_some() {
            rows += 1;
        }
        assert_eq!(rows, 10);
        assert_eq!(pulled.get(), 20_000);
    }

    #[test]
    fn budget_counts_bytes_not_tuples() {
        // The same number of rows spills only when the rows are wide.
        let run = |width: usize| {
            let rows: Vec<Row> = (0..200)
                .map(|i| {
                    vec![
                        Some(DataValue::Int(i)),
                        Some(DataValue::Varchar("x".repeat(width))),
                    ]
                })
                .collect();
            let mut hj = join(rows.clone(), rows, JoinType::Inner, Some(256 * 1024));
            assert_eq!(drain(&mut hj).len(), 200);
            hj.did_spill()
        };
        assert!(!run(10));
        assert!(run(4000));
    }

    #[test]
    fn one_hot_key_stays_within_the_budget() {
        // Every build row shares one key, so no partitioning can split
        // them; they must still not be loaded all at once.
        let hot = |count: usize, tag: i32| -> Vec<Row> {
            (0..count)
                .map(|i| {
                    vec![
                        Some(DataValue::Int(7)),
                        Some(DataValue::Int(tag + i as i32)),
                    ]
                })
                .collect()
        };
        let (left, right) = (hot(3000, 0), hot(40, 100_000));
        let row_bytes = estimate_tuple_bytes(&Tuple::new(left[0].clone()));
        let budget = 16 * 1024;
        assert!(3000 * row_bytes > 20 * budget);

        for join_type in [JoinType::Inner, JoinType::Full] {
            let mut hj = join(left.clone(), right.clone(), join_type, Some(budget));
            let mut rows = 0;
            while let Some(t) = hj.next().unwrap() {
                assert!(t.values.iter().all(|v| v.is_some()));
                rows += 1;
            }
            assert_eq!(rows, 3000 * 40);
            assert!(hj.did_spill());
            assert!(
                hj.peak_build_bytes() <= budget + row_bytes,
                "{:?}: held {} bytes against a budget of {}",
                join_type,
                hj.peak_build_bytes(),
                budget
            );
        }
    }

    #[test]
    fn spill_keeps_values_whose_type_differs_from_the_declared_one() {
        use crate::types::value::NumericValue;
        // Every column is declared INT; the values are not.
        let payload = |i: i32| match i % 5 {
            0 => DataValue::BigInt(i64::MAX - i as i64),
            1 => DataValue::Varchar(format!("text-{}", i)),
            2 => DataValue::Numeric(NumericValue {
                unscaled: 123_456_789_012_345_678_901_234_567 + i as i128,
                scale: 9,
            }),
            3 => DataValue::Char("ab  ".to_string()),
            _ => DataValue::SmallInt(i as i16),
        };
        let left: Vec<Row> = (0..100)
            .map(|i| vec![Some(DataValue::BigInt(i as i64)), Some(payload(i))])
            .collect();
        let right: Vec<Row> = (0..100)
            .map(|i| vec![Some(DataValue::Int(i)), Some(payload(i + 1))])
            .collect();

        let mut in_memory = join(left.clone(), right.clone(), JoinType::Full, None);
        let want = drain(&mut in_memory);
        assert!(!in_memory.did_spill());
        assert_eq!(want.len(), 100);
        assert_eq!(want, expected_by_canonical_key(&left, &right));

        let mut spilled = join(left, right, JoinType::Full, Some(1024));
        let got = drain(&mut spilled);
        assert!(spilled.did_spill());
        assert_eq!(got, want);
    }

    /// BIGINT and INT keys join on their numeric value.
    fn expected_by_canonical_key(left: &[Row], right: &[Row]) -> Vec<Row> {
        let mut out: Vec<Row> = left
            .iter()
            .zip(right)
            .map(|(l, r)| [l.clone(), r.clone()].concat())
            .collect();
        out.sort_by_cached_key(|row| format!("{:?}", row));
        out
    }

    #[test]
    fn spill_encoding_round_trips_every_value_type() {
        use crate::types::value::{NumericValue, OrderedF32, OrderedF64};
        use chrono::{NaiveDate, NaiveTime};
        let date = NaiveDate::from_ymd_opt(1969, 7, 20).unwrap();
        let time = NaiveTime::from_hms_nano_opt(20, 17, 40, 123_456_789).unwrap();
        let tuple = Tuple::new(vec![
            Some(DataValue::SmallInt(-7)),
            Some(DataValue::Int(i32::MIN)),
            Some(DataValue::BigInt(i64::MAX)),
            Some(DataValue::Real(OrderedF32(-0.5))),
            Some(DataValue::DoublePrecision(OrderedF64(f64::MAX))),
            Some(DataValue::Numeric(NumericValue {
                unscaled: i128::MIN + 1,
                scale: 30,
            })),
            Some(DataValue::Bool(true)),
            Some(DataValue::Char("pad  ".to_string())),
            Some(DataValue::Varchar("naïve ✓".to_string())),
            Some(DataValue::Varchar(String::new())),
            Some(DataValue::Date(date)),
            Some(DataValue::Time(time)),
            Some(DataValue::Bit("10110".to_string())),
            Some(DataValue::Timestamp(date.and_time(time))),
            None,
        ]);
        let mut buf = Vec::new();
        encode_tuple(&tuple, &mut buf);
        encode_tuple(&Tuple::empty(), &mut buf);
        let mut input = buf.as_slice();
        assert_eq!(decode_tuple(&mut input).unwrap(), tuple);
        assert_eq!(decode_tuple(&mut input).unwrap(), Tuple::empty());
        assert!(input.is_empty());
    }

    #[test]
    fn spill_files_are_removed() {
        let rows = keyed_rows(300, 0, 50, 9, 0);
        // Exhausted: the directory goes as soon as the last row is out.
        let mut hj = join(rows.clone(), rows.clone(), JoinType::Full, Some(2048));
        assert!(hj.next().unwrap().is_some());
        let dir = hj.spill.as_ref().expect("spilling").dir.clone();
        assert!(dir.exists());
        while hj.next().unwrap().is_some() {}
        assert!(!dir.exists());

        // Dropped mid-stream.
        let mut hj = join(rows.clone(), rows, JoinType::Full, Some(2048));
        assert!(hj.next().unwrap().is_some());
        let dir = hj.spill.as_ref().expect("spilling").dir.clone();
        assert!(dir.exists());
        drop(hj);
        assert!(!dir.exists());
    }
}
