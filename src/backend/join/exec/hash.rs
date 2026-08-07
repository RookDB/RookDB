//! Hash join: in-memory, hybrid and Grace, as one operator.
//!
//! Which of the three it behaves as is a runtime consequence of how much of
//! the build side fits in the memory budget, not a separate algorithm the
//! planner picks between:
//!
//! * everything fits - one resident hash table, one pass over each input;
//! * it does not - the rows already built are repartitioned in place, the
//!   first partition stays resident, and the rest spill. Probe rows belonging
//!   to the resident partition are joined as they arrive rather than written
//!   out and read back, which is the whole point of the hybrid form;
//! * a partition still does not fit - it is repartitioned again, up to a
//!   depth limit.
//!
//! Past that limit the partition is loaded anyway. A partition that will not
//! shrink is one where a single key dominates it, and no amount of further
//! hashing separates rows that share a key. That is recorded in
//! `ExecStats::oversized_partitions` so the adaptive operator can see it and
//! switch to a nested loop, which is the only strategy that handles it.
//!
//! The build side is the *right* input. Unmatched probe rows are therefore
//! unmatched left rows and stream out immediately; unmatched build rows are
//! known only once a partition's probe side is exhausted.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use crate::types::value::DataValue;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::error::JoinError;
use super::super::key::JoinKey;
use super::super::memory::{HASH_ENTRY_OVERHEAD, MemoryAccountant, row_footprint};
use super::super::row::{RowBuilder, RowCodec};
use super::super::schema::OutputSchema;
use super::super::source::RowSource;
use super::super::spill::{RowBuffer, RowBufferBuilder, RunHandle, RunWriter, SpillScope};
use super::{ExecStats, MatchEvaluator, RowStream, StatsHandle, new_stats};

/// Partitions created at each level of partitioning.
const FAN_OUT: usize = 16;

/// Beyond this, further partitioning cannot help: rows that will not separate
/// share a key, and hashing does not split a key.
const MAX_REPARTITION_DEPTH: u32 = 3;

/// Chooses a partition for a key at a given recursion depth.
///
/// The depth is mixed into the hash so repartitioning actually redistributes;
/// hashing the same way again would put every row of an oversized partition
/// straight back into one partition.
fn partition_of(key: &JoinKey, depth: u32, count: usize) -> usize {
    if count <= 1 {
        return 0;
    }
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET_BASIS ^ u64::from(depth).wrapping_mul(PRIME);
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    (hash % count as u64) as usize
}

/// Build-side rows, keyed for probing.
struct HashTable {
    buckets: HashMap<JoinKey, Vec<u32>>,
    rows: Vec<Vec<u8>>,
    matched: Vec<bool>,
    track_matches: bool,
    charged: u64,
}

impl HashTable {
    fn new(track_matches: bool) -> Self {
        Self {
            buckets: HashMap::new(),
            rows: Vec::new(),
            matched: Vec::new(),
            track_matches,
            charged: 0,
        }
    }

    /// Insert a row. Returns whether the budget still had room; the row goes
    /// in either way, so a caller mid-build can finish the row it holds before
    /// changing strategy.
    fn insert(&mut self, key: JoinKey, row: Vec<u8>, budget: &MemoryAccountant) -> bool {
        let footprint = row_footprint(row.len()) + key.byte_len() as u64 + HASH_ENTRY_OVERHEAD;
        let accepted = budget.charge(footprint).is_ok();
        if accepted {
            self.charged += footprint;
        }

        let index = self.rows.len() as u32;
        self.rows.push(row);
        if self.track_matches {
            self.matched.push(false);
        }
        self.buckets.entry(key).or_default().push(index);
        accepted
    }

    fn candidates(&self, key: &JoinKey) -> &[u32] {
        self.buckets.get(key).map_or(&[][..], |indices| indices)
    }

    fn release(&mut self, budget: &MemoryAccountant) {
        budget.release(self.charged);
        self.charged = 0;
    }
}

/// One (build, probe) pair still to be joined.
struct PartitionPair {
    build: RowBuffer,
    probe: RunHandle,
    depth: u32,
}

/// Writes probe rows into the run belonging to their partition.
struct ProbeRouter {
    writers: Vec<RunWriter>,
    /// The partition held in memory, whose rows are joined on arrival rather
    /// than written out. `None` once even that partition has spilled.
    resident: Option<usize>,
    depth: u32,
}

struct ProbeContext {
    table: HashTable,
    probe: Box<dyn Iterator<Item = Result<Vec<u8>, JoinError>>>,
    router: Option<ProbeRouter>,
}

enum Stage {
    NotStarted,
    Probing(ProbeContext),
    NextPartition,
    /// Build rows whose key was NULL, owed to RIGHT and FULL joins.
    NullBuild,
    Finished,
}

pub struct HashJoin {
    join_type: JoinType,
    evaluator: MatchEvaluator,
    builder: RowBuilder,
    schema: Arc<OutputSchema>,
    probe_codec: RowCodec,
    build_codec: RowCodec,
    build_fingerprint: u64,
    probe_fingerprint: u64,
    budget: Rc<MemoryAccountant>,
    scope: Arc<SpillScope>,

    probe_input: Option<Box<dyn RowStream>>,
    build_input: Option<Box<dyn RowSource>>,

    stage: Stage,
    /// Build partitions waiting for their probe runs to be closed.
    staged_builds: Vec<RowBuffer>,
    queue: VecDeque<PartitionPair>,
    null_build: Option<RowBuffer>,
    pending: VecDeque<Vec<u8>>,
    stats: StatsHandle,

    /// Reused across rows so probing does not allocate per row.
    probe_values: Vec<Option<DataValue>>,
    build_values: Vec<Option<DataValue>>,
}

impl HashJoin {
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        probe: Box<dyn RowStream>,
        build: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        budget: Rc<MemoryAccountant>,
        scope: Arc<SpillScope>,
    ) -> Self {
        let probe_codec = RowCodec::new(probe.schema().types.clone());
        let build_codec = RowCodec::new(build.schema().types.clone());
        let probe_fingerprint = probe.schema().fingerprint;
        let build_fingerprint = build.schema().fingerprint;

        Self {
            join_type: spec.join_type(),
            evaluator,
            builder: RowBuilder::new(&schema),
            schema,
            probe_codec,
            build_codec,
            build_fingerprint,
            probe_fingerprint,
            budget,
            scope,
            probe_input: Some(probe),
            build_input: Some(build),
            stage: Stage::NotStarted,
            staged_builds: Vec::new(),
            queue: VecDeque::new(),
            null_build: None,
            pending: VecDeque::new(),
            stats: new_stats(),
            probe_values: Vec::new(),
            build_values: Vec::new(),
        }
    }

    fn tracks_build_matches(&self) -> bool {
        self.join_type.keeps_unmatched_right()
    }

    fn take_probe_stream(&mut self) -> Box<dyn Iterator<Item = Result<Vec<u8>, JoinError>>> {
        match self.probe_input.take() {
            Some(stream) => Box::new(stream),
            None => Box::new(std::iter::empty()),
        }
    }

    // ── Build ────────────────────────────────────────────────────────────────

    fn build(&mut self) -> Result<Stage, JoinError> {
        let Some(source) = self.build_input.take() else {
            return Ok(Stage::Finished);
        };

        let mut table = HashTable::new(self.tracks_build_matches());
        let mut nulls = RowBufferBuilder::new(&self.scope, "build-null", self.build_fingerprint);
        let mut writers: Option<Vec<RunWriter>> = None;
        let mut resident_spilled = false;

        let mut input = source.open()?;
        while let Some(row) = input.next() {
            let row = row?;
            self.stats.borrow_mut().inner_rows += 1;
            self.build_codec.decode_into(&row, &mut self.build_values)?;

            let Some(key) = self.evaluator.keys().right_key(&self.build_values)? else {
                // No key, so no partition and no match - but RIGHT and FULL
                // still owe this row.
                nulls.push(&row, &self.budget)?;
                continue;
            };

            match &mut writers {
                // Already partitioning: route this row.
                Some(writers) => {
                    let partition = partition_of(&key, 0, writers.len());
                    if partition == 0 && !resident_spilled {
                        if !table.insert(key, row, &self.budget) {
                            // Even the resident partition no longer fits.
                            // Flush it and become a pure Grace join.
                            flush_table_into(&mut table, &mut writers[0], &self.budget)?;
                            resident_spilled = true;
                        }
                    } else {
                        writers[partition].write_row(&row)?;
                    }
                }
                None => {
                    if !table.insert(key, row, &self.budget) {
                        writers = Some(self.split_build(&mut table)?);
                    }
                }
            }
        }

        self.null_build = Some(nulls.finish(&self.budget)?);

        let Some(writers) = writers else {
            // Everything fit.
            let probe = self.take_probe_stream();
            return Ok(Stage::Probing(ProbeContext {
                table,
                probe,
                router: None,
            }));
        };

        if resident_spilled {
            table.release(&self.budget);
        }

        // Close the build partitions and open a probe run per partition.
        let mut build_partitions = Vec::with_capacity(writers.len());
        let mut probe_writers = Vec::with_capacity(writers.len());
        for (index, writer) in writers.into_iter().enumerate() {
            let handle = writer.finish()?;
            self.stats.borrow_mut().spilled_bytes += handle.bytes();
            build_partitions.push(RowBuffer::Disk(handle));
            probe_writers.push(RunWriter::create(
                &self.scope,
                &format!("probe-p{index:02}"),
                self.probe_fingerprint,
            )?);
        }
        self.stats.borrow_mut().partitions += build_partitions.len() as u64;

        self.staged_builds = build_partitions;
        let probe = self.take_probe_stream();

        Ok(Stage::Probing(ProbeContext {
            table,
            probe,
            router: Some(ProbeRouter {
                writers: probe_writers,
                resident: if resident_spilled { None } else { Some(0) },
                depth: 0,
            }),
        }))
    }

    /// Spread an over-full table across partitions, keeping partition 0
    /// resident. This is the single-pass transition to a hybrid join: the
    /// build input is never re-read.
    fn split_build(&mut self, table: &mut HashTable) -> Result<Vec<RunWriter>, JoinError> {
        let mut writers = Vec::with_capacity(FAN_OUT);
        for index in 0..FAN_OUT {
            writers.push(RunWriter::create(
                &self.scope,
                &format!("build-p{index:02}"),
                self.build_fingerprint,
            )?);
        }

        let buckets = std::mem::take(&mut table.buckets);
        let rows = std::mem::take(&mut table.rows);
        table.matched.clear();
        table.release(&self.budget);

        for (key, indices) in buckets {
            let partition = partition_of(&key, 0, FAN_OUT);
            for index in indices {
                let row = &rows[index as usize];
                if partition == 0 {
                    table.insert(key.clone(), row.clone(), &self.budget);
                } else {
                    writers[partition].write_row(row)?;
                }
            }
        }

        Ok(writers)
    }

    // ── Probing ──────────────────────────────────────────────────────────────

    fn probe_row(&mut self, context: &mut ProbeContext, row: Vec<u8>) -> Result<(), JoinError> {
        self.stats.borrow_mut().outer_rows += 1;
        self.probe_codec.decode_into(&row, &mut self.probe_values)?;

        let Some(key) = self.evaluator.keys().left_key(&self.probe_values)? else {
            // A NULL key matches nothing, in any algorithm and at any depth.
            return self.emit_unmatched_probe();
        };

        if let Some(router) = &mut context.router {
            let partition = partition_of(&key, router.depth, router.writers.len());
            if router.resident != Some(partition) {
                return router.writers[partition].write_row(&row);
            }
        }

        self.join_probe_row(&mut context.table, &key)
    }

    fn join_probe_row(&mut self, table: &mut HashTable, key: &JoinKey) -> Result<(), JoinError> {
        let candidates = table.candidates(key).to_vec();
        let mut matched = false;

        for index in candidates {
            let build_row = table.rows[index as usize].clone();
            self.build_codec
                .decode_into(&build_row, &mut self.build_values)?;
            self.stats.borrow_mut().candidate_pairs += 1;

            // Bucket membership proves the keys are equal; the residual is
            // whatever the condition asked for beyond that.
            if !self
                .evaluator
                .residual_matches(&self.probe_values, &self.build_values)?
            {
                continue;
            }

            matched = true;
            if table.track_matches {
                table.matched[index as usize] = true;
            }

            if self.join_type.emits_left_only() {
                // SEMI and ANTI care only whether a match exists. Stopping
                // here is not just an optimisation: emitting per match would
                // duplicate the left row.
                break;
            }

            let built = self
                .builder
                .build(Some(&self.probe_values), Some(&self.build_values))?;
            self.pending.push_back(built);
        }

        if matched {
            if self.join_type == JoinType::Semi {
                let built = self.builder.build(Some(&self.probe_values), None)?;
                self.pending.push_back(built);
            }
            Ok(())
        } else {
            self.emit_unmatched_probe()
        }
    }

    fn emit_unmatched_probe(&mut self) -> Result<(), JoinError> {
        let emit = match self.join_type {
            JoinType::Anti => true,
            JoinType::Semi => false,
            other => other.keeps_unmatched_left(),
        };
        if emit {
            let built = self.builder.build(Some(&self.probe_values), None)?;
            self.pending.push_back(built);
        }
        Ok(())
    }

    /// End of a probe stream: emit unmatched build rows, then hand any routed
    /// probe runs to the partition queue.
    fn finish_probe(&mut self, context: ProbeContext) -> Result<(), JoinError> {
        let ProbeContext {
            mut table, router, ..
        } = context;

        if self.tracks_build_matches() {
            for index in 0..table.rows.len() {
                if table.matched.get(index).copied().unwrap_or(false) {
                    continue;
                }
                let row = table.rows[index].clone();
                self.build_codec.decode_into(&row, &mut self.build_values)?;
                let built = self.builder.build(None, Some(&self.build_values))?;
                self.pending.push_back(built);
            }
        }
        table.release(&self.budget);

        let Some(router) = router else {
            return Ok(());
        };

        let builds = std::mem::take(&mut self.staged_builds);
        let resident = router.resident;
        for (index, writer) in router.writers.into_iter().enumerate() {
            let probe = writer.finish()?;
            self.stats.borrow_mut().spilled_bytes += probe.bytes();

            // The resident partition was joined as its rows arrived.
            if resident == Some(index) {
                continue;
            }
            let Some(build) = builds.get(index) else {
                continue;
            };
            self.queue.push_back(PartitionPair {
                build: build.clone(),
                probe,
                depth: router.depth,
            });
        }

        Ok(())
    }

    // ── Partitions ───────────────────────────────────────────────────────────

    fn next_partition(&mut self) -> Result<Stage, JoinError> {
        let Some(pair) = self.queue.pop_front() else {
            return Ok(Stage::NullBuild);
        };

        // An empty pair still matters: probe rows with no build partition are
        // unmatched, and build rows with no probe rows are unmatched too.
        let mut table = HashTable::new(self.tracks_build_matches());
        let mut overflowed = false;

        for row in pair.build.reader()? {
            let row = row?;
            self.build_codec.decode_into(&row, &mut self.build_values)?;
            let Some(key) = self.evaluator.keys().right_key(&self.build_values)? else {
                continue;
            };
            if !table.insert(key, row, &self.budget) {
                overflowed = true;
                break;
            }
        }

        if overflowed && pair.depth < MAX_REPARTITION_DEPTH {
            table.release(&self.budget);
            self.repartition(pair)?;
            return Ok(Stage::NextPartition);
        }

        if overflowed {
            // One key dominates this partition, so no further hashing helps.
            // Load it anyway and record that the budget was exceeded.
            self.stats.borrow_mut().oversized_partitions += 1;
            table.release(&self.budget);
            table = HashTable::new(self.tracks_build_matches());
            for row in pair.build.reader()? {
                let row = row?;
                self.build_codec.decode_into(&row, &mut self.build_values)?;
                let Some(key) = self.evaluator.keys().right_key(&self.build_values)? else {
                    continue;
                };
                table.insert(key, row, &self.budget);
            }
        }

        Ok(Stage::Probing(ProbeContext {
            table,
            probe: Box::new(pair.probe.reader()?),
            router: None,
        }))
    }

    /// Split one oversized pair into `FAN_OUT` smaller pairs at the next
    /// depth.
    fn repartition(&mut self, pair: PartitionPair) -> Result<(), JoinError> {
        let depth = pair.depth + 1;
        {
            let mut stats = self.stats.borrow_mut();
            stats.repartition_depth = stats.repartition_depth.max(depth);
        }

        let mut build_writers = Vec::with_capacity(FAN_OUT);
        let mut probe_writers = Vec::with_capacity(FAN_OUT);
        for index in 0..FAN_OUT {
            build_writers.push(RunWriter::create(
                &self.scope,
                &format!("build-d{depth}p{index:02}"),
                self.build_fingerprint,
            )?);
            probe_writers.push(RunWriter::create(
                &self.scope,
                &format!("probe-d{depth}p{index:02}"),
                self.probe_fingerprint,
            )?);
        }

        for row in pair.build.reader()? {
            let row = row?;
            self.build_codec.decode_into(&row, &mut self.build_values)?;
            let Some(key) = self.evaluator.keys().right_key(&self.build_values)? else {
                continue;
            };
            build_writers[partition_of(&key, depth, FAN_OUT)].write_row(&row)?;
        }

        for row in pair.probe.reader()? {
            let row = row?;
            self.probe_codec.decode_into(&row, &mut self.probe_values)?;
            let Some(key) = self.evaluator.keys().left_key(&self.probe_values)? else {
                continue;
            };
            probe_writers[partition_of(&key, depth, FAN_OUT)].write_row(&row)?;
        }

        for (build, probe) in build_writers.into_iter().zip(probe_writers) {
            let build = build.finish()?;
            let probe = probe.finish()?;
            {
                let mut stats = self.stats.borrow_mut();
                stats.spilled_bytes += build.bytes() + probe.bytes();
                stats.partitions += 1;
            }
            self.queue.push_back(PartitionPair {
                build: RowBuffer::Disk(build),
                probe,
                depth,
            });
        }

        Ok(())
    }

    /// Build rows whose key was NULL never entered a partition.
    fn drain_null_build(&mut self) -> Result<(), JoinError> {
        let Some(buffer) = self.null_build.take() else {
            return Ok(());
        };
        if !self.tracks_build_matches() {
            return Ok(());
        }

        let rows: Vec<Vec<u8>> = buffer.reader()?.collect::<Result<Vec<_>, JoinError>>()?;
        for row in rows {
            self.build_codec.decode_into(&row, &mut self.build_values)?;
            let built = self.builder.build(None, Some(&self.build_values))?;
            self.pending.push_back(built);
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<bool, JoinError> {
        // Taking the stage out lets the handlers borrow both it and `self`.
        let stage = std::mem::replace(&mut self.stage, Stage::Finished);

        match stage {
            Stage::NotStarted => {
                self.stage = self.build()?;
                Ok(true)
            }
            Stage::Probing(mut context) => match context.probe.next() {
                Some(row) => {
                    let row = row?;
                    self.probe_row(&mut context, row)?;
                    self.stage = Stage::Probing(context);
                    Ok(true)
                }
                None => {
                    self.finish_probe(context)?;
                    self.stage = Stage::NextPartition;
                    Ok(true)
                }
            },
            Stage::NextPartition => {
                self.stage = self.next_partition()?;
                Ok(true)
            }
            Stage::NullBuild => {
                self.drain_null_build()?;
                self.stage = Stage::Finished;
                Ok(true)
            }
            Stage::Finished => {
                self.stage = Stage::Finished;
                Ok(false)
            }
        }
    }
}

/// Move every row of a resident table into a partition writer.
fn flush_table_into(
    table: &mut HashTable,
    writer: &mut RunWriter,
    budget: &MemoryAccountant,
) -> Result<(), JoinError> {
    for row in &table.rows {
        writer.write_row(row)?;
    }
    table.buckets.clear();
    table.rows.clear();
    table.matched.clear();
    table.release(budget);
    Ok(())
}

impl Iterator for HashJoin {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                self.stats.borrow_mut().rows_out += 1;
                return Some(Ok(row));
            }
            match self.advance() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl RowStream for HashJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
