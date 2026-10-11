use std::collections::HashMap;
use std::path::PathBuf;

use super::super::expr::{Expr, Predicate, evaluate_predicate};
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::{RookError, RookResult};
use crate::backend::heap::heap_manager::HeapManager;
use crate::backend::index::btree::BTree;

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

// ── NestedLoopJoin Operator ───────────────────────────────────────────────────

/// A nested-loop join operator that works for any join predicate.
///
/// For each tuple from the left (outer) child, scans all tuples from the
/// right (inner) child and evaluates the join predicate on each pair.
/// Tuples that match the predicate are concatenated and yielded.
///
/// Supports: INNER, LEFT, RIGHT, FULL, CROSS join types.
/// For CROSS joins (no predicate), all pairs match.
pub struct NestedLoopJoinOperator {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,
    /// The join predicate (None for CROSS JOIN).
    predicate: Option<Predicate>,
    /// The join type.
    join_type: JoinType,
    /// Output schema (left columns + right columns).
    output_schema: Vec<ColumnInfo>,

    // ── Runtime state ──
    /// All tuples from the left child (materialised on first pass).
    left_tuples: Vec<Tuple>,
    /// All tuples from the right child (materialised on first pass).
    right_tuples: Vec<Tuple>,
    /// Which left tuples have found at least one match (for LEFT/FULL OUTER).
    left_matched: Vec<bool>,
    /// Which right tuples have found at least one match (for RIGHT/FULL OUTER).
    right_matched: Vec<bool>,
    /// Output buffer of result tuples.
    output_buffer: Vec<Tuple>,
    /// Current read position in output_buffer.
    output_pos: usize,
    /// Whether children have been consumed.
    consumed: bool,
    /// Equi-join keys `(left side, right side)`. When set, candidate pairs
    /// come from a hash table on the right input instead of the full cross
    /// product; the predicate is still evaluated on every candidate.
    equi_keys: Option<(Vec<Expr>, Vec<Expr>)>,
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

        Self {
            left,
            right,
            predicate,
            join_type,
            output_schema,
            left_tuples: Vec::new(),
            right_tuples: Vec::new(),
            left_matched: Vec::new(),
            right_matched: Vec::new(),
            output_buffer: Vec::new(),
            output_pos: 0,
            consumed: false,
            equi_keys: None,
        }
    }

    /// Outer join on equality keys: same output as the nested loop, but each
    /// left row only meets the right rows that share its key.
    pub fn with_equi_keys(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        predicate: Option<Predicate>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
    ) -> Self {
        let mut op = Self::new(left, right, predicate, join_type);
        op.equi_keys = Some((left_keys, right_keys));
        op
    }

    /// Materialise all tuples from both children, compute the join, and buffer results.
    fn materialise(&mut self) -> RookResult<()> {
        // Consume both children in batches
        self.left_tuples.clear();
        self.right_tuples.clear();
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.left.next_batch(&mut child_batch)? > 0 {
            self.left_tuples.append(&mut child_batch);
        }
        while self.right.next_batch(&mut child_batch)? > 0 {
            self.right_tuples.append(&mut child_batch);
        }

        let num_left = self.left_tuples.len();
        let num_right = self.right_tuples.len();

        // Initialise match tracking for outer joins
        self.left_matched = vec![false; num_left];
        self.right_matched = vec![false; num_right];

        let mut joined = Tuple::empty();

        // Compute the join
        if self.join_type != JoinType::Cross
            && let Some(pred) = &self.predicate
            && let Some((left_keys, right_keys)) = &self.equi_keys
        {
            // A NULL key matches nothing, so those rows stay out of the table
            // and surface only through the unmatched-row passes below.
            let left_schema = self.left.schema().to_vec();
            let right_schema = self.right.schema().to_vec();
            let mut table: HashMap<Vec<DataValue>, Vec<usize>> = HashMap::new();
            for (r_idx, tuple) in self.right_tuples.iter().enumerate() {
                if let Some(key) =
                    HashJoinOperator::make_hash_key(right_keys, tuple, &right_schema)?
                {
                    table.entry(key).or_default().push(r_idx);
                }
            }
            for l_idx in 0..num_left {
                let Some(key) = HashJoinOperator::make_hash_key(
                    left_keys,
                    &self.left_tuples[l_idx],
                    &left_schema,
                )?
                else {
                    continue;
                };
                let Some(candidates) = table.get(&key) else {
                    continue;
                };
                for &r_idx in candidates {
                    self.left_tuples[l_idx]
                        .concatenate_into(&self.right_tuples[r_idx], &mut joined);
                    if let Some(true) = evaluate_predicate(pred, &joined, &self.output_schema)? {
                        self.output_buffer.push(joined.clone());
                        self.left_matched[l_idx] = true;
                        self.right_matched[r_idx] = true;
                    }
                }
            }
        } else if self.join_type != JoinType::Cross
            && let Some(pred) = &self.predicate
        {
            for l_idx in 0..num_left {
                for r_idx in 0..num_right {
                    self.left_tuples[l_idx]
                        .concatenate_into(&self.right_tuples[r_idx], &mut joined);
                    if let Some(true) = evaluate_predicate(pred, &joined, &self.output_schema)? {
                        self.output_buffer.push(joined.clone());
                        self.left_matched[l_idx] = true;
                        self.right_matched[r_idx] = true;
                    }
                }
            }
        } else {
            // CROSS JOIN (no predicate) or predicate is None
            for l_idx in 0..num_left {
                for r_idx in 0..num_right {
                    self.left_tuples[l_idx]
                        .concatenate_into(&self.right_tuples[r_idx], &mut joined);
                    self.output_buffer.push(joined.clone());
                    self.left_matched[l_idx] = true;
                    self.right_matched[r_idx] = true;
                }
            }
        }

        // Handle outer join NULL-extensions
        let left_cols = self.left.schema().to_vec();
        let right_cols = self.right.schema().to_vec();

        match self.join_type {
            JoinType::Left | JoinType::Full => {
                // Emit NULL-extended rows for unmatched left tuples
                for (l_idx, &matched) in self.left_matched.iter().enumerate() {
                    if !matched {
                        let null_values: Vec<Option<DataValue>> =
                            right_cols.iter().map(|_| None).collect();
                        let null_right = Tuple::new(null_values);
                        self.left_tuples[l_idx].concatenate_into(&null_right, &mut joined);
                        self.output_buffer.push(joined.clone());
                    }
                }
            }
            _ => {}
        }

        match self.join_type {
            JoinType::Right | JoinType::Full => {
                // Emit NULL-extended rows for unmatched right tuples
                for (r_idx, &matched) in self.right_matched.iter().enumerate() {
                    if !matched {
                        let null_values: Vec<Option<DataValue>> =
                            left_cols.iter().map(|_| None).collect();
                        let null_left = Tuple::new(null_values);
                        null_left.concatenate_into(&self.right_tuples[r_idx], &mut joined);
                        self.output_buffer.push(joined.clone());
                    }
                }
            }
            _ => {}
        }

        self.consumed = true;
        Ok(())
    }
}

impl PhysicalOperator for NestedLoopJoinOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.consumed {
            self.materialise()?;
        }

        if self.output_pos < self.output_buffer.len() {
            let tuple = self.output_buffer[self.output_pos].clone();
            self.output_pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        if !self.consumed {
            self.materialise()?;
        }

        let available = self.output_buffer.len().saturating_sub(self.output_pos);
        if available == 0 {
            return Ok(0);
        }
        let take = available.min(super::trait_::DEFAULT_BATCH_SIZE);
        batch.reserve(take);
        for i in 0..take {
            batch.push(self.output_buffer[self.output_pos + i].clone());
        }
        self.output_pos += take;
        Ok(take)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.left.reset()?;
        self.right.reset()?;
        self.left_tuples.clear();
        self.right_tuples.clear();
        self.left_matched.clear();
        self.right_matched.clear();
        self.output_buffer.clear();
        self.output_pos = 0;
        self.consumed = false;
        Ok(())
    }

    fn name(&self) -> &'static str {
        if self.equi_keys.is_some() {
            "HashOuterJoin"
        } else {
            "NestedLoopJoin"
        }
    }
}

// ── HashJoin Operator ─────────────────────────────────────────────────────────

/// A hash join operator for equality joins.
///
/// Builds a hash table on the left (build) side, then probes with the
/// right (probe) side. Only supports INNER joins with equality conditions.
///
/// For non-equality conditions or outer joins, use NestedLoopJoinOperator.
pub struct HashJoinOperator {
    build: Box<dyn PhysicalOperator>,
    probe: Box<dyn PhysicalOperator>,
    /// Expressions to hash on the build side.
    build_keys: Vec<Expr>,
    /// Expressions to hash on the probe side.
    probe_keys: Vec<Expr>,
    /// Any remaining predicate (non-equality conditions that must also be satisfied).
    remaining_predicate: Option<Predicate>,
    /// Output schema.
    output_schema: Vec<ColumnInfo>,

    // ── Runtime state ──
    /// Hash table: key values → list of build tuples.
    hash_table: HashMap<Vec<DataValue>, Vec<Tuple>>,
    /// All probe tuples (for replay during reset).
    probe_tuples: Vec<Tuple>,
    /// Current position in probe_tuples.
    probe_pos: usize,
    /// Current batch of matching build tuples for the current probe tuple.
    current_matches: Vec<Tuple>,
    /// Current position within current_matches.
    match_pos: usize,
    /// Whether build phase is complete.
    build_done: bool,
    /// Whether all output has been consumed.
    exhausted: bool,

    // ── Spill state (grace hash join) ──
    /// Build tuples buffered before the memory budget is exceeded.
    build_buffer: Vec<Tuple>,
    /// Total build tuples currently held in `hash_table`.
    build_count: usize,
    /// `Some` once the build side spilled: both sides are hash-partitioned
    /// to temp files and joined partition-by-partition.
    spill: Option<SpillState>,
    /// True once this join has taken the spill path (stays set after the
    /// temp files are cleaned up — useful for tests and diagnostics).
    did_spill: bool,
}

/// Tunable in-memory budget for the hash-join build side (tuples).
/// 0 means "unset" → [`DEFAULT_SPILL_BUDGET`]. Tests shrink it to force the
/// spill path; operators can raise it via `ROOK_HASH_JOIN_BUDGET`.
pub static SPILL_BUDGET: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
const DEFAULT_SPILL_BUDGET: usize = 100_000;
/// Hash partitions created when spilling. Memory is then bounded by the
/// largest partition rather than the whole build side.
const SPILL_PARTITIONS: usize = 64;

fn spill_budget() -> usize {
    let configured = SPILL_BUDGET.load(std::sync::atomic::Ordering::Relaxed);
    if configured > 0 {
        return configured;
    }
    static ENV: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *ENV.get_or_init(|| {
        std::env::var("ROOK_HASH_JOIN_BUDGET")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 16)
            .unwrap_or(DEFAULT_SPILL_BUDGET)
    })
}

/// Temp files for one spilled hash join: both sides partitioned by
/// `hash(key) % SPILL_PARTITIONS`.
struct SpillState {
    dir: PathBuf,
    /// Partition currently being joined (0-based); `>= SPILL_PARTITIONS`
    /// means all partitions are done.
    part: usize,
    build_paths: Vec<PathBuf>,
    probe_paths: Vec<PathBuf>,
    /// Open writers for the build side (taken/flushed when the build
    /// stream ends) and probe side (flushed when the probe stream ends).
    build_writers: Vec<std::io::BufWriter<std::fs::File>>,
    probe_writers: Vec<std::io::BufWriter<std::fs::File>>,
}

impl SpillState {
    fn create(build_types: Vec<DataType>, probe_types: Vec<DataType>) -> std::io::Result<Self> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "rookdb_hj_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir)?;

        let mut build_paths = Vec::with_capacity(SPILL_PARTITIONS);
        let mut probe_paths = Vec::with_capacity(SPILL_PARTITIONS);
        let mut build_writers = Vec::with_capacity(SPILL_PARTITIONS);
        let mut probe_writers = Vec::with_capacity(SPILL_PARTITIONS);
        for p in 0..SPILL_PARTITIONS {
            let bp = dir.join(format!("build_{p}.bin"));
            let pp = dir.join(format!("probe_{p}.bin"));
            build_writers.push(std::io::BufWriter::new(std::fs::File::create(&bp)?));
            probe_writers.push(std::io::BufWriter::new(std::fs::File::create(&pp)?));
            build_paths.push(bp);
            probe_paths.push(pp);
        }
        let _ = (build_types, probe_types);
        Ok(Self {
            dir,
            part: 0,
            build_paths,
            probe_paths,
            build_writers,
            probe_writers,
        })
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

/// Serialise one tuple's values (schema is constant per side and stays in
/// memory, so only values hit the disk): `[u32 arity][per value: u8 tag +
/// u32 len + bytes]`.
fn write_tuple_values(
    w: &mut std::io::BufWriter<std::fs::File>,
    tuple: &Tuple,
    types: &[DataType],
) -> std::io::Result<()> {
    use std::io::Write;
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&(tuple.values.len() as u32).to_le_bytes());
    for (i, v) in tuple.values.iter().enumerate() {
        match v {
            None => buf.push(0),
            Some(dv) => {
                buf.push(1);
                let bytes = if let Some(ty) = types.get(i) {
                    dv.to_bytes_for_type(ty).unwrap_or_else(|_| dv.to_bytes())
                } else {
                    dv.to_bytes()
                };
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
            }
        }
    }
    w.write_all(&buf)
}

fn read_tuple_values(
    r: &mut std::io::BufReader<std::fs::File>,
    types: &[DataType],
    _info: &[ColumnInfo],
) -> std::io::Result<Option<Tuple>> {
    use std::io::Read;
    let mut arity_buf = [0u8; 4];
    if r.read_exact(&mut arity_buf).is_err() {
        return Ok(None); // clean EOF
    }
    let arity = u32::from_le_bytes(arity_buf) as usize;
    let mut values = Vec::with_capacity(arity);
    for i in 0..arity {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        if tag[0] == 0 {
            values.push(None);
            continue;
        }
        let mut len_buf = [0u8; 4];
        r.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        r.read_exact(&mut bytes)?;
        let ty = types.get(i).cloned().unwrap_or(DataType::Int);
        let dv = DataValue::from_bytes(&ty, &bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        values.push(Some(dv));
    }
    Ok(Some(Tuple::new(values)))
}

/// Partition index for a hash key (must agree between build and probe).
fn partition_of(keys: &[DataValue]) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    keys.hash(&mut h);
    h.finish() as usize % SPILL_PARTITIONS
}

impl HashJoinOperator {
    /// Create a new hash join operator.
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
        let mut output_schema = build.schema().to_vec();
        output_schema.extend(probe.schema().iter().cloned());

        Self {
            build,
            probe,
            build_keys,
            probe_keys,
            remaining_predicate,
            output_schema,
            hash_table: HashMap::new(),
            probe_tuples: Vec::new(),
            probe_pos: 0,
            current_matches: Vec::new(),
            match_pos: 0,
            build_done: false,
            exhausted: false,
            build_buffer: Vec::new(),
            build_count: 0,
            spill: None,
            did_spill: false,
        }
    }

    /// Whether this join spilled to disk (diagnostics/tests).
    pub fn did_spill(&self) -> bool {
        self.did_spill
    }

    /// Compute the hash key values from a tuple using a set of key expressions.
    /// Returns `None` if any key is NULL (meaning this tuple cannot match in an
    /// INNER hash join, since SQL NULL != NULL).
    fn make_hash_key(
        keys: &[Expr],
        tuple: &Tuple,
        schema: &[ColumnInfo],
    ) -> RookResult<Option<Vec<DataValue>>> {
        let mut parts = Vec::with_capacity(keys.len());
        for expr in keys {
            let val = expr.evaluate(tuple, schema)?;
            match val {
                Some(dv) => parts.push(Self::canonicalize_hash_key(dv)),
                None => return Ok(None), // NULL key → can never match (NULL != NULL)
            }
        }
        Ok(Some(parts))
    }

    fn canonicalize_hash_key(dv: DataValue) -> DataValue {
        super::utils::canonical_key(dv)
    }
}

impl HashJoinOperator {
    /// Build the hash table from the build side.
    /// Tuples with NULL join keys are skipped (they can never match in SQL).
    fn build_hash_table(&mut self) -> RookResult<()> {
        let budget = spill_budget();
        let build_schema = self.build.schema().to_vec();
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.build.next_batch(&mut child_batch)? > 0 {
            for tuple in child_batch.drain(..) {
                // NULL-keyed tuples are skipped — they can never match (NULL != NULL)
                let Some(key) = Self::make_hash_key(&self.build_keys, &tuple, &build_schema)?
                else {
                    continue;
                };

                if self.spill.is_some() {
                    // Already spilling: every tuple goes straight to its partition.
                    let p = partition_of(&key);
                    let types = self.build_type_cache();
                    if let Some(spill) = &mut self.spill {
                        write_tuple_values(&mut spill.build_writers[p], &tuple, &types)
                            .map_err(|e| format!("hash join spill write failed: {}", e))?;
                    }
                    continue;
                }

                self.hash_table.entry(key).or_default().push(tuple);
                self.build_count += 1;
                if self.build_count >= budget {
                    self.begin_spill()?;
                }
            }
        }
        self.build_done = true;
        if self.spill.is_some() {
            self.finish_build_spill()?;
        }
        Ok(())
    }

    /// Data types of the build side (cached once for the spill codec).
    fn build_type_cache(&mut self) -> Vec<DataType> {
        self.build
            .schema()
            .iter()
            .map(|c| c.data_type.clone())
            .collect()
    }

    fn probe_type_cache(&mut self) -> Vec<DataType> {
        self.probe
            .schema()
            .iter()
            .map(|c| c.data_type.clone())
            .collect()
    }

    /// The in-memory build side exceeded the budget: hash-partition
    /// everything to temp files, then keep streaming into the partitions.
    fn begin_spill(&mut self) -> RookResult<()> {
        let build_types = self.build_type_cache();
        let probe_types = self.probe_type_cache();
        let mut spill = SpillState::create(build_types, probe_types)
            .map_err(|e| format!("hash join spill setup failed: {}", e))?;

        // Drain the in-memory hash table into partitions.
        let build_types = self.build_type_cache();
        for (key, tuples) in self.hash_table.drain() {
            let p = partition_of(&key);
            for t in &tuples {
                write_tuple_values(&mut spill.build_writers[p], t, &build_types)
                    .map_err(|e| format!("hash join spill write failed: {}", e))?;
            }
        }
        self.hash_table.clear();
        self.build_count = 0;
        log::info!(
            "[HashJoin] build side exceeded {} tuples — spilling to {} partitions in {:?}",
            spill_budget(),
            SPILL_PARTITIONS,
            spill.dir
        );
        self.did_spill = true;
        self.spill = Some(spill);
        Ok(())
    }

    /// Build stream ended while spilling: close build writers.
    fn finish_build_spill(&mut self) -> RookResult<()> {
        if let Some(spill) = &mut self.spill {
            for w in spill.build_writers.drain(..) {
                w.into_inner()
                    .map_err(|e| format!("hash join spill flush failed: {}", e))?;
            }
        }
        Ok(())
    }

    /// Consume the probe side. In spill mode this partitions the probe
    /// stream to disk instead of buffering it in memory.
    fn load_probe(&mut self) -> RookResult<()> {
        let spilling = self.spill.is_some();
        let probe_schema = self.probe.schema().to_vec();
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.probe.next_batch(&mut child_batch)? > 0 {
            if !spilling {
                self.probe_tuples.append(&mut child_batch);
                continue;
            }
            for tuple in child_batch.drain(..) {
                // Spill mode: NULL-keyed probe tuples can never match — skip.
                let Some(key) = Self::make_hash_key(&self.probe_keys, &tuple, &probe_schema)?
                else {
                    continue;
                };
                let p = partition_of(&key);
                let types = self.probe_type_cache();
                if let Some(spill) = &mut self.spill {
                    write_tuple_values(&mut spill.probe_writers[p], &tuple, &types)
                        .map_err(|e| format!("hash join spill write failed: {}", e))?;
                }
            }
        }
        if spilling && let Some(spill) = &mut self.spill {
            for w in spill.probe_writers.drain(..) {
                w.into_inner()
                    .map_err(|e| format!("hash join spill flush failed: {}", e))?;
            }
        }
        Ok(())
    }

    /// Load partition `p`: build side into the hash table, probe side into
    /// `probe_tuples`, resetting the match cursors. Memory is then bounded
    /// by the largest partition rather than the whole join.
    fn load_spill_partition(&mut self, p: usize) -> RookResult<()> {
        self.hash_table.clear();
        self.probe_tuples.clear();
        self.probe_pos = 0;
        self.current_matches.clear();
        self.match_pos = 0;

        let (build_path, probe_path) = {
            let spill = self.spill.as_ref().expect("spill state");
            (spill.build_paths[p].clone(), spill.probe_paths[p].clone())
        };
        let build_types = self.build_type_cache();
        let probe_types = self.probe_type_cache();
        let build_info = self.build.schema().to_vec();
        let probe_info = self.probe.schema().to_vec();
        let build_schema = self.build.schema().to_vec();

        let mut br = std::io::BufReader::new(
            std::fs::File::open(&build_path)
                .map_err(|e| format!("hash join spill read failed: {}", e))?,
        );
        while let Some(tuple) = read_tuple_values(&mut br, &build_types, &build_info)
            .map_err(|e| format!("hash join spill read failed: {}", e))?
        {
            let key = Self::make_hash_key(&self.build_keys, &tuple, &build_schema)?;
            if let Some(key) = key {
                self.hash_table.entry(key).or_default().push(tuple);
            }
        }

        let mut pr = std::io::BufReader::new(
            std::fs::File::open(&probe_path)
                .map_err(|e| format!("hash join spill read failed: {}", e))?,
        );
        while let Some(tuple) = read_tuple_values(&mut pr, &probe_types, &probe_info)
            .map_err(|e| format!("hash join spill read failed: {}", e))?
        {
            self.probe_tuples.push(tuple);
        }
        Ok(())
    }
}

impl PhysicalOperator for HashJoinOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        // Build phase
        if !self.build_done {
            self.build_hash_table()?;
            self.load_probe()?;
        }

        loop {
            if self.exhausted {
                return Ok(None);
            }

            // Try to yield from current batch of matches
            if self.match_pos < self.current_matches.len() {
                let build_tuple = &self.current_matches[self.match_pos];
                self.match_pos += 1;
                let joined = build_tuple.concatenate(&self.probe_tuples[self.probe_pos - 1]);
                return Ok(Some(joined));
            }

            // Find the next probe tuple that has matches
            let probe_schema = self.probe.schema().to_vec();
            while self.probe_pos < self.probe_tuples.len() {
                let probe_tuple = &self.probe_tuples[self.probe_pos];
                self.probe_pos += 1;

                let key = match Self::make_hash_key(&self.probe_keys, probe_tuple, &probe_schema)? {
                    Some(k) => k,
                    None => continue, // NULL probe key → skip (NULL != NULL)
                };

                if let Some(build_matches) = self.hash_table.get(&key) {
                    // Check remaining predicate for each match
                    self.current_matches.clear();
                    for build_tuple in build_matches {
                        if let Some(ref remaining) = self.remaining_predicate {
                            let joined = build_tuple.concatenate(probe_tuple);
                            if let Some(true) =
                                evaluate_predicate(remaining, &joined, &self.output_schema)?
                            {
                                self.current_matches.push(build_tuple.clone())
                            }
                        } else {
                            self.current_matches.push(build_tuple.clone());
                        }
                    }

                    if !self.current_matches.is_empty() {
                        self.match_pos = 1;
                        let build_tuple = &self.current_matches[0];
                        let joined = build_tuple.concatenate(probe_tuple);
                        return Ok(Some(joined));
                    }
                }
                // No matches — skip this probe tuple (INNER JOIN semantics)
            }

            // In spill mode the current partition is exhausted: advance to
            // the next partition, or finish when all are consumed.
            if self.spill.is_some() {
                let next_part = self.spill.as_ref().unwrap().part;
                if next_part < SPILL_PARTITIONS {
                    self.load_spill_partition(next_part)?;
                    self.spill.as_mut().unwrap().part = next_part + 1;
                    continue;
                }
                // All partitions done — clean up temp files.
                if let Some(spill) = self.spill.take() {
                    spill.remove_dir();
                }
            }

            self.exhausted = true;
            return Ok(None);
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        // Build phase
        if !self.build_done {
            self.build_hash_table()?;
            self.load_probe()?;
        }

        let target_len = super::trait_::DEFAULT_BATCH_SIZE;

        loop {
            if self.exhausted {
                return Ok(batch.len());
            }

            // Try to yield from current batch of matches
            while self.match_pos < self.current_matches.len() {
                let build_tuple = &self.current_matches[self.match_pos];
                self.match_pos += 1;
                let mut joined = Tuple::empty();
                build_tuple.concatenate_into(&self.probe_tuples[self.probe_pos - 1], &mut joined);
                batch.push(joined);
                if batch.len() >= target_len {
                    return Ok(batch.len());
                }
            }

            // Find the next probe tuple that has matches
            let probe_schema = self.probe.schema().to_vec();
            let mut found_matches = false;
            let mut scratch_joined = Tuple::empty();

            while self.probe_pos < self.probe_tuples.len() {
                let probe_tuple = &self.probe_tuples[self.probe_pos];
                self.probe_pos += 1;

                let key = match Self::make_hash_key(&self.probe_keys, probe_tuple, &probe_schema)? {
                    Some(k) => k,
                    None => continue, // NULL probe key -> skip (NULL != NULL)
                };

                if let Some(build_matches) = self.hash_table.get(&key) {
                    self.current_matches.clear();
                    for build_tuple in build_matches {
                        if let Some(ref remaining) = self.remaining_predicate {
                            build_tuple.concatenate_into(probe_tuple, &mut scratch_joined);
                            if let Some(true) =
                                evaluate_predicate(remaining, &scratch_joined, &self.output_schema)?
                            {
                                self.current_matches.push(build_tuple.clone());
                            }
                        } else {
                            self.current_matches.push(build_tuple.clone());
                        }
                    }

                    if !self.current_matches.is_empty() {
                        self.match_pos = 0;
                        found_matches = true;
                        break;
                    }
                }
            }

            if found_matches {
                continue;
            }

            // In spill mode the current partition is exhausted: advance to
            // the next partition, or finish when all are consumed.
            if self.spill.is_some() {
                let next_part = self.spill.as_ref().unwrap().part;
                if next_part < SPILL_PARTITIONS {
                    self.load_spill_partition(next_part)?;
                    self.spill.as_mut().unwrap().part = next_part + 1;
                    continue;
                }
                // All partitions done - clean up temp files.
                if let Some(spill) = self.spill.take() {
                    spill.remove_dir();
                }
            }

            self.exhausted = true;
            return Ok(batch.len());
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.build.reset()?;
        self.probe.reset()?;
        self.hash_table.clear();
        self.probe_tuples.clear();
        self.probe_pos = 0;
        self.current_matches.clear();
        self.match_pos = 0;
        self.build_done = false;
        self.exhausted = false;
        if let Some(spill) = self.spill.take() {
            spill.remove_dir();
        }
        self.build_buffer.clear();
        self.build_count = 0;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "HashJoin"
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
    btree: BTree,
    heap_manager: HeapManager,
    outer_key_idx: usize,
    inner_schema: Vec<ColumnInfo>,
    inner_schema_types: Vec<DataType>,
    output_schema: Vec<ColumnInfo>,
    residual_predicate: Option<Predicate>,
    join_type: JoinType,

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
        btree: BTree,
        heap_manager: HeapManager,
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
            btree,
            heap_manager,
            outer_key_idx,
            inner_schema,
            inner_schema_types,
            output_schema,
            residual_predicate,
            join_type,
            current_outer: None,
            inner_matches: Vec::new(),
            match_pos: 0,
            had_match: false,
            exhausted: false,
        }
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
                            let tids =
                                self.btree.search_range(&key_val, &key_val).map_err(|e| {
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
        self.outer.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "IndexNestedLoopJoin"
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

    struct MockChild {
        rows: Vec<Vec<Option<DataValue>>>,
        pos: usize,
        info: Vec<ColumnInfo>,
    }

    impl MockChild {
        fn int_rows(count: usize, name: &str) -> Self {
            let rows = (0..count)
                .map(|i| vec![Some(DataValue::Int(i as i32))])
                .collect();
            Self {
                rows,
                pos: 0,
                info: vec![ColumnInfo {
                    name: name.to_string(),
                    data_type: DataType::Int,
                    table: None,
                }],
            }
        }

        fn with_nulls(count: usize, null_at: &[usize], name: &str) -> Self {
            let rows = (0..count)
                .map(|i| {
                    if null_at.contains(&i) {
                        vec![None]
                    } else {
                        vec![Some(DataValue::Int(i as i32))]
                    }
                })
                .collect();
            Self {
                rows,
                pos: 0,
                info: vec![ColumnInfo {
                    name: name.to_string(),
                    data_type: DataType::Int,
                    table: None,
                }],
            }
        }
    }

    impl PhysicalOperator for MockChild {
        fn next(&mut self) -> RookResult<Option<Tuple>> {
            if self.pos < self.rows.len() {
                let row = self.rows[self.pos].clone();
                self.pos += 1;
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

    /// Serialises tests that mutate the process-global SPILL_BUDGET —
    /// cargo runs tests in parallel threads and a budget set by one test
    /// would leak into another's join.
    static BUDGET_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct BudgetGuard(
        usize,
        #[allow(dead_code)] std::sync::MutexGuard<'static, ()>,
    );
    impl BudgetGuard {
        /// Locks the budget mutex, sets the budget, and returns a guard
        /// that restores the previous value on drop (lock released after
        /// the restore since it is the later field).
        fn set(n: usize) -> Self {
            let lock = BUDGET_LOCK.lock().unwrap_or_else(|p| p.into_inner());
            let prev = SPILL_BUDGET.load(std::sync::atomic::Ordering::Relaxed);
            SPILL_BUDGET.store(n, std::sync::atomic::Ordering::Relaxed);
            BudgetGuard(prev, lock)
        }
    }
    impl Drop for BudgetGuard {
        fn drop(&mut self) {
            SPILL_BUDGET.store(self.0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn key_expr() -> Vec<Expr> {
        vec![Expr::Column {
            table: None,
            column: "k".to_string(),
        }]
    }

    fn collect(op: &mut dyn PhysicalOperator) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        while let Some(t) = op.next().unwrap() {
            let k = match &t.values[0] {
                Some(DataValue::Int(v)) => *v,
                other => panic!("unexpected build value {:?}", other),
            };
            let p = match &t.values[1] {
                Some(DataValue::Int(v)) => *v,
                other => panic!("unexpected probe value {:?}", other),
            };
            out.push((k, p));
        }
        out.sort();
        out
    }

    #[test]
    fn spilled_join_matches_in_memory_results() {
        let _guard = BudgetGuard::set(8); // force spilling after 8 build tuples

        // Build: unique keys 0..60. Probe: keys 0..119 — probes 60..119
        // have no build partner, probes 0..59 match exactly once. Spilling
        // scatters both sides across 64 partitions, so this also proves
        // partition assignment agrees between build and probe.
        let build = MockChild::int_rows(60, "k");
        let probe = MockChild::int_rows(120, "k");
        let mut hj = HashJoinOperator::new(
            Box::new(build),
            Box::new(probe),
            key_expr(),
            key_expr(),
            None,
        );

        let out = collect(&mut hj);
        assert!(hj.did_spill(), "join should have taken the spill path");

        let expected: Vec<(i32, i32)> = (0..60i32).map(|k| (k, k)).collect();
        assert_eq!(out.len(), expected.len(), "match count mismatch");
        assert_eq!(out, expected, "spilled join produced wrong pairs");
    }

    #[test]
    fn spill_path_matches_in_memory_path_exactly() {
        // Same join run twice: once in memory, once forced to spill.
        let run = |force_spill: bool| {
            let _guard = BudgetGuard::set(if force_spill { 4 } else { 0 });
            let build = MockChild::int_rows(30, "k");
            let probe = MockChild::int_rows(45, "k");
            let mut hj = HashJoinOperator::new(
                Box::new(build),
                Box::new(probe),
                key_expr(),
                key_expr(),
                None,
            );
            (collect(&mut hj), hj.did_spill())
        };
        let (in_memory, spilled_a) = run(false);
        assert!(!spilled_a);
        let (spilled, spilled_b) = run(true);
        assert!(spilled_b);
        assert_eq!(
            in_memory, spilled,
            "spill path diverged from in-memory path"
        );
    }

    #[test]
    fn spill_skips_null_keys_on_both_sides() {
        let _guard = BudgetGuard::set(4);
        // Build: 10 rows, NULL at 3 and 7. Probe: 20 rows, NULL at 5 and 15.
        let build = MockChild::with_nulls(10, &[3, 7], "k");
        let probe = MockChild::with_nulls(20, &[5, 15], "k");
        let mut hj = HashJoinOperator::new(
            Box::new(build),
            Box::new(probe),
            key_expr(),
            key_expr(),
            None,
        );
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
}
