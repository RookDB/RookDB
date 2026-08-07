//! Shared helpers for the join test suite.
//!
//! Pulled into each flat `tests/<name>.rs` target with
//! `#[path = "join_common/mod.rs"] mod common;`. Cargo does not auto-discover
//! this file as a test target because the directory has no `main.rs`.
//!
//! Every test binary includes the whole module but uses only part of it, so
//! unused-item warnings are expected and silenced here rather than per item.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use storage_manager::catalog::Column;
use storage_manager::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
};
use storage_manager::heap::HeapManager;
use storage_manager::join::{
    JoinError, JoinType, MatchEvaluator, RelationSchema, RowCodec, RowStream, TableRef,
};
use storage_manager::types::row::serialize_nullable_typed_row;
use storage_manager::types::{DataType, DataValue, NumericValue, OrderedF32, OrderedF64};

// ── Deterministic RNG ────────────────────────────────────────────────────────

/// SplitMix64. Chosen because it is ten lines, has no dependency, and is
/// seeded - a failing randomized case can always be replayed from its seed.
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`. Returns 0 when `bound` is 0.
    pub fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }

    pub fn range_i64(&mut self, low: i64, high_inclusive: i64) -> i64 {
        if high_inclusive <= low {
            return low;
        }
        let span = (high_inclusive - low) as u64 + 1;
        low + (self.next_u64() % span) as i64
    }

    pub fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        if denominator == 0 {
            return false;
        }
        (self.next_u64() % u64::from(denominator)) < u64::from(numerator)
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// Resolves the seed for a randomized test: `ROOKDB_JOIN_SEED` if set and
/// parseable, otherwise the supplied default. Tests print whichever they use.
pub fn seed_from_env(default: u64) -> u64 {
    match std::env::var("ROOKDB_JOIN_SEED") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(seed) => seed,
            Err(_) => {
                eprintln!("ROOKDB_JOIN_SEED={raw:?} is not a u64; using default {default}");
                default
            }
        },
        Err(_) => default,
    }
}

// ── Schema generation ────────────────────────────────────────────────────────

/// A random data type, covering every variant the engine supports.
pub fn random_data_type(rng: &mut Rng) -> DataType {
    match rng.below(15) {
        0 => DataType::SmallInt,
        1 => DataType::Int,
        2 => DataType::BigInt,
        3 => DataType::Real,
        4 => DataType::DoublePrecision,
        5 => {
            let precision = 1 + rng.below(18) as u8;
            let scale = rng.below(usize::from(precision).min(7)) as u8;
            DataType::Numeric { precision, scale }
        }
        6 => {
            let precision = 1 + rng.below(18) as u8;
            let scale = rng.below(usize::from(precision).min(7)) as u8;
            DataType::Decimal { precision, scale }
        }
        7 => DataType::Bool,
        8 => DataType::Char(1 + rng.below(12) as u16),
        9 => DataType::Character(1 + rng.below(12) as u16),
        10 => DataType::Varchar(1 + rng.below(40) as u16),
        11 => DataType::Date,
        12 => DataType::Time,
        13 => DataType::Bit(1 + rng.below(24) as u16),
        _ => DataType::Timestamp,
    }
}

pub fn random_schema(rng: &mut Rng, min_columns: usize, max_columns: usize) -> Vec<DataType> {
    let span = max_columns.saturating_sub(min_columns) + 1;
    let count = min_columns + rng.below(span);
    (0..count).map(|_| random_data_type(rng)).collect()
}

/// Turn a type list into named, nullable catalog columns (`c0`, `c1`, …).
pub fn columns_from_types(types: &[DataType]) -> Vec<Column> {
    types
        .iter()
        .enumerate()
        .map(|(i, ty)| Column::new(format!("c{i}"), ty.clone()))
        .collect()
}

/// A relation whose columns are named `c0`, `c1`, … over the given types.
pub fn relation(alias: &str, types: &[DataType]) -> RelationSchema {
    RelationSchema::new(alias, columns_from_types(types))
}

/// A relation with explicit column names.
pub fn named_relation(alias: &str, columns: &[(&str, DataType)]) -> RelationSchema {
    RelationSchema::new(
        alias,
        columns
            .iter()
            .map(|(name, ty)| Column::new((*name).to_string(), ty.clone()))
            .collect(),
    )
}

// ── Predicate builders ───────────────────────────────────────────────────────

pub fn col(name: &str) -> Expr {
    Expr::Column(ColumnReference::new(name.to_string()))
}

/// A column already bound to an index in the concatenated `left ++ right`
/// space, for testing the residual evaluator without going through resolution.
pub fn vcol(index: usize) -> Expr {
    Expr::Column(ColumnReference::with_index(format!("v{index}"), index))
}

pub fn int_literal(value: i32) -> Expr {
    Expr::Constant(Constant::Int(value))
}

pub fn text_literal(value: &str) -> Expr {
    Expr::Constant(Constant::Text(value.to_string()))
}

pub fn null_literal() -> Expr {
    Expr::Constant(Constant::Null)
}

pub fn compare(left: Expr, op: ComparisonOp, right: Expr) -> Predicate {
    Predicate::Compare(Box::new(left), op, Box::new(right))
}

pub fn eq(left: Expr, right: Expr) -> Predicate {
    compare(left, ComparisonOp::Equals, right)
}

pub fn lt(left: Expr, right: Expr) -> Predicate {
    compare(left, ComparisonOp::LessThan, right)
}

/// Conjoin a list of predicates left-to-right; panics on an empty list because
/// an empty conjunction is not a predicate.
pub fn all_of(parts: Vec<Predicate>) -> Predicate {
    let mut iter = parts.into_iter();
    let first = iter.next().expect("at least one predicate");
    iter.fold(first, Predicate::and)
}

// ── Value generation ─────────────────────────────────────────────────────────

/// A value valid for `ty`.
///
/// Deliberately includes the awkward cases - `-0.0`, NaN, empty strings,
/// trailing-space CHARs - because those are exactly where the row codec and
/// the key encoder are most likely to disagree with upstream.
pub fn random_value(rng: &mut Rng, ty: &DataType) -> DataValue {
    match ty {
        DataType::SmallInt => DataValue::SmallInt(rng.range_i64(-32768, 32767) as i16),
        DataType::Int => DataValue::Int(rng.range_i64(-100_000, 100_000) as i32),
        DataType::BigInt => DataValue::BigInt(rng.range_i64(-10_000_000, 10_000_000)),
        DataType::Real => DataValue::Real(OrderedF32(random_f32(rng))),
        DataType::DoublePrecision => DataValue::DoublePrecision(OrderedF64(random_f64(rng))),
        DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale } => {
            DataValue::Numeric(random_numeric(rng, *precision, *scale))
        }
        DataType::Bool => DataValue::Bool(rng.chance(1, 2)),
        DataType::Char(n) | DataType::Character(n) => {
            DataValue::Char(random_ascii(rng, usize::from(*n)))
        }
        DataType::Varchar(n) => DataValue::Varchar(random_ascii(rng, usize::from(*n))),
        DataType::Date => DataValue::Date(random_date(rng)),
        DataType::Time => DataValue::Time(random_time(rng)),
        DataType::Bit(n) => DataValue::Bit(random_bits(rng, usize::from(*n))),
        DataType::Timestamp => DataValue::Timestamp(random_timestamp(rng)),
    }
}

/// A full row of values, with `null_in` chances out of 8 of any column
/// being NULL.
pub fn random_row(rng: &mut Rng, schema: &[DataType], null_in: u32) -> Vec<Option<DataValue>> {
    schema
        .iter()
        .map(|ty| {
            if rng.chance(null_in, 8) {
                None
            } else {
                Some(random_value(rng, ty))
            }
        })
        .collect()
}

fn random_f32(rng: &mut Rng) -> f32 {
    match rng.below(12) {
        0 => 0.0,
        1 => -0.0,
        2 => f32::NAN,
        3 => f32::INFINITY,
        4 => f32::NEG_INFINITY,
        _ => rng.range_i64(-1_000_000, 1_000_000) as f32 / 1000.0,
    }
}

fn random_f64(rng: &mut Rng) -> f64 {
    match rng.below(12) {
        0 => 0.0,
        1 => -0.0,
        2 => f64::NAN,
        3 => f64::INFINITY,
        4 => f64::NEG_INFINITY,
        _ => rng.range_i64(-1_000_000_000, 1_000_000_000) as f64 / 1_000_000.0,
    }
}

fn random_numeric(rng: &mut Rng, precision: u8, scale: u8) -> NumericValue {
    // Keep the unscaled magnitude inside `precision` decimal digits, which is
    // what the BCD encoder requires.
    let digits = u32::from(precision).min(18);
    let bound = 10_i64.saturating_pow(digits).saturating_sub(1);
    let capped = bound.min(1_000_000_000_000_000_000);
    let unscaled = i128::from(rng.range_i64(-capped, capped));
    NumericValue { unscaled, scale }
}

fn random_ascii(rng: &mut Rng, max_len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdeXYZ 019";
    let len = rng.below(max_len + 1);
    (0..len).map(|_| char::from(*rng.pick(ALPHABET))).collect()
}

fn random_bits(rng: &mut Rng, len: usize) -> String {
    (0..len)
        .map(|_| if rng.chance(1, 2) { '1' } else { '0' })
        .collect()
}

fn random_date(rng: &mut Rng) -> NaiveDate {
    let year = rng.range_i64(1900, 2200) as i32;
    let month = 1 + rng.below(12) as u32;
    let day = 1 + rng.below(28) as u32;
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(2000, 1, 1).expect("2000-01-01 is a valid date"))
}

fn random_time(rng: &mut Rng) -> NaiveTime {
    let hour = rng.below(24) as u32;
    let minute = rng.below(60) as u32;
    let second = rng.below(60) as u32;
    let micro = rng.below(1_000_000) as u32;
    NaiveTime::from_hms_micro_opt(hour, minute, second, micro)
        .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is a valid time"))
}

fn random_timestamp(rng: &mut Rng) -> NaiveDateTime {
    random_date(rng).and_time(random_time(rng))
}

// ── Scratch database ─────────────────────────────────────────────────────────

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A throwaway directory of heap files, removed when it drops.
///
/// The join subsystem never reads the catalog and never resolves paths through
/// `layout::*`, so tests do not share the process-wide `database/` tree. That
/// is what lets these tests run in parallel with no lock - and therefore with
/// no lock to poison when one of them fails.
pub struct TempDb {
    root: PathBuf,
}

impl TempDb {
    pub fn new() -> Self {
        let unique = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("rookdb-join-{}-{}", std::process::id(), unique));
        // A leftover directory from a killed run would otherwise be reused.
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create scratch directory");
        Self { root }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Create an empty heap file and return a handle for populating it.
    pub fn create_table(&self, name: &str, columns: &[(&str, DataType)]) -> TableHandle {
        let path = self.root.join(format!("{name}.dat"));
        let manager = HeapManager::create(path.clone()).expect("create heap file");
        let columns: Vec<Column> = columns
            .iter()
            .map(|(name, ty)| Column::new((*name).to_string(), ty.clone()))
            .collect();

        TableHandle {
            table: TableRef::new(name, path, columns),
            manager,
            rows: Vec::new(),
        }
    }
}

impl Default for TempDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A populated relation, plus the rows it was given.
///
/// Inserts go through `serialize_nullable_typed_row` rather than
/// `insert_single_tuple`, because that entry point takes `&[&str]` and so
/// cannot express a NULL at all - which would make the entire NULL test matrix
/// impossible to write.
pub struct TableHandle {
    table: TableRef,
    manager: HeapManager,
    rows: Vec<Vec<Option<DataValue>>>,
}

impl TableHandle {
    pub fn table_ref(&self) -> TableRef {
        self.table.clone()
    }

    pub fn relation_schema(&self) -> RelationSchema {
        self.table.relation_schema()
    }

    pub fn types(&self) -> Vec<DataType> {
        self.table
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect()
    }

    /// The rows as inserted, in insertion order - the oracle's input.
    pub fn rows(&self) -> &[Vec<Option<DataValue>>] {
        &self.rows
    }

    pub fn insert(&mut self, values: Vec<Option<DataValue>>) {
        let bytes =
            serialize_nullable_typed_row(&self.types(), &values).expect("row should serialize");
        self.manager
            .insert_tuple(&bytes)
            .expect("insert should succeed");
        self.rows.push(values);
    }

    pub fn insert_all(&mut self, rows: Vec<Vec<Option<DataValue>>>) {
        for row in rows {
            self.insert(row);
        }
    }

    /// Must be called before the table is scanned.
    pub fn flush(&mut self) {
        self.manager.flush().expect("flush should succeed");
    }

    /// Delete the first `count` rows, keeping the recorded row list in step.
    ///
    /// Used to check that statistics report live rows rather than slot
    /// entries: the two differ until the table is compacted.
    pub fn delete_first(&mut self, count: usize) -> usize {
        let targets: Vec<(u32, u32)> = self
            .manager
            .scan()
            .take(count)
            .filter_map(|item| item.ok().map(|(page, slot, _)| (page, slot)))
            .collect();

        for (page, slot) in &targets {
            self.manager
                .delete_tuple(*page, *slot)
                .expect("delete should succeed");
        }
        self.manager.flush().expect("flush should succeed");
        self.rows.drain(..targets.len().min(self.rows.len()));
        targets.len()
    }
}

// ── Reference join ───────────────────────────────────────────────────────────

/// A deliberately naive join, used as the oracle for every operator.
///
/// It shares the match evaluator with the real operators - predicate semantics
/// are covered exhaustively by their own tests, and re-deriving them here
/// would only test this file. What it does *not* share is the loop: no
/// blocking, no partitioning, no spilling, no sorting, no early exit. That is
/// precisely the part the operators get wrong.
pub fn reference_join(
    left_rows: &[Vec<Option<DataValue>>],
    right_rows: &[Vec<Option<DataValue>>],
    join_type: JoinType,
    evaluator: &MatchEvaluator,
    left_width: usize,
    right_width: usize,
) -> Result<Vec<Vec<Option<DataValue>>>, JoinError> {
    let mut out = Vec::new();
    let mut right_matched = vec![false; right_rows.len()];
    let left_only = join_type.emits_left_only();

    let concat = |left: Option<&Vec<Option<DataValue>>>, right: Option<&Vec<Option<DataValue>>>| {
        let mut row = Vec::with_capacity(left_width + right_width);
        match left {
            Some(values) => row.extend(values.iter().cloned()),
            None => row.extend(std::iter::repeat_n(None, left_width)),
        }
        if !left_only {
            match right {
                Some(values) => row.extend(values.iter().cloned()),
                None => row.extend(std::iter::repeat_n(None, right_width)),
            }
        }
        row
    };

    for left in left_rows {
        let mut matched = false;
        for (index, right) in right_rows.iter().enumerate() {
            if evaluator.matches(left, right)? {
                matched = true;
                right_matched[index] = true;
                if !left_only {
                    out.push(concat(Some(left), Some(right)));
                }
            }
        }

        let emit_unmatched = match join_type {
            JoinType::Semi => matched,
            JoinType::Anti => !matched,
            _ => !matched && join_type.keeps_unmatched_left(),
        };
        if emit_unmatched {
            out.push(concat(Some(left), None));
        }
    }

    if join_type.keeps_unmatched_right() {
        for (index, right) in right_rows.iter().enumerate() {
            if !right_matched[index] {
                out.push(concat(None, Some(right)));
            }
        }
    }

    Ok(out)
}

// ── Comparing results ────────────────────────────────────────────────────────

/// Decode every row a stream produces.
pub fn collect_rows(
    mut stream: Box<dyn RowStream>,
) -> Result<Vec<Vec<Option<DataValue>>>, JoinError> {
    let codec = RowCodec::new(stream.schema().types.clone());
    let mut out = Vec::new();
    while let Some(row) = stream.next() {
        out.push(codec.decode(&row?)?);
    }
    Ok(out)
}

fn canonical(row: &[Option<DataValue>]) -> String {
    format!("{row:?}")
}

/// Assert two result sets are equal as multisets.
///
/// Join output order is not defined, but multiplicity is: a join that emits a
/// row twice is as wrong as one that drops it. Comparing lengths alone - which
/// is all the previous test suite ever did - would miss wrong values, wrong
/// column order, and NULLs in the wrong place.
pub fn assert_rows_eq(
    actual: &[Vec<Option<DataValue>>],
    expected: &[Vec<Option<DataValue>>],
    context: &str,
) {
    let mut actual_sorted: Vec<String> = actual.iter().map(|r| canonical(r)).collect();
    let mut expected_sorted: Vec<String> = expected.iter().map(|r| canonical(r)).collect();
    actual_sorted.sort();
    expected_sorted.sort();

    if actual_sorted == expected_sorted {
        return;
    }

    let missing: Vec<&String> = expected_sorted
        .iter()
        .filter(|row| !actual_sorted.contains(row))
        .collect();
    let extra: Vec<&String> = actual_sorted
        .iter()
        .filter(|row| !expected_sorted.contains(row))
        .collect();

    panic!(
        "{context}: join result differs from the reference\n\
         expected {} rows, got {}\n\
         missing ({}): {:#?}\n\
         unexpected ({}): {:#?}",
        expected_sorted.len(),
        actual_sorted.len(),
        missing.len(),
        missing,
        extra.len(),
        extra
    );
}
