//! Order-preserving join key encoding.
//!
//! A [`JoinKey`] is a byte string whose `Hash`, `Eq` and `Ord` all derive from
//! the same bytes. Hash joins and sort-merge joins therefore agree on equality
//! and ordering by construction rather than by convention, and the ordering is
//! a genuine total order even for values `Comparable::compare` would refuse.
//!
//! The encoding deliberately follows `types::comparison::Comparable`, not
//! `DataValue`'s derived `PartialEq` - the two disagree, and the SQL-correct
//! answer is the former. `docs/join/design-rationale.md` records where and why.

use chrono::{Datelike, Timelike};

use crate::types::datatype::DataType;
use crate::types::value::DataValue;

use super::error::JoinError;

// ── Key classes ──────────────────────────────────────────────────────────────

/// The set of values a join key column may hold.
///
/// Two values in the same class are always comparable; two values in different
/// classes never are. Classes therefore collapse exactly the coercions
/// `Comparable::compare` performs (integer widening, CHAR/CHARACTER) and
/// separate exactly the ones it refuses (INT vs REAL, CHAR vs VARCHAR).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// SMALLINT, INT and BIGINT - mutually comparable by widening.
    Integer,
    Real,
    Double,
    /// NUMERIC/DECIMAL. The scale is part of the class: `compare` normalises
    /// scales with unchecked arithmetic that can overflow, so cross-scale
    /// joins are refused at plan time instead.
    Numeric {
        scale: u8,
    },
    Bool,
    /// CHAR and CHARACTER, compared with trailing whitespace stripped.
    Char,
    Varchar,
    Date,
    Time,
    Timestamp,
    Bit,
}

impl KeyClass {
    /// Every supported type belongs to exactly one class, so this is total.
    pub fn of(data_type: &DataType) -> Self {
        match data_type {
            DataType::SmallInt | DataType::Int | DataType::BigInt => KeyClass::Integer,
            DataType::Real => KeyClass::Real,
            DataType::DoublePrecision => KeyClass::Double,
            DataType::Numeric { scale, .. } | DataType::Decimal { scale, .. } => {
                KeyClass::Numeric { scale: *scale }
            }
            DataType::Bool => KeyClass::Bool,
            DataType::Char(_) | DataType::Character(_) => KeyClass::Char,
            DataType::Varchar(_) => KeyClass::Varchar,
            DataType::Date => KeyClass::Date,
            DataType::Time => KeyClass::Time,
            DataType::Timestamp => KeyClass::Timestamp,
            DataType::Bit(_) => KeyClass::Bit,
        }
    }

    /// Discriminator byte written ahead of each component.
    ///
    /// Within one join every component's class is fixed, so these bytes are
    /// constant and do not influence ordering; they exist so a key is
    /// self-describing and two classes can never collide.
    fn tag(self) -> u8 {
        match self {
            KeyClass::Integer => 0x10,
            KeyClass::Real => 0x20,
            KeyClass::Double => 0x21,
            KeyClass::Numeric { .. } => 0x30,
            KeyClass::Bool => 0x40,
            KeyClass::Char => 0x50,
            KeyClass::Varchar => 0x51,
            KeyClass::Date => 0x60,
            KeyClass::Time => 0x61,
            KeyClass::Timestamp => 0x62,
            KeyClass::Bit => 0x70,
        }
    }
}

/// Resolve the key class shared by both sides of an equijoin conjunct.
///
/// Returns an error rather than coercing. An implicit cast here would route
/// through a string literal and could truncate (`REAL` to `INT`) or fail per
/// row (`VARCHAR` to `INT`), turning a wrong query into a wrong answer.
pub fn resolve_key_class(left: &DataType, right: &DataType) -> Result<KeyClass, JoinError> {
    let left_class = KeyClass::of(left);
    let right_class = KeyClass::of(right);

    if left_class == right_class {
        return Ok(left_class);
    }

    let detail = match (left_class, right_class) {
        (KeyClass::Numeric { scale: l }, KeyClass::Numeric { scale: r }) => {
            format!("NUMERIC scales differ ({l} and {r}) and rescaling can overflow")
        }
        (KeyClass::Char, KeyClass::Varchar) | (KeyClass::Varchar, KeyClass::Char) => {
            "CHAR ignores trailing spaces and VARCHAR does not, so they are not comparable"
                .to_string()
        }
        _ => format!("{left_class:?} and {right_class:?} are not comparable"),
    };

    Err(JoinError::KeyTypeMismatch {
        left: left.to_string(),
        right: right.to_string(),
        detail,
    })
}

// ── The key ──────────────────────────────────────────────────────────────────

/// An encoded join key.
///
/// `Hash`, `Eq` and `Ord` all come from the same byte slice, so a hash table
/// and a sort-merge both see exactly one notion of equality and one ordering.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JoinKey(Box<[u8]>);

impl JoinKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn byte_len(&self) -> usize {
        self.0.len()
    }
}

/// Terminator for a variable-width payload. Lower than any escaped `0x00`
/// (`0x00 0xFF`), so a prefix always sorts before a longer string.
const TERMINATOR: [u8; 2] = [0x00, 0x01];

/// Append `raw` with `0x00` escaped, then terminate it.
///
/// Rust strings may contain NUL, so escaping is required for the
/// concatenation of two components to order like the tuple of components.
fn push_escaped(raw: &[u8], out: &mut Vec<u8>) {
    for &byte in raw {
        if byte == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&TERMINATOR);
}

/// Map a sign-magnitude float bit pattern onto an unsigned integer whose
/// natural order matches the float's.
fn order_bits_32(bits: u32) -> u32 {
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000
    }
}

fn order_bits_64(bits: u64) -> u64 {
    if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    }
}

/// `Ord for OrderedF32` treats every NaN as equal and greater than any real
/// value, and `+0.0` as equal to `-0.0`. Both are normalised here so the key
/// encoding says the same thing.
///
/// `u32::MAX` is above the encoding of `+INFINITY` (`0xFF80_0000`) and is not
/// produced by any non-NaN value, so it is a safe canonical NaN.
fn encode_f32(value: f32, out: &mut Vec<u8>) {
    let ordered = if value.is_nan() {
        u32::MAX
    } else if value == 0.0 {
        order_bits_32(0.0_f32.to_bits())
    } else {
        order_bits_32(value.to_bits())
    };
    out.extend_from_slice(&ordered.to_be_bytes());
}

fn encode_f64(value: f64, out: &mut Vec<u8>) {
    let ordered = if value.is_nan() {
        u64::MAX
    } else if value == 0.0 {
        order_bits_64(0.0_f64.to_bits())
    } else {
        order_bits_64(value.to_bits())
    };
    out.extend_from_slice(&ordered.to_be_bytes());
}

fn encode_i64(value: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&((value as u64) ^ (1 << 63)).to_be_bytes());
}

fn encode_i128(value: i128, out: &mut Vec<u8>) {
    out.extend_from_slice(&((value as u128) ^ (1 << 127)).to_be_bytes());
}

/// Append one component of a key.
///
/// The value must belong to `class`; a mismatch is an internal invariant
/// violation, since plan-time resolution fixes the class of every key column.
fn encode_component(
    class: KeyClass,
    value: &DataValue,
    out: &mut Vec<u8>,
) -> Result<(), JoinError> {
    out.push(class.tag());

    match (class, value) {
        (KeyClass::Integer, DataValue::SmallInt(v)) => encode_i64(i64::from(*v), out),
        (KeyClass::Integer, DataValue::Int(v)) => encode_i64(i64::from(*v), out),
        (KeyClass::Integer, DataValue::BigInt(v)) => encode_i64(*v, out),

        (KeyClass::Real, DataValue::Real(v)) => encode_f32(v.0, out),
        (KeyClass::Double, DataValue::DoublePrecision(v)) => encode_f64(v.0, out),

        (KeyClass::Numeric { scale }, DataValue::Numeric(v)) => {
            // Same scale is guaranteed by `resolve_key_class`, so ordering by
            // the unscaled integer is ordering by value. If it ever is not,
            // say so rather than silently comparing unlike magnitudes.
            if v.scale != scale {
                return Err(JoinError::key_encoding(format!(
                    "NUMERIC value has scale {} but the key column resolved to scale {scale}",
                    v.scale
                )));
            }
            encode_i128(v.unscaled, out);
        }

        (KeyClass::Bool, DataValue::Bool(v)) => out.push(u8::from(*v)),

        // `compare` uses `str::trim_end`, which strips all trailing Unicode
        // whitespace, not only spaces.
        (KeyClass::Char, DataValue::Char(v)) => push_escaped(v.trim_end().as_bytes(), out),
        (KeyClass::Varchar, DataValue::Varchar(v)) => push_escaped(v.as_bytes(), out),
        (KeyClass::Bit, DataValue::Bit(v)) => push_escaped(v.as_bytes(), out),

        (KeyClass::Date, DataValue::Date(v)) => {
            out.extend_from_slice(&((v.num_days_from_ce() as u32) ^ 0x8000_0000).to_be_bytes());
        }
        (KeyClass::Time, DataValue::Time(v)) => encode_time(v, out),
        (KeyClass::Timestamp, DataValue::Timestamp(v)) => {
            let date = v.date();
            out.extend_from_slice(&((date.num_days_from_ce() as u32) ^ 0x8000_0000).to_be_bytes());
            encode_time(&v.time(), out);
        }

        (class, value) => {
            return Err(JoinError::key_encoding(format!(
                "value {value:?} does not belong to key class {class:?}"
            )));
        }
    }

    Ok(())
}

/// Encode one value on its own, in the key class its column resolves to.
///
/// Statistics use this so distinct-value counts, minima and maxima are
/// measured in exactly the equivalence classes the join matches on: two CHAR
/// values differing only in trailing spaces count once, as do two NUMERICs
/// that differ only in representation. Estimates and execution therefore
/// cannot drift apart.
pub fn encode_value(class: KeyClass, value: &DataValue) -> Result<Vec<u8>, JoinError> {
    let mut buffer = Vec::with_capacity(16);
    encode_component(class, value, &mut buffer)?;
    Ok(buffer)
}

/// Encode a time as `(seconds, nanoseconds)` rather than a single nanosecond
/// count.
///
/// `NaiveTime` orders on that pair, and during a leap second its nanosecond
/// field exceeds one second - so a flattened count would order a leap second
/// after the following whole second. The pair cannot.
fn encode_time(value: &chrono::NaiveTime, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.num_seconds_from_midnight().to_be_bytes());
    out.extend_from_slice(&value.nanosecond().to_be_bytes());
}

// ── Key specifications ───────────────────────────────────────────────────────

/// One equijoin column pair, with the class both sides encode to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyColumn {
    pub left_index: usize,
    pub right_index: usize,
    pub class: KeyClass,
}

/// The equijoin key of a join: every equality conjunct between the two sides,
/// with orientation already normalised so the left column is always first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeySpec {
    pub columns: Vec<KeyColumn>,
}

impl KeySpec {
    pub fn new(columns: Vec<KeyColumn>) -> Self {
        Self { columns }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Encode the key of a left-side row.
    ///
    /// `Ok(None)` means at least one key component is NULL. There is no
    /// encoding for NULL, so a NULL key cannot equal anything - including
    /// another NULL - no matter which algorithm or how many times the row is
    /// written to and read back from a spill file.
    pub fn left_key(&self, row: &[Option<DataValue>]) -> Result<Option<JoinKey>, JoinError> {
        self.encode(row, |column| column.left_index, "left")
    }

    /// Encode the key of a right-side row. See [`KeySpec::left_key`].
    pub fn right_key(&self, row: &[Option<DataValue>]) -> Result<Option<JoinKey>, JoinError> {
        self.encode(row, |column| column.right_index, "right")
    }

    fn encode(
        &self,
        row: &[Option<DataValue>],
        index_of: impl Fn(&KeyColumn) -> usize,
        side: &str,
    ) -> Result<Option<JoinKey>, JoinError> {
        let mut buffer = Vec::with_capacity(self.columns.len() * 12);

        for column in &self.columns {
            let index = index_of(column);
            let slot = row.get(index).ok_or_else(|| {
                JoinError::key_encoding(format!(
                    "{side} key column {index} is out of range for a {}-column row",
                    row.len()
                ))
            })?;

            match slot {
                None => return Ok(None),
                Some(value) => encode_component(column.class, value, &mut buffer)?,
            }
        }

        Ok(Some(JoinKey(buffer.into_boxed_slice())))
    }
}
