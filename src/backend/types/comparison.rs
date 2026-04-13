//! SQL-99 comparison semantics for typed values.
//!
//! Provides:
//! - [`ComparisonError`] — the typed error returned for incompatible types.
//! - [`Comparable`] — the comparison trait implemented by [`DataValue`].
//! - [`compare_nullable`] / [`nullable_equals`] — NULL-aware wrappers that
//!   return `None` (UNKNOWN) when either operand is NULL, matching SQL semantics.

use std::cmp::Ordering;
use std::fmt;

use crate::types::value::DataValue;

// ── Error type ────────────────────────────────────────────────────────────────

/// Error returned when two incompatible types are compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonError {
    /// The left and right operands have types that cannot be compared.
    TypeMismatch { left: String, right: String },
}

impl fmt::Display for ComparisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonError::TypeMismatch { left, right } => {
                write!(f, "Cannot compare {} with {}", left, right)
            }
        }
    }
}

impl std::error::Error for ComparisonError {}

// ── Comparable trait ──────────────────────────────────────────────────────────

/// Trait for types that support SQL-style ordered comparison.
///
/// Numeric types (`SMALLINT`, `INT`, `BIGINT`) are mutually comparable via
/// implicit widening promotion. All other types are only comparable to
/// themselves. Cross-type comparisons return [`ComparisonError::TypeMismatch`].
pub trait Comparable: Sized {
    /// Compare `self` to `other`, returning the ordering relationship.
    fn compare(&self, other: &Self) -> Result<Ordering, ComparisonError>;

    /// Returns `true` if `self` and `other` are equal under SQL semantics.
    fn is_equal(&self, other: &Self) -> Result<bool, ComparisonError> {
        Ok(self.compare(other)? == Ordering::Equal)
    }
}

// ── Internal helper ───────────────────────────────────────────────────────────

/// Returns a static SQL type name string for a `DataValue` variant.
/// Used in error messages to identify the type of an unexpected value.
pub(crate) fn value_type_name(value: &DataValue) -> &'static str {
    match value {
        DataValue::SmallInt(_) => "SMALLINT",
        DataValue::Int(_) => "INT",
        DataValue::BigInt(_) => "BIGINT",
        DataValue::Real(_) => "REAL",
        DataValue::DoublePrecision(_) => "DOUBLE PRECISION",
        DataValue::Numeric(_) => "NUMERIC",
        DataValue::Bool(_) => "BOOLEAN",
        DataValue::Char(_) => "CHAR",
        DataValue::Varchar(_) => "VARCHAR",
        DataValue::Date(_) => "DATE",
        DataValue::Time(_) => "TIME",
        DataValue::Bit(_) => "BIT",
        DataValue::Timestamp(_) => "TIMESTAMP",
    }
}

// ── DataValue comparison ──────────────────────────────────────────────────────

impl Comparable for DataValue {
    /// Compare two `DataValue` instances.
    ///
    /// **Numeric promotion:** `SMALLINT` ↔ `INT` ↔ `BIGINT` comparisons are
    /// supported by widening the narrower operand before comparing.
    ///
    /// **NUMERIC/DECIMAL:** compared by normalising scales before comparing
    /// the unscaled integer representations.
    ///
    /// **CHAR:** trailing spaces are stripped before lexicographic comparison,
    /// matching SQL padding-ignoring semantics.
    ///
    /// All other cross-type combinations return [`ComparisonError::TypeMismatch`].
    fn compare(&self, other: &Self) -> Result<Ordering, ComparisonError> {
        match (self, other) {
            // Same-type integer comparisons
            (DataValue::SmallInt(a), DataValue::SmallInt(b)) => Ok(a.cmp(b)),
            (DataValue::Int(a), DataValue::Int(b)) => Ok(a.cmp(b)),
            (DataValue::BigInt(a), DataValue::BigInt(b)) => Ok(a.cmp(b)),

            // Cross-width integer promotion (widen to the larger type)
            (DataValue::SmallInt(a), DataValue::Int(b)) => Ok((*a as i32).cmp(b)),
            (DataValue::Int(a), DataValue::SmallInt(b)) => Ok(a.cmp(&(*b as i32))),
            (DataValue::SmallInt(a), DataValue::BigInt(b)) => Ok((*a as i64).cmp(b)),
            (DataValue::BigInt(a), DataValue::SmallInt(b)) => Ok(a.cmp(&(*b as i64))),
            (DataValue::Int(a), DataValue::BigInt(b)) => Ok((*a as i64).cmp(b)),
            (DataValue::BigInt(a), DataValue::Int(b)) => Ok(a.cmp(&(*b as i64))),

            // Floating-point (NaN-aware via OrderedF32/OrderedF64)
            (DataValue::Real(a), DataValue::Real(b)) => Ok(a.cmp(b)),
            (DataValue::DoublePrecision(a), DataValue::DoublePrecision(b)) => Ok(a.cmp(b)),

            // Exact decimal — normalise scales before comparing unscaled values
            (DataValue::Numeric(a), DataValue::Numeric(b)) => {
                let ordering = if a.scale == b.scale {
                    // Same scale: compare unscaled integers directly
                    a.unscaled.cmp(&b.unscaled)
                } else if a.scale > b.scale {
                    // a has more fractional digits; scale up b to match
                    let factor = 10_i128.pow((a.scale - b.scale) as u32);
                    a.unscaled.cmp(&(b.unscaled * factor))
                } else {
                    // b has more fractional digits; scale up a to match
                    let factor = 10_i128.pow((b.scale - a.scale) as u32);
                    (a.unscaled * factor).cmp(&b.unscaled)
                };
                Ok(ordering)
            }

            // Boolean: false < true
            (DataValue::Bool(a), DataValue::Bool(b)) => Ok(a.cmp(b)),

            // String types
            // CHAR: ignore trailing spaces (SQL padding-ignoring semantics)
            (DataValue::Char(a), DataValue::Char(b)) => Ok(a.trim_end().cmp(b.trim_end())),
            // VARCHAR: exact byte-wise lexicographic comparison
            (DataValue::Varchar(a), DataValue::Varchar(b)) => Ok(a.cmp(b)),

            // Temporal types: chronological ordering
            (DataValue::Date(a), DataValue::Date(b)) => Ok(a.cmp(b)),
            (DataValue::Time(a), DataValue::Time(b)) => Ok(a.cmp(b)),
            (DataValue::Timestamp(a), DataValue::Timestamp(b)) => Ok(a.cmp(b)),

            // BIT: lexicographic over the '0'/'1' string representation
            (DataValue::Bit(a), DataValue::Bit(b)) => Ok(a.cmp(b)),

            // Anything else is a type mismatch
            _ => Err(ComparisonError::TypeMismatch {
                left: value_type_name(self).to_string(),
                right: value_type_name(other).to_string(),
            }),
        }
    }
}

// ── NULL-aware wrappers ───────────────────────────────────────────────────────

/// Compare two nullable values following SQL three-valued logic.
///
/// Returns:
/// - `Ok(Some(ordering))` — both operands are non-NULL and comparable.
/// - `Ok(None)` — at least one operand is `NULL` (result is UNKNOWN).
/// - `Err(e)` — both operands are non-NULL but incompatible types.
pub fn compare_nullable(
    left: Option<&DataValue>,
    right: Option<&DataValue>,
) -> Result<Option<Ordering>, ComparisonError> {
    match (left, right) {
        (Some(l), Some(r)) => Ok(Some(l.compare(r)?)),
        // Any NULL operand → UNKNOWN
        _ => Ok(None),
    }
}

/// Test equality of two nullable values following SQL three-valued logic.
///
/// Returns `Ok(None)` (UNKNOWN) if either operand is NULL.
pub fn nullable_equals(
    left: Option<&DataValue>,
    right: Option<&DataValue>,
) -> Result<Option<bool>, ComparisonError> {
    Ok(compare_nullable(left, right)?.map(|o| o == Ordering::Equal))
}
