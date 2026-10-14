//! SQL-99 comparison semantics for typed values.
//!
//! Provides:
//! - [`ComparisonError`] — the typed error returned for incompatible types.
//! - [`Comparable`] — the comparison trait implemented by [`DataValue`].
//! - [`compare_nullable`] / [`nullable_equals`] — NULL-aware wrappers that
//!   return `None` (UNKNOWN) when either operand is NULL, matching SQL semantics.

use std::cmp::Ordering;
use std::fmt;

use crate::types::value::{DataValue, NumericValue, OrderedF64};

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

            // Cross-type numeric: integer/REAL/DOUBLE PRECISION widen to f64
            (
                DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_),
                DataValue::Real(_) | DataValue::DoublePrecision(_),
            )
            | (
                DataValue::Real(_) | DataValue::DoublePrecision(_),
                DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_),
            )
            | (DataValue::Real(_), DataValue::DoublePrecision(_))
            | (DataValue::DoublePrecision(_), DataValue::Real(_)) => {
                compare_approximate(self, other)
            }

            // Cross-type NUMERIC ↔ INTEGER / FLOAT, and exact decimal
            (
                DataValue::Numeric(_),
                DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_),
            )
            | (
                DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_),
                DataValue::Numeric(_),
            ) => compare_numeric_integer(self, other),
            (DataValue::Numeric(_), DataValue::Real(_) | DataValue::DoublePrecision(_))
            | (DataValue::Real(_) | DataValue::DoublePrecision(_), DataValue::Numeric(_)) => {
                compare_numeric_float(self, other)
            }
            (DataValue::Numeric(a), DataValue::Numeric(b)) => compare_numeric_exact(a, b),

            // Boolean: false < true
            (DataValue::Bool(a), DataValue::Bool(b)) => Ok(a.cmp(b)),

            // String types
            // CHAR: ignore trailing spaces (SQL padding-ignoring semantics)
            (DataValue::Char(a), DataValue::Char(b)) => Ok(a.trim_end().cmp(b.trim_end())),
            // VARCHAR: exact byte-wise lexicographic comparison
            (DataValue::Varchar(a), DataValue::Varchar(b)) => Ok(a.cmp(b)),

            // Cross-type CHAR ↔ VARCHAR: trim CHAR trailing spaces, compare as strings
            // A CHAR comparison is blank-padded: trailing spaces on either
            // side are not significant.
            (DataValue::Char(a), DataValue::Varchar(b)) => Ok(a.trim_end().cmp(b.trim_end())),
            (DataValue::Varchar(a), DataValue::Char(b)) => Ok(a.trim_end().cmp(b.trim_end())),

            // Temporal types: chronological ordering
            (DataValue::Date(a), DataValue::Date(b)) => Ok(a.cmp(b)),
            (DataValue::Time(a), DataValue::Time(b)) => Ok(a.cmp(b)),
            (DataValue::Timestamp(a), DataValue::Timestamp(b)) => Ok(a.cmp(b)),

            // Cross-type temporal: DATE/TIME ↔ TIMESTAMP
            (DataValue::Date(_), DataValue::Timestamp(_))
            | (DataValue::Timestamp(_), DataValue::Date(_))
            | (DataValue::Time(_), DataValue::Timestamp(_))
            | (DataValue::Timestamp(_), DataValue::Time(_)) => {
                compare_date_timestamp_time(self, other)
            }

            // Cross-type temporal: DATE/TIMESTAMP/TIME ↔ VARCHAR/CHAR
            (DataValue::Date(_), DataValue::Varchar(_) | DataValue::Char(_))
            | (DataValue::Varchar(_) | DataValue::Char(_), DataValue::Date(_)) => {
                compare_date_text(self, other)
            }
            (DataValue::Timestamp(_), DataValue::Varchar(_) | DataValue::Char(_))
            | (DataValue::Varchar(_) | DataValue::Char(_), DataValue::Timestamp(_)) => {
                compare_timestamp_text(self, other)
            }
            (DataValue::Time(_), DataValue::Varchar(_) | DataValue::Char(_))
            | (DataValue::Varchar(_) | DataValue::Char(_), DataValue::Time(_)) => {
                compare_time_text(self, other)
            }

            // BIT: lexicographic over the '0'/'1' string representation
            (DataValue::Bit(a), DataValue::Bit(b)) => Ok(a.cmp(b)),

            // Anything else is a type mismatch
            _ => Err(type_mismatch(self, other)),
        }
    }
}

// ── Cross-type comparison helpers ─────────────────────────────────────────────

/// Cross-type comparison where at least one side is REAL or DOUBLE PRECISION and
/// the other is an integer or the other float width; both sides widen to `f64`.
fn compare_approximate(left: &DataValue, right: &DataValue) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type integer ↔ REAL comparisons (widen to f64 via OrderedF64)
        (DataValue::SmallInt(a), DataValue::Real(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0 as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Real(a), DataValue::SmallInt(b)) => {
            let af = OrderedF64(a.0 as f64);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Int(a), DataValue::Real(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0 as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Real(a), DataValue::Int(b)) => {
            let af = OrderedF64(a.0 as f64);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::BigInt(a), DataValue::Real(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0 as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Real(a), DataValue::BigInt(b)) => {
            let af = OrderedF64(a.0 as f64);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }

        // Cross-type REAL ↔ DOUBLE PRECISION promotion (widen Real to Double)
        (DataValue::Real(a), DataValue::DoublePrecision(b)) => {
            let af = OrderedF64(a.0 as f64);
            Ok(af.cmp(b))
        }
        (DataValue::DoublePrecision(a), DataValue::Real(b)) => {
            let bf = OrderedF64(b.0 as f64);
            Ok(a.cmp(&bf))
        }

        // Cross-type integer ↔ DOUBLE PRECISION comparisons
        (DataValue::SmallInt(a), DataValue::DoublePrecision(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0);
            Ok(af.cmp(&bf))
        }
        (DataValue::DoublePrecision(a), DataValue::SmallInt(b)) => {
            let af = OrderedF64(a.0);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Int(a), DataValue::DoublePrecision(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0);
            Ok(af.cmp(&bf))
        }
        (DataValue::DoublePrecision(a), DataValue::Int(b)) => {
            let af = OrderedF64(a.0);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::BigInt(a), DataValue::DoublePrecision(b)) => {
            let af = OrderedF64(*a as f64);
            let bf = OrderedF64(b.0);
            Ok(af.cmp(&bf))
        }
        (DataValue::DoublePrecision(a), DataValue::BigInt(b)) => {
            let af = OrderedF64(a.0);
            let bf = OrderedF64(*b as f64);
            Ok(af.cmp(&bf))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// NUMERIC against an integer type: scale the integer up and compare exactly.
fn compare_numeric_integer(
    left: &DataValue,
    right: &DataValue,
) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type NUMERIC ↔ INTEGER: widen integer to NumericValue at the same scale, then compare exactly
        (DataValue::Numeric(a), DataValue::SmallInt(b)) => {
            let factor = 10_i128.pow(a.scale as u32);
            let b_scaled = (*b as i128) * factor;
            Ok(a.unscaled.cmp(&b_scaled))
        }
        (DataValue::SmallInt(a), DataValue::Numeric(b)) => {
            let factor = 10_i128.pow(b.scale as u32);
            let a_scaled = (*a as i128) * factor;
            Ok(a_scaled.cmp(&b.unscaled))
        }
        (DataValue::Numeric(a), DataValue::Int(b)) => {
            let factor = 10_i128.pow(a.scale as u32);
            let b_scaled = (*b as i128) * factor;
            Ok(a.unscaled.cmp(&b_scaled))
        }
        (DataValue::Int(a), DataValue::Numeric(b)) => {
            let factor = 10_i128.pow(b.scale as u32);
            let a_scaled = (*a as i128) * factor;
            Ok(a_scaled.cmp(&b.unscaled))
        }
        (DataValue::Numeric(a), DataValue::BigInt(b)) => {
            let factor = 10_i128.pow(a.scale as u32);
            let b_scaled = (*b as i128) * factor;
            Ok(a.unscaled.cmp(&b_scaled))
        }
        (DataValue::BigInt(a), DataValue::Numeric(b)) => {
            let factor = 10_i128.pow(b.scale as u32);
            let a_scaled = (*a as i128) * factor;
            Ok(a_scaled.cmp(&b.unscaled))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// NUMERIC against REAL or DOUBLE PRECISION: convert the NUMERIC to `f64`.
fn compare_numeric_float(left: &DataValue, right: &DataValue) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type NUMERIC ↔ FLOAT: convert NUMERIC to f64, compare via OrderedF64
        (DataValue::Numeric(a), DataValue::Real(b)) => {
            let af = OrderedF64(a.unscaled as f64 / 10_f64.powi(a.scale as i32));
            let bf = OrderedF64(b.0 as f64);
            Ok(af.cmp(&bf))
        }
        (DataValue::Real(a), DataValue::Numeric(b)) => {
            let af = OrderedF64(a.0 as f64);
            let bf = OrderedF64(b.unscaled as f64 / 10_f64.powi(b.scale as i32));
            Ok(af.cmp(&bf))
        }
        (DataValue::Numeric(a), DataValue::DoublePrecision(b)) => {
            let af = OrderedF64(a.unscaled as f64 / 10_f64.powi(a.scale as i32));
            Ok(af.cmp(b))
        }
        (DataValue::DoublePrecision(a), DataValue::Numeric(b)) => {
            let bf = OrderedF64(b.unscaled as f64 / 10_f64.powi(b.scale as i32));
            Ok(a.cmp(&bf))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// Exact decimal comparison: normalise scales before comparing unscaled values.
fn compare_numeric_exact(a: &NumericValue, b: &NumericValue) -> Result<Ordering, ComparisonError> {
    let scale_up = |unscaled: i128, diff: u32| -> Result<i128, Ordering> {
        if unscaled == 0 {
            return Ok(0);
        }
        let factor = 10_i128.checked_pow(diff);
        match factor.and_then(|f| unscaled.checked_mul(f)) {
            Some(val) => Ok(val),
            None => {
                if unscaled > 0 {
                    Err(Ordering::Greater)
                } else {
                    Err(Ordering::Less)
                }
            }
        }
    };

    let ordering = if a.scale == b.scale {
        // Same scale: compare unscaled integers directly
        a.unscaled.cmp(&b.unscaled)
    } else if a.scale > b.scale {
        // a has more fractional digits; scale up b to match
        match scale_up(b.unscaled, (a.scale - b.scale) as u32) {
            Ok(b_scaled) => a.unscaled.cmp(&b_scaled),
            Err(Ordering::Greater) => Ordering::Less,
            Err(Ordering::Less) => Ordering::Greater,
            Err(Ordering::Equal) => Ordering::Equal,
        }
    } else {
        // b has more fractional digits; scale up a to match
        match scale_up(a.unscaled, (b.scale - a.scale) as u32) {
            Ok(a_scaled) => a_scaled.cmp(&b.unscaled),
            Err(Ordering::Greater) => Ordering::Greater,
            Err(Ordering::Less) => Ordering::Less,
            Err(Ordering::Equal) => Ordering::Equal,
        }
    };
    Ok(ordering)
}

/// DATE/TIME against TIMESTAMP: promote the DATE to midnight, or take the time component.
fn compare_date_timestamp_time(
    left: &DataValue,
    right: &DataValue,
) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type temporal: DATE ↔ TIMESTAMP (promote Date to midnight Timestamp)
        (DataValue::Date(a), DataValue::Timestamp(b)) => {
            let midnight = a
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ComparisonError::TypeMismatch {
                    left: "DATE".to_string(),
                    right: "TIMESTAMP".to_string(),
                })?;
            Ok(midnight.cmp(b))
        }
        (DataValue::Timestamp(a), DataValue::Date(b)) => {
            let midnight = b
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ComparisonError::TypeMismatch {
                    left: "TIMESTAMP".to_string(),
                    right: "DATE".to_string(),
                })?;
            Ok(a.cmp(&midnight))
        }

        // Cross-type temporal: TIME ↔ TIMESTAMP (extract time component from Timestamp)
        (DataValue::Time(a), DataValue::Timestamp(b)) => Ok(a.cmp(&b.time())),
        (DataValue::Timestamp(a), DataValue::Time(b)) => Ok(a.time().cmp(b)),

        _ => Err(type_mismatch(left, right)),
    }
}

/// DATE against a VARCHAR/CHAR holding a date literal.
fn compare_date_text(left: &DataValue, right: &DataValue) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type temporal: DATE ↔ VARCHAR/CHAR
        (DataValue::Date(a), DataValue::Varchar(s)) | (DataValue::Date(a), DataValue::Char(s)) => {
            let parsed = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").map_err(|_| {
                ComparisonError::TypeMismatch {
                    left: "DATE".to_string(),
                    right: "VARCHAR".to_string(),
                }
            })?;
            Ok(a.cmp(&parsed))
        }
        (DataValue::Varchar(s), DataValue::Date(b)) | (DataValue::Char(s), DataValue::Date(b)) => {
            let parsed = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").map_err(|_| {
                ComparisonError::TypeMismatch {
                    left: "VARCHAR".to_string(),
                    right: "DATE".to_string(),
                }
            })?;
            Ok(parsed.cmp(b))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// TIMESTAMP against a VARCHAR/CHAR holding a timestamp (or date) literal.
fn compare_timestamp_text(
    left: &DataValue,
    right: &DataValue,
) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type temporal: TIMESTAMP ↔ VARCHAR/CHAR
        (DataValue::Timestamp(a), DataValue::Varchar(s))
        | (DataValue::Timestamp(a), DataValue::Char(s)) => {
            let raw = s.trim();
            let parsed = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| {
                    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
                .map_err(|_| ComparisonError::TypeMismatch {
                    left: "TIMESTAMP".to_string(),
                    right: "VARCHAR".to_string(),
                })?;
            Ok(a.cmp(&parsed))
        }
        (DataValue::Varchar(s), DataValue::Timestamp(b))
        | (DataValue::Char(s), DataValue::Timestamp(b)) => {
            let raw = s.trim();
            let parsed = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                .or_else(|_| {
                    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
                .map_err(|_| ComparisonError::TypeMismatch {
                    left: "VARCHAR".to_string(),
                    right: "TIMESTAMP".to_string(),
                })?;
            Ok(parsed.cmp(b))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// TIME against a VARCHAR/CHAR holding a time literal.
fn compare_time_text(left: &DataValue, right: &DataValue) -> Result<Ordering, ComparisonError> {
    match (left, right) {
        // Cross-type temporal: TIME ↔ VARCHAR/CHAR
        (DataValue::Time(a), DataValue::Varchar(s)) | (DataValue::Time(a), DataValue::Char(s)) => {
            let raw = s.trim();
            let parsed = chrono::NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(raw, "%H:%M:%S"))
                .map_err(|_| ComparisonError::TypeMismatch {
                    left: "TIME".to_string(),
                    right: "VARCHAR".to_string(),
                })?;
            Ok(a.cmp(&parsed))
        }
        (DataValue::Varchar(s), DataValue::Time(b)) | (DataValue::Char(s), DataValue::Time(b)) => {
            let raw = s.trim();
            let parsed = chrono::NaiveTime::parse_from_str(raw, "%H:%M:%S%.f")
                .or_else(|_| chrono::NaiveTime::parse_from_str(raw, "%H:%M:%S"))
                .map_err(|_| ComparisonError::TypeMismatch {
                    left: "VARCHAR".to_string(),
                    right: "TIME".to_string(),
                })?;
            Ok(parsed.cmp(b))
        }

        _ => Err(type_mismatch(left, right)),
    }
}

/// The error for two operands whose types cannot be compared.
fn type_mismatch(left: &DataValue, right: &DataValue) -> ComparisonError {
    ComparisonError::TypeMismatch {
        left: value_type_name(left).to_string(),
        right: value_type_name(right).to_string(),
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
