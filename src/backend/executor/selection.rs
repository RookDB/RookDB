//! Predicate-based tuple filtering with SQL NULL semantics.
//!
//! This is the selection (σ) operator — a stack-based bytecode VM that implements
//! three-valued logic with lazy column extraction. The predicate AST gets compiled
//! once at plan time into a flat instruction stream. At runtime the hot loop is a
//! tight dispatch loop: no recursion, no per-tuple heap allocations, no full-row
//! deserialization unless we actually need the column.
//!
//! # Physical Layout of a Row
//!
//! `serialize_nullable_row` writes rows in this format:
//! ```text
//! [Header 4B] [Null Bitmap] [Var-Len Offset Table] [Fixed-Length Data] [Var-Len Payloads]
//! ```
//! - bytes 0-1 : `u16` num_cols (little-endian)
//! - bytes 2-3 : `u16` num_varlen (little-endian)
//! - null bitmap : `ceil(num_cols/8)` bytes, one bit per logical column
//! - var-len offset table : `num_varlen * 2` bytes; each entry is a `u16` absolute
//!   row offset into the payload region (0x0000 = NULL sentinel)
//! - fixed-length data : packed in **physical** fixed-col order, with alignment padding
//! - var-len payloads : packed in **physical** var-len order, no length prefix

use regex::Regex;
use std::cmp::Ordering;
use std::collections::HashSet;
use chrono::NaiveDate;
use crate::types::{
    DataType as SqlDataType,
    DataValue,
    OrderedF64,
    compare_nullable,
};
use crate::types::row_layout::{PhysicalSchema, RowLayout};
use crate::types::null_bitmap::NullBitmap;
use std::borrow::Cow;

// Public AST types (kept stable for callers)

/// The three truth values SQL uses — True, False, and Unknown (the NULL case).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriValue {
    True,
    False,
    Unknown,
}

/// A literal value that appears directly in the predicate (not a column ref).
#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null,
}

/// Expression node — either a column ref, a literal constant, or an arithmetic combo.
#[derive(Debug, Clone)]
pub enum Expr {
    Column(ColumnReference),
    Constant(Constant),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

/// A column reference by name; gets resolved to a schema index during executor init.
#[derive(Debug, Clone)]
pub struct ColumnReference {
    pub column_name: String,
    pub column_index: Option<usize>,
}

impl ColumnReference {
    /// Create a column ref by name only — the index gets filled in later during resolve.
    pub fn new(column_name: String) -> Self {
        ColumnReference {
            column_name,
            column_index: None,
        }
    }

    /// Create a column ref that's already resolved to a schema index (useful for tests).
    pub fn with_index(column_name: String, column_index: usize) -> Self {
        ColumnReference {
            column_name,
            column_index: Some(column_index),
        }
    }
}

/// The predicate tree used to filter tuples.
///
/// Leaf nodes are comparisons over `Expr` operands; internal nodes are logical ops
/// (AND/OR/NOT). This tree only lives at planning time — `SelectionExecutor::new()`
/// compiles it down to bytecode and the tree itself is never touched at runtime.
#[derive(Debug, Clone)]
pub enum Predicate {
    Compare(Box<Expr>, ComparisonOp, Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Not(Box<Predicate>),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Between(Box<Expr>, Box<Expr>, Box<Expr>),
    In(Box<Expr>, Vec<Expr>),
    Like(Box<Expr>, String, Option<Regex>),
    // Not a real correlated subquery EXISTS — just a logical wrapper around another predicate.
    Exists(Box<Predicate>),
}

impl Predicate {
    pub fn and(left: Predicate, right: Predicate) -> Self {
        Predicate::And(Box::new(left), Box::new(right))
    }

    pub fn or(left: Predicate, right: Predicate) -> Self {
        Predicate::Or(Box::new(left), Box::new(right))
    }

    pub fn not(inner: Predicate) -> Self {
        Predicate::Not(Box::new(inner))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Equals,
    LessThan,
    GreaterThan,
    LessOrEqual,
    GreaterOrEqual,
    NotEquals,
}

// VM data structures

/// Stack value that can be either raw data or a boolean result — needed to distinguish the two at runtime.
#[derive(Debug, Clone)]
pub enum VMValue<'a> {
    Data(Option<Cow<'a, DataValue>>),
    Bool(TriValue),
}

/// A pre-compiled LIKE pattern so we don't re-parse the SQL string on every tuple.
#[derive(Debug, Clone)]
pub enum LikePattern {
    /// `prefix%` — nothing tricky, just a prefix check.
    StartsWith(String),
    /// `%suffix` — suffix check only.
    EndsWith(String),
    /// `%substring%` — substring check only.
    Contains(String),
    /// Anything more complex falls back to a compiled regex.
    Regex(Regex),
}

// TODO(perf): could split this into Small(Vec) / Large(HashSet) to get O(1) lookups
// for big IN lists. Not worth it right now — typical SQL IN clauses are tiny.
/// Pre-computed IN-set for the `In` instruction.
///
/// `DataValue` is `PartialEq + Eq` but NOT `Hash` (floats break it), so we
/// store a `Vec` and do a linear scan. For the IN lists we actually see in SQL
/// (< 100 items) this beats the HashMap overhead easily.
#[derive(Debug, Clone)]
pub struct InSet {
    /// The actual non-null values to compare against.
    pub values: Vec<DataValue>,
    /// Set if the list had at least one NULL — matters for 3VL: x IN (1, NULL) = UNKNOWN when x != 1.
    pub has_null: bool,
}

/// One instruction in the stack VM's bytecode.
///
/// Cache-line discipline: heavy heap objects (`InSet`, `LikePattern`) are NOT
/// stored inline. `In` and `Like` instead carry a `usize` pool index into
/// `in_sets` / `like_patterns` on the executor. This keeps every variant ≤ 16 bytes
/// so the whole bytecode vector stays tight in L1/L2 during the dispatch loop.
#[derive(Debug, Clone)]
pub enum Instruction {
    /// Lazily extract logical column `idx` from the raw tuple bytes and push it.
    PushColumn(usize),
    /// Push a non-null constant value.
    PushConstant(DataValue),
    /// Push SQL NULL.
    PushNull,
    // Arithmetic ops
    Add,
    Sub,
    Mul,
    Div,
    // Comparison ops
    CmpEquals,
    CmpNotEquals,
    CmpLessThan,
    CmpLessOrEqual,
    CmpGreaterThan,
    CmpGreaterOrEqual,
    // Logical ops (3VL)
    And,
    Or,
    Not,
    IsNull,
    IsNotNull,
    // Advanced predicates — pool-indexed so the instruction stays small
    /// Index into `SelectionExecutor::in_sets`.
    In(usize),
    /// Index into `SelectionExecutor::like_patterns`.
    Like(usize),
    // Control flow — short-circuit evaluation
    /// Jump to `pc = offset` when top-of-stack is `Bool(False)`.
    /// The value is **left on the stack** so the following `And` instruction
    /// still has its left operand. Falls through on True/Unknown (also retained).
    JumpIfFalse(usize),
    /// Jump to `pc = offset` when top-of-stack is `Bool(True)`.
    /// Same stack-retention contract as `JumpIfFalse` — the value stays for `Or`.
    JumpIfTrue(usize),
}

// Catalog import — reuse the lightweight schema types defined there
pub use crate::catalog::types::{ColumnSchema, TableSchema};

// Small type-check helpers

fn is_numeric(t: &SqlDataType) -> bool {
    matches!(
        t,
        SqlDataType::Int
            | SqlDataType::SmallInt
            | SqlDataType::BigInt
            | SqlDataType::DoublePrecision
            | SqlDataType::Real
    )
}

fn is_textual(t: &SqlDataType) -> bool {
    matches!(t, SqlDataType::Varchar(_) | SqlDataType::Char(_) | SqlDataType::Character(_))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Constant(Constant::Null))
}

/// Walk an expression and figure out its result type from the schema + literal types.
fn infer_expr_type(expr: &Expr, schema: &TableSchema) -> Result<SqlDataType, String> {
    match expr {
        Expr::Column(col_ref) => {
            let idx = col_ref.column_index
                .ok_or_else(|| "Column not resolved".to_string())?;
            Ok(schema.columns[idx].data_type.clone())
        }
        Expr::Constant(c) => {
            match c {
                Constant::Int(_) => Ok(SqlDataType::Int),
                Constant::Float(_) => Ok(SqlDataType::DoublePrecision),
                Constant::Text(_) => Ok(SqlDataType::Varchar(u16::MAX)),
                Constant::Date(_) => Ok(SqlDataType::Date),
                Constant::Null => Err("Cannot infer type for NULL literal".to_string()),
            }
        }
        Expr::Add(l, r) | Expr::Sub(l, r) | Expr::Mul(l, r) => {
            if is_null_literal(l) && is_null_literal(r) {
                return Ok(SqlDataType::Int);
            }
            if is_null_literal(l) {
                return infer_expr_type(r, schema);
            }
            if is_null_literal(r) {
                return infer_expr_type(l, schema);
            }

            let left_type = infer_expr_type(l, schema)?;
            let right_type = infer_expr_type(r, schema)?;

            if !is_numeric(&left_type) || !is_numeric(&right_type) {
                return Err(format!(
                    "Arithmetic operation requires numeric types, got {:?} and {:?}",
                    left_type, right_type
                ));
            }

            if matches!(left_type, SqlDataType::DoublePrecision | SqlDataType::Real)
                || matches!(right_type, SqlDataType::DoublePrecision | SqlDataType::Real)
            {
                Ok(SqlDataType::DoublePrecision)
            } else {
                Ok(left_type)
            }
        }

        Expr::Div(l, r) => {
            if is_null_literal(l) && is_null_literal(r) {
                // NULL / NULL — we can't know the type at planning time, so just fall back to Int.
                return Ok(SqlDataType::Int);
            }
            if is_null_literal(l) {
                return infer_expr_type(r, schema);
            }
            if is_null_literal(r) {
                return infer_expr_type(l, schema);
            }

            let left_type = infer_expr_type(l, schema)?;
            let right_type = infer_expr_type(r, schema)?;

            if !is_numeric(&left_type) || !is_numeric(&right_type) {
                return Err(format!(
                    "Arithmetic operation requires numeric types, got {:?} and {:?}",
                    left_type, right_type
                ));
            }

            // Int / Int must stay Int (PostgreSQL truncating division — e.g. 5/2 = 2, not 2.5).
            // If either side is float, promote the whole thing to DoublePrecision.
            if matches!(left_type, SqlDataType::DoublePrecision | SqlDataType::Real)
                || matches!(right_type, SqlDataType::DoublePrecision | SqlDataType::Real)
            {
                Ok(SqlDataType::DoublePrecision)
            } else {
                // Both sides are integer — pick the wider one to avoid silent truncation.
                if matches!(left_type, SqlDataType::BigInt) || matches!(right_type, SqlDataType::BigInt) {
                    Ok(SqlDataType::BigInt)
                } else {
                    Ok(left_type)
                }
            }
        }
    }
}

// SelectionExecutor — bytecode-based predicate evaluator

/// The executor that runs predicate filtering against raw tuple bytes.
///
/// Predicate gets compiled to flat bytecode once in `new()`. After that,
/// `evaluate_tuple` is a hot stack-machine dispatch loop with:
/// - Hot loop (no allocs): stack is a fixed `[VMValue; 128]` on the call frame.
/// - Cache-friendly bytecode: `InSet`/`LikePattern` live in constant pools;
///   instructions only hold a `usize` index so every variant stays ≤ 16 bytes.
/// - Lazy column extraction: only the columns the bytecode actually touches get decoded.
/// - Short-circuit AND/OR via `JumpIfFalse`/`JumpIfTrue`.
pub struct SelectionExecutor {
    column_types: Vec<SqlDataType>,
    bytecode: Vec<Instruction>,
    /// Constant pool for `Instruction::In(idx)` — holds the pre-built `InSet` objects.
    in_sets: Vec<InSet>,
    /// Constant pool for `Instruction::Like(idx)` — holds the pre-built `LikePattern` objects.
    like_patterns: Vec<LikePattern>,
}

impl SelectionExecutor {
    /// Build the executor: normalize the predicate, bind column names to indices, then compile.
    pub fn new(mut predicate: Predicate, schema: TableSchema) -> Result<Self, String> {
        // Planning phase — fold constants, normalize shapes (BETWEEN → AND, etc.)
        Self::normalize_predicate(&mut predicate);
        Self::resolve_columns(&mut predicate, &schema)?;

        // Grab column types up front — cheaper than hitting the schema struct per column access
        let column_types = schema
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>();

        // Compile the predicate AST into bytecode and populate the constant pools
        let mut bytecode: Vec<Instruction> = Vec::new();
        let mut _used_columns: HashSet<usize> = HashSet::new();
        let mut in_sets: Vec<InSet> = Vec::new();
        let mut like_patterns: Vec<LikePattern> = Vec::new();
        Self::compile_predicate(
            &predicate,
            &mut bytecode,
            &mut _used_columns,
            &mut in_sets,
            &mut like_patterns,
        )?;

        Ok(SelectionExecutor {
            column_types,
            bytecode,
            in_sets,
            like_patterns,
        })
    }

    // Planning phase helpers

    /// Pre-process the predicate tree: fold constants, expand BETWEEN into AND, pre-compile regexes.
    fn normalize_predicate(predicate: &mut Predicate) {
        match predicate {
            Predicate::Compare(left, op, right) => {
                Self::normalize_expr(left);
                Self::normalize_expr(right);

                // Canonical form: column goes on the left, constant on the right
                let should_swap = matches!(**left, Expr::Constant(_)) && matches!(**right, Expr::Column(_));

                if should_swap {
                    std::mem::swap(left, right);
                    *op = match *op {
                        ComparisonOp::LessThan => ComparisonOp::GreaterThan,
                        ComparisonOp::GreaterThan => ComparisonOp::LessThan,
                        ComparisonOp::LessOrEqual => ComparisonOp::GreaterOrEqual,
                        ComparisonOp::GreaterOrEqual => ComparisonOp::LessOrEqual,
                        ComparisonOp::Equals => ComparisonOp::Equals,
                        ComparisonOp::NotEquals => ComparisonOp::NotEquals,
                    };
                }
            }
            Predicate::IsNull(expr) => {
                Self::normalize_expr(expr);
            }
            Predicate::IsNotNull(expr) => {
                Self::normalize_expr(expr);
            }
            Predicate::Not(inner) => {
                Self::normalize_predicate(inner);
            }
            Predicate::And(left, right) => {
                Self::normalize_predicate(left);
                Self::normalize_predicate(right);
            }
            Predicate::Or(left, right) => {
                Self::normalize_predicate(left);
                Self::normalize_predicate(right);
            }
            Predicate::Between(expr, low, high) => {
                Self::normalize_expr(expr);
                Self::normalize_expr(low);
                Self::normalize_expr(high);

                let new_pred = Predicate::And(
                    Box::new(Predicate::Compare(
                        expr.clone(),
                        ComparisonOp::GreaterOrEqual,
                        low.clone(),
                    )),
                    Box::new(Predicate::Compare(
                        expr.clone(),
                        ComparisonOp::LessOrEqual,
                        high.clone(),
                    )),
                );

                *predicate = new_pred;

                Self::normalize_predicate(predicate);
                return;
            }
            Predicate::In(expr, list) => {
                Self::normalize_expr(expr);
                for item in list {
                    Self::normalize_expr(item);
                }
            }
            Predicate::Like(expr, pattern, regex_opt) => {
                Self::normalize_expr(expr);

                // Pre-compile the SQL LIKE pattern into a regex at plan time, not per-tuple
                if regex_opt.is_none() {
                    let mut regex_pattern = String::from("^");
                    for ch in pattern.chars() {
                        match ch {
                            '%' => regex_pattern.push_str(".*"),
                            '_' => regex_pattern.push('.'),
                            _ => regex_pattern.push_str(&regex::escape(&ch.to_string())),
                        }
                    }
                    regex_pattern.push('$');

                    match Regex::new(&regex_pattern) {
                        Ok(compiled_regex) => {
                            *regex_opt = Some(compiled_regex);
                        }
                        Err(_) => {
                            // Bad pattern — leave it as None; resolve_columns will reject it
                        }
                    }
                }
            }
            Predicate::Exists(inner) => {
                Self::normalize_predicate(inner);
            }
        }
    }

    fn normalize_expr(expr: &mut Expr) {
        let folded = match expr {
            Expr::Add(l, r) => {
                Self::normalize_expr(l);
                Self::normalize_expr(r);

                match (&**l, &**r) {
                    (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => Some(Constant::Null),
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Int(a + b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(a + b))
                    }
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(*a as f64 + b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Float(a + *b as f64))
                    }
                    _ => None,
                }
            }

            Expr::Sub(l, r) => {
                Self::normalize_expr(l);
                Self::normalize_expr(r);

                match (&**l, &**r) {
                    (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => Some(Constant::Null),
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Int(a - b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(a - b))
                    }
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(*a as f64 - b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Float(a - *b as f64))
                    }
                    _ => None,
                }
            }

            Expr::Mul(l, r) => {
                Self::normalize_expr(l);
                Self::normalize_expr(r);

                match (&**l, &**r) {
                    (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => Some(Constant::Null),
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Int(a * b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(a * b))
                    }
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(*a as f64 * b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Float(a * *b as f64))
                    }
                    _ => None,
                }
            }

            Expr::Div(l, r) => {
                Self::normalize_expr(l);
                Self::normalize_expr(r);

                match (&**l, &**r) {
                    (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => Some(Constant::Null),
                    (_, Expr::Constant(Constant::Int(0))) => Some(Constant::Null),
                    (_, Expr::Constant(Constant::Float(b))) if *b == 0.0 => Some(Constant::Null),

                    // PATCH 4: Int / Int → Int (truncating, matches runtime compute_arithmetic).
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Int(a / b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(a / b))
                    }
                    (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Float(b))) => {
                        Some(Constant::Float(*a as f64 / b))
                    }
                    (Expr::Constant(Constant::Float(a)), Expr::Constant(Constant::Int(b))) => {
                        Some(Constant::Float(a / *b as f64))
                    }
                    _ => None,
                }
            }

            Expr::Column(_) | Expr::Constant(_) => None,
        };

        if let Some(c) = folded {
            *expr = Expr::Constant(c);
        }
    }

    /// Walk the predicate tree and bind every column name to its schema index.
    fn resolve_columns(predicate: &mut Predicate, schema: &TableSchema) -> Result<(), String> {
        match predicate {
            Predicate::Compare(left, op, right) => {
                Self::resolve_expr(left, schema)?;
                Self::resolve_expr(right, schema)?;

                if is_null_literal(left) || is_null_literal(right) {
                    return Ok(());
                }

                // Make sure both sides of the comparison are type-compatible
                let left_type = infer_expr_type(left, schema)?;
                let right_type = infer_expr_type(right, schema)?;

                if !(is_numeric(&left_type) && is_numeric(&right_type)
                    || (is_textual(&left_type) && is_textual(&right_type))
                    || (matches!(left_type, SqlDataType::Date)
                        && matches!(right_type, SqlDataType::Date)))
                {
                    return Err(format!(
                        "Type mismatch in comparison: {:?} {} {:?}",
                        left_type, format!("{:?}", op), right_type
                    ));
                }
                Ok(())
            }

            Predicate::IsNull(expr) | Predicate::IsNotNull(expr) => {
                Self::resolve_expr(expr, schema)?;
                Ok(())
            }

            Predicate::Not(inner) => {
                Self::resolve_columns(inner, schema)
            }

            Predicate::And(left, right) | Predicate::Or(left, right) => {
                Self::resolve_columns(left, schema)?;
                Self::resolve_columns(right, schema)?;
                Ok(())
            }

            Predicate::In(expr, list) => {
                Self::resolve_expr(expr, schema)?;

                if is_null_literal(expr) {
                    return Ok(());
                }

                let expr_type = infer_expr_type(expr, schema)?;

                for item in list {
                    Self::resolve_expr(item, schema)?;

                    if is_null_literal(item) {
                        continue;
                    }

                    let item_type = infer_expr_type(item, schema)?;

                    // Each item must be type-compatible with the expression being checked
                    if !(is_numeric(&expr_type) && is_numeric(&item_type)
                        || (is_textual(&expr_type) && is_textual(&item_type))
                        || (matches!(expr_type, SqlDataType::Date)
                            && matches!(item_type, SqlDataType::Date)))
                    {
                        return Err(format!(
                            "Type mismatch in IN clause: expected {:?}, got {:?}",
                            expr_type, item_type
                        ));
                    }
                }
                Ok(())
            }

            Predicate::Like(expr, _pattern, regex_opt) => {
                Self::resolve_expr(expr, schema)?;

                if is_null_literal(expr) {
                    return Ok(());
                }

                // LIKE only makes sense on text columns
                let expr_type = infer_expr_type(expr, schema)?;
                if !is_textual(&expr_type) {
                    return Err(format!("LIKE requires TEXT type, got {:?}", expr_type));
                }

                // The regex should have been compiled in normalize_predicate; if not, bail
                if regex_opt.is_none() {
                    return Err(format!("Invalid SQL LIKE pattern: '{}'", _pattern));
                }

                Ok(())
            }

            Predicate::Exists(inner) => {
                Self::resolve_columns(inner, schema)
            }

            Predicate::Between(_, _, _) => {
                unreachable!("BETWEEN should not reach this stage after normalization")
            }
        }
    }

    /// Walk an expression tree and resolve every column name to its schema index.
    fn resolve_expr(expr: &mut Expr, schema: &TableSchema) -> Result<(), String> {
        match expr {
            Expr::Column(col_ref) => {
                let idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == col_ref.column_name)
                    .ok_or_else(|| format!("Column '{}' not found", col_ref.column_name))?;
                col_ref.column_index = Some(idx);
            }

            Expr::Add(l, r)
            | Expr::Sub(l, r)
            | Expr::Mul(l, r)
            | Expr::Div(l, r) => {
                Self::resolve_expr(l, schema)?;
                Self::resolve_expr(r, schema)?;
            }

            Expr::Constant(_) => {}
        }
        Ok(())
    }

    // Bytecode compilation

    /// Recursively lower the predicate tree into the flat bytecode stream.
    ///
    /// AND short-circuit: compile(left) → JumpIfFalse(past_And) → compile(right) → And
    /// OR short-circuit:  compile(left) → JumpIfTrue(past_Or)   → compile(right) → Or
    ///
    /// The JumpIf* targets are emitted with a placeholder (0) and then back-patched
    /// once we know where the instruction after `And`/`Or` actually lands.
    ///
    /// Heavy objects (`InSet`, `LikePattern`) never go inline into an instruction —
    /// they land in the constant pools and the instruction just carries the index.
    fn compile_predicate(
        pred: &Predicate,
        bytecode: &mut Vec<Instruction>,
        used_columns: &mut HashSet<usize>,
        in_sets: &mut Vec<InSet>,
        like_patterns: &mut Vec<LikePattern>,
    ) -> Result<(), String> {
        match pred {
            Predicate::Compare(left, op, right) => {
                Self::compile_expr(left, bytecode, used_columns);
                Self::compile_expr(right, bytecode, used_columns);
                let cmp_instr = match op {
                    ComparisonOp::Equals        => Instruction::CmpEquals,
                    ComparisonOp::NotEquals     => Instruction::CmpNotEquals,
                    ComparisonOp::LessThan      => Instruction::CmpLessThan,
                    ComparisonOp::LessOrEqual   => Instruction::CmpLessOrEqual,
                    ComparisonOp::GreaterThan   => Instruction::CmpGreaterThan,
                    ComparisonOp::GreaterOrEqual => Instruction::CmpGreaterOrEqual,
                };
                bytecode.push(cmp_instr);
            }

            Predicate::IsNull(expr) => {
                Self::compile_expr(expr, bytecode, used_columns);
                bytecode.push(Instruction::IsNull);
            }

            Predicate::IsNotNull(expr) => {
                Self::compile_expr(expr, bytecode, used_columns);
                bytecode.push(Instruction::IsNotNull);
            }

            Predicate::Not(inner) => {
                Self::compile_predicate(inner, bytecode, used_columns, in_sets, like_patterns)?;
                bytecode.push(Instruction::Not);
            }

            Predicate::And(left, right) => {
                // Emit left, drop in a placeholder JumpIfFalse, emit right, back-patch the jump.
                Self::compile_predicate(left, bytecode, used_columns, in_sets, like_patterns)?;
                let jump_idx = bytecode.len();
                bytecode.push(Instruction::JumpIfFalse(0)); // placeholder — will be patched below
                Self::compile_predicate(right, bytecode, used_columns, in_sets, like_patterns)?;
                bytecode.push(Instruction::And);
                let target = bytecode.len();
                bytecode[jump_idx] = Instruction::JumpIfFalse(target);
            }

            Predicate::Or(left, right) => {
                // Same pattern for OR — placeholder JumpIfTrue, emit right, back-patch.
                Self::compile_predicate(left, bytecode, used_columns, in_sets, like_patterns)?;
                let jump_idx = bytecode.len();
                bytecode.push(Instruction::JumpIfTrue(0)); // placeholder — will be patched below
                Self::compile_predicate(right, bytecode, used_columns, in_sets, like_patterns)?;
                bytecode.push(Instruction::Or);
                let target = bytecode.len();
                bytecode[jump_idx] = Instruction::JumpIfTrue(target);
            }

            Predicate::In(expr, list) => {
                Self::compile_expr(expr, bytecode, used_columns);

                let mut values: Vec<DataValue> = Vec::new();
                let mut has_null = false;

                for item in list {
                    match item {
                        Expr::Constant(Constant::Null) => { has_null = true; }
                        _ => {
                            if let Some(dv) = constant_expr_to_data_value(item) {
                                values.push(dv);
                            }
                            // Non-constant expressions in IN lists are uncommon and deliberately
                            // skipped here — normalize_expr() should have folded them already.
                        }
                    }
                }

                // Store the whole InSet in the pool and emit just the index — keeps instructions small.
                let idx = in_sets.len();
                in_sets.push(InSet { values, has_null });
                bytecode.push(Instruction::In(idx));
            }

            Predicate::Like(expr, pattern, regex_opt) => {
                Self::compile_expr(expr, bytecode, used_columns);

                let compiled_regex = regex_opt.as_ref()
                    .ok_or_else(|| format!("LIKE pattern '{}' was not compiled", pattern))?;

                let like_pattern = Self::compile_like_pattern(pattern, compiled_regex);

                // Same trick for LikePattern — pool it, emit the index only.
                let idx = like_patterns.len();
                like_patterns.push(like_pattern);
                bytecode.push(Instruction::Like(idx));
            }

            Predicate::Exists(inner) => {
                Self::compile_predicate(inner, bytecode, used_columns, in_sets, like_patterns)?;
                // EXISTS just wraps another predicate — Unknown/False both get filtered out
                // by the final Bool(True) check at the end of evaluate_tuple. No extra opcode needed.
            }

            Predicate::Between(_, _, _) => {
                unreachable!("BETWEEN should not reach compilation stage after normalization")
            }
        }
        Ok(())
    }

    /// Lower an expression into a sequence of push/arithmetic instructions.
    fn compile_expr(
        expr: &Expr,
        bytecode: &mut Vec<Instruction>,
        used_columns: &mut HashSet<usize>,
    ) {
        match expr {
            Expr::Column(col_ref) => {
                let idx = col_ref.column_index.expect("column must be resolved before compilation");
                used_columns.insert(idx);
                bytecode.push(Instruction::PushColumn(idx));
            }
            Expr::Constant(c) => {
                match constant_to_data_value(c) {
                    Some(dv) => bytecode.push(Instruction::PushConstant(dv)),
                    None => bytecode.push(Instruction::PushNull),
                }
            }
            Expr::Add(l, r) => {
                Self::compile_expr(l, bytecode, used_columns);
                Self::compile_expr(r, bytecode, used_columns);
                bytecode.push(Instruction::Add);
            }
            Expr::Sub(l, r) => {
                Self::compile_expr(l, bytecode, used_columns);
                Self::compile_expr(r, bytecode, used_columns);
                bytecode.push(Instruction::Sub);
            }
            Expr::Mul(l, r) => {
                Self::compile_expr(l, bytecode, used_columns);
                Self::compile_expr(r, bytecode, used_columns);
                bytecode.push(Instruction::Mul);
            }
            Expr::Div(l, r) => {
                Self::compile_expr(l, bytecode, used_columns);
                Self::compile_expr(r, bytecode, used_columns);
                bytecode.push(Instruction::Div);
            }
        }
    }

    /// Pick the most efficient `LikePattern` variant — avoid the regex machinery when we don't need it.
    fn compile_like_pattern(pattern: &str, _fallback_regex: &Regex) -> LikePattern {
        let has_underscore = pattern.contains('_');

        if !has_underscore {
            let pct_count = pattern.chars().filter(|&c| c == '%').count();

            if pct_count == 1 {
                if let Some(prefix) = pattern.strip_suffix('%') {
                    if !prefix.contains('%') {
                        return LikePattern::StartsWith(prefix.to_string());
                    }
                }
                if let Some(suffix) = pattern.strip_prefix('%') {
                    if !suffix.contains('%') {
                        return LikePattern::EndsWith(suffix.to_string());
                    }
                }
            }

            if pct_count == 2 {
                if let Some(inner) = pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%')) {
                    if !inner.contains('%') {
                        return LikePattern::Contains(inner.to_string());
                    }
                }
            }
        }

        LikePattern::Regex(_fallback_regex.clone())
    }

    // Lazy column extraction

    /// Pull a single logical column out of the raw row bytes without decoding
    /// the full row. Mirrors the layout logic from `deserialize_nullable_row`
    /// in `types/row.rs` but only touches the bytes we actually need.
    ///
    /// Row layout (quick ref):
    /// - `[0..2)`  : u16 num_cols
    /// - `[2..4)`  : u16 num_varlen
    /// - `[4 .. 4+bm_size)` : null bitmap (logical order)
    /// - `[4+bm_size .. 4+bm_size+vt_size)` : var-len offset table (physical var-len order)
    /// - fixed data section : packed in physical order
    /// - var-len payloads : packed in physical order, no length prefix
    fn extract_column(
        types: &[SqlDataType],
        tuple: &[u8],
        target_idx: usize,
    ) -> Result<Option<DataValue>, String> {
        let total_row_size = tuple.len();

        if total_row_size < 4 {
            return Err("Row too short to contain header".to_string());
        }

        // Read the 4-byte header
        let num_cols_stored = u16::from_le_bytes([tuple[0], tuple[1]]) as usize;
        let num_varlen_stored = u16::from_le_bytes([tuple[2], tuple[3]]) as usize;

        if num_cols_stored != types.len() {
            return Err(format!(
                "Header column count {} does not match schema length {}",
                num_cols_stored,
                types.len()
            ));
        }

        let physical = PhysicalSchema::from_logical(types);
        let layout = RowLayout::compute(&physical);

        if num_varlen_stored != physical.num_varlen() {
            return Err(format!(
                "Header var-len count {} does not match schema var-len count {}",
                num_varlen_stored,
                physical.num_varlen()
            ));
        }

        if total_row_size < layout.min_row_size() {
            return Err(format!(
                "Row ({} bytes) shorter than minimum layout size ({} bytes)",
                total_row_size,
                layout.min_row_size()
            ));
        }

        // Check the null bitmap — if the column is null we're done
        let bm_start = RowLayout::bitmap_offset();
        let bitmap = NullBitmap::from_bytes(
            types.len(),
            &tuple[bm_start..bm_start + layout.null_bitmap_size],
        )?;

        if bitmap.is_null(target_idx) {
            return Ok(None);
        }

        let ty = &types[target_idx];
        let phys_idx = physical.logical_to_physical[target_idx];

        if ty.is_fixed_length() {
            // Fixed-length path: O(1) jump straight to the column via pre-computed offsets
            let rank = phys_idx; // fixed cols land in physical slots 0..n_fixed
            let col_start = layout.fixed_data_start + layout.fixed_col_offsets[rank];
            let col_size = ty.fixed_size()
                .expect("fixed-length type must have fixed_size") as usize;
            let value = DataValue::from_bytes(ty, &tuple[col_start..col_start + col_size])?;
            return Ok(Some(value));
        }

        // Variable-length column — scan the offset table to find start/end
        // No allocations: just a two-pointer walk over the offset table entries.
        //
        // BUG FIX: the old code compared the loop index `r` directly to `vl_rank`.
        // That's wrong — `vl_rank` is the rank among *non-null* var-len cols,
        // not among all physical var-len slots. If any earlier var-len col is NULL
        // (raw offset == 0), `r` and the actual non-null rank drift apart and you
        // silently read the wrong payload.
        //
        // The fix: track `seen_non_null` separately, incrementing it only for
        // entries with a real (non-zero) offset. Match on that instead of `r`.
        let vl_rank = phys_idx - physical.num_fixed();
        let vt_start = layout.varlen_table_offset();

        let mut target_start_offset: Option<usize> = None;
        let mut target_end_offset:   Option<usize> = None;
        let mut seen_non_null: usize = 0;

        for r in 0..physical.num_varlen() {
            let slot = vt_start + r * 2;
            let raw = u16::from_le_bytes([tuple[slot], tuple[slot + 1]]) as usize;

            if raw == 0 {
                continue; // NULL sentinel — no payload, don't count it
            }

            if target_start_offset.is_some() {
                // Already found our target's start — this next non-null entry gives us the end.
                target_end_offset = Some(raw);
                break;
            }

            if seen_non_null == vl_rank {
                // Found our target's non-null slot — record the start.
                target_start_offset = Some(raw);
                // Keep going so we can pick up the end boundary in the next iteration.
            }

            seen_non_null += 1;
        }

        match target_start_offset {
            None => {
                // The offset table has 0x0000 for this rank even though the bitmap said non-null.
                // Treat it as null — keeps behavior consistent with how the serializer handles
                // layout inconsistencies rather than hard-erroring.
                Ok(None)
            }
            Some(start) => {
                let end = target_end_offset.unwrap_or(total_row_size);
                let payload = &tuple[start..end];
                let value = DataValue::from_bytes(ty, payload)?;
                Ok(Some(value))
            }
        }
    }

    // Hot loop — the stack machine dispatch

    /// Run the compiled bytecode against a raw tuple byte slice and return True/False/Unknown.
    ///
    /// Hot loop (no allocs): the stack is a fixed `[VMValue; 128]` on the call frame —
    /// nothing hits the heap on the critical path.
    /// `sp` is the stack pointer (next free slot; top = `stack[sp-1]`).
    /// Stack overflow (> 128) returns `Err` immediately.
    ///
    /// All stack accesses are bounds-checked: underflow and type mismatches both
    /// return descriptive `Err` strings rather than panicking.
    pub fn evaluate_tuple(&self, tuple: &[u8]) -> Result<TriValue, String> {
        // Fixed-size array stack — no heap per tuple.
        // Bumped from 32 → 128 slots after hitting overflow with deeply nested predicates;
        // 128 comfortably covers any realistic SQL query depth.
        let mut stack: [VMValue<'_>; 128] = std::array::from_fn(|_| VMValue::Bool(TriValue::Unknown));
        let mut sp: usize = 0; // next free slot (top of stack is stack[sp-1])
        let mut pc: usize = 0;

        // Stack macros — cleaner than writing the bounds check inline every time
        macro_rules! push {
            ($val:expr) => {{
                if sp >= 128 {
                    return Err("Stack limit exceeded (128). Query too complex.".into());
                }
                stack[sp] = $val;
                sp += 1;
            }};
        }
        macro_rules! pop {
            ($ctx:literal) => {{
                if sp == 0 {
                    return Err(concat!("Stack underflow on ", $ctx).into());
                }
                sp -= 1;
                // SAFETY: sp > 0 was checked above, so sp after decrement is valid.
                // Swap in a sentinel so we don't leave stale data sitting in the slot.
                std::mem::replace(&mut stack[sp], VMValue::Bool(TriValue::Unknown))
            }};
        }

        while pc < self.bytecode.len() {
            match &self.bytecode[pc] {
                // Push ops
                Instruction::PushColumn(idx) => {
                    let val = Self::extract_column(&self.column_types, tuple, *idx)?;
                    push!(VMValue::Data(val.map(Cow::Owned)));
                    pc += 1;
                }

                Instruction::PushConstant(dv) => {
                    push!(VMValue::Data(Some(Cow::Borrowed(dv))));
                    pc += 1;
                }

                Instruction::PushNull => {
                    push!(VMValue::Data(None));
                    pc += 1;
                }

                // Arithmetic ops
                Instruction::Add
                | Instruction::Sub
                | Instruction::Mul
                | Instruction::Div => {
                    let right = match pop!("arithmetic (right)") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on arithmetic rhs, got Bool".into()),
                    };
                    let left = match pop!("arithmetic (left)") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on arithmetic lhs, got Bool".into()),
                    };
                    let result = compute_arithmetic(
                        &self.bytecode[pc],
                        left.map(|c| c.into_owned()),
                        right.map(|c| c.into_owned()),
                    )?;
                    push!(VMValue::Data(result.map(Cow::Owned)));
                    pc += 1;
                }

                // Comparison ops
                Instruction::CmpEquals
                | Instruction::CmpNotEquals
                | Instruction::CmpLessThan
                | Instruction::CmpLessOrEqual
                | Instruction::CmpGreaterThan
                | Instruction::CmpGreaterOrEqual => {
                    let right = match pop!("comparison (right)") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on comparison rhs, got Bool".into()),
                    };
                    let left = match pop!("comparison (left)") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on comparison lhs, got Bool".into()),
                    };

                    // Any NULL operand → UNKNOWN (standard 3VL)
                    let tri = match compare_nullable(left.as_deref(), right.as_deref()) {
                        Ok(Some(ordering)) => {
                            let matched = match &self.bytecode[pc] {
                                Instruction::CmpEquals        => ordering == Ordering::Equal,
                                Instruction::CmpNotEquals     => ordering != Ordering::Equal,
                                Instruction::CmpLessThan      => ordering == Ordering::Less,
                                Instruction::CmpLessOrEqual   => ordering != Ordering::Greater,
                                Instruction::CmpGreaterThan   => ordering == Ordering::Greater,
                                Instruction::CmpGreaterOrEqual => ordering != Ordering::Less,
                                _ => unreachable!(),
                            };
                            if matched { TriValue::True } else { TriValue::False }
                        }
                        Ok(None) => TriValue::Unknown, // NULL involved — result is Unknown
                        Err(e)   => return Err(e.to_string()),
                    };

                    push!(VMValue::Bool(tri));
                    pc += 1;
                }

                // Logical ops (3VL)
                Instruction::And => {
                    let right = match pop!("And (right)") {
                        VMValue::Bool(b) => b,
                        VMValue::Data(_) => return Err("Type error: expected Bool on And rhs, got Data".into()),
                    };
                    let left = match pop!("And (left)") {
                        VMValue::Bool(b) => b,
                        VMValue::Data(_) => return Err("Type error: expected Bool on And lhs, got Data".into()),
                    };
                    push!(VMValue::Bool(apply_and(left, right)));
                    pc += 1;
                }

                Instruction::Or => {
                    let right = match pop!("Or (right)") {
                        VMValue::Bool(b) => b,
                        VMValue::Data(_) => return Err("Type error: expected Bool on Or rhs, got Data".into()),
                    };
                    let left = match pop!("Or (left)") {
                        VMValue::Bool(b) => b,
                        VMValue::Data(_) => return Err("Type error: expected Bool on Or lhs, got Data".into()),
                    };
                    push!(VMValue::Bool(apply_or(left, right)));
                    pc += 1;
                }

                Instruction::Not => {
                    let val = match pop!("Not") {
                        VMValue::Bool(b) => b,
                        VMValue::Data(_) => return Err("Type error: expected Bool on Not, got Data".into()),
                    };
                    let result = match val {
                        TriValue::True    => TriValue::False,
                        TriValue::False   => TriValue::True,
                        TriValue::Unknown => TriValue::Unknown,
                    };
                    push!(VMValue::Bool(result));
                    pc += 1;
                }

                Instruction::IsNull => {
                    let val = match pop!("IsNull") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on IsNull, got Bool".into()),
                    };
                    let tri = if val.is_none() { TriValue::True } else { TriValue::False };
                    push!(VMValue::Bool(tri));
                    pc += 1;
                }

                Instruction::IsNotNull => {
                    let val = match pop!("IsNotNull") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on IsNotNull, got Bool".into()),
                    };
                    let tri = if val.is_some() { TriValue::True } else { TriValue::False };
                    push!(VMValue::Bool(tri));
                    pc += 1;
                }

                // Advanced predicates — pool lookup
                Instruction::In(pool_idx) => {
                    // Indices are always emitted by compile_predicate so they're always in bounds;
                    // the debug_assert here is just a sanity check during development.
                    debug_assert!(*pool_idx < self.in_sets.len(), "InSet pool index out of bounds");
                    let set = unsafe { self.in_sets.get_unchecked(*pool_idx) };
                    let val = match pop!("In") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on In, got Bool".into()),
                    };

                    let tri = match val {
                        None => TriValue::Unknown, // NULL IN (...) is always UNKNOWN per SQL spec
                        Some(v) => {
                            // Linear scan — DataValue can't implement Hash (floats), so no HashSet here
                            if set.values.iter().any(|item| item == v.as_ref()) {
                                TriValue::True
                            } else if set.has_null {
                                TriValue::Unknown
                            } else {
                                TriValue::False
                            }
                        }
                    };
                    push!(VMValue::Bool(tri));
                    pc += 1;
                }

                Instruction::Like(pool_idx) => {
                    // Same pool-index safety story as In above.
                    debug_assert!(*pool_idx < self.like_patterns.len(), "LikePattern pool index out of bounds");
                    let pattern = unsafe { self.like_patterns.get_unchecked(*pool_idx) };
                    let val = match pop!("Like") {
                        VMValue::Data(v) => v,
                        VMValue::Bool(_) => return Err("Type error: expected Data on Like, got Bool".into()),
                    };

                    let tri = match val.as_deref() {
                        None => TriValue::Unknown, // NULL LIKE anything → UNKNOWN
                        Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => {
                            let matched = match pattern {
                                LikePattern::StartsWith(prefix) => s.starts_with(prefix.as_str()),
                                LikePattern::EndsWith(suffix)   => s.ends_with(suffix.as_str()),
                                LikePattern::Contains(sub)      => s.contains(sub.as_str()),
                                LikePattern::Regex(re)          => re.is_match(s),
                            };
                            if matched { TriValue::True } else { TriValue::False }
                        }
                        Some(_) => TriValue::Unknown, // shouldn't happen (resolve catches it), but safe to Unknown
                    };
                    push!(VMValue::Bool(tri));
                    pc += 1;
                }

                // Control flow — short-circuit
                //
                // JumpIfFalse contract:
                //   Pop the top. If Bool(False) → push it back and jump to offset.
                //   True or Unknown → push it back and fall through.
                //   Either way, `And` always has its left operand on the stack when it runs.
                Instruction::JumpIfFalse(offset) => {
                    let top = pop!("JumpIfFalse");
                    match &top {
                        VMValue::Bool(TriValue::False) => {
                            push!(top);       // keep False — it becomes the AND result
                            pc = *offset;     // skip RHS + And entirely
                        }
                        _ => {
                            push!(top);       // True/Unknown — leave it for And to consume
                            pc += 1;
                        }
                    }
                }

                // JumpIfTrue contract (OR short-circuit mirror of the above):
                //   Pop the top. If Bool(True) → push it back and jump.
                //   False or Unknown → push it back and fall through.
                //   `Or` always has its left operand on the stack when it runs.
                Instruction::JumpIfTrue(offset) => {
                    let top = pop!("JumpIfTrue");
                    match &top {
                        VMValue::Bool(TriValue::True) => {
                            push!(top);       // keep True — it becomes the OR result
                            pc = *offset;     // skip RHS + Or entirely
                        }
                        _ => {
                            push!(top);       // False/Unknown — leave it for Or to consume
                            pc += 1;
                        }
                    }
                }
            }
        }

        // After the last instruction there should be exactly one Bool left on the stack
        if sp != 1 {
            return Err(format!(
                "Dirty stack after evaluation: expected depth 1, got {}",
                sp
            ));
        }
        match pop!("final result") {
            VMValue::Bool(b) => Ok(b),
            VMValue::Data(_) => Err("Invalid execution result: top of stack is Data, not Bool".into()),
        }
    }
}

// Arithmetic dispatch

/// Apply one arithmetic op to two nullable SQL values.
///
/// Dispatches on `Instruction::{Add, Sub, Mul, Div}`.
///
/// NULL propagation: if either operand is `None`, the result is `None` (SQL NULL).
///
/// Integer division: `Int / Int` → `Int` with truncation toward zero, matching
/// what PostgreSQL does — so `5 / 2 = 2`, not `2.5`. You get float results only
/// when at least one operand is `DoublePrecision`.
///
/// Division by zero: returns `Err` — no silent NaN or infinity.
fn compute_arithmetic(
    op: &Instruction,
    left: Option<DataValue>,
    right: Option<DataValue>,
) -> Result<Option<DataValue>, String> {
    let (l, r) = match (left, right) {
        (Some(lv), Some(rv)) => (lv, rv),
        _ => return Ok(None), // either operand is NULL → result is NULL
    };

    match (&l, &r) {
        // Int × Int
        (DataValue::Int(a), DataValue::Int(b)) => match op {
            Instruction::Add => Ok(Some(DataValue::Int(a.saturating_add(*b)))),
            Instruction::Sub => Ok(Some(DataValue::Int(a.saturating_sub(*b)))),
            Instruction::Mul => Ok(Some(DataValue::Int(a.saturating_mul(*b)))),
            // PostgreSQL integer division: truncates toward zero.
            // 5/2 = 2, -7/2 = -3. Floats go through the DoublePrecision arm instead.
            Instruction::Div => {
                if *b == 0 { return Err("Division by zero".to_string()); }
                Ok(Some(DataValue::Int(a / b)))
            }
            _ => Err("Invalid arithmetic instruction".to_string()),
        },

        // BigInt × BigInt
        (DataValue::BigInt(a), DataValue::BigInt(b)) => match op {
            Instruction::Add => Ok(Some(DataValue::BigInt(a.saturating_add(*b)))),
            Instruction::Sub => Ok(Some(DataValue::BigInt(a.saturating_sub(*b)))),
            Instruction::Mul => Ok(Some(DataValue::BigInt(a.saturating_mul(*b)))),
            // Same PostgreSQL truncating division — consistent with the Int arm.
            Instruction::Div => {
                if *b == 0 { return Err("Division by zero".to_string()); }
                Ok(Some(DataValue::BigInt(a / b)))
            }
            _ => Err("Invalid arithmetic instruction".to_string()),
        },

        // SmallInt × SmallInt
        (DataValue::SmallInt(a), DataValue::SmallInt(b)) => match op {
            Instruction::Add => Ok(Some(DataValue::SmallInt(a.saturating_add(*b)))),
            Instruction::Sub => Ok(Some(DataValue::SmallInt(a.saturating_sub(*b)))),
            Instruction::Mul => Ok(Some(DataValue::SmallInt(a.saturating_mul(*b)))),
            // Same PostgreSQL truncating division — consistent with the Int arm.
            Instruction::Div => {
                if *b == 0 { return Err("Division by zero".to_string()); }
                Ok(Some(DataValue::SmallInt(a / b)))
            }
            _ => Err("Invalid arithmetic instruction".to_string()),
        },

        // DoublePrecision × DoublePrecision
        (DataValue::DoublePrecision(a), DataValue::DoublePrecision(b)) => {
            let res = match op {
                Instruction::Add => a.0 + b.0,
                Instruction::Sub => a.0 - b.0,
                Instruction::Mul => a.0 * b.0,
                Instruction::Div => {
                    if b.0 == 0.0 { return Err("Division by zero".to_string()); }
                    a.0 / b.0
                }
                _ => return Err("Invalid arithmetic instruction".to_string()),
            };
            Ok(Some(DataValue::DoublePrecision(OrderedF64(res))))
        }

        // Cross-type widening — promote the narrower type before re-dispatching
        (DataValue::Int(a), DataValue::DoublePrecision(_)) => {
            compute_arithmetic(op, Some(DataValue::DoublePrecision(OrderedF64(*a as f64))), Some(r.clone()))
        }
        (DataValue::DoublePrecision(_), DataValue::Int(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::DoublePrecision(OrderedF64(*b as f64))))
        }
        (DataValue::SmallInt(a), DataValue::Int(_)) => {
            compute_arithmetic(op, Some(DataValue::Int(*a as i32)), Some(r.clone()))
        }
        (DataValue::Int(_), DataValue::SmallInt(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::Int(*b as i32)))
        }
        (DataValue::Int(a), DataValue::BigInt(_)) => {
            compute_arithmetic(op, Some(DataValue::BigInt(*a as i64)), Some(r.clone()))
        }
        (DataValue::BigInt(_), DataValue::Int(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::BigInt(*b as i64)))
        }
        (DataValue::SmallInt(a), DataValue::BigInt(_)) => {
            compute_arithmetic(op, Some(DataValue::BigInt(*a as i64)), Some(r.clone()))
        }
        (DataValue::BigInt(_), DataValue::SmallInt(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::BigInt(*b as i64)))
        }
        (DataValue::BigInt(a), DataValue::DoublePrecision(_)) => {
            compute_arithmetic(op, Some(DataValue::DoublePrecision(OrderedF64(*a as f64))), Some(r.clone()))
        }
        (DataValue::DoublePrecision(_), DataValue::BigInt(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::DoublePrecision(OrderedF64(*b as f64))))
        }
        (DataValue::SmallInt(a), DataValue::DoublePrecision(_)) => {
            compute_arithmetic(op, Some(DataValue::DoublePrecision(OrderedF64(*a as f64))), Some(r.clone()))
        }
        (DataValue::DoublePrecision(_), DataValue::SmallInt(b)) => {
            compute_arithmetic(op, Some(l.clone()), Some(DataValue::DoublePrecision(OrderedF64(*b as f64))))
        }

        _ => Err(format!("Unsupported types for arithmetic: {:?} and {:?}", l, r)),
    }
}

// Utility functions

fn constant_to_data_value(constant: &Constant) -> Option<DataValue> {
    match constant {
        Constant::Int(i) => Some(DataValue::Int(*i)),
        Constant::Float(f) => Some(DataValue::DoublePrecision(OrderedF64(*f))),
        Constant::Date(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d").ok().map(DataValue::Date),
        Constant::Text(s) => Some(DataValue::Varchar(s.clone())),
        Constant::Null => None,
    }
}

/// Statically evaluate a constant expression at compile time.
/// Returns `None` for anything that involves a column (can't fold those at plan time).
fn constant_expr_to_data_value(expr: &Expr) -> Option<DataValue> {
    match expr {
        Expr::Constant(c) => constant_to_data_value(c),
        _ => None, // column refs and expressions over them can't be folded at plan time
    }
}

// SQL three-valued logic combinators

/// SQL AND under three-valued logic: False dominates; need both True for True.
#[doc(hidden)]
pub fn apply_and(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::False, _) | (_, TriValue::False) => TriValue::False,
        (TriValue::True, TriValue::True) => TriValue::True,
        _ => TriValue::Unknown,
    }
}

/// SQL OR under three-valued logic: True dominates; need both False for False.
#[doc(hidden)]
pub fn apply_or(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::True, _) | (_, TriValue::True) => TriValue::True,
        (TriValue::False, TriValue::False) => TriValue::False,
        _ => TriValue::Unknown,
    }
}

// Public filter API

/// Run the executor over a batch of tuples and return only the ones that matched (True).
pub fn filter_tuples(
    executor: &SelectionExecutor,
    tuples: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, String> {
    let mut result = Vec::new();

    for tuple in tuples.iter() {
        if executor.evaluate_tuple(tuple)? == TriValue::True {
            result.push(tuple.clone());
        }
    }

    Ok(result)
}

/// Like `filter_tuples` but splits results into matched/rejected/unknown buckets — handy for debugging.
pub fn filter_tuples_detailed(
    executor: &SelectionExecutor,
    tuples: Vec<Vec<u8>>,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>, Vec<Vec<u8>>), String> {
    let mut matched = Vec::new();
    let mut rejected = Vec::new();
    let mut unknown = Vec::new();

    for tuple in tuples {
        let evaluation_result = executor.evaluate_tuple(&tuple)?;

        match evaluation_result {
            TriValue::True    => matched.push(tuple),
            TriValue::False   => rejected.push(tuple),
            TriValue::Unknown => unknown.push(tuple),
        }
    }

    Ok((matched, rejected, unknown))
}

/// Count how many tuples matched without actually materializing them.
pub fn count_matching_tuples(
    executor: &SelectionExecutor,
    tuples: &[Vec<u8>],
) -> Result<usize, String> {
    let mut count = 0;

    for tuple in tuples.iter() {
        let evaluation_result = executor.evaluate_tuple(tuple)?;
        if evaluation_result == TriValue::True {
            count += 1;
        }
    }

    Ok(count)
}

/// Streaming filter — evaluates tuples one at a time and calls the output callback for matches.
/// Nothing gets buffered; memory footprint stays flat regardless of result set size.
pub fn filter_tuples_streaming(
    executor: &SelectionExecutor,
    tuple_iter: impl Iterator<Item = Result<Vec<u8>, String>>,
    mut output: impl FnMut(&[u8]),
) -> Result<usize, String> {
    let mut count = 0;
    for tuple_res in tuple_iter {
        let tuple = tuple_res?;
        if executor.evaluate_tuple(&tuple)? == TriValue::True {
            output(&tuple);
            count += 1;
        }
    }
    Ok(count)
}

/// Lazy iterator adapter — wraps any tuple iterator and filters on the fly.
pub fn filter_iter<'a>(
    executor: &'a SelectionExecutor,
    iter: impl Iterator<Item = Result<Vec<u8>, String>> + 'a,
) -> impl Iterator<Item = Result<Vec<u8>, String>> + 'a {
    iter.filter_map(move |tuple_res| match tuple_res {
        Ok(tuple) => match executor.evaluate_tuple(&tuple) {
            Ok(TriValue::True) => Some(Ok(tuple)),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        },
        Err(e) => Some(Err(e)),
    })
}
