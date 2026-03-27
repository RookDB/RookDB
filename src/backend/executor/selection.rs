//! Predicate-based tuple filtering with SQL NULL semantics.
//!
//! Provides streaming selection (σ) operator using three-valued logic and offset-based
//! tuple access. Operates in the execution layer without disk I/O.

use regex::Regex;

/// SQL three-valued logic result for predicate evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriValue {
    True,
    False,
    Unknown,
}

/// Internal type representation for type checking at planning time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Text,
    Date,
    Null,
}

/// Literal constant in predicate expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null,
}

/// Expression node for predicate operands (columns, constants, arithmetic).
#[derive(Debug, Clone)]
pub enum Expr {
    Column(ColumnReference),
    Constant(Constant),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

/// Reference to a table column, optionally resolved to schema position.
#[derive(Debug, Clone)]
pub struct ColumnReference {
    pub column_name: String,
    pub column_index: Option<usize>,
}

impl ColumnReference {
    /// Creates column reference by name. Index resolved during executor initialization.
    pub fn new(column_name: String) -> Self {
        ColumnReference {
            column_name,
            column_index: None,
        }
    }

    /// Creates column reference with pre-resolved schema index.
    pub fn with_index(column_name: String, column_index: usize) -> Self {
        ColumnReference {
            column_name,
            column_index: Some(column_index),
        }
    }
}

/// Predicate expression tree for tuple filtering.
///
/// Leaf nodes are comparisons (using Expr operands), internal nodes are logical operators.
/// Evaluation produces three-valued logic results via recursive tree traversal.
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
    // Not SQL EXISTS, only logical wrapper
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

/// Errors during tuple parsing or field access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleError {
    TupleTooShort,
    LengthMismatch,
    OffsetOutOfBounds,
    OffsetNotMonotonic,
    FieldRegionOutOfBounds,
    IncompleteOffsetArray,
    InvalidColumnIndex,
}

/// Typed field value decoded from tuple storage.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i32),
    Float(f64),
    Date(String),
    Text(String),
    Null,
}

/// Tuple decoder providing constant-time field access via offset indirection.
///
/// Physical layout: [Header | NULL Bitmap | Offset Array | Field Data]
/// Offsets stored in the array are relative to the start of the field data region.
/// A sentinel offset at position num_columns marks the end of the final field.
pub struct TupleAccessor<'a> {
    tuple: &'a [u8],
    tuple_length: u32,
    num_columns: usize,
    null_bitmap_start: usize,
    null_bitmap_len: usize,
    offset_array_start: usize,
    field_data_start: usize,
}

impl<'a> TupleAccessor<'a> {
    /// Decodes tuple structure and validates against expected column count.
    ///
    /// Validates header integrity, offset array monotonicity, and buffer bounds.
    pub fn new(tuple: &'a [u8], num_columns: usize) -> Result<Self, TupleError> {
        let min_tuple_size = 8 + ((num_columns + 7) / 8) + ((num_columns + 1) * 4);
        if tuple.len() < min_tuple_size {
            return Err(TupleError::TupleTooShort);
        }

        // Header format:
        // [0-3]: tuple_length (u32)
        // [4]:   version (u8)
        // [5]:   flags (u8)
        // [6-7]: column_count (u16)
        let tuple_length = u32::from_le_bytes(
            tuple[0..4]
                .try_into()
                .map_err(|_| TupleError::TupleTooShort)?,
        );
        let _version = tuple[4];
        let _flags = tuple[5];
        let header_column_count = u16::from_le_bytes(
            tuple[6..8]
                .try_into()
                .map_err(|_| TupleError::TupleTooShort)?,
        ) as usize;

        if header_column_count != num_columns {
            return Err(TupleError::IncompleteOffsetArray);
        }

        if tuple_length as usize != tuple.len() {
            return Err(TupleError::LengthMismatch);
        }

        let null_bitmap_start = 8;
        let null_bitmap_len = (num_columns + 7) / 8;
        let offset_array_start = null_bitmap_start + null_bitmap_len;
        let field_data_start = offset_array_start + ((num_columns + 1) * 4);

        if tuple.len() < field_data_start {
            return Err(TupleError::IncompleteOffsetArray);
        }

        let accessor = TupleAccessor {
            tuple,
            tuple_length,
            num_columns,
            null_bitmap_start,
            null_bitmap_len,
            offset_array_start,
            field_data_start,
        };

        accessor.validate_offsets()?;

        Ok(accessor)
    }

    /// Fast constructor that skips validation (use only when tuple is known to be valid).
    pub fn new_unchecked(tuple: &'a [u8], num_columns: usize) -> Self {
        let tuple_length = u32::from_le_bytes([tuple[0], tuple[1], tuple[2], tuple[3]]);

        let null_bitmap_start = 8;
        let null_bitmap_len = (num_columns + 7) / 8;
        let offset_array_start = null_bitmap_start + null_bitmap_len;
        let field_data_start = offset_array_start + ((num_columns + 1) * 4);

        TupleAccessor {
            tuple,
            tuple_length,
            num_columns,
            null_bitmap_start,
            null_bitmap_len,
            offset_array_start,
            field_data_start,
        }
    }

    /// Validates offset array: monotonicity and bounds.
    fn validate_offsets(&self) -> Result<(), TupleError> {
        let mut prev_offset = 0u32;

        for col_idx in 0..=self.num_columns {
            let relative_offset = self.get_offset(col_idx)?;
            
            if relative_offset < prev_offset {
                return Err(TupleError::OffsetNotMonotonic);
            }
            
            let absolute_offset = self.field_data_start as u32 + relative_offset;
            if absolute_offset > self.tuple_length {
                return Err(TupleError::OffsetOutOfBounds);
            }

            prev_offset = relative_offset;
        }

        Ok(())
    }

    /// Reads column offset from offset array. Includes sentinel at index num_columns.
    fn get_offset(&self, col_idx: usize) -> Result<u32, TupleError> {
        if col_idx > self.num_columns {
            return Err(TupleError::InvalidColumnIndex);
        }

        let offset_pos = self.offset_array_start + (col_idx * 4);
        let offset = u32::from_le_bytes(
            self.tuple[offset_pos..offset_pos + 4]
                .try_into()
                .map_err(|_| TupleError::OffsetOutOfBounds)?,
        );

        Ok(offset)
    }

    /// Tests NULL bitmap for column nullability.
    pub fn is_null(&self, col_idx: usize) -> Result<bool, TupleError> {
        if col_idx >= self.num_columns {
            return Err(TupleError::InvalidColumnIndex);
        }

        let byte_idx = col_idx / 8;
        let bit_idx = col_idx % 8;
        let byte = self.tuple[self.null_bitmap_start + byte_idx];
        let is_null = (byte & (1 << bit_idx)) != 0;

        Ok(is_null)
    }

    /// Returns raw field bytes. Empty slice for NULL fields.
    pub fn get_field_bytes(&self, col_idx: usize) -> Result<&[u8], TupleError> {
        if col_idx >= self.num_columns {
            return Err(TupleError::InvalidColumnIndex);
        }

        if self.is_null(col_idx)? {
            return Ok(&[]);
        }

        // Read offsets only once
        let relative_start = self.get_offset(col_idx)?;
        let relative_end = self.get_offset(col_idx + 1)?;

        let start_offset = self.field_data_start + relative_start as usize;
        let end_offset = self.field_data_start + relative_end as usize;

        let tuple_len = self.tuple.len();

        // Check start_offset within bounds
        if start_offset > tuple_len {
            return Err(TupleError::FieldRegionOutOfBounds);
        }

        // Check end_offset within bounds
        if end_offset > tuple_len {
            return Err(TupleError::FieldRegionOutOfBounds);
        }

        // Ensure offsets are monotonic
        if start_offset > end_offset {
            return Err(TupleError::OffsetNotMonotonic);
        }

        Ok(&self.tuple[start_offset..end_offset])
    }

    /// Deserializes typed value from column's field data.
    pub fn get_value(&self, col_idx: usize, data_type: &str) -> Result<Value, TupleError> {
        if self.is_null(col_idx)? {
            return Ok(Value::Null);
        }

        let field_bytes = self.get_field_bytes(col_idx)?;

        match data_type.to_uppercase().as_str() {
            "INT" => {
                if field_bytes.len() == 4 {
                    let val = i32::from_le_bytes(field_bytes.try_into().unwrap());
                    Ok(Value::Int(val))
                } else {
                    Err(TupleError::FieldRegionOutOfBounds)
                }
            }
            "FLOAT" => {
                if field_bytes.len() == 8 {
                    let val = f64::from_le_bytes(field_bytes.try_into().unwrap());
                    Ok(Value::Float(val))
                } else {
                    Err(TupleError::FieldRegionOutOfBounds)
                }
            }
            "DATE" => {
                let text = std::str::from_utf8(field_bytes)
                    .map_err(|_| TupleError::FieldRegionOutOfBounds)?;
                Ok(Value::Date(text.to_owned()))
            }
            "TEXT" | "STRING" => {
                let text = std::str::from_utf8(field_bytes)
                    .map_err(|_| TupleError::FieldRegionOutOfBounds)?;
                Ok(Value::Text(text.to_owned()))
            }
            _ => {
                let text = std::str::from_utf8(field_bytes)
                    .map_err(|_| TupleError::FieldRegionOutOfBounds)?;
                Ok(Value::Text(text.to_owned()))
            }
        }
    }

    /// Fast value deserializer using enum-based type matching (avoids string comparisons).
    pub fn get_value_fast(
        &self,
        col_idx: usize,
        data_type: &DataType,
    ) -> Result<Value, TupleError> {
        if self.is_null(col_idx)? {
            return Ok(Value::Null);
        }

        let field_bytes = self.get_field_bytes(col_idx)?;

        match data_type {
            DataType::Int => {
                if field_bytes.len() == 4 {
                    let val = i32::from_le_bytes(field_bytes.try_into().unwrap());
                    Ok(Value::Int(val))
                } else {
                    Err(TupleError::FieldRegionOutOfBounds)
                }
            }
            DataType::Float => {
                if field_bytes.len() == 8 {
                    let val = f64::from_le_bytes(field_bytes.try_into().unwrap());
                    Ok(Value::Float(val))
                } else {
                    Err(TupleError::FieldRegionOutOfBounds)
                }
            }
            DataType::Date => {
                let text = std::str::from_utf8(field_bytes)
                    .map_err(|_| TupleError::FieldRegionOutOfBounds)?;
                Ok(Value::Date(text.to_owned()))
            }
            DataType::Text => {
                let text = std::str::from_utf8(field_bytes)
                    .map_err(|_| TupleError::FieldRegionOutOfBounds)?;
                Ok(Value::Text(text.to_owned()))
            }
            DataType::Null => Ok(Value::Null),
        }
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub fn tuple_length(&self) -> u32 {
        self.tuple_length
    }
}

use crate::catalog::types::Table;

/// Infers the data type of an expression based on schema and constants.
fn infer_expr_type(expr: &Expr, schema: &Table) -> Result<DataType, String> {
    match expr {
        Expr::Column(col_ref) => {
            let idx = col_ref.column_index
                .ok_or_else(|| "Column not resolved".to_string())?;
            let schema_type = schema.columns[idx].data_type.to_uppercase();

            match schema_type.as_str() {
                "INT" => Ok(DataType::Int),
                "FLOAT" => Ok(DataType::Float),
                "TEXT" | "STRING" => Ok(DataType::Text),
                "DATE" => Ok(DataType::Date),
                _ => Err(format!("Unknown type: {}", schema_type)),
            }
        }
        Expr::Constant(c) => {
            match c {
                Constant::Int(_) => Ok(DataType::Int),
                Constant::Float(_) => Ok(DataType::Float),
                Constant::Text(_) => Ok(DataType::Text),
                Constant::Date(_) => Ok(DataType::Date),
                Constant::Null => Ok(DataType::Null),
            }
        }
        Expr::Add(l, r) | Expr::Sub(l, r) | Expr::Mul(l, r) => {
            let left_type = infer_expr_type(l, schema)?;
            let right_type = infer_expr_type(r, schema)?;

            // Validate numeric types
            match (&left_type, &right_type) {
                (DataType::Null, _) | (_, DataType::Null) => Ok(DataType::Null),
                (DataType::Int, DataType::Int) => Ok(DataType::Int),
                (DataType::Float, DataType::Float) => Ok(DataType::Float),
                (DataType::Int, DataType::Float) | (DataType::Float, DataType::Int) => Ok(DataType::Float),
                _ => Err(format!("Arithmetic operation requires numeric types, got {:?} and {:?}", left_type, right_type)),
            }
        }

        Expr::Div(l, r) => {
            let left_type = infer_expr_type(l, schema)?;
            let right_type = infer_expr_type(r, schema)?;

            // Int / Int promotes to Float, matching runtime behavior
            match (&left_type, &right_type) {
                (DataType::Null, _) | (_, DataType::Null) => Ok(DataType::Null),
                (DataType::Int, DataType::Int) => Ok(DataType::Float),
                (DataType::Int, DataType::Float)
                | (DataType::Float, DataType::Int)
                | (DataType::Float, DataType::Float) => Ok(DataType::Float),
                _ => Err(format!("Arithmetic operation requires numeric types, got {:?} and {:?}", left_type, right_type)),
            }
        }
    }
}

/// Executor for predicate-based tuple filtering with resolved column bindings.
pub struct SelectionExecutor {
    predicate: Predicate,
    schema: Table,
    column_types: Vec<DataType>,
}

impl SelectionExecutor {
    /// Creates executor and binds column references to schema positions.
    pub fn new(mut predicate: Predicate, schema: Table) -> Result<Self, String> {
        Self::normalize_predicate(&mut predicate);
        Self::resolve_columns(&mut predicate, &schema)?;

        // Pre-parse column types once for fast access
        let column_types = schema
            .columns
            .iter()
            .map(|c| match c.data_type.as_str() {
                "INT" => DataType::Int,
                "FLOAT" => DataType::Float,
                "TEXT" | "STRING" => DataType::Text,
                "DATE" => DataType::Date,
                _ => DataType::Null,
            })
            .collect();

        // All column references are resolved during initialization; runtime assumes validity.
        Ok(SelectionExecutor {
            predicate,
            schema,
            column_types,
        })
    }

    /// Preprocessing step for initialization optimization.
    fn normalize_predicate(predicate: &mut Predicate) {
        match predicate {
            Predicate::Compare(left, op, right) => {
                Self::normalize_expr(left);
                Self::normalize_expr(right);

                // Normalize: if left is constant and right is column, swap
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

                // Compile regex pattern at planning time
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
                            // Invalid pattern, leave None; will fail at validation
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
                (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => None,
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
                (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => None,
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
                (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => None,
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
                (Expr::Constant(Constant::Null), _) | (_, Expr::Constant(Constant::Null)) => None,
                (_, Expr::Constant(Constant::Int(0))) => None,
                (_, Expr::Constant(Constant::Float(b))) if *b == 0.0 => None,

                (Expr::Constant(Constant::Int(a)), Expr::Constant(Constant::Int(b))) => {
                    Some(Constant::Float(*a as f64 / *b as f64))
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

    /// Recursively binds column names to schema indices in predicate tree.
    fn resolve_columns(predicate: &mut Predicate, schema: &Table) -> Result<(), String> {
        match predicate {
            Predicate::Compare(left, op, right) => {
                Self::resolve_expr(left, schema)?;
                Self::resolve_expr(right, schema)?;

                // Type validation
                let left_type = infer_expr_type(left, schema)?;
                let right_type = infer_expr_type(right, schema)?;

                // Check type compatibility
                match (&left_type, &right_type) {
                    (DataType::Null, _) | (_, DataType::Null) => {
                        // NULL is compatible with any type
                    }
                    (DataType::Int, DataType::Int) | (DataType::Float, DataType::Float) => {
                        // Same numeric types
                    }
                    (DataType::Int, DataType::Float) | (DataType::Float, DataType::Int) => {
                        // Coercible numeric types
                    }
                    (DataType::Text, DataType::Text) => {
                        // Text comparison
                    }
                    (DataType::Date, DataType::Date) => {
                        // Date comparison
                    }
                    _ => {
                        return Err(format!(
                            "Type mismatch in comparison: {:?} {} {:?}",
                            left_type, format!("{:?}", op), right_type
                        ));
                    }
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

                let expr_type = infer_expr_type(expr, schema)?;

                for item in list {
                    Self::resolve_expr(item, schema)?;

                    let item_type = infer_expr_type(item, schema)?;

                    // Validate type compatibility
                    match (&expr_type, &item_type) {
                        (DataType::Null, _) | (_, DataType::Null) => {
                            // NULL is compatible
                        }
                        (DataType::Int, DataType::Int) | (DataType::Float, DataType::Float) => {}
                        (DataType::Int, DataType::Float) | (DataType::Float, DataType::Int) => {}
                        (DataType::Text, DataType::Text) => {}
                        (DataType::Date, DataType::Date) => {}
                        _ => {
                            return Err(format!(
                                "Type mismatch in IN clause: expected {:?}, got {:?}",
                                expr_type, item_type
                            ));
                        }
                    }
                }
                Ok(())
            }

            Predicate::Like(expr, _pattern, regex_opt) => {
                Self::resolve_expr(expr, schema)?;

                // Validate LIKE requires TEXT
                let expr_type = infer_expr_type(expr, schema)?;
                if expr_type != DataType::Text && expr_type != DataType::Null {
                    return Err(format!("LIKE requires TEXT type, got {:?}", expr_type));
                }

                // Ensure regex was compiled successfully
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

    /// Recursively binds column names to schema indices in an expression tree.
    fn resolve_expr(expr: &mut Expr, schema: &Table) -> Result<(), String> {
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

    /// Evaluates predicate against tuple, returning three-valued logic result.
    pub fn evaluate_tuple(&self, tuple: &[u8]) -> Result<TriValue, String> {
        let accessor = TupleAccessor::new_unchecked(tuple, self.schema.columns.len());

        self.evaluate_predicate(&self.predicate, &accessor)
    }

    /// Recursively evaluates predicate tree with short-circuit logic.
    fn evaluate_predicate(
        &self,
        predicate: &Predicate,
        accessor: &TupleAccessor,
    ) -> Result<TriValue, String> {
        match predicate {
            Predicate::Compare(left, op, right) => {
                let lval = self.evaluate_expr(left, accessor)?;
                let rval = self.evaluate_expr(right, accessor)?;
                Ok(compare_values(&lval, &rval, *op))
            }

            Predicate::IsNull(expr) => {
                let val = self.evaluate_expr(expr, accessor)?;
                Ok(if matches!(val, Value::Null) { TriValue::True } else { TriValue::False })
            }

            Predicate::IsNotNull(expr) => {
                let val = self.evaluate_expr(expr, accessor)?;
                Ok(if matches!(val, Value::Null) { TriValue::False } else { TriValue::True })
            }

            Predicate::Not(inner) => {
                match self.evaluate_predicate(inner, accessor)? {
                    TriValue::True => Ok(TriValue::False),
                    TriValue::False => Ok(TriValue::True),
                    TriValue::Unknown => Ok(TriValue::Unknown),
                }
            }

            Predicate::And(left, right) => {
                let left_result = self.evaluate_predicate(left, accessor)?;

                if left_result == TriValue::False {
                    return Ok(TriValue::False);
                }

                let right_result = self.evaluate_predicate(right, accessor)?;
                Ok(apply_and(left_result, right_result))
            }
            Predicate::Or(left, right) => {
                let left_result = self.evaluate_predicate(left, accessor)?;

                if left_result == TriValue::True {
                    return Ok(TriValue::True);
                }

                let right_result = self.evaluate_predicate(right, accessor)?;
                Ok(apply_or(left_result, right_result))
            }

            Predicate::In(expr, list) => {
                let val = self.evaluate_expr(expr, accessor)?;

                if matches!(val, Value::Null) {
                    return Ok(TriValue::Unknown);
                }

                let mut has_null = false;

                for item in list.iter() {
                    let item_val = self.evaluate_expr(item, accessor)?;

                    if matches!(item_val, Value::Null) {
                        has_null = true;
                        continue;
                    }

                    if compare_values(&val, &item_val, ComparisonOp::Equals) == TriValue::True {
                        return Ok(TriValue::True);
                    }
                }

                if has_null {
                    Ok(TriValue::Unknown)
                } else {
                    Ok(TriValue::False)
                }
            }

            Predicate::Like(expr, _pattern, regex_opt) => {
                let val = self.evaluate_expr(expr, accessor)?;

                if matches!(val, Value::Null) {
                    return Ok(TriValue::Unknown);
                }

                match val {
                    Value::Text(s) => {
                        // Use precompiled regex
                        let regex = regex_opt.as_ref().unwrap();

                        if regex.is_match(&s) {
                            Ok(TriValue::True)
                        } else {
                            Ok(TriValue::False)
                        }
                    }
                    _ => Ok(TriValue::Unknown),
                }
            }

            Predicate::Exists(inner) => {
                let result = self.evaluate_predicate(inner, accessor)?;

                match result {
                    TriValue::True => Ok(TriValue::True),
                    _ => Ok(TriValue::False),
                }
            }

            Predicate::Between(_, _, _) => {
                unreachable!("BETWEEN should not reach this stage after normalization")
            }
        }
    }

    /// Evaluates an expression tree against a tuple row, returning a typed Value.
    fn evaluate_expr(
        &self,
        expr: &Expr,
        accessor: &TupleAccessor,
    ) -> Result<Value, String> {
        match expr {
            Expr::Column(col_ref) => {
                let idx = col_ref.column_index.ok_or("Unresolved column")?;
                accessor
                    .get_value_fast(idx, &self.column_types[idx])
                    .map_err(|e| format!("{:?}", e))
            }
            Expr::Constant(c) => Ok(constant_to_value(c)),

            Expr::Add(l, r) => {
                let lv = self.evaluate_expr(l, accessor)?;
                let rv = self.evaluate_expr(r, accessor)?;
                if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
                    return Ok(Value::Null);
                }
                match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
                    _ => Err("Type mismatch in ADD".into()),
                }
            }

            Expr::Sub(l, r) => {
                let lv = self.evaluate_expr(l, accessor)?;
                let rv = self.evaluate_expr(r, accessor)?;
                if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
                    return Ok(Value::Null);
                }
                match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
                    _ => Err("Type mismatch in SUB".into()),
                }
            }

            Expr::Mul(l, r) => {
                let lv = self.evaluate_expr(l, accessor)?;
                let rv = self.evaluate_expr(r, accessor)?;
                if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
                    return Ok(Value::Null);
                }
                match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
                    _ => Err("Type mismatch in MUL".into()),
                }
            }

            Expr::Div(l, r) => {
                let lv = self.evaluate_expr(l, accessor)?;
                let rv = self.evaluate_expr(r, accessor)?;
                if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
                    return Ok(Value::Null);
                }
                match (lv, rv) {
                    (_, Value::Int(0)) | (_, Value::Float(0.0)) => {
                        return Ok(Value::Null)
                    }
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Float(a as f64 / b as f64)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
                    _ => Err("Type mismatch in DIV".into()),
                }
            }
        }
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

fn constant_to_value(constant: &Constant) -> Value {
    match constant {
        Constant::Int(i) => Value::Int(*i),
        Constant::Float(f) => Value::Float(*f),
        Constant::Date(s) => Value::Date(s.clone()),
        Constant::Text(s) => Value::Text(s.clone()),
        Constant::Null => Value::Null,
    }
}

/// Compares values using specified operator. Returns Unknown for NULL operands.
fn compare_values(left: &Value, right: &Value, op: ComparisonOp) -> TriValue {
    const EPSILON: f64 = 1e-2;

    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return TriValue::Unknown;
    }

    let result = match (left, right, op) {
        (Value::Int(l), Value::Int(r), ComparisonOp::Equals) => l == r,
        (Value::Int(l), Value::Int(r), ComparisonOp::LessThan) => l < r,
        (Value::Int(l), Value::Int(r), ComparisonOp::GreaterThan) => l > r,
        (Value::Int(l), Value::Int(r), ComparisonOp::LessOrEqual) => l <= r,
        (Value::Int(l), Value::Int(r), ComparisonOp::GreaterOrEqual) => l >= r,
        (Value::Int(l), Value::Int(r), ComparisonOp::NotEquals) => l != r,

        (Value::Float(l), Value::Float(r), op) => {
            let diff = (l - r).abs();
            match op {
                ComparisonOp::Equals => diff < EPSILON,
                ComparisonOp::NotEquals => diff >= EPSILON,
                ComparisonOp::LessThan => l < r && diff >= EPSILON,
                ComparisonOp::GreaterThan => l > r && diff >= EPSILON,
                ComparisonOp::LessOrEqual => l < r || diff < EPSILON,
                ComparisonOp::GreaterOrEqual => l > r || diff < EPSILON,
            }
        }

        (Value::Date(l), Value::Date(r), ComparisonOp::Equals) => l == r,
        (Value::Date(l), Value::Date(r), ComparisonOp::LessThan) => l < r,
        (Value::Date(l), Value::Date(r), ComparisonOp::GreaterThan) => l > r,
        (Value::Date(l), Value::Date(r), ComparisonOp::LessOrEqual) => l <= r,
        (Value::Date(l), Value::Date(r), ComparisonOp::GreaterOrEqual) => l >= r,
        (Value::Date(l), Value::Date(r), ComparisonOp::NotEquals) => l != r,

        (Value::Text(l), Value::Text(r), ComparisonOp::Equals) => l == r,
        (Value::Text(l), Value::Text(r), ComparisonOp::LessThan) => l < r,
        (Value::Text(l), Value::Text(r), ComparisonOp::GreaterThan) => l > r,
        (Value::Text(l), Value::Text(r), ComparisonOp::LessOrEqual) => l <= r,
        (Value::Text(l), Value::Text(r), ComparisonOp::GreaterOrEqual) => l >= r,
        (Value::Text(l), Value::Text(r), ComparisonOp::NotEquals) => l != r,

        (Value::Int(l), Value::Float(_), _) => {
            return compare_values(&Value::Float(*l as f64), right, op);
        }

        (Value::Float(_), Value::Int(r), _) => {
            return compare_values(left, &Value::Float(*r as f64), op);
        }

        // TEXT → INT coercion
        (Value::Text(l), Value::Int(_), op) => {
            if let Ok(parsed) = l.parse::<i32>() {
                return compare_values(&Value::Int(parsed), right, op);
            }
            return TriValue::Unknown;
        }

        (Value::Int(_), Value::Text(r), op) => {
            if let Ok(parsed) = r.parse::<i32>() {
                return compare_values(left, &Value::Int(parsed), op);
            }
            return TriValue::Unknown;
        }

        // TEXT → FLOAT coercion
        (Value::Text(l), Value::Float(_), op) => {
            if let Ok(parsed) = l.parse::<f64>() {
                return compare_values(&Value::Float(parsed), right, op);
            }
            return TriValue::Unknown;
        }

        (Value::Float(_), Value::Text(r), op) => {
            if let Ok(parsed) = r.parse::<f64>() {
                return compare_values(left, &Value::Float(parsed), op);
            }
            return TriValue::Unknown;
        }

        _ => return TriValue::Unknown,
    };

    if result {
        TriValue::True
    } else {
        TriValue::False
    }
}

/// SQL AND with three-valued logic. False dominates, both True required for True.
#[doc(hidden)]
pub fn apply_and(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::False, _) | (_, TriValue::False) => TriValue::False,
        (TriValue::True, TriValue::True) => TriValue::True,
        _ => TriValue::Unknown,
    }
}

/// SQL OR with three-valued logic. True dominates, both False required for False.
#[doc(hidden)]
pub fn apply_or(left: TriValue, right: TriValue) -> TriValue {
    match (left, right) {
        (TriValue::True, _) | (_, TriValue::True) => TriValue::True,
        (TriValue::False, TriValue::False) => TriValue::False,
        _ => TriValue::Unknown,
    }
}

/// Filters tuple stream, emitting only tuples matching the predicate (True).
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

/// Filters tuples into matched/rejected/unknown categories for diagnostics.
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
            TriValue::True => matched.push(tuple),
            TriValue::False => rejected.push(tuple),
            TriValue::Unknown => unknown.push(tuple),
        }
    }

    Ok((matched, rejected, unknown))
}

/// Counts matching tuples without materialization.
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

/// Filters tuples using a zero-buffering streaming model.
/// Evaluates an iterator of tuples and pushes matches to a callback function.
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

/// Iterator-based selection filter. Consumes and yields tuples lazily.
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
