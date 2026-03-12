//! Predicate-based tuple filtering with SQL NULL semantics.
//!
//! Provides streaming selection (σ) operator using three-valued logic and offset-based
//! tuple access. Operates in the execution layer without disk I/O.

/// SQL three-valued logic result for predicate evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriValue {
    True,
    False,
    Unknown,
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
/// Leaf nodes are comparisons, internal nodes are logical operators (AND/OR).
/// Evaluation produces three-valued logic results via recursive tree traversal.
#[derive(Debug, Clone)]
pub enum Predicate {
    Equals(ColumnReference, Constant),
    LessThan(ColumnReference, Constant),
    GreaterThan(ColumnReference, Constant),
    LessOrEqual(ColumnReference, Constant),
    GreaterOrEqual(ColumnReference, Constant),
    NotEquals(ColumnReference, Constant),
    IsNull(ColumnReference),
    IsNotNull(ColumnReference),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
}

impl Predicate {
    pub fn and(left: Predicate, right: Predicate) -> Self {
        Predicate::And(Box::new(left), Box::new(right))
    }

    pub fn or(left: Predicate, right: Predicate) -> Self {
        Predicate::Or(Box::new(left), Box::new(right))
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

        let relative_start = self.get_offset(col_idx)? as usize;
        let relative_end = self.get_offset(col_idx + 1)? as usize;
        
        let start_offset = self.field_data_start + relative_start;
        let end_offset = self.field_data_start + relative_end;

        if end_offset > self.tuple_length as usize || start_offset > end_offset {
            return Err(TupleError::FieldRegionOutOfBounds);
        }

        Ok(&self.tuple[start_offset..end_offset])
    }

    /// Deserializes typed value from column's field data.
    pub fn get_value(&self, col_idx: usize, data_type: &str) -> Result<Value, TupleError> {
        if self.is_null(col_idx)? {
            return Ok(Value::Null);
        }

        let field_bytes = self.get_field_bytes(col_idx)?;

        match data_type {
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
                let text = String::from_utf8_lossy(field_bytes).to_string();
                Ok(Value::Date(text))
            }
            "TEXT" | "STRING" => {
                let text = String::from_utf8_lossy(field_bytes).to_string();
                Ok(Value::Text(text))
            }
            _ => {
                let text = String::from_utf8_lossy(field_bytes).to_string();
                Ok(Value::Text(text))
            }
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

/// Executor for predicate-based tuple filtering with resolved column bindings.
pub struct SelectionExecutor {
    predicate: Predicate,
    schema: Table,
}

impl SelectionExecutor {
    /// Creates executor and binds column references to schema positions.
    pub fn new(mut predicate: Predicate, schema: Table) -> Result<Self, String> {
        Self::resolve_columns(&mut predicate, &schema)?;
        Ok(SelectionExecutor { predicate, schema })
    }

    /// Recursively binds column names to schema indices in predicate tree.
    fn resolve_columns(predicate: &mut Predicate, schema: &Table) -> Result<(), String> {
        match predicate {
            Predicate::Equals(col_ref, constant)
            | Predicate::LessThan(col_ref, constant)
            | Predicate::GreaterThan(col_ref, constant)
            | Predicate::LessOrEqual(col_ref, constant)
            | Predicate::GreaterOrEqual(col_ref, constant)
            | Predicate::NotEquals(col_ref, constant) => {
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == col_ref.column_name)
                    .ok_or_else(|| {
                        format!("Column '{}' not found in schema", col_ref.column_name)
                    })?;

                col_ref.column_index = Some(col_idx);

                // Strict type validation: check column type matches constant type
                let data_type = &schema.columns[col_idx].data_type;
                let type_matches = match (data_type.as_str(), constant) {
                    ("INT", Constant::Int(_)) => true,
                    ("FLOAT", Constant::Float(_)) => true,
                    ("DATE", Constant::Date(_)) => true,
                    ("TEXT", Constant::Text(_)) => true,
                    ("STRING", Constant::Text(_)) => true,
                    (_, Constant::Null) => true, // NULL allowed for any type
                    _ => false,
                };

                if !type_matches {
                    return Err(format!(
                        "Type mismatch: Column '{}' of type {} cannot be compared to the provided constant",
                        col_ref.column_name, data_type
                    ));
                }

                Ok(())
            }

            Predicate::IsNull(col_ref) | Predicate::IsNotNull(col_ref) => {
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == col_ref.column_name)
                    .ok_or_else(|| {
                        format!("Column '{}' not found in schema", col_ref.column_name)
                    })?;

                col_ref.column_index = Some(col_idx);
                Ok(())
            }

            Predicate::And(left, right) | Predicate::Or(left, right) => {
                Self::resolve_columns(left, schema)?;
                Self::resolve_columns(right, schema)?;
                Ok(())
            }
        }
    }

    /// Evaluates predicate against tuple, returning three-valued logic result.
    pub fn evaluate_tuple(&self, tuple: &[u8]) -> Result<TriValue, String> {
        let accessor = TupleAccessor::new(tuple, self.schema.columns.len())
            .map_err(|e| format!("Tuple parsing error: {:?}", e))?;

        self.evaluate_predicate(&self.predicate, &accessor)
    }

    /// Recursively evaluates predicate tree with short-circuit logic.
    fn evaluate_predicate(
        &self,
        predicate: &Predicate,
        accessor: &TupleAccessor,
    ) -> Result<TriValue, String> {
        match predicate {
            Predicate::Equals(col_ref, constant) => {
                self.evaluate_comparison(col_ref, constant, accessor, ComparisonOp::Equals)
            }
            Predicate::LessThan(col_ref, constant) => {
                self.evaluate_comparison(col_ref, constant, accessor, ComparisonOp::LessThan)
            }
            Predicate::GreaterThan(col_ref, constant) => {
                self.evaluate_comparison(col_ref, constant, accessor, ComparisonOp::GreaterThan)
            }
            Predicate::LessOrEqual(col_ref, constant) => {
                self.evaluate_comparison(col_ref, constant, accessor, ComparisonOp::LessOrEqual)
            }
            Predicate::GreaterOrEqual(col_ref, constant) => {
                self.evaluate_comparison(
                    col_ref,
                    constant,
                    accessor,
                    ComparisonOp::GreaterOrEqual,
                )
            }
            Predicate::NotEquals(col_ref, constant) => {
                self.evaluate_comparison(col_ref, constant, accessor, ComparisonOp::NotEquals)
            }

            Predicate::IsNull(col_ref) => {
                self.evaluate_is_null(col_ref, accessor)
            }
            Predicate::IsNotNull(col_ref) => {
                self.evaluate_is_not_null(col_ref, accessor)
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
        }
    }

    /// Evaluates comparison operator between column value and constant.
    fn evaluate_comparison(
        &self,
        col_ref: &ColumnReference,
        constant: &Constant,
        accessor: &TupleAccessor,
        op: ComparisonOp,
    ) -> Result<TriValue, String> {
        let col_idx = col_ref
            .column_index
            .ok_or_else(|| format!("Column '{}' index not resolved", col_ref.column_name))?;

        let data_type = &self.schema.columns[col_idx].data_type;

        let column_value = accessor
            .get_value(col_idx, data_type)
            .map_err(|e| format!("Failed to extract column value: {:?}", e))?;

        let constant_value = constant_to_value(constant);

        Ok(compare_values(&column_value, &constant_value, op))
    }

    /// Evaluates IS NULL predicate by checking the NULL bitmap.
    fn evaluate_is_null(
        &self,
        col_ref: &ColumnReference,
        accessor: &TupleAccessor,
    ) -> Result<TriValue, String> {
        let col_idx = col_ref
            .column_index
            .ok_or_else(|| format!("Column '{}' index not resolved", col_ref.column_name))?;

        let is_null = accessor
            .is_null(col_idx)
            .map_err(|e| format!("Failed to check NULL status: {:?}", e))?;

        if is_null {
            Ok(TriValue::True)
        } else {
            Ok(TriValue::False)
        }
    }

    /// Evaluates IS NOT NULL predicate by checking the NULL bitmap.
    fn evaluate_is_not_null(
        &self,
        col_ref: &ColumnReference,
        accessor: &TupleAccessor,
    ) -> Result<TriValue, String> {
        let col_idx = col_ref
            .column_index
            .ok_or_else(|| format!("Column '{}' index not resolved", col_ref.column_name))?;

        let is_null = accessor
            .is_null(col_idx)
            .map_err(|e| format!("Failed to check NULL status: {:?}", e))?;

        if is_null {
            Ok(TriValue::False)
        } else {
            Ok(TriValue::True)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComparisonOp {
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
    const EPSILON: f64 = 1e-9;

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

        _ => false,
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
    tuples: Vec<Vec<u8>>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut result = Vec::new();

    for tuple in tuples {
        if executor.evaluate_tuple(&tuple)? == TriValue::True {
            result.push(tuple);
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
    tuples: Vec<Vec<u8>>,
) -> Result<usize, String> {
    let mut count = 0;

    for tuple in tuples {
        let evaluation_result = executor.evaluate_tuple(&tuple)?;
        if evaluation_result == TriValue::True {
            count += 1;
        }
    }

    Ok(count)
}
