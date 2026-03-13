//! Tuple representation and deserialization for join operations.
use crate::catalog::types::Column;

/// Represents a typed column value.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int(i32),
    Text(String),
    Null,
}

impl ColumnValue {
    /// Compare two ColumnValues for ordering. Returns None if types mismatch or Null.
    pub fn partial_cmp_values(&self, other: &ColumnValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (ColumnValue::Int(a), ColumnValue::Int(b)) => Some(a.cmp(b)),
            (ColumnValue::Text(a), ColumnValue::Text(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Check equality between two ColumnValues.
    pub fn eq_value(&self, other: &ColumnValue) -> bool {
        match (self, other) {
            (ColumnValue::Int(a), ColumnValue::Int(b)) => a == b,
            (ColumnValue::Text(a), ColumnValue::Text(b)) => a == b,
            _ => false,
        }
    }
}

impl std::fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Int(v) => write!(f, "{}", v),
            ColumnValue::Text(v) => write!(f, "{}", v.trim()),
            ColumnValue::Null => write!(f, "NULL"),
        }
    }
}

/// A fully deserialized row from a table.
#[derive(Debug, Clone)]
pub struct Tuple {
    pub values: Vec<ColumnValue>,
    pub schema: Vec<Column>,
}

impl Tuple {
    /// Get a field value by column name.
    pub fn get_field(&self, col_name: &str) -> Option<&ColumnValue> {
        for (i, col) in self.schema.iter().enumerate() {
            if col.name == col_name {
                return self.values.get(i);
            }
        }
        None
    }

    /// Merge two tuples into one (concatenate fields and schemas).
    pub fn merge(left: &Tuple, right: &Tuple) -> Tuple {
        let mut values = left.values.clone();
        values.extend(right.values.clone());

        let mut schema = left.schema.clone();
        schema.extend(right.schema.clone());

        Tuple { values, schema }
    }

    /// Create a tuple with NULLs for all columns of the given schema.
    pub fn null_tuple(schema: &[Column]) -> Tuple {
        Tuple {
            values: schema.iter().map(|_| ColumnValue::Null).collect(),
            schema: schema.to_vec(),
        }
    }
}

/// Deserialize raw bytes from a page into a Tuple using the table schema.
/// INT = 4 bytes (i32 LE), TEXT = 10 bytes (fixed-width, space-padded).
pub fn deserialize_tuple(bytes: &[u8], schema: &[Column]) -> Tuple {
    let mut values = Vec::new();
    let mut cursor = 0usize;

    for col in schema {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= bytes.len() {
                    let val = i32::from_le_bytes(
                        bytes[cursor..cursor + 4].try_into().unwrap(),
                    );
                    values.push(ColumnValue::Int(val));
                    cursor += 4;
                } else {
                    values.push(ColumnValue::Null);
                }
            }
            "TEXT" => {
                if cursor + 10 <= bytes.len() {
                    let text = String::from_utf8_lossy(&bytes[cursor..cursor + 10])
                        .to_string();
                    values.push(ColumnValue::Text(text));
                    cursor += 10;
                } else {
                    values.push(ColumnValue::Null);
                }
            }
            _ => {
                values.push(ColumnValue::Null);
            }
        }
    }

    Tuple {
        values,
        schema: schema.to_vec(),
    }
}