//! Relation and output schemas for join plans.
//!
//! A join's output schema is derived once, at plan time, and carried by both
//! the plan node and the stream it produces. Rationale, including why the
//! fingerprint exists, is in `docs/join/design-rationale.md`.

use crate::catalog::Column;
use crate::types::DataType;

/// Which input a joined output column was drawn from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationSide {
    Left,
    Right,
}

/// One input relation of a join, with the alias used to qualify its columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationSchema {
    pub alias: String,
    pub columns: Vec<Column>,
}

impl RelationSchema {
    pub fn new(alias: impl Into<String>, columns: Vec<Column>) -> Self {
        Self {
            alias: alias.into(),
            columns,
        }
    }

    /// Logical column types, in declaration order.
    pub fn types(&self) -> Vec<DataType> {
        self.columns.iter().map(|c| c.data_type.clone()).collect()
    }

    /// Index of a column by exact (unqualified) name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// One column of a join's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputColumn {
    pub source: RelationSide,
    pub source_index: usize,
    pub qualified_name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// The full output schema of a join node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSchema {
    pub columns: Vec<OutputColumn>,
    /// Column types in output order - the schema rows are encoded against.
    pub types: Vec<DataType>,
    /// Stable hash of `types`. Written into spill-file headers so a run can
    /// never be read back under a different schema.
    pub fingerprint: u64,
}

impl OutputSchema {
    /// Left columns followed by right columns.
    ///
    /// `null_extend_left` / `null_extend_right` force the corresponding side's
    /// columns nullable, which is what an outer join does to its inner side.
    pub fn concat(
        left: &RelationSchema,
        right: &RelationSchema,
        null_extend_left: bool,
        null_extend_right: bool,
    ) -> Self {
        let mut columns = Vec::with_capacity(left.len() + right.len());
        push_side(&mut columns, left, RelationSide::Left, null_extend_left);
        push_side(&mut columns, right, RelationSide::Right, null_extend_right);
        Self::from_columns(columns)
    }

    /// Only the left relation's columns - the output shape of SEMI and ANTI
    /// joins, which never emit right-side data.
    pub fn left_only(left: &RelationSchema) -> Self {
        let mut columns = Vec::with_capacity(left.len());
        push_side(&mut columns, left, RelationSide::Left, false);
        Self::from_columns(columns)
    }

    fn from_columns(columns: Vec<OutputColumn>) -> Self {
        let types: Vec<DataType> = columns.iter().map(|c| c.data_type.clone()).collect();
        let fingerprint = fingerprint_of(&types);
        Self {
            columns,
            types,
            fingerprint,
        }
    }

    /// Number of output columns drawn from the left relation.
    pub fn left_width(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.source == RelationSide::Left)
            .count()
    }

    /// Number of output columns drawn from the right relation.
    pub fn right_width(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| c.source == RelationSide::Right)
            .count()
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Index of a column by its `alias.column` qualified name.
    pub fn column_index(&self, qualified_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.qualified_name == qualified_name)
    }
}

fn push_side(
    out: &mut Vec<OutputColumn>,
    relation: &RelationSchema,
    source: RelationSide,
    null_extend: bool,
) {
    for (index, column) in relation.columns.iter().enumerate() {
        out.push(OutputColumn {
            source,
            source_index: index,
            qualified_name: format!("{}.{}", relation.alias, column.name),
            data_type: column.data_type.clone(),
            nullable: column.nullable || null_extend,
        });
    }
}

/// FNV-1a over the rendered type list.
///
/// `DataType`'s `Display` is its canonical serialized form (upstream
/// `Serialize` writes the same string), so this is stable across runs and
/// across processes, which is what makes it usable in a spill-file header.
fn fingerprint_of(types: &[DataType]) -> u64 {
    use std::fmt::Write as _;

    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut rendered = String::new();
    for ty in types {
        // Writing into a String is infallible; the separator keeps
        // ["INT", "INT"] distinct from ["INTINT"].
        let _ = write!(rendered, "{ty};");
    }

    let mut hash = OFFSET_BASIS;
    for byte in rendered.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
