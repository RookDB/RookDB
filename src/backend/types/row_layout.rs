//! Physical schema and row layout computation for the new tuple format.
//!
//! The **logical schema** is the user-defined column order (as in `CREATE TABLE`).
//! The **physical schema** regroups columns so all fixed-length columns come first,
//! followed by all variable-length columns. This enables O(1) access to any
//! fixed-length column without scanning variable-length data.
//!
//! On-disk row format:
//! ```text
//! [Header 4B] [Null Bitmap] [Var-Len Offset Table] [Fixed-Length Data] [Var-Len Data]
//! ```

use crate::types::datatype::DataType;

// ── PhysicalSchema ────────────────────────────────────────────────────────────

/// Reorders logical columns into two physical groups:
///   Group 1 (fixed): all fixed-length columns, in their original logical order.
///   Group 2 (var):   all variable-length columns, in their original logical order.
///
/// Index mappings allow translating between logical and physical positions.
#[derive(Debug, Clone)]
pub struct PhysicalSchema {
    /// logical index → physical index
    pub logical_to_physical: Vec<usize>,
    /// physical index → logical index
    pub physical_to_logical: Vec<usize>,
    /// logical indices of fixed-length columns (in schema order)
    pub fixed_indices_logical: Vec<usize>,
    /// logical indices of variable-length columns (in schema order)
    pub varlen_indices_logical: Vec<usize>,
    /// DataType for each column in **physical** order
    pub physical_types: Vec<DataType>,
}

impl PhysicalSchema {
    /// Build a physical schema from the logical (user-defined) schema.
    /// Fixed-length columns occupy physical slots 0..n_fixed-1.
    /// Variable-length columns occupy physical slots n_fixed..n_total-1.
    pub fn from_logical(schema: &[DataType]) -> Self {
        let mut fixed_indices_logical: Vec<usize> = Vec::new();
        let mut varlen_indices_logical: Vec<usize> = Vec::new();

        for (i, ty) in schema.iter().enumerate() {
            if ty.is_fixed_length() {
                fixed_indices_logical.push(i);
            } else {
                varlen_indices_logical.push(i);
            }
        }

        let n = schema.len();
        let n_fixed = fixed_indices_logical.len();

        let mut logical_to_physical = vec![0usize; n];
        let mut physical_to_logical = vec![0usize; n];
        let mut physical_types: Vec<DataType> = Vec::with_capacity(n);

        // Physical slots 0..n_fixed → fixed columns
        for (phys, &log) in fixed_indices_logical.iter().enumerate() {
            logical_to_physical[log] = phys;
            physical_to_logical[phys] = log;
            physical_types.push(schema[log].clone());
        }

        // Physical slots n_fixed..n → var-len columns
        for (offset, &log) in varlen_indices_logical.iter().enumerate() {
            let phys = n_fixed + offset;
            logical_to_physical[log] = phys;
            physical_to_logical[phys] = log;
            physical_types.push(schema[log].clone());
        }

        Self {
            logical_to_physical,
            physical_to_logical,
            fixed_indices_logical,
            varlen_indices_logical,
            physical_types,
        }
    }

    pub fn num_fixed(&self) -> usize {
        self.fixed_indices_logical.len()
    }

    pub fn num_varlen(&self) -> usize {
        self.varlen_indices_logical.len()
    }
}

// ── RowLayout ─────────────────────────────────────────────────────────────────

/// Byte-level geometry of a row, derived from a `PhysicalSchema`.
///
/// Layout regions (byte offsets from row start):
/// ```text
/// [0..4)                              Header (4 bytes)
/// [4 .. 4+null_bitmap_size)           Null bitmap (logical column order)
/// [.. .. ..+varlen_table_size)        Var-len offset table (physical var-len order)
/// [fixed_data_start .. +fixed_data_size) Fixed-length data (physical fixed order)
/// [varies]                            Var-len payloads (physical var-len order, no prefix)
/// ```
#[derive(Debug, Clone)]
pub struct RowLayout {
    pub num_columns: usize,
    pub num_varlen_cols: usize,
    /// Bytes occupied by the null bitmap: ceil(num_columns / 8)
    pub null_bitmap_size: usize,
    /// Bytes occupied by the var-len offset table: num_varlen_cols * 2
    pub varlen_table_size: usize,
    /// Byte offset from row start where fixed-length data begins:
    /// 4 (header) + null_bitmap_size + varlen_table_size
    pub fixed_data_start: usize,
    /// Per-column byte offset **within** the fixed-data region, indexed by
    /// position in `PhysicalSchema::fixed_indices_logical`.
    pub fixed_col_offsets: Vec<usize>,
    /// Total size (bytes) of the fixed-data region (includes alignment padding).
    pub fixed_data_size: usize,
}

impl RowLayout {
    /// Compute layout geometry from a physical schema.
    pub fn compute(physical: &PhysicalSchema) -> Self {
        let num_columns = physical.logical_to_physical.len();
        let num_varlen_cols = physical.num_varlen();
        let null_bitmap_size = num_columns.div_ceil(8);
        let varlen_table_size = num_varlen_cols * 2;
        let fixed_data_start = 4 + null_bitmap_size + varlen_table_size;

        // Compute per-column offsets within the fixed-data region,
        // respecting each type's natural alignment.
        let mut fixed_col_offsets: Vec<usize> = Vec::with_capacity(physical.num_fixed());
        let mut cursor: usize = 0;

        for &log_idx in &physical.fixed_indices_logical {
            let ty = &physical.physical_types[physical.logical_to_physical[log_idx]];
            let align = ty.alignment() as usize;
            // Pad cursor to alignment boundary
            if align > 1 && cursor % align != 0 {
                cursor += align - (cursor % align);
            }
            fixed_col_offsets.push(cursor);
            cursor += ty.fixed_size().expect("fixed-length type must have fixed_size") as usize;
        }

        Self {
            num_columns,
            num_varlen_cols,
            null_bitmap_size,
            varlen_table_size,
            fixed_data_start,
            fixed_col_offsets,
            fixed_data_size: cursor,
        }
    }

    /// Minimum on-disk row size: all static regions (no var-len payloads).
    pub fn min_row_size(&self) -> usize {
        self.fixed_data_start + self.fixed_data_size
    }

    /// Byte offset from row start to the null bitmap.
    pub fn bitmap_offset() -> usize {
        4
    }

    /// Byte offset from row start to the var-len offset table.
    pub fn varlen_table_offset(&self) -> usize {
        4 + self.null_bitmap_size
    }
}
