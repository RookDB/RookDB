use crate::types::datatype::DataType;
use crate::types::value::DataValue;

/// Metadata describing one column in a query result set.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    /// Optional table name for qualified column references (e.g. `t1.id`).
    /// Used by join operators to disambiguate columns with the same name
    /// from different tables. Currently informational; lookups use `name` only.
    pub table: Option<String>,
}

/// A single row in the result set.
///
/// `values[i]` corresponds to `column_info[i]` — both use the same logical
/// ordering (the projection order for the query, or the table's schema order
/// for a full `SELECT *`).
///
/// The optional `page_id` / `slot_id` fields record the heap location of the
/// source row.  These are populated by scan operators and propagated through
/// filter/evaluation operators, allowing DML mutations (UPDATE/DELETE) to
/// identify which physical row to modify without a separate heap scan.
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    pub values: Vec<Option<DataValue>>,
    /// Heap page identifier where this tuple originated (if known).
    pub page_id: Option<u32>,
    /// Heap slot identifier within `page_id` (if known).
    pub slot_id: Option<u32>,
}

impl Tuple {
    /// Create an empty tuple (used for buffer reuse).
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Create a new tuple from deserialized values.
    pub fn new(values: Vec<Option<DataValue>>) -> Self {
        Self {
            values,
            page_id: None,
            slot_id: None,
        }
    }

    /// Create a new tuple with heap location metadata.
    pub fn new_with_location(values: Vec<Option<DataValue>>, page_id: u32, slot_id: u32) -> Self {
        Self {
            values,
            page_id: Some(page_id),
            slot_id: Some(slot_id),
        }
    }

    /// Set heap location metadata.
    pub fn with_location(mut self, page_id: u32, slot_id: u32) -> Self {
        self.page_id = Some(page_id);
        self.slot_id = Some(slot_id);
        self
    }

    /// Attach the source tuple's heap location (if any) to this derived
    /// tuple.
    ///
    /// Operators that build NEW tuples from one input row per output row
    /// (e.g. Projection) must carry the row's `(page_id, slot_id)` through,
    /// otherwise pointer-based UPDATE/DELETE cannot address the row. A
    /// synthetic source (aggregate output, single-row stub) simply stays
    /// location-less.
    pub fn with_location_from(mut self, src: &Tuple) -> Self {
        self.page_id = src.page_id;
        self.slot_id = src.slot_id;
        self
    }

    /// How many columns this tuple has.
    pub fn arity(&self) -> usize {
        self.values.len()
    }

    /// Return a reference to the value at `index`, if within bounds.
    pub fn get(&self, index: usize) -> Option<&Option<DataValue>> {
        self.values.get(index)
    }

    /// Concatenate two tuples into one (used by join operators).
    /// The resulting tuple has `self`'s values followed by `other`'s values.
    /// Location metadata is cleared (joined tuples don't have a single source).
    pub fn concatenate(&self, other: &Tuple) -> Self {
        let mut combined_values = self.values.clone();
        combined_values.extend(other.values.iter().cloned());
        Self {
            values: combined_values,
            page_id: None,
            slot_id: None,
        }
    }

    /// Concatenate two tuples into a destination tuple buffer, reusing its allocation.
    pub fn concatenate_into(&self, other: &Tuple, out: &mut Tuple) {
        out.values.clear();
        out.values.reserve(self.values.len() + other.values.len());
        out.values.extend(self.values.iter().cloned());
        out.values.extend(other.values.iter().cloned());
        out.page_id = None;
        out.slot_id = None;
    }

    /// Concatenate two owned tuples without reallocating `self`'s vector.
    pub fn concatenate_owned(mut self, other: Tuple) -> Self {
        self.values.extend(other.values);
        self.page_id = None;
        self.slot_id = None;
        self
    }

    /// Project a subset of columns from this tuple.
    pub fn project(&self, indices: &[usize]) -> Self {
        let mut projected_values = Vec::with_capacity(indices.len());
        for &i in indices {
            projected_values.push(self.values.get(i).cloned().unwrap_or(None));
        }
        Self {
            values: projected_values,
            page_id: self.page_id,
            slot_id: self.slot_id,
        }
    }

    /// Project a subset of columns into an existing output tuple buffer.
    pub fn project_into(&self, indices: &[usize], out: &mut Tuple) {
        out.values.clear();
        out.values.reserve(indices.len());
        for &i in indices {
            out.values.push(self.values.get(i).cloned().unwrap_or(None));
        }
        out.page_id = self.page_id;
        out.slot_id = self.slot_id;
    }

    /// In-place projection: modifies `self.values` to retain only `indices`.
    pub fn project_in_place(&mut self, indices: &[usize]) {
        let mut projected = Vec::with_capacity(indices.len());
        for &i in indices {
            projected.push(if i < self.values.len() {
                self.values[i].take()
            } else {
                None
            });
        }
        self.values = projected;
    }
}

// ── Display / Formatting ──────────────────────────────────────────────────────

/// Format a single tuple as a human-readable string.
pub fn format_tuple(tuple: &Tuple, schema: &[ColumnInfo], _col_width: usize) -> String {
    let mut parts = Vec::new();
    for (i, val_opt) in tuple.values.iter().enumerate() {
        let name = schema.get(i).map(|c| c.name.as_str()).unwrap_or("?");
        match val_opt {
            Some(val) => parts.push(format!("{}={}", name, val)),
            None => parts.push(format!("{}=NULL", name)),
        }
    }
    if parts.is_empty() {
        "(empty row)".to_string()
    } else {
        parts.join(" | ")
    }
}

/// Display a set of tuples in a formatted table with borders.
///
/// Returns the number of tuples displayed.
pub fn display_tuples(tuples: &[Tuple], schema: &[ColumnInfo]) -> usize {
    if tuples.is_empty() {
        println!("(0 rows)");
        return 0;
    }

    let col_count = schema.len();
    if col_count == 0 {
        println!("({} rows)\n", tuples.len());
        return tuples.len();
    }

    // Format column headers
    let headers: Vec<String> = schema
        .iter()
        .map(|col| format!("{}: {}", col.name, col.data_type))
        .collect();

    // Determine row number column width
    let row_header = "row number";
    let row_num_width = std::cmp::max(row_header.len(), format!("{}", tuples.len()).len());

    // Compute width per column: max of header length and data value lengths (clamped between 4 and 40)
    let mut col_widths = Vec::with_capacity(col_count);
    for (idx, header) in headers.iter().enumerate() {
        let header_len = header.chars().count();
        let max_val_len = tuples
            .iter()
            .map(|t| {
                t.values
                    .get(idx)
                    .and_then(|v| v.as_ref())
                    .map(|v| format!("{}", v).chars().count())
                    .unwrap_or(4) // "NULL"
            })
            .max()
            .unwrap_or(0);
        let w = std::cmp::max(header_len, max_val_len).clamp(4, 40);
        col_widths.push(w);
    }

    // Build borders with row number column included
    let row_pad = "─".repeat(row_num_width + 2);
    let mut top_border = format!("┌{}┬", row_pad);
    let mut mid_border = format!("├{}┼", row_pad);
    let mut bot_border = format!("└{}┴", row_pad);

    for (idx, w) in col_widths.iter().enumerate() {
        let line = "─".repeat(w + 2);
        if idx < col_count - 1 {
            top_border.push_str(&format!("{}┬", line));
            mid_border.push_str(&format!("{}┼", line));
            bot_border.push_str(&format!("{}┴", line));
        } else {
            top_border.push_str(&format!("{}┐", line));
            mid_border.push_str(&format!("{}┤", line));
            bot_border.push_str(&format!("{}┘", line));
        }
    }

    println!("{}", top_border);
    print!("│ {:<width$} │", row_header, width = row_num_width);
    for (display, w) in headers.iter().zip(col_widths.iter()) {
        let w = *w;
        let truncated = if display.chars().count() > w {
            format!("{}…", display.chars().take(w - 1).collect::<String>())
        } else {
            display.clone()
        };
        print!(" {:<width$} │", truncated, width = w);
    }
    println!();
    println!("{}", mid_border);

    for (row_idx, tuple) in tuples.iter().enumerate() {
        print!("│ {:>width$} │", row_idx + 1, width = row_num_width);
        for (idx, &w) in col_widths.iter().enumerate() {
            let display = match tuple.values.get(idx).and_then(|v| v.as_ref()) {
                Some(val) => format!("{}", val),
                None => "NULL".to_string(),
            };
            if display.chars().count() > w {
                print!(" {}… │", display.chars().take(w - 1).collect::<String>());
            } else {
                print!(" {:<width$} │", display, width = w);
            }
        }
        println!();
    }

    println!("{}", bot_border);
    println!("({} rows)\n", tuples.len());
    tuples.len()
}
