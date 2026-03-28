//! JoinResult: holds the output of a join operation.

use crate::catalog::types::Column;
use super::tuple::Tuple;

/// Holds the output of a join operation.
pub struct JoinResult {
    pub tuples: Vec<Tuple>,
    pub schema: Vec<Column>,
}

impl JoinResult {
    /// Create a new JoinResult with a combined schema from left and right relations.
    pub fn new(left_schema: &[Column], right_schema: &[Column], left_alias: &str, right_alias: &str) -> JoinResult {
        let mut schema = Vec::new();

        for col in left_schema {
            schema.push(Column {
                name: format!("{}.{}", left_alias, col.name),
                data_type: col.data_type.clone(),
            });
        }
        for col in right_schema {
            schema.push(Column {
                name: format!("{}.{}", right_alias, col.name),
                data_type: col.data_type.clone(),
            });
        }

        JoinResult {
            tuples: Vec::new(),
            schema,
        }
    }

    /// Add a merged tuple to the result.
    pub fn add(&mut self, t: Tuple) {
        self.tuples.push(t);
    }

    /// Display the join result as a formatted table.
    pub fn display(&self) {
        if self.tuples.is_empty() {
            println!("\n(No matching tuples found)\n");
            return;
        }

        // Calculate column widths
        let mut widths: Vec<usize> = self.schema.iter().map(|c| c.name.len()).collect();

        for tuple in &self.tuples {
            for (i, val) in tuple.values.iter().enumerate() {
                if i < widths.len() {
                    let val_len = format!("{}", val).len();
                    if val_len > widths[i] {
                        widths[i] = val_len;
                    }
                }
            }
        }

        // Print header
        let header: Vec<String> = self.schema.iter().enumerate()
            .map(|(i, c)| format!("{:width$}", c.name, width = widths[i]))
            .collect();
        let separator: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();

        println!("\n{}", header.join(" | "));
        println!("{}", separator.join("-+-"));

        // Print rows
        for tuple in &self.tuples {
            let row: Vec<String> = tuple.values.iter().enumerate()
                .map(|(i, v)| {
                    let w = if i < widths.len() { widths[i] } else { 10 };
                    format!("{:width$}", format!("{}", v), width = w)
                })
                .collect();
            println!("{}", row.join(" | "));
        }

        println!("\n({} rows)\n", self.tuples.len());
    }
}
