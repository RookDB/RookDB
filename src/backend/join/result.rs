//! Join result container and tabular display.

use crate::catalog::types::Column;
use super::tuple::Tuple;

/// Holds the output tuples and combined schema produced by a join.
pub struct JoinResult {
    pub tuples: Vec<Tuple>,
    pub schema: Vec<Column>,
}

impl JoinResult {
    /// Create an empty result with a combined schema.
    pub fn new(left_schema: &[Column], right_schema: &[Column], left_alias: &str, right_alias: &str) -> JoinResult {
        let mut schema = Vec::with_capacity(left_schema.len() + right_schema.len());
        for col in left_schema {
            schema.push(Column { name: format!("{}.{}", left_alias, col.name), data_type: col.data_type.clone() });
        }
        for col in right_schema {
            schema.push(Column { name: format!("{}.{}", right_alias, col.name), data_type: col.data_type.clone() });
        }
        JoinResult { tuples: Vec::new(), schema }
    }

    /// Append a merged tuple to the result.
    pub fn add(&mut self, t: Tuple) {
        self.tuples.push(t);
    }

    /// Pretty-print the result as an ASCII table.
    pub fn display(&self) {
        if self.tuples.is_empty() {
            println!("\n(No matching tuples found)\n");
            return;
        }
        let mut widths: Vec<usize> = self.schema.iter().map(|c| c.name.len()).collect();
        for tuple in &self.tuples {
            for (i, val) in tuple.values.iter().enumerate() {
                if i < widths.len() {
                    let vl = format!("{}", val).len();
                    if vl > widths[i] { widths[i] = vl; }
                }
            }
        }
        let header: Vec<String> = self.schema.iter().enumerate()
            .map(|(i, c)| format!("{:w$}", c.name, w = widths[i])).collect();
        let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        println!("\n{}", header.join(" | "));
        println!("{}", sep.join("-+-"));
        for tuple in &self.tuples {
            let row: Vec<String> = tuple.values.iter().enumerate()
                .map(|(i, v)| { let w = if i < widths.len() { widths[i] } else { 10 }; format!("{:w$}", format!("{}", v), w = w) })
                .collect();
            println!("{}", row.join(" | "));
        }
        println!("\n({} rows)\n", self.tuples.len());
    }
}
