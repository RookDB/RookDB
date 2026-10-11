//! Physical plans as data: what `EXPLAIN` shows.
//!
//! Every operator describes itself and its children as a [`PlanNode`]
//! ([`PhysicalOperator::explain`](super::operators::PhysicalOperator::explain)):
//! which operator runs and what was decided for it (the join algorithm, the
//! index and lookup mode of a scan, a top-N sort and its memory budget).
//! Decisions that leave no operator in the tree — a sort that index order made
//! unnecessary, a CTE or subquery evaluated while planning — are collected by
//! the planner in a [`PlanTrace`]. Together they make a [`PlanDescription`],
//! so a front end shows the plan from data instead of reading the engine's log.

/// One operator of a physical plan and what was decided for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanNode {
    /// The operator, e.g. `HashJoin` or `IndexScan`.
    pub operator: String,
    /// The decisions, as `(name, value)` pairs in the order they are shown.
    pub details: Vec<(String, String)>,
    /// The operators this one reads from.
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn new(operator: impl Into<String>) -> Self {
        PlanNode {
            operator: operator.into(),
            details: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Add one decision.
    pub fn with(mut self, name: &str, value: impl ToString) -> Self {
        self.details.push((name.to_string(), value.to_string()));
        self
    }

    /// Add an input.
    pub fn child(mut self, child: PlanNode) -> Self {
        self.children.push(child);
        self
    }

    /// The value recorded for `name`, if any.
    pub fn detail(&self, name: &str) -> Option<&str> {
        self.details
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// The first node named `operator` in this subtree, depth first.
    pub fn find(&self, operator: &str) -> Option<&PlanNode> {
        if self.operator == operator {
            return Some(self);
        }
        self.children.iter().find_map(|c| c.find(operator))
    }

    /// Whether this subtree contains an operator named `operator`.
    pub fn contains(&self, operator: &str) -> bool {
        self.find(operator).is_some()
    }

    /// This node alone on one line: `Sort (keys=sal DESC, top_n=3)`.
    pub fn label(&self) -> String {
        if self.details.is_empty() {
            return self.operator.clone();
        }
        let details: Vec<String> = self
            .details
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect();
        format!("{} ({})", self.operator, details.join(", "))
    }

    fn write_lines(&self, depth: usize, out: &mut Vec<String>) {
        out.push(format!("{}{}", "  ".repeat(depth), self.label()));
        for child in &self.children {
            child.write_lines(depth + 1, out);
        }
    }
}

/// What the planner decided or did that no operator of the tree shows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanTrace {
    /// Plans of the CTEs evaluated while planning, by name, in the order
    /// they were evaluated.
    pub ctes: Vec<(String, PlanNode)>,
    /// One line per other decision, in the order they were made.
    pub notes: Vec<String>,
}

/// The physical plan of one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanDescription {
    /// The operator tree that produces the statement's rows.
    pub root: PlanNode,
    /// See [`PlanTrace`].
    pub trace: PlanTrace,
}

impl PlanDescription {
    /// The plan as text, one operator per line, inputs indented under the
    /// operator that reads them; then the CTE plans and the notes.
    pub fn lines(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.root.write_lines(0, &mut out);
        for (name, plan) in &self.trace.ctes {
            out.push(format!("CTE {}", name));
            plan.write_lines(1, &mut out);
        }
        for note in &self.trace.notes {
            out.push(format!("Note: {}", note));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_indent_children_and_list_details_in_order() {
        let plan = PlanDescription {
            root: PlanNode::new("Limit").with("limit", 3).child(
                PlanNode::new("Sort")
                    .with("keys", "sal DESC")
                    .with("top_n", 3)
                    .child(PlanNode::new("SeqScan").with("table", "emp")),
            ),
            trace: PlanTrace {
                ctes: vec![("c".to_string(), PlanNode::new("SingleRow"))],
                notes: vec!["something happened".to_string()],
            },
        };
        assert_eq!(
            plan.lines(),
            vec![
                "Limit (limit=3)",
                "  Sort (keys=sal DESC, top_n=3)",
                "    SeqScan (table=emp)",
                "CTE c",
                "  SingleRow",
                "Note: something happened",
            ]
        );
        assert_eq!(plan.root.find("Sort").unwrap().detail("top_n"), Some("3"));
        assert!(plan.root.contains("SeqScan"));
        assert!(!plan.root.contains("HashJoin"));
    }
}
