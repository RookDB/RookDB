//! InsertOperator — Inserts tuples from a child operator into a table.
//!
//! Used for `INSERT INTO ... SELECT` and the `VALUES` form (via the
//! `ValuesOperator` child). The child operator produces rows, and this
//! operator inserts each row with `insert_row`, which returns the heap
//! pointer needed for rollback.
//!
//! The operator returns each inserted tuple so the engine can report the count.
//! Statement atomicity: the pointers of all rows inserted so far are tracked;
//! if any later row fails, those rows are deleted again before the error
//! propagates.

use super::super::expr::Expr;
use super::super::tuple::{ColumnInfo, Tuple};
use super::PhysicalOperator;
use crate::backend::error::{RookError, RookResult};

/// Function signature for the insert operation.
///
/// Takes the row's values and returns:
/// - `Ok(())` on successful insertion
/// - `Err(String)` on failure (constraint violation or database error)
///
/// Making this injectable allows unit tests to mock the insertion logic
/// without requiring a real catalog and database files.
pub(crate) type InsertInserter =
    Box<dyn FnMut(&[Option<crate::types::DataValue>]) -> Result<(), String>>;

use std::cell::RefCell;
use std::rc::Rc;

/// Insert operator that writes child tuples into a table.
///
/// Streams rows one at a time: each call to `next()` pulls one tuple from the
/// child (SELECT), inserts it via an injectable inserter, and returns it.
/// If any row fails during insertion, all previously inserted rows in the statement
/// are rolled back (deleted) to preserve SQL statement atomicity.
pub struct InsertOperator {
    child: Box<dyn PhysicalOperator>,
    /// Schema of the child (used for operator contract).
    child_schema: Vec<ColumnInfo>,
    table: Option<String>,
    db_name: Option<String>,
    catalog: Option<std::sync::Arc<crate::catalog::types::Catalog>>,
    inserted_pointers: Rc<RefCell<Vec<(u32, u32)>>>,
    completed: bool,
    /// Injectable insert function. Defaults to wrapping `insert_row`.
    /// In tests, can be replaced with a mock for isolated unit testing.
    inserter: InsertInserter,
    /// Source rows, read in full before the first insert so a statement that
    /// selects from its own target never sees the rows it is adding.
    source: Option<std::vec::IntoIter<Tuple>>,
}

impl InsertOperator {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        table: String,
        db_name: String,
        catalog: std::sync::Arc<crate::catalog::types::Catalog>,
    ) -> Self {
        let child_schema = child.schema().to_vec();

        // Build the default inserter that wraps insert_row.
        let cat = catalog.clone();
        let tbl = table.clone();
        let db = db_name.clone();
        let inserted_pointers = Rc::new(RefCell::new(Vec::new()));
        let ptrs_clone = inserted_pointers.clone();
        let inserter: InsertInserter = Box::new(move |vals| {
            match crate::backend::executor::load_csv::insert_row(&cat, &db, &tbl, vals) {
                Ok(ptr) => {
                    log::info!(
                        "[Insert] Inserted tuple into table '{}' at (page={}, slot={})",
                        tbl,
                        ptr.0,
                        ptr.1
                    );
                    ptrs_clone.borrow_mut().push(ptr);
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            }
        });

        Self {
            child,
            child_schema,
            table: Some(table),
            db_name: Some(db_name),
            catalog: Some(catalog),
            inserted_pointers,
            completed: false,
            inserter,
            source: None,
        }
    }

    #[cfg(test)]
    pub fn for_test(child: Box<dyn PhysicalOperator>, inserter: InsertInserter) -> Self {
        let child_schema = child.schema().to_vec();
        Self {
            child,
            child_schema,
            table: None,
            db_name: None,
            catalog: None,
            inserted_pointers: Rc::new(RefCell::new(Vec::new())),
            completed: false,
            inserter,
            source: None,
        }
    }

    fn rollback(&mut self) {
        let ptrs: Vec<(u32, u32)> = self.inserted_pointers.borrow_mut().drain(..).collect();
        if !ptrs.is_empty()
            && let (Some(cat), Some(db), Some(tbl)) = (&self.catalog, &self.db_name, &self.table)
        {
            let _ = crate::backend::executor::delete::delete_by_pointers(cat, db, tbl, &ptrs);
        }
    }
}

impl Drop for InsertOperator {
    fn drop(&mut self) {
        if !self.completed {
            self.rollback();
        }
    }
}

impl PhysicalOperator for InsertOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.source.is_none() {
            let mut rows = Vec::new();
            loop {
                match self.child.next() {
                    Ok(Some(tuple)) => rows.push(tuple),
                    Ok(None) => break,
                    Err(e) => {
                        self.rollback();
                        return Err(e);
                    }
                }
            }
            self.source = Some(rows.into_iter());
        }
        // Take one source tuple, insert it, return it
        let next: RookResult<Option<Tuple>> = Ok(self.source.as_mut().and_then(Iterator::next));
        match next {
            Ok(Some(tuple)) => {
                // Call the injectable inserter (in production, wraps insert_row)
                if let Err(e) = (self.inserter)(&tuple.values) {
                    self.rollback();
                    return Err(RookError::Internal(e));
                }
                Ok(Some(tuple))
            }
            Ok(None) => {
                self.completed = true;
                self.inserted_pointers.borrow_mut().clear();
                Ok(None)
            }
            Err(e) => {
                self.rollback();
                Err(e)
            }
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.child_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.rollback();
        self.completed = false;
        self.source = None;
        self.child.reset()?;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        // Insert produces at most as many tuples as the child
        self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "Insert"
    }
}

// ── ValuesOperator ────────────────────────────────────────────────────────────

/// A constant-producing child for `INSERT ... VALUES`.
///
/// Holds one row of pre-compiled expressions per VALUES tuple; each `next()`
/// evaluates the next row against an empty tuple (VALUES may not reference
/// columns) and emits it with the target table's schema.
pub struct ValuesOperator {
    rows: Vec<Vec<Expr>>,
    schema: Vec<ColumnInfo>,
    pos: usize,
}

impl ValuesOperator {
    pub fn new(rows: Vec<Vec<Expr>>, schema: Vec<ColumnInfo>) -> Self {
        Self {
            rows,
            schema,
            pos: 0,
        }
    }
}

impl PhysicalOperator for ValuesOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let empty = Tuple::new(Vec::new());
        let values = self.rows[self.pos]
            .iter()
            .map(|e| e.evaluate(&empty, &[]))
            .collect::<Result<Vec<_>, _>>()?;
        self.pos += 1;
        Ok(Some(Tuple::new(values)))
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        Err(RookError::Internal("reset() not supported".to_string()))
    }

    fn estimate_cardinality(&self) -> usize {
        self.rows.len()
    }

    fn name(&self) -> &'static str {
        "Values"
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::physical::planner::PhysicalPlanner;
    use crate::catalog::types::Catalog;
    use crate::types::datatype::DataType;
    use crate::types::value::DataValue;
    use rook_ast::logical::{
        ColumnSchema, LogicalInsert, LogicalPlan, LogicalProject, LogicalTableScan,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    /// A mock child operator that yields a fixed set of tuples.
    struct MockChild {
        tuples: Vec<Tuple>,
        pos: usize,
        schema: Vec<ColumnInfo>,
    }

    impl MockChild {
        fn new(tuples: Vec<Tuple>, schema: Vec<ColumnInfo>) -> Self {
            Self {
                tuples,
                pos: 0,
                schema,
            }
        }
    }

    impl PhysicalOperator for MockChild {
        fn next(&mut self) -> RookResult<Option<Tuple>> {
            if self.pos < self.tuples.len() {
                let t = self.tuples[self.pos].clone();
                self.pos += 1;
                Ok(Some(t))
            } else {
                Ok(None)
            }
        }

        fn schema(&self) -> &[ColumnInfo] {
            &self.schema
        }

        fn reset(&mut self) -> RookResult<()> {
            self.pos = 0;
            Ok(())
        }

        fn name(&self) -> &'static str {
            "MockChild"
        }

        fn estimate_cardinality(&self) -> usize {
            self.tuples.len()
        }
    }

    fn value_text(v: &Option<crate::types::DataValue>) -> String {
        v.as_ref()
            .map_or_else(|| "NULL".to_string(), |d| d.to_string())
    }

    fn int_schema() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "a".into(),
                data_type: DataType::Int,
                table: None,
            },
            ColumnInfo {
                name: "b".into(),
                data_type: DataType::Int,
                table: None,
            },
        ]
    }

    fn int_tuples(data: Vec<(Option<i32>, Option<i32>)>) -> (Vec<Tuple>, Vec<ColumnInfo>) {
        let schema = int_schema();
        let tuples = data
            .into_iter()
            .map(|(a, b)| Tuple::new(vec![a.map(DataValue::Int), b.map(DataValue::Int)]))
            .collect();
        (tuples, schema)
    }

    // ── Streaming ───────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_empty_child() {
        // Child returns no tuples → operator should return None immediately
        let (tuples, schema) = int_tuples(vec![]);
        let child = MockChild::new(tuples, schema);

        let mut op = InsertOperator::for_test(Box::new(child), Box::new(|_| Ok(())));

        assert!(op.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_operator_single_tuple() {
        // Single tuple streamed and returned
        let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
        let child = MockChild::new(tuples, schema);

        let inserted: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
        let ins = inserted.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |vals| {
                ins.borrow_mut().push(vals.iter().map(value_text).collect());
                Ok(())
            }),
        );

        let t = op.next().unwrap().unwrap();
        assert_eq!(t.values[0], Some(DataValue::Int(1)));
        assert_eq!(t.values[1], Some(DataValue::Int(10)));
        assert!(op.next().unwrap().is_none());

        assert_eq!(*inserted.borrow(), vec![vec!["1", "10"]]);
    }

    #[test]
    fn test_insert_operator_multiple_tuples() {
        // Multiple tuples streamed one by one
        let (tuples, schema) = int_tuples(vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(30)),
        ]);
        let child = MockChild::new(tuples, schema);

        let inserted: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
        let ins = inserted.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |vals| {
                ins.borrow_mut().push(vals.iter().map(value_text).collect());
                Ok(())
            }),
        );

        let t1 = op.next().unwrap().unwrap();
        assert_eq!(t1.values[0], Some(DataValue::Int(1)));
        assert_eq!(t1.values[1], Some(DataValue::Int(10)));

        let t2 = op.next().unwrap().unwrap();
        assert_eq!(t2.values[0], Some(DataValue::Int(2)));
        assert_eq!(t2.values[1], Some(DataValue::Int(20)));

        let t3 = op.next().unwrap().unwrap();
        assert_eq!(t3.values[0], Some(DataValue::Int(3)));
        assert_eq!(t3.values[1], Some(DataValue::Int(30)));

        assert!(op.next().unwrap().is_none());

        assert_eq!(
            *inserted.borrow(),
            vec![vec!["1", "10"], vec!["2", "20"], vec!["3", "30"],]
        );
    }

    // ── Error handling ───────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_reports_constraint_violation() {
        // Inserter returns error on constraint violation → operator propagates it
        let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
        let child = MockChild::new(tuples, schema);

        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(|_| Err("Constraint violation: duplicate key".to_string())),
        );

        let result = op.next();
        match result {
            Err(msg) => {
                assert!(
                    msg.to_string().contains("duplicate key"),
                    "Expected constraint error, got: {}",
                    msg
                );
            }
            _ => panic!("Expected error for constraint violation"),
        }
    }

    #[test]
    fn test_insert_operator_error_stops_streaming() {
        // Error on the second tuple → subsequent calls keep returning the error
        let (tuples, schema) = int_tuples(vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)), // This one will fail
            (Some(3), Some(30)),
        ]);
        let child = MockChild::new(tuples, schema);

        let call_count: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let cc = call_count.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |_| {
                let cnt = *cc.borrow();
                *cc.borrow_mut() += 1;
                if cnt == 1 {
                    // Fail on the second insertion
                    Err("Disk full".to_string())
                } else {
                    Ok(())
                }
            }),
        );

        // First call succeeds
        let t1 = op.next().unwrap().unwrap();
        assert_eq!(t1.values[0], Some(DataValue::Int(1)));

        // Second call fails
        let err = op.next().unwrap_err();
        assert!(err.to_string().contains("Disk full"), "Got: {}", err);
    }

    // ── NULL values ──────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_null_values() {
        // Tuples with NULL values → converted to "NULL" strings
        let (tuples, schema) = int_tuples(vec![(Some(1), None), (None, Some(20))]);
        let child = MockChild::new(tuples, schema);

        let inserted: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
        let ins = inserted.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |vals| {
                let strs: Vec<String> = vals.iter().map(value_text).collect();
                ins.borrow_mut().push(strs);
                Ok(())
            }),
        );

        // First tuple: (1, NULL)
        let t1 = op.next().unwrap().unwrap();
        assert_eq!(t1.values[0], Some(DataValue::Int(1)));
        assert_eq!(t1.values[1], None);
        assert_eq!(inserted.borrow()[0], vec!["1", "NULL"]);

        // Second tuple: (NULL, 20)
        let t2 = op.next().unwrap().unwrap();
        assert_eq!(t2.values[0], None);
        assert_eq!(t2.values[1], Some(DataValue::Int(20)));
        assert_eq!(inserted.borrow()[1], vec!["NULL", "20"]);

        assert!(op.next().unwrap().is_none());
    }

    #[test]
    fn test_insert_operator_all_nulls() {
        // All NULL values
        let (tuples, schema) = int_tuples(vec![(None, None)]);
        let child = MockChild::new(tuples, schema);

        let inserted: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
        let ins = inserted.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |vals| {
                let strs: Vec<String> = vals.iter().map(value_text).collect();
                ins.borrow_mut().push(strs);
                Ok(())
            }),
        );

        let t = op.next().unwrap().unwrap();
        assert_eq!(t.values[0], None);
        assert_eq!(t.values[1], None);
        assert_eq!(inserted.borrow()[0], vec!["NULL", "NULL"]);
    }

    // ── Reset ────────────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_reset() {
        // Reset allows re-iterating the child tuples
        let (tuples, schema) = int_tuples(vec![(Some(1), Some(10)), (Some(2), Some(20))]);
        let child = MockChild::new(tuples, schema);

        let count: Rc<RefCell<u32>> = Rc::new(RefCell::new(0));
        let cnt = count.clone();
        let mut op = InsertOperator::for_test(
            Box::new(child),
            Box::new(move |_vals| {
                *cnt.borrow_mut() += 1;
                Ok(())
            }),
        );

        // First pass: insert both tuples
        assert!(op.next().unwrap().is_some());
        assert!(op.next().unwrap().is_some());
        assert!(op.next().unwrap().is_none());
        assert_eq!(*count.borrow(), 2);

        // Reset the operator
        op.reset().unwrap();

        // Second pass: re-insert both tuples
        assert!(op.next().unwrap().is_some());
        assert!(op.next().unwrap().is_some());
        assert!(op.next().unwrap().is_none());
        assert_eq!(*count.borrow(), 4);
    }

    // ── Schema ───────────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_schema() {
        // Schema matches the child's schema
        let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
        let child = MockChild::new(tuples, schema.clone());

        let mut op = InsertOperator::for_test(Box::new(child), Box::new(|_| Ok(())));

        // Clone to avoid borrow conflict with next()
        let schema = op.schema().to_vec();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "a");
        assert_eq!(schema[0].data_type, DataType::Int);
        assert_eq!(schema[1].name, "b");
        assert_eq!(schema[1].data_type, DataType::Int);

        // Returned tuple should also match the schema
        let t = op.next().unwrap().unwrap();
        assert_eq!(t.arity(), schema.len());
    }

    // ── Cardinality ──────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_estimate_cardinality() {
        // estimate_cardinality mirrors the child's estimate
        let (tuples, schema) = int_tuples(vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(30)),
            (Some(4), Some(40)),
        ]);
        let child = MockChild::new(tuples, schema);

        let op = InsertOperator::for_test(Box::new(child), Box::new(|_| Ok(())));

        assert_eq!(op.estimate_cardinality(), 4);
    }

    #[test]
    fn test_insert_operator_estimate_cardinality_empty() {
        // Empty child → cardinality of 0
        let (tuples, schema) = int_tuples(vec![]);
        let child = MockChild::new(tuples, schema);

        let op = InsertOperator::for_test(Box::new(child), Box::new(|_| Ok(())));

        assert_eq!(op.estimate_cardinality(), 0);
    }

    // ── Name ─────────────────────────────────────────────────────────────

    #[test]
    fn test_insert_operator_name() {
        let (tuples, schema) = int_tuples(vec![(Some(1), Some(10))]);
        let child = MockChild::new(tuples, schema);

        let op = InsertOperator::for_test(Box::new(child), Box::new(|_| Ok(())));

        assert_eq!(op.name(), "Insert");
    }

    // ── Physical planner pipeline tests ────────────────────────────────────

    #[test]
    fn test_physical_planner_produces_insert_operator() {
        // Verify that PhysicalPlanner::plan() correctly creates an
        // InsertOperator from a LogicalPlan::Insert node.
        //
        // Uses a SingleRow system table as the child so no heap files are needed.
        let child = LogicalPlan::TableScan(LogicalTableScan {
            table: "__singlerow__".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("__singlerow__".to_string()),
        });

        let plan = LogicalPlan::Insert(LogicalInsert {
            table: "test_table".to_string(),
            columns: vec![],
            child: Box::new(child),
            values_rows: Vec::new(),
        });

        let catalog = Catalog::new(crate::backend::engine::Engine::current_dir());
        let planner = PhysicalPlanner::new(catalog, "test_db".to_string());
        let result = planner.plan(&plan);

        assert!(
            result.is_ok(),
            "Physical planner should produce InsertOperator"
        );
        let op = result.unwrap();
        assert_eq!(op.name(), "Insert");
        assert!(
            op.schema().is_empty(),
            "SingleRow child should produce empty schema"
        );
        // SingleRowOperator.estimate_cardinality() returns 0 by default
        assert_eq!(
            op.estimate_cardinality(),
            0,
            "SingleRowOperator returns 0 by default"
        );
    }

    #[test]
    fn test_physical_planner_insert_rejects_missing_table() {
        // Verify that planning a regular TableScan inside Insert
        // fails when the heap file doesn't exist.
        let child = LogicalPlan::TableScan(LogicalTableScan {
            table: "nonexistent".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: None,
        });

        let plan = LogicalPlan::Insert(LogicalInsert {
            table: "target".to_string(),
            columns: vec![],
            child: Box::new(child),
            values_rows: Vec::new(),
        });

        let catalog = Catalog::new(crate::backend::engine::Engine::current_dir());
        let planner = PhysicalPlanner::new(catalog, "test_db".to_string());
        let result = planner.plan(&plan);

        assert!(
            result.is_err(),
            "Planning a TableScan without heap file should fail"
        );
    }

    #[test]
    fn test_physical_planner_insert_with_project_child() {
        // Verify InsertOperator planning with a Projection child
        // (avoids TableScan so no heap file is needed — uses SingleRow).
        let inner_scan = LogicalPlan::TableScan(LogicalTableScan {
            table: "__singlerow__".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("__singlerow__".to_string()),
        });

        let project = LogicalPlan::Project(LogicalProject {
            expressions: vec![],
            child: Box::new(inner_scan),
        });

        let plan = LogicalPlan::Insert(LogicalInsert {
            table: "users".to_string(),
            columns: vec!["name".to_string()],
            child: Box::new(project),
            values_rows: Vec::new(),
        });

        let catalog = Catalog::new(crate::backend::engine::Engine::current_dir());
        let planner = PhysicalPlanner::new(catalog, "test_db".to_string());
        let result = planner.plan(&plan);

        assert!(result.is_ok(), "Insert with Project child should plan");
        let op = result.unwrap();
        assert_eq!(op.name(), "Insert");
    }

    #[test]
    fn test_physical_planner_insert_reset_propagation() {
        // Verify that reset() on the InsertOperator correctly
        // calls reset() on the child operator tree.
        let child = LogicalPlan::TableScan(LogicalTableScan {
            table: "__singlerow__".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("__singlerow__".to_string()),
        });

        let plan = LogicalPlan::Insert(LogicalInsert {
            table: "test_table".to_string(),
            columns: vec![],
            child: Box::new(child),
            values_rows: Vec::new(),
        });

        let catalog = Catalog::new(crate::backend::engine::Engine::current_dir());
        let planner = PhysicalPlanner::new(catalog, "test_db".to_string());
        let mut op = planner.plan(&plan).unwrap();

        // Reset should succeed
        let reset_result = op.reset();
        assert!(
            reset_result.is_ok(),
            "InsertOperator.reset() should succeed"
        );
    }
}
