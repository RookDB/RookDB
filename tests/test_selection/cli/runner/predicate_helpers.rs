// Helper functions to create predicates without all the boilerplate.

use storage_manager::backend::executor::selection::{ColumnReference, ComparisonOp, Constant, Expr, Predicate};

// Quick way to reference a column by name
pub fn col_ref(name: &str) -> ColumnReference {
    ColumnReference::new(name.to_string())
}

// Integer predicates

pub fn int_eq(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

pub fn int_ne(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

pub fn int_lt(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

pub fn int_gt(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

pub fn int_le(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

pub fn int_ge(column: &str, value: i32) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Int(value))),
    )
}

// Float predicates

pub fn float_eq(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

pub fn float_ne(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

pub fn float_lt(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

pub fn float_gt(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

pub fn float_le(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

pub fn float_ge(column: &str, value: f64) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Float(value))),
    )
}

// Text/string predicates

pub fn text_eq(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

pub fn text_ne(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

pub fn text_lt(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

pub fn text_gt(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

pub fn text_le(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

pub fn text_ge(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Text(value.to_string()))),
    )
}

// Date predicates

pub fn date_eq(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

pub fn date_ne(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::NotEquals,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

pub fn date_lt(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

pub fn date_gt(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

pub fn date_le(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

pub fn date_ge(column: &str, value: &str) -> Predicate {
    Predicate::Compare(
        Box::new(Expr::Column(col_ref(column))),
        ComparisonOp::GreaterOrEqual,
        Box::new(Expr::Constant(Constant::Date(value.to_string()))),
    )
}

// NULL predicates

pub fn is_null(column: &str) -> Predicate {
    Predicate::IsNull(Box::new(Expr::Column(col_ref(column))))
}

pub fn is_not_null(column: &str) -> Predicate {
    Predicate::IsNotNull(Box::new(Expr::Column(col_ref(column))))
}

// Logical operators (AND/OR)

pub fn and(left: Predicate, right: Predicate) -> Predicate {
    Predicate::and(left, right)
}

pub fn or(left: Predicate, right: Predicate) -> Predicate {
    Predicate::or(left, right)
}
