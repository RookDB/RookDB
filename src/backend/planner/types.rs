//! Types of expressions: what each one evaluates to, and whether its
//! operands fit together.
//!
//! One implementation for every caller: the binder types each expression it
//! binds (so a type error is reported while planning, whatever the data),
//! and the physical planner types the outputs it builds from the same rules,
//! reading column types from its rows' schema instead.
//!
//! A type of `None` is unknown (a NULL literal, or a column whose type is not
//! known while binding) and fits anything.

use rook_ast::{
    ArithOp, ConstantValue as CV, ExprNode as E, FunctionArg, PredicateNode, SubqueryInfo,
};

use crate::backend::error::{RookError, RookResult};
use crate::backend::executor::physical::operators::{
    AggregateFunction, infer_aggregate_output_type,
};
use crate::types::datatype::DataType;

/// Where an expression's columns and subqueries get their types.
pub trait TypeEnv {
    /// The type of a column reference (`Column`, `Compound`, `OuterColumn`).
    fn column(&self, expr: &E) -> RookResult<Option<DataType>>;
    /// The type of a scalar subquery's value.
    fn subquery(&self, info: &SubqueryInfo) -> RookResult<Option<DataType>>;
    /// Whether operands that do not fit are an error here (the binder), or
    /// are left for the executor to report (the physical planner, whose
    /// column types are sometimes placeholders).
    fn strict(&self) -> bool {
        true
    }
}

/// `Err(error)` when `env` is strict.
fn reject(env: &dyn TypeEnv, error: RookError) -> RookResult<()> {
    if env.strict() { Err(error) } else { Ok(()) }
}

/// The broad kind of a type. Values of one kind compare with each other;
/// values of different kinds do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeKind {
    Number,
    Bool,
    Text,
    DateTime,
    Time,
    Bit,
}

pub fn type_kind(t: &DataType) -> TypeKind {
    match t {
        DataType::SmallInt
        | DataType::Int
        | DataType::BigInt
        | DataType::Real
        | DataType::DoublePrecision
        | DataType::Numeric { .. }
        | DataType::Decimal { .. } => TypeKind::Number,
        DataType::Bool => TypeKind::Bool,
        DataType::Char(_) | DataType::Character(_) | DataType::Varchar(_) => TypeKind::Text,
        DataType::Date | DataType::Timestamp => TypeKind::DateTime,
        DataType::Time => TypeKind::Time,
        DataType::Bit(_) => TypeKind::Bit,
    }
}

/// Whether the executor can compare values of types `a` and `b`: those of one
/// kind, a date or time with a string (read as one), and a TIME or DATE with
/// a TIMESTAMP.
fn comparable(a: &DataType, b: &DataType) -> bool {
    use TypeKind as K;
    matches!(
        (type_kind(a), type_kind(b)),
        (x, y) if x == y
    ) || matches!(
        (type_kind(a), type_kind(b)),
        (K::DateTime | K::Time, K::Text)
            | (K::Text, K::DateTime | K::Time)
            | (K::Time, K::DateTime)
            | (K::DateTime, K::Time)
    )
}

/// `t` for a message: a string of unknown length is plain VARCHAR.
fn shown(t: &DataType) -> String {
    match t {
        DataType::Varchar(u16::MAX) => "VARCHAR".to_string(),
        other => other.to_string(),
    }
}

/// `Ok` when values of types `a` and `b` can be compared.
fn check_comparable(
    env: &dyn TypeEnv,
    a: &Option<DataType>,
    b: &Option<DataType>,
) -> RookResult<()> {
    match (a, b) {
        (Some(a), Some(b)) if !comparable(a, b) => reject(
            env,
            RookError::TypeMismatch(format!("Cannot compare {} with {}", shown(a), shown(b))),
        ),
        _ => Ok(()),
    }
}

/// `Ok` when `t` is unknown or of `kind`; otherwise an error naming `what`
/// needs it.
fn check_kind(
    env: &dyn TypeEnv,
    t: &Option<DataType>,
    kind: TypeKind,
    what: &str,
) -> RookResult<()> {
    match t {
        Some(t) if type_kind(t) != kind => reject(
            env,
            RookError::TypeMismatch(format!(
                "{} needs a {} operand, got {}",
                what,
                match kind {
                    TypeKind::Number => "numeric",
                    TypeKind::Bool => "BOOLEAN",
                    TypeKind::Text => "string",
                    TypeKind::DateTime => "date",
                    TypeKind::Time => "TIME",
                    TypeKind::Bit => "BIT",
                },
                t
            )),
        ),
        _ => Ok(()),
    }
}

/// Promote two types to a common type using SQL-99 implicit promotion rules.
///
/// Rules:
///   - If either is DOUBLE → DOUBLE
///   - If either is REAL → DOUBLE
///   - If either is NUMERIC → DOUBLE
///   - If either is BIGINT → BIGINT
///   - If either is INT → INT
///   - SMALLINT + SMALLINT → SMALLINT
///   - Otherwise → INT (non-numeric fallback)
pub fn promote_numeric_type(a: &DataType, b: &DataType) -> DataType {
    use DataType as DT;

    if type_kind(a) != TypeKind::Number || type_kind(b) != TypeKind::Number {
        return DT::Int;
    }

    let rank = |t: &DataType| -> u8 {
        match t {
            DT::SmallInt => 1,
            DT::Int => 2,
            DT::BigInt => 3,
            DT::Real => 4,
            DT::Numeric { .. } | DT::Decimal { .. } => 4,
            DT::DoublePrecision => 5,
            _ => 0,
        }
    };

    match rank(a).max(rank(b)) {
        1 => DT::SmallInt,
        2 => DT::Int,
        3 => DT::BigInt,
        _ => DT::DoublePrecision,
    }
}

/// Result type of arithmetic between a NUMERIC and another exact number
/// (NUMERIC or an integer), which the executor computes exactly as a NUMERIC:
/// `SUM(bigint_col) + 1` is a NUMERIC, not a DOUBLE PRECISION. `None` when
/// neither side is a NUMERIC or one side is not exact.
fn exact_numeric_result_type(a: &DataType, op: ArithOp, b: &DataType) -> Option<DataType> {
    use DataType as DT;

    // Scale of an exact operand; integers have none.
    let scale = |t: &DataType| -> Option<u8> {
        match t {
            DT::SmallInt | DT::Int | DT::BigInt => Some(0),
            DT::Numeric { scale, .. } | DT::Decimal { scale, .. } => Some(*scale),
            _ => None,
        }
    };
    let is_decimal = |t: &DataType| matches!(t, DT::Numeric { .. } | DT::Decimal { .. });
    if !is_decimal(a) && !is_decimal(b) {
        return None;
    }
    let (sa, sb) = (scale(a)?, scale(b)?);
    // The scales the executor's numeric_add/sub/mul/div produce.
    let scale = match op {
        ArithOp::Mul => sa.saturating_add(sb).min(MAX_NUMERIC_PRECISION),
        ArithOp::Div => crate::backend::executor::physical::expr::numeric_division_scale(sa, sb),
        _ => sa.max(sb),
    };
    Some(DT::Numeric {
        precision: MAX_NUMERIC_PRECISION,
        scale,
    })
}

/// Widest NUMERIC precision the engine has.
const MAX_NUMERIC_PRECISION: u8 = 38;

fn op_symbol(op: ArithOp) -> &'static str {
    match op {
        ArithOp::Add => "+",
        ArithOp::Sub => "-",
        ArithOp::Mul => "*",
        ArithOp::Div => "/",
    }
}

/// The type `expr` evaluates to, checking that its operands fit together.
/// A NULL literal has no type of its own: it takes the type of the other
/// operand, branch or argument it stands beside.
pub fn expr_type(expr: &E, env: &dyn TypeEnv) -> RookResult<Option<DataType>> {
    // The first argument or branch whose type is known.
    let first_known = |exprs: &mut dyn Iterator<Item = &E>| -> RookResult<Option<DataType>> {
        let mut found = None;
        for e in exprs {
            let t = expr_type(e, env)?;
            if found.is_none() {
                found = t;
            }
        }
        Ok(found)
    };
    // The type of COALESCE / CASE: that of the first branch whose type is
    // known; every other branch must be of the same kind, or rows of one
    // column would hold values that cannot be compared (`COALESCE(text, 1)`).
    // A string literal is untyped, as in PostgreSQL, and fits any kind.
    let branches =
        |what: &str, exprs: &mut dyn Iterator<Item = &E>| -> RookResult<Option<DataType>> {
            let mut result: Option<DataType> = None;
            let mut typed: Option<DataType> = None;
            for e in exprs {
                let Some(t) = expr_type(e, env)? else {
                    continue;
                };
                if result.is_none() {
                    result = Some(t.clone());
                }
                if matches!(e, E::Constant(CV::Text(_))) {
                    continue;
                }
                match &typed {
                    None => typed = Some(t),
                    // Rows of one column holding both kinds could not be
                    // ordered, so this is an error in either mode.
                    Some(first) if type_kind(first) != type_kind(&t) => {
                        return Err(RookError::TypeMismatch(format!(
                            "{} types {} and {} cannot be matched",
                            what,
                            shown(first),
                            shown(&t)
                        )));
                    }
                    Some(_) => {}
                }
            }
            Ok(result)
        };
    let boolean = |e: &E, what: &str| -> RookResult<()> {
        check_kind(env, &expr_type(e, env)?, TypeKind::Bool, what)
    };
    Ok(match expr {
        E::Column(_) | E::Compound(_) | E::OuterColumn(_) => env.column(expr)?,
        E::Constant(cv) => match cv {
            CV::Null => None,
            CV::Int(i) if i32::try_from(*i).is_ok() => Some(DataType::Int),
            CV::Int(_) => Some(DataType::BigInt),
            CV::Float(_) => Some(DataType::DoublePrecision),
            CV::Text(_) => Some(DataType::Varchar(u16::MAX)),
            CV::Boolean(_) => Some(DataType::Bool),
        },
        E::Binary { left, op, right } => {
            match (expr_type(left, env)?, expr_type(right, env)?) {
                // DATE ± integer days is a DATE.
                (Some(DataType::Date), Some(n)) | (Some(n), Some(DataType::Date))
                    if matches!(op, ArithOp::Add) && type_kind(&n) == TypeKind::Number =>
                {
                    Some(DataType::Date)
                }
                (Some(DataType::Date), Some(n))
                    if matches!(op, ArithOp::Sub) && type_kind(&n) == TypeKind::Number =>
                {
                    Some(DataType::Date)
                }
                (Some(l), Some(r)) => {
                    if type_kind(&l) != TypeKind::Number || type_kind(&r) != TypeKind::Number {
                        reject(
                            env,
                            RookError::TypeMismatch(format!(
                                "operator {} is not defined for {} and {}",
                                op_symbol(*op),
                                shown(&l),
                                shown(&r)
                            )),
                        )?;
                    }
                    Some(
                        exact_numeric_result_type(&l, *op, &r)
                            .unwrap_or_else(|| promote_numeric_type(&l, &r)),
                    )
                }
                (Some(t), None) | (None, Some(t)) => {
                    let date = matches!(op, ArithOp::Add | ArithOp::Sub) && t == DataType::Date;
                    if !date {
                        check_kind(env, &Some(t.clone()), TypeKind::Number, op_symbol(*op))?;
                    }
                    Some(t)
                }
                (None, None) => None,
            }
        }
        E::Cast { expr, data_type } => {
            expr_type(expr, env)?;
            Some(data_type.parse::<DataType>().map_err(|e| {
                RookError::TypeMismatch(format!("Invalid CAST target type '{}': {}", data_type, e))
            })?)
        }
        E::ScalarSubquery(info) => env.subquery(info)?,
        E::Function { name, args, .. } => {
            let mut arg_exprs = args.iter().filter_map(|a| match a {
                FunctionArg::Expr(e) => Some(e.as_ref()),
                FunctionArg::Star => None,
            });
            let upper = name.to_ascii_uppercase();
            let aggregate = match upper.as_str() {
                "COUNT" => Some(AggregateFunction::Count),
                "SUM" => Some(AggregateFunction::Sum),
                "AVG" => Some(AggregateFunction::Avg),
                "MIN" => Some(AggregateFunction::Min),
                "MAX" => Some(AggregateFunction::Max),
                _ => None,
            };
            if let Some(function) = aggregate {
                let input = match arg_exprs.next() {
                    Some(e) => expr_type(e, env)?,
                    None => None,
                };
                if matches!(function, AggregateFunction::Sum | AggregateFunction::Avg) {
                    check_kind(env, &input, TypeKind::Number, &upper)?;
                }
                return Ok(Some(infer_aggregate_output_type(function, input.as_ref())));
            }
            // The type of the first argument, which must be of `kind`.
            let mut first_of = |kind: TypeKind| -> RookResult<Option<DataType>> {
                let first = match arg_exprs.next() {
                    Some(e) => expr_type(e, env)?,
                    None => None,
                };
                check_kind(env, &first, kind, &upper)?;
                for e in arg_exprs.by_ref() {
                    expr_type(e, env)?;
                }
                Ok(first)
            };
            match upper.as_str() {
                "UPPER" | "UCASE" | "LOWER" | "LCASE" | "TRIM" | "LTRIM" | "RTRIM"
                | "SUBSTRING" | "SUBSTR" => {
                    first_of(TypeKind::Text)?;
                    Some(DataType::Varchar(u16::MAX))
                }
                "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                    first_of(TypeKind::Text)?;
                    Some(DataType::Int)
                }
                "||" | "CONCAT" => {
                    first_known(&mut arg_exprs)?;
                    Some(DataType::Varchar(u16::MAX))
                }
                "POSITION" | "CHARINDEX" | "EXTRACT" | "DATE_PART" => {
                    first_known(&mut arg_exprs)?;
                    Some(DataType::Int)
                }
                "POWER" | "POW" | "SQRT" => {
                    first_of(TypeKind::Number)?;
                    Some(DataType::DoublePrecision)
                }
                "CURRENT_DATE" => Some(DataType::Date),
                "CURRENT_TIME" => Some(DataType::Time),
                "CURRENT_TIMESTAMP" | "NOW" => Some(DataType::Timestamp),
                // The result has the type of the (first) argument.
                "ABS" | "FLOOR" | "CEIL" | "CEILING" | "ROUND" => first_of(TypeKind::Number)?,
                "NULLIF" => {
                    let a = arg_exprs.next().map(|e| expr_type(e, env)).transpose()?;
                    let b = arg_exprs.next().map(|e| expr_type(e, env)).transpose()?;
                    check_comparable(env, &a.clone().flatten(), &b.flatten())?;
                    a.flatten()
                }
                "MOD" => first_known(&mut arg_exprs)?,
                "COALESCE" => branches("COALESCE", &mut arg_exprs)?,
                _ => {
                    return Err(RookError::InvalidQuery(format!(
                        "Unknown scalar function: '{}'",
                        name
                    )));
                }
            }
        }
        E::Case {
            when_then_pairs,
            else_result,
        } => {
            for (when, _) in when_then_pairs {
                boolean(when, "CASE WHEN")?;
            }
            branches(
                "CASE",
                &mut when_then_pairs
                    .iter()
                    .map(|(_, then)| then.as_ref())
                    .chain(else_result.as_deref()),
            )?
        }
        E::Compare { left, right, .. } => {
            check_comparable(env, &expr_type(left, env)?, &expr_type(right, env)?)?;
            Some(DataType::Bool)
        }
        E::Logical { left, op, right } => {
            let what = match op {
                rook_ast::BinaryOp::And => "AND",
                rook_ast::BinaryOp::Or => "OR",
            };
            boolean(left, what)?;
            boolean(right, what)?;
            Some(DataType::Bool)
        }
        E::Not(inner) => {
            boolean(inner, "NOT")?;
            Some(DataType::Bool)
        }
        E::IsNull(inner) | E::IsNotNull(inner) => {
            expr_type(inner, env)?;
            Some(DataType::Bool)
        }
        E::Predicate(p) => {
            check_predicate(p, env)?;
            Some(DataType::Bool)
        }
    })
}

/// Check that the operands of every comparison, test and connective in
/// `pred` fit together.
pub fn check_predicate(pred: &PredicateNode, env: &dyn TypeEnv) -> RookResult<()> {
    use PredicateNode as P;
    match pred {
        P::BinaryOp { left, right, .. } => {
            check_predicate(left, env)?;
            check_predicate(right, env)
        }
        P::Not(p) => check_predicate(p, env),
        P::Compare { left, right, .. } | P::IsDistinctFrom { left, right } => {
            check_comparable(env, &expr_type(left, env)?, &expr_type(right, env)?)
        }
        P::IsNull(e) | P::IsNotNull(e) => expr_type(e, env).map(|_| ()),
        P::Between { expr, low, high } => {
            let t = expr_type(expr, env)?;
            check_comparable(env, &t, &expr_type(low, env)?)?;
            check_comparable(env, &t, &expr_type(high, env)?)
        }
        P::InList { expr, list } => {
            let t = expr_type(expr, env)?;
            for item in list {
                check_comparable(env, &t, &expr_type(item, env)?)?;
            }
            Ok(())
        }
        P::Like { expr, .. } => check_kind(env, &expr_type(expr, env)?, TypeKind::Text, "LIKE"),
        P::IsBoolean { expr, .. } => {
            check_kind(env, &expr_type(expr, env)?, TypeKind::Bool, "IS TRUE")
        }
        P::Exists(_) => Ok(()),
        P::InSubquery { expr, subquery, .. } => {
            check_comparable(env, &expr_type(expr, env)?, &env.subquery(subquery)?)
        }
    }
}
