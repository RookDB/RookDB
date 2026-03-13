//! Types subsystem for fixed-length and parametric SQL data types.

pub mod bit_utils;
pub mod comparison;
pub mod datatype;
pub mod functions;
pub mod null_bitmap;
pub mod row;
pub mod validation;
pub mod value;

pub use comparison::{Comparable, ComparisonError, compare_nullable, nullable_equals};
pub use datatype::DataType;
pub use functions::{DatePart, FunctionError, extract, length, lower, substring, trim, upper};
pub use null_bitmap::NullBitmap;
pub use row::{Row, deserialize_nullable_row, serialize_nullable_row};
pub use validation::{
    TypeValidationError, validate_bigint, validate_bit, validate_bool, validate_date, validate_int,
    validate_smallint, validate_value, validate_varchar,
};
pub use value::DataValue;

#[cfg(test)]
mod tests;
