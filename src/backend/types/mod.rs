//! Types subsystem for fixed-length and parametric SQL data types.

pub mod bit_utils;
pub mod comparison;
pub mod datatype;
pub mod functions;
pub mod null_bitmap;
pub mod row;
pub mod row_layout;
pub mod validation;
pub mod value;

pub use comparison::{Comparable, ComparisonError, compare_nullable, nullable_equals};
pub use datatype::DataType;
pub use functions::{
    DatePart, FunctionError, abs, cast, ceiling, coalesce, current_date, current_time,
    current_timestamp, extract, floor, length, lower, ltrim, nullif, round, rtrim, substring,
    trim, upper,
};
pub use null_bitmap::NullBitmap;
pub use row::{Row, deserialize_nullable_row, serialize_nullable_row};
pub use row_layout::{PhysicalSchema, RowLayout};
pub use validation::{
    TypeValidationError, validate_bigint, validate_bit, validate_bool, validate_char,
    validate_date, validate_double, validate_int, validate_numeric, validate_real,
    validate_smallint, validate_time, validate_timestamp, validate_value, validate_varchar,
};
pub use value::{DataValue, NumericValue, OrderedF32, OrderedF64};

#[cfg(test)]
mod tests;
