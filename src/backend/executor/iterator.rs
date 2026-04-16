use crate::backend::executor::Tuple;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ExecutorError {
    Overflow,
    TypeMismatch(String),
    IoError(String),
    InternalError(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorError::Overflow => write!(f, "Numerical overflow occurred during execution"),
            ExecutorError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            ExecutorError::IoError(e) => write!(f, "IO Error: {}", e),
            ExecutorError::InternalError(msg) => write!(f, "Internal Engine Error: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl ExecutorError {
    /// Maps internal execution errors to standard backend HTTP-style status codes.
    pub fn status_code(&self) -> u16 {
        match self {
            ExecutorError::TypeMismatch(_) => 400, // Bad Request equivalents
            ExecutorError::Overflow => 500,        // Internal Server Error equivalents
            ExecutorError::IoError(_) => 500,
            ExecutorError::InternalError(_) => 500,
        }
    }
}

pub trait Executor{
    /// Advances the iterator, returning the next tuple, None if exhausted, or an ExecutorError.
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
}