//! Error type for the join subsystem.

use std::fmt;

/// Every fallible operation in the join subsystem returns this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    /// A row could not be decoded from, or encoded to, the on-disk row format.
    Codec(String),
    /// A value did not match the key class its column was resolved to.
    KeyEncoding(String),
    /// The two sides of an equijoin predicate are not comparable, so no
    /// key-based algorithm can be used. Raised at plan time.
    KeyTypeMismatch {
        left: String,
        right: String,
        detail: String,
    },
    /// A column reference could not be resolved, or resolved ambiguously.
    Schema(String),
    /// The requested plan cannot be built - an algorithm that does not support
    /// the join type, a hash join without equijoin keys, and so on.
    Plan(String),
    /// A filesystem failure while reading a relation or a spill file.
    Io(String),
    /// An operator that cannot spill ran out of memory. Recoverable by
    /// choosing an algorithm that can, which is what the adaptive operator
    /// does automatically.
    OutOfMemory(String),
}

impl JoinError {
    pub(crate) fn codec(message: impl Into<String>) -> Self {
        JoinError::Codec(message.into())
    }

    pub(crate) fn key_encoding(message: impl Into<String>) -> Self {
        JoinError::KeyEncoding(message.into())
    }

    pub(crate) fn schema(message: impl Into<String>) -> Self {
        JoinError::Schema(message.into())
    }

    pub(crate) fn plan(message: impl Into<String>) -> Self {
        JoinError::Plan(message.into())
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Codec(message) => write!(f, "row codec error: {message}"),
            JoinError::KeyEncoding(message) => write!(f, "join key error: {message}"),
            JoinError::KeyTypeMismatch {
                left,
                right,
                detail,
            } => write!(
                f,
                "cannot join {left} to {right}: {detail}; add an explicit cast to one side"
            ),
            JoinError::Schema(message) => write!(f, "schema error: {message}"),
            JoinError::Plan(message) => write!(f, "plan error: {message}"),
            JoinError::Io(message) => write!(f, "i/o error: {message}"),
            JoinError::OutOfMemory(message) => write!(f, "out of memory: {message}"),
        }
    }
}

impl std::error::Error for JoinError {}

impl From<std::io::Error> for JoinError {
    fn from(error: std::io::Error) -> Self {
        JoinError::Io(error.to_string())
    }
}
