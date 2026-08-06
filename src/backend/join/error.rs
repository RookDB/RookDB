//! Error type for the join subsystem.

use std::fmt;

/// Every fallible operation in the join subsystem returns this type.
///
/// Variants are added as the phases that construct them land; there is no
/// speculative variant. See `docs/join/design-rationale.md` for why the
/// subsystem does not reuse `std::io::Error` as its universal error type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    /// A row could not be decoded from, or encoded to, the on-disk row format.
    Codec(String),
}

impl JoinError {
    pub(crate) fn codec(message: impl Into<String>) -> Self {
        JoinError::Codec(message.into())
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Codec(message) => write!(f, "row codec error: {message}"),
        }
    }
}

impl std::error::Error for JoinError {}
