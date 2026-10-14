//! Typed error hierarchy for programmatic error handling.
//!
//! Historically every fallible API returned `Result<T, String>`, forcing
//! callers to string-match to distinguish a constraint violation from an
//! I/O failure. [`RookError`] gives errors structure while remaining cheap
//! to adopt: `From<String>` lets existing string-producing code paths flow
//! into typed results through `?` unchanged, and `Display` renders the same
//! human-readable messages as before.
//!
//! ```text
//! match validate_row_insert(&catalog, "db", "t", &["NULL"]) {
//!     Err(RookError::ConstraintViolation { kind: ConstraintKind::NotNull, .. }) => { … }
//!     Err(e) if e.is_io() => { … }
//!     Err(e) => eprintln!("{}", e),
//!     Ok(()) => { … }
//! }
//! ```

use std::fmt;

/// The class of constraint that was violated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintKind {
    NotNull,
    Unique,
    ForeignKey,
    Check,
}

impl ConstraintKind {
    /// Stable machine-readable name (suitable for logs / API responses).
    pub fn as_str(&self) -> &'static str {
        match self {
            ConstraintKind::NotNull => "NOT NULL",
            ConstraintKind::Unique => "UNIQUE",
            ConstraintKind::ForeignKey => "FOREIGN KEY",
            ConstraintKind::Check => "CHECK",
        }
    }
}

impl fmt::Display for ConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured error type for RookDB operations.
#[derive(Debug)]
pub enum RookError {
    /// A NOT NULL / UNIQUE / FOREIGN KEY / CHECK constraint was violated.
    ConstraintViolation {
        kind: ConstraintKind,
        table: String,
        column: Option<String>,
        message: String,
    },
    /// A referenced database / table / column does not exist.
    NotFound { entity: &'static str, name: String },
    /// A bare column name matches columns of more than one joined input.
    AmbiguousColumn(String),
    /// A name failed path-safety validation (see `name_validation`); holds
    /// the full human-readable reason.
    InvalidIdentifier(String),
    /// A value could not be interpreted for its column's data type.
    TypeMismatch(String),
    /// Underlying storage I/O failure.
    Io(std::io::Error),
    /// A storage failure whose message is shown as-is (no `I/O error:`
    /// prefix), optionally led by a context phrase: `"context: source"`.
    ///
    /// Used where a layer above the disk manager already rendered an
    /// `io::Error` into its own user-facing sentence; unlike [`RookError::Io`]
    /// plus [`RookError::with_context`], the text stays exactly that sentence,
    /// while `is_io()` and `source()` still reach the original `io::Error`.
    Storage {
        context: Option<String>,
        source: std::io::Error,
    },
    /// An error with additional human-readable context attached.
    ///
    /// Produced by [`RookError::with_context`]; the inner error keeps its
    /// semantics — the accessors (`is_io`, `is_constraint_violation`,
    /// `constraint_kind`) match through to the source — while `Display`
    /// renders `"context: source"`.
    Contextual {
        context: String,
        source: Box<RookError>,
    },
    /// Anything not yet covered by a structured variant.
    Internal(String),
}

impl RookError {
    /// Convenience constructor for constraint violations.
    pub fn constraint(
        kind: ConstraintKind,
        table: &str,
        column: Option<&str>,
        message: impl Into<String>,
    ) -> Self {
        RookError::ConstraintViolation {
            kind,
            table: table.to_string(),
            column: column.map(str::to_string),
            message: message.into(),
        }
    }

    /// A storage failure rendered as `"context: source"`.
    pub fn storage(context: impl Into<String>, source: std::io::Error) -> Self {
        RookError::Storage {
            context: Some(context.into()),
            source,
        }
    }

    /// A storage failure rendered as the bare `io::Error` text.
    pub fn storage_bare(source: std::io::Error) -> Self {
        RookError::Storage {
            context: None,
            source,
        }
    }

    /// `true` when this error represents any constraint violation.
    pub fn is_constraint_violation(&self) -> bool {
        matches!(self, RookError::ConstraintViolation { .. })
    }

    /// The violated constraint kind, if this is a constraint violation.
    pub fn constraint_kind(&self) -> Option<ConstraintKind> {
        match self {
            RookError::ConstraintViolation { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// `true` when the failure came from the storage layer.
    pub fn is_io(&self) -> bool {
        match self {
            RookError::Io(_) | RookError::Storage { .. } => true,
            RookError::Contextual { source, .. } => source.is_io(),
            _ => false,
        }
    }

    /// Attach human-readable context to this error, preserving its variant
    /// semantics (see [`RookError::Contextual`]). Contexts chain when applied
    /// repeatedly: the most recent context renders first.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        RookError::Contextual {
            context: context.into(),
            source: Box::new(self),
        }
    }
}

impl fmt::Display for RookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RookError::ConstraintViolation { message, .. } => f.write_str(message),
            RookError::NotFound { entity, name } => {
                write!(f, "{} '{}' not found", entity, name)
            }
            RookError::AmbiguousColumn(name) => {
                write!(f, "Column reference '{}' is ambiguous", name)
            }
            RookError::InvalidIdentifier(message) => f.write_str(message),
            RookError::TypeMismatch(message) => f.write_str(message),
            RookError::Io(e) => write!(f, "I/O error: {}", e),
            RookError::Storage {
                context: Some(context),
                source,
            } => write!(f, "{}: {}", context, source),
            RookError::Storage {
                context: None,
                source,
            } => write!(f, "{}", source),
            RookError::Contextual { context, source } => write!(f, "{}: {}", context, source),
            RookError::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RookError::Io(e) => Some(e),
            RookError::Storage { source, .. } => Some(source),
            RookError::Contextual { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

/// Bridge legacy `String` errors into the typed hierarchy.
impl From<String> for RookError {
    fn from(s: String) -> Self {
        RookError::Internal(s)
    }
}

impl From<&str> for RookError {
    fn from(s: &str) -> Self {
        RookError::from(s.to_string())
    }
}

impl From<RookError> for String {
    fn from(e: RookError) -> Self {
        e.to_string()
    }
}

impl From<std::io::Error> for RookError {
    fn from(e: std::io::Error) -> Self {
        RookError::Io(e)
    }
}

/// Attach a context phrase to a failure while keeping its kind: an
/// `io::Error` becomes [`RookError::Storage`], a [`RookError`] gets
/// [`RookError::with_context`]. Either way the text renders `"context: cause"`.
pub trait ResultContext<T> {
    fn context(self, context: impl Into<String>) -> RookResult<T>;
}

impl<T> ResultContext<T> for Result<T, std::io::Error> {
    fn context(self, context: impl Into<String>) -> RookResult<T> {
        self.map_err(|e| RookError::storage(context, e))
    }
}

impl<T> ResultContext<T> for Result<T, RookError> {
    fn context(self, context: impl Into<String>) -> RookResult<T> {
        self.map_err(|e| e.with_context(context))
    }
}

/// Result alias used by newly migrated APIs.
pub type RookResult<T> = Result<T, RookError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_matches_legacy_message_text() {
        let e = RookError::constraint(
            ConstraintKind::NotNull,
            "users",
            Some("name"),
            "NOT NULL constraint violated: column 'name' cannot be null",
        );
        assert_eq!(
            e.to_string(),
            "NOT NULL constraint violated: column 'name' cannot be null"
        );
    }

    #[test]
    fn kind_accessors() {
        let e = RookError::constraint(ConstraintKind::Unique, "t", None, "boom");
        assert!(e.is_constraint_violation());
        assert_eq!(e.constraint_kind(), Some(ConstraintKind::Unique));
        assert!(!e.is_io());

        let io = RookError::from(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(io.is_io());
        assert_eq!(io.constraint_kind(), None);
    }

    #[test]
    fn storage_renders_without_io_prefix_and_keeps_kind() {
        let src = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let e = RookError::storage("Failed to open heap", src);
        assert_eq!(e.to_string(), "Failed to open heap: gone");
        assert!(e.is_io());
        assert!(std::error::Error::source(&e).is_some());
        let bare = RookError::storage_bare(std::io::Error::other("boom"));
        assert_eq!(bare.to_string(), "boom");
    }

    #[test]
    fn from_string_maps_to_internal() {
        let e: RookError = "some random failure".into();
        assert!(!e.is_constraint_violation());
        assert!(matches!(e, RookError::Internal(_)));

        let e2: RookError = "UNIQUE constraint violated: value '1' dup".into();
        assert!(matches!(e2, RookError::Internal(_)));
    }

    #[test]
    fn with_context_preserves_semantics_and_renders_first() {
        let io = RookError::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no such file",
        ));
        let ctx = io.with_context("Failed to open heap for table 'users'");

        // Semantics preserved through the wrapper
        assert!(ctx.is_io());
        assert_eq!(ctx.constraint_kind(), None);
        // Context renders first, source after
        assert_eq!(
            ctx.to_string(),
            "Failed to open heap for table 'users': I/O error: no such file"
        );
        // std::error::Error::source chains to the inner error (one hop at a
        // time: Contextual → Io → std::io::Error)
        let src = std::error::Error::source(&ctx).expect("contextual error has a source");
        let inner = src
            .downcast_ref::<RookError>()
            .expect("source is a RookError");
        assert!(inner.is_io());
        assert!(
            std::error::Error::source(inner).is_some(),
            "Io wraps the io::Error"
        );
    }

    #[test]
    fn question_mark_auto_converts_strings() {
        fn inner() -> Result<(), String> {
            Err("some error".to_string())
        }
        fn outer() -> RookResult<()> {
            inner()?;
            Ok(())
        }
        let err = outer().unwrap_err();
        assert!(matches!(err, RookError::Internal(_)));
    }
}
