//! Identifier validation — path-safety checks for database and table names.
//!
//! Database and table names are interpolated directly into filesystem paths
//! (`database/base/{db}/{table}.dat`), so an unsanitised name containing
//! `/`, `\`, `..`, or null bytes could escape the data directory.
//!
//! Every CREATE/DROP entry point validates names through [`validate_identifier`]
//! before any filesystem work happens.

use crate::backend::error::{RookError, RookResult};

/// Characters that must never appear in a database/table/index identifier:
/// path separators and NUL, plus the ones a file name cannot hold on Windows
/// (a quoted identifier may contain any character).
const FORBIDDEN_CHARS: &[char] = &['/', '\\', '\0', '<', '>', ':', '"', '|', '?', '*'];

/// Validate a database, table, or index name for safe use in file paths.
///
/// Rejects:
/// - empty names
/// - names containing `/`, `\`, NUL, or `<>:"|?*` (path traversal / characters
///   a file name cannot hold)
/// - names containing `..` path segments (e.g. `..`, `../etc`, `a/../b`)
/// - names starting with `.` (hidden files such as `.git`, `..foo`)
/// - names consisting solely of dots
/// - control characters
///
/// Returns `Ok(())` or an error describing the first problem found.
pub fn validate_identifier(name: &str) -> RookResult<()> {
    if name.is_empty() {
        return Err(RookError::InvalidIdentifier(
            "Identifier cannot be empty".to_string(),
        ));
    }

    if name.len() > 255 {
        return Err(RookError::InvalidIdentifier(format!(
            "Identifier '{}' exceeds the 255 character limit",
            name
        )));
    }

    if let Some(ch) = name.chars().find(|c| FORBIDDEN_CHARS.contains(c)) {
        return Err(RookError::InvalidIdentifier(format!(
            "Identifier '{}' contains forbidden character {:?}",
            escape_control(name),
            ch
        )));
    }

    if name.starts_with('.') {
        return Err(RookError::InvalidIdentifier(format!(
            "Identifier '{}' cannot start with a dot",
            escape_control(name)
        )));
    }

    // Reject any "." or ".." path segment (covers "..", "a/..", "a..b" is fine,
    // but "a/../b" is not).
    if name == ".."
        || name.split('/').any(|seg| seg == "..")
        || name.split('\\').any(|seg| seg == "..")
    {
        return Err(RookError::InvalidIdentifier(format!(
            "Identifier '{}' cannot contain '..' path segments",
            escape_control(name)
        )));
    }

    if name.chars().any(|c| c.is_control()) {
        return Err(RookError::InvalidIdentifier(format!(
            "Identifier '{}' contains control characters",
            escape_control(name)
        )));
    }

    Ok(())
}

/// Validate a database name (same rules as [`validate_identifier`]).
pub fn validate_database_name(name: &str) -> RookResult<()> {
    validate_identifier(name)
        .map_err(|e| RookError::InvalidIdentifier(format!("Invalid database name: {}", e)))
}

/// Validate a table name (same rules as [`validate_identifier`]).
pub fn validate_table_name(name: &str) -> RookResult<()> {
    validate_identifier(name)
        .map_err(|e| RookError::InvalidIdentifier(format!("Invalid table name: {}", e)))
}

/// Validate an index name.
pub fn validate_index_name(name: &str) -> RookResult<()> {
    validate_identifier(name)
        .map_err(|e| RookError::InvalidIdentifier(format!("Invalid index name: {}", e)))
}

fn escape_control(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_names() {
        for good in [
            "users",
            "my_db",
            "db1",
            "Table_2",
            "a",
            "order-details",
            "café",
        ] {
            assert!(
                validate_identifier(good).is_ok(),
                "'{}' should be accepted",
                good
            );
        }
    }

    #[test]
    fn rejects_empty_name() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn rejects_path_separators() {
        assert!(validate_identifier("a/b").is_err());
        assert!(validate_identifier("a\\b").is_err());
        assert!(validate_identifier("../../etc").is_err());
        assert!(validate_identifier("base/../../evil").is_err());
    }

    #[test]
    fn rejects_parent_directory_segments() {
        assert!(validate_identifier("..").is_err());
        assert!(validate_identifier("../etc").is_err());
        assert!(
            validate_identifier("a..b").is_ok(),
            "'a..b' has no path segment '..'"
        );
    }

    #[test]
    fn rejects_null_bytes_and_controls() {
        assert!(validate_identifier("a\0b").is_err());
        assert!(validate_identifier("a\nb").is_err());
    }

    #[test]
    fn rejects_dot_prefixed_names() {
        assert!(validate_identifier(".hidden").is_err());
        assert!(validate_identifier("..").is_err());
    }

    #[test]
    fn rejects_overlong_names() {
        let long = "a".repeat(256);
        assert!(validate_identifier(&long).is_err());
        let ok = "a".repeat(255);
        assert!(validate_identifier(&ok).is_ok());
    }

    #[test]
    fn typed_helpers_prefix_errors() {
        let err = validate_database_name("x/y").unwrap_err();
        assert!(err.to_string().starts_with("Invalid database name"));
        let err = validate_table_name("x/y").unwrap_err();
        assert!(err.to_string().starts_with("Invalid table name"));
        let err = validate_index_name("").unwrap_err();
        assert!(err.to_string().starts_with("Invalid index name"));
    }
}
