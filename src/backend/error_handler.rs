use std::io;
use std::fmt;

/// Custom error types for RookDB operations
#[derive(Debug)]
pub enum RookDBError {
    FileNotFound(String),
    InvalidPath(String),
    InvalidData(String),
    InvalidDataType(String),
    ValidationError(String),
    IoError(io::Error),
    CatalogError(String),
    TableError(String),
    DiskFull(String),
}

impl fmt::Display for RookDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RookDBError::FileNotFound(path) => {
                write!(f, "File not found at: {}", path)
            }
            RookDBError::InvalidPath(path) => {
                write!(f, "Invalid path: {}", path)
            }
            RookDBError::InvalidData(msg) => {
                write!(f, "Invalid data: {}", msg)
            }
            RookDBError::InvalidDataType(msg) => {
                write!(f, "Invalid data type: {}", msg)
            }
            RookDBError::ValidationError(msg) => {
                write!(f, "Validation error: {}", msg)
            }
            RookDBError::IoError(err) => {
                write!(f, "I/O error: {}", err)
            }
            RookDBError::CatalogError(msg) => {
                write!(f, "Catalog error: {}", msg)
            }
            RookDBError::TableError(msg) => {
                write!(f, "Table error: {}", msg)
            }
            RookDBError::DiskFull(msg) => {
                write!(f, "Disk full error: {}", msg)
            }
        }
    }
}

impl From<io::Error> for RookDBError {
    fn from(err: io::Error) -> Self {
        use std::io::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => {
                RookDBError::FileNotFound(err.to_string())
            }
            ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
                RookDBError::DiskFull(format!("Permission denied or read-only filesystem: {}", err))
            }
            _ => RookDBError::IoError(err),
        }
    }
}

impl std::error::Error for RookDBError {}

/// Result type alias for RookDB operations
pub type RookResult<T> = Result<T, RookDBError>;

/// Validate if a file path exists and is readable
pub fn validate_file_path(path: &str) -> RookResult<()> {
    debug_print_error(&format!("Validating file path: '{}'", path));
    
    let path_obj = std::path::Path::new(path);
    
    if !path_obj.exists() {
        debug_print_error(&format!("Path does not exist: '{}'", path));
        return Err(RookDBError::FileNotFound(path.to_string()));
    }
    
    if path_obj.is_dir() {
        debug_print_error(&format!("Path is a directory, not a file: '{}'", path));
        return Err(RookDBError::InvalidPath(format!("'{}' is a directory, not a file", path)));
    }
    
    debug_print_error(&format!("File path is valid: '{}'", path));
    Ok(())
}

/// Handle CSV path verification before processing
pub fn verify_csv_path(csv_path: &str) -> RookResult<()> {
    debug_print_error(&format!("Verifying CSV file path: '{}'", csv_path));
    
    if csv_path.trim().is_empty() {
        debug_print_error("CSV path is empty");
        return Err(RookDBError::InvalidPath("CSV path cannot be empty".to_string()));
    }
    
    if !csv_path.ends_with(".csv") {
        debug_print_error(&format!("CSV file doesn't end with .csv: '{}'", csv_path));
        log::error!("Warning: File does not have .csv extension. Continuing anyway...");
    }
    
    validate_file_path(csv_path)?;
    
    debug_print_error(&format!("CSV path verified successfully: '{}'", csv_path));
    Ok(())
}

/// Print graceful error message and guidance
pub fn print_error_with_guidance(error: &RookDBError) {
    log::error!("\n{}", error);
    
    match error {
        RookDBError::FileNotFound(_) => {
            log::error!("Please check that the file path is correct and the file exists.");
        }
        RookDBError::InvalidPath(_) => {
            log::error!("Please provide a valid file path.");
        }
        RookDBError::InvalidDataType(_) => {
            log::error!("Supported data types are: INT, TEXT");
        }
        RookDBError::ValidationError(_) => {
            log::error!("Please check your input data and try again.");
        }
        RookDBError::DiskFull(_) => {
            log::error!("Check disk space and file permissions.");
        }
        _ => {}
    }
}

/// Safe file read wrapper
pub fn safe_read_file(path: &str) -> RookResult<String> {
    debug_print_error(&format!("Reading file: '{}'", path));
    
    match std::fs::read_to_string(path) {
        Ok(content) => {
            debug_print_error(&format!("Successfully read {} bytes", content.len()));
            Ok(content)
        }
        Err(err) => {
            let rookdb_err = RookDBError::from(err);
            debug_print_error(&format!("Error reading file: {}", rookdb_err));
            Err(rookdb_err)
        }
    }
}

/// Safe file write wrapper
pub fn safe_write_file(path: &str, content: &str) -> RookResult<()> {
    debug_print_error(&format!("Writing {} bytes to file: '{}'", content.len(), path));
    
    match std::fs::write(path, content) {
        Ok(_) => {
            debug_print_error(&format!("Successfully wrote to: '{}'", path));
            Ok(())
        }
        Err(err) => {
            let rookdb_err = RookDBError::from(err);
            debug_print_error(&format!("Error writing file: {}", rookdb_err));
            Err(rookdb_err)
        }
    }
}

/// Print debug information for error operations
fn debug_print_error(msg: &str) {
    if cfg!(debug_assertions) {
        log::debug!("[ERROR_HANDLER] {}", msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = RookDBError::FileNotFound("test.csv".to_string());
        let msg = err.to_string();
        assert!(msg.contains("File not found"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "test");
        let rookdb_err = RookDBError::from(io_err);
        match rookdb_err {
            RookDBError::FileNotFound(_) => (), // Expected
            _ => panic!("Expected FileNotFound"),
        }
    }
}
