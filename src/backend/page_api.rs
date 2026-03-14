
use std::io;
use std::fs::File;

use crate::backend::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE};

/// Get the lower pointer (insertion point) from a page header
pub fn get_lower(page: &Page) -> io::Result<u32> {
    if page.data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid page: too small for header",
        ));
    }
    let lower = u32::from_le_bytes(
        page.data[0..4].try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid byte slice"))?
    );
    debug_print_page(&format!("get_lower: {} bytes", lower));
    Ok(lower)
}

/// Get the upper pointer (data start point) from a page header
pub fn get_upper(page: &Page) -> io::Result<u32> {
    if page.data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid page: too small for header",
        ));
    }
    let upper = u32::from_le_bytes(
        page.data[4..8].try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid byte slice"))?
    );
    debug_print_page(&format!("get_upper: {} bytes", upper));
    Ok(upper)
}

/// Set the lower pointer in a page header
pub fn set_lower(page: &mut Page, value: u32) -> io::Result<()> {
    if page.data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid page: too small for header",
        ));
    }
    debug_print_page(&format!("set_lower: {} bytes", value));
    page.data[0..4].copy_from_slice(&value.to_le_bytes());
    Ok(())
}

/// Set the upper pointer in a page header
pub fn set_upper(page: &mut Page, value: u32) -> io::Result<()> {
    if page.data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid page: too small for header",
        ));
    }
    debug_print_page(&format!("set_upper: {} bytes", value));
    page.data[4..8].copy_from_slice(&value.to_le_bytes());
    Ok(())
}

/// Get the number of tuples stored in a page
pub fn get_tuple_count(page: &Page) -> io::Result<u32> {
    let lower = get_lower(page)?;
    if lower < PAGE_HEADER_SIZE {
        return Ok(0);
    }
    let count = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
    debug_print_page(&format!("get_tuple_count: {} tuples", count));
    Ok(count)
}

/// Get free space available in a page (in bytes)
pub fn get_free_space(page: &Page) -> io::Result<u32> {
    let lower = get_lower(page)?;
    let upper = get_upper(page)?;
    
    if lower >= upper {
        let free = 0u32;
        debug_print_page(&format!("get_free_space: {} bytes (page full)", free));
        return Ok(free);
    }
    
    let free = upper - lower;
    debug_print_page(&format!("get_free_space: {} bytes available", free));
    Ok(free)
}

/// Check if page can accommodate a tuple of given size
pub fn can_fit_tuple(page: &Page, tuple_size: u32) -> io::Result<bool> {
    let required_space = tuple_size + ITEM_ID_SIZE; // Tuple data + item ID
    let free_space = get_free_space(page)?;
    
    let can_fit = free_space >= required_space;
    debug_print_page(&format!(
        "can_fit_tuple: needs {}, available {}. Result: {}",
        required_space, free_space, can_fit
    ));
    
    Ok(can_fit)
}

/// Get page count from a file header
pub fn get_page_count_from_file(file: &mut File) -> io::Result<u32> {
    let metadata = file.metadata()?;
    let file_size = metadata.len() as u32;
    let page_size = 8192u32; // PAGE_SIZE constant
    
    let count = (file_size + page_size - 1) / page_size; // Ceiling division
    debug_print_page(&format!(
        "get_page_count: file_size={}, page_size={}, count={}",
        file_size, page_size, count
    ));
    
    Ok(count)
}

/// Validate page header integrity
pub fn validate_page_header(page: &Page) -> io::Result<()> {
    let lower = get_lower(page)?;
    let upper = get_upper(page)?;
    
    debug_print_page(&format!("Validating page header: lower={}, upper={}", lower, upper));
    
    // Lower should be at least PAGE_HEADER_SIZE
    if lower < PAGE_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid lower pointer: {} < {}", lower, PAGE_HEADER_SIZE),
        ));
    }
    
    // Lower should be <= upper
    if lower > upper {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid page pointers: lower({}) > upper({})", lower, upper),
        ));
    }
    
    // Both should be within page bounds (8192)
    const PAGE_SIZE: u32 = 8192;
    if upper > PAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Upper pointer out of bounds: {} > {}", upper, PAGE_SIZE),
        ));
    }
    
    debug_print_page("Page header validation passed");
    Ok(())
}

/// Get detailed page statistics as a formatted string
pub fn get_page_stats(page: &Page) -> io::Result<String> {
    let lower = get_lower(page)?;
    let upper = get_upper(page)?;
    let tuple_count = get_tuple_count(page)?;
    let free_space = get_free_space(page)?;
    
    let stats = format!(
        "Lower: {}, Upper: {}, Tuples: {}, Free Space: {} bytes",
        lower, upper, tuple_count, free_space
    );
    
    debug_print_page(&format!("Page stats: {}", stats));
    Ok(stats)
}

/// Reset a page to initial empty state
pub fn reset_page(page: &mut Page) -> io::Result<()> {
    debug_print_page("Resetting page to empty state");
    
    const PAGE_HEADER_SIZE_VAL: u32 = 8u32;
    const PAGE_SIZE: u32 = 8192u32;
    
    // Set lower and upper pointers
    set_lower(page, PAGE_HEADER_SIZE_VAL)?;
    set_upper(page, PAGE_SIZE)?;
    
    debug_print_page("Page reset successfully");
    Ok(())
}

/// Print debug information for page operations
fn debug_print_page(msg: &str) {
    if cfg!(debug_assertions) {
        println!("[PAGE_API] {}", msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_pointers() {
        let mut page = Page::new();
        let _ = set_lower(&mut page, 16);
        let _ = set_upper(&mut page, 8192);
        
        let lower = get_lower(&page).unwrap();
        let upper = get_upper(&page).unwrap();
        
        assert_eq!(lower, 16);
        assert_eq!(upper, 8192);
    }

    #[test]
    fn test_validate_page_header() {
        let mut page = Page::new();
        let _ = set_lower(&mut page, 8);
        let _ = set_upper(&mut page, 8192);
        
        assert!(validate_page_header(&page).is_ok());
    }

    #[test]
    fn test_invalid_page_header() {
        let mut page = Page::new();
        let _ = set_lower(&mut page, 9000);
        let _ = set_upper(&mut page, 8192);
        
        assert!(validate_page_header(&page).is_err());
    }
}
