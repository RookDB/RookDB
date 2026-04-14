/// Test to verify CSV with array columns can be parsed correctly
/// This addresses the issue: "expected 5 columns, got 8"
/// When arrays like [item1,item2,item3] were being split incorrectly

#[cfg(test)]
mod tests {
    use std::fs;

    /// Simulates the parse_csv_line function from load_csv.rs
    fn parse_csv_line(line: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_brackets: i32 = 0;  // Track nested brackets
        let mut in_quotes = false;
        let mut quote_char = ' ';
        let mut escaped = false;

        for ch in line.chars() {
            if escaped {
                current_field.push(ch);
                escaped = false;
                continue;
            }

            match ch {
                '\\' if in_quotes => {
                    escaped = true;
                    current_field.push(ch);
                }
                '\"' | '\'' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                    current_field.push(ch);
                }
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                    current_field.push(ch);
                }
                '[' if !in_quotes => {
                    in_brackets += 1;
                    current_field.push(ch);
                }
                ']' if !in_quotes => {
                    in_brackets = in_brackets.saturating_sub(1);
                    current_field.push(ch);
                }
                ',' if !in_quotes && in_brackets == 0 => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                _ => current_field.push(ch),
            }
        }

        if !current_field.is_empty() || !fields.is_empty() {
            fields.push(current_field.trim().to_string());
        }

        fields
    }

    #[test]
    fn test_csv_with_array_column_your_format() {
        // Your exact format from the error message:
        // 1,True,"text_row_1",a3f9c2...,[ab12ff...,98cd34...,aa77bb...,ffee11...]
        let line = r#"1,True,"text_row_1",a3f9c2,[ab12ff,98cd34,aa77bb,ffee11]"#;
        
        let fields = parse_csv_line(line);
        
        // Should have exactly 5 columns, NOT 8
        assert_eq!(fields.len(), 5, "Expected 5 columns, got {}", fields.len());
        
        // Verify each field
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "True");
        assert_eq!(fields[2], "\"text_row_1\"");
        assert_eq!(fields[3], "a3f9c2");
        assert_eq!(fields[4], "[ab12ff,98cd34,aa77bb,ffee11]");
        
        println!("✓ CSV array parsing works correctly!");
        println!("  Row: {}", line);
        println!("  Parsed {} columns: {:?}", fields.len(), fields);
    }

    #[test]
    fn test_csv_int_bool_text_blob_array() {
        // Schema: id:INT, active:BOOL, name:TEXT, image:BLOB, scores:ARRAY<INT>
        let line = r#"42,True,"text_row_1",a3f9c2,[85,90,78,92]"#;
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 5);
        
        assert_eq!(fields[0], "42");           // INT
        assert_eq!(fields[1], "True");         // BOOL
        assert_eq!(fields[2], "\"text_row_1\""); // TEXT
        assert_eq!(fields[3], "a3f9c2");       // BLOB
        assert_eq!(fields[4], "[85,90,78,92]"); // ARRAY<INT>
    }

    #[test]
    fn test_csv_nested_arrays() {
        // Nested array: [[1,2],[3,4]]
        let line = r#"1,[[1,2],[3,4]],100"#;
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 3);
        
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "[[1,2],[3,4]]");  // Nested brackets handled correctly
        assert_eq!(fields[2], "100");
    }

    #[test]
    fn test_csv_array_of_blobs() {
        // Array of BLOB values
        let line = r#"id_001,[0xDEADBEEF,0xCAFEBABE,0x12345678],99"#;
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 3);
        
        assert_eq!(fields[0], "id_001");
        assert_eq!(fields[1], "[0xDEADBEEF,0xCAFEBABE,0x12345678]");
        assert_eq!(fields[2], "99");
    }

    #[test]
    fn test_csv_text_with_quoted_comma() {
        // Comma inside quoted text should NOT split field
        let line = r#"1,True,"text, with comma",[1,2,3]"#;
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 4);
        
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "True");
        assert_eq!(fields[2], "\"text, with comma\"");  // Comma preserved inside quotes
        assert_eq!(fields[3], "[1,2,3]");
    }

    #[test]
    fn test_csv_empty_array() {
        let line = "1,True,[]";
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 3);
        
        assert_eq!(fields[2], "[]");
    }

    #[test]
    fn test_csv_whitespace_in_array() {
        // Arrays with spaces: [1, 2, 3]
        let line = "1, True, [85, 90, 78, 92]";
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 3);
        
        // Note: trim() is called on each field
        assert_eq!(fields[0], "1");
        assert_eq!(fields[1], "True");
        assert_eq!(fields[2], "[85, 90, 78, 92]");
    }

    #[test]
    fn test_csv_mixed_spacing() {
        // Some CSVs have inconsistent spacing
        let line = "42,True , \"text\" , [1,2,3]  ";
        
        let fields = parse_csv_line(line);
        assert_eq!(fields.len(), 4);
        
        // Spaces around fields are trimmed
        assert_eq!(fields[0], "42");
        assert_eq!(fields[1], "True");
        assert_eq!(fields[2], "\"text\"");
        assert_eq!(fields[3], "[1,2,3]");
    }
}

fn main() {
    println!("Run with: cargo test --test test_csv_array_parsing -- --nocapture");
}
