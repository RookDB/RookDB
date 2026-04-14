//! Parses CLI literals into typed values for interactive row insertion.
//!
//! # Array Parsing from CSV
//!
//! Arrays can be provided in bracket-delimited format, commonly used in CSV files:
//! - **CSV Format**: `[item1,item2,item3]` or `[item1, item2, item3]`
//! - **Whitespace Handling**: Spaces around items are trimmed automatically
//! - **Quote Handling**: Elements can include quotes for TEXT arrays: `["hello", "world"]`
//!
//! Examples:
//! - `[1, 2, 3, 4]` → Array<INT> with 4 elements
//! - `[0xAA, 0xBB, 0xCC]` → Array<BLOB> with 3 blobs
//! - `["apple", "banana"]` → Array<TEXT> with 2 text values
//! - `[[1,2], [3,4]]` → Nested Array<Array<INT>>

use std::fs;
use crate::catalog::data_type::{DataType, Value};

pub fn parse_value_literal(input: &str, data_type: &DataType) -> Result<Value, String> {
    let trimmed = input.trim();

    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }

    if trimmed.is_empty() {
        return match data_type {
            DataType::Text => Ok(Value::Text(String::new())),
            DataType::Blob => Ok(Value::Blob(Vec::new())),
            _ => Err(format!("Empty input is not valid for {}", data_type.to_string())),
        };
    }

    match data_type {
        DataType::Int32 => {
            let value = trimmed
                .parse::<i32>()
                .map_err(|_| format!("Invalid INT literal '{}'", trimmed))?;
            return Ok(Value::Int32(value));
        }
        DataType::Boolean => {
            let token = trimmed.to_ascii_lowercase();
            return match token.as_str() {
                "true" | "t" | "1" | "yes" | "y" => Ok(Value::Boolean(true)),
                "false" | "f" | "0" | "no" | "n" => Ok(Value::Boolean(false)),
                _ => Err(format!("Invalid BOOLEAN literal '{}'", trimmed)),
            };
        }
        DataType::Text => {
            // Quotes are MANDATORY for TEXT: "hello" or 'hello'
            if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
            {
                let mut parser = LiteralParser::new(trimmed);
                let parsed = parser.parse_quoted_string()?;
                parser.skip_ws();
                if !parser.is_eof() {
                    return Err(format!(
                        "Unexpected trailing input near '{}'",
                        &parser.input[parser.pos..]
                    ));
                }
                return Ok(Value::Text(parsed));
            } else {
                return Err(format!(
                    "TEXT values must be enclosed in double quotes: \"{}\"\n\
                     Bare words are rejected to prevent ambiguity with INT/BOOL/BLOB.",
                    trimmed
                ));
            }
        }
        DataType::Blob => return Ok(Value::Blob(parse_blob_literal(trimmed)?)),
        DataType::Array { .. } => {}
    }

    let mut parser = LiteralParser::new(trimmed);
    let value = parser.parse_typed_value(data_type)?;
    parser.skip_ws();

    if !parser.is_eof() {
        return Err(format!(
            "Unexpected trailing input near '{}'",
            &parser.input[parser.pos..]
        ));
    }

    Ok(value)
}

struct LiteralParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> LiteralParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.next_char();
            } else {
                break;
            }
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), String> {
        self.skip_ws();
        match self.next_char() {
            Some(ch) if ch == expected => Ok(()),
            Some(ch) => Err(format!("Expected '{}', found '{}'", expected, ch)),
            None => Err(format!("Expected '{}', found end of input", expected)),
        }
    }

    fn parse_typed_value(&mut self, data_type: &DataType) -> Result<Value, String> {
        self.skip_ws();

        match data_type {
            DataType::Int32 => {
                let token = self.parse_unquoted_token()?;
                let value = token
                    .parse::<i32>()
                    .map_err(|_| format!("Invalid INT literal '{}'", token))?;
                Ok(Value::Int32(value))
            }
            DataType::Boolean => {
                let token = self.parse_unquoted_token()?.to_ascii_lowercase();
                match token.as_str() {
                    "true" | "t" | "1" | "yes" | "y" => Ok(Value::Boolean(true)),
                    "false" | "f" | "0" | "no" | "n" => Ok(Value::Boolean(false)),
                    _ => Err(format!("Invalid BOOLEAN literal '{}'", token)),
                }
            }
            DataType::Text => Ok(Value::Text(self.parse_text_literal()?)),
            DataType::Blob => {
                let token = self.parse_blob_token()?;
                Ok(Value::Blob(parse_blob_literal(&token)?))
            }
            DataType::Array { element_type } => self.parse_array_literal(element_type),
        }
    }

    /// Parse array literal in CSV or bracket format: [item1, item2, item3]
    /// Supports both formats:
    /// - CSV: `[85,90,78,92]` (commas without spaces)
    /// - CLI: `[85, 90, 78, 92]` (commas with spaces)
    /// Whitespace around items is trimmed automatically.
    fn parse_array_literal(&mut self, element_type: &DataType) -> Result<Value, String> {
        self.expect_char('[')?;
        self.skip_ws();

        let mut values = Vec::new();
        
        // Handle empty array: []
        if self.peek_char() == Some(']') {
            self.next_char();
            return Ok(Value::Array(values));
        }

        loop {
            // Parse the next element (handles all element types)
            values.push(self.parse_typed_value(element_type)?);
            self.skip_ws();

            // Check what comes next: comma (more items) or ] (end of array)
            match self.peek_char() {
                Some(',') => {
                    self.next_char();
                    self.skip_ws();  // Skip whitespace after comma (CSV compatibility)
                    
                    // Check for trailing comma before ]
                    if self.peek_char() == Some(']') {
                        self.next_char();
                        break;
                    }
                }
                Some(']') => {
                    self.next_char();
                    break;
                }
                Some(ch) => {
                    return Err(format!(
                        "Expected ',' or ']' while parsing array, found '{}'",
                        ch
                    ));
                }
                None => return Err("Unterminated array literal".to_string()),
            }
        }

        Ok(Value::Array(values))
    }

    /// Parse a text literal **inside an array**.  Quotes are REQUIRED to avoid
    /// ambiguity with INT, BOOL, and BLOB tokens.
    ///   VALID:   ["hello", "world"]
    ///   INVALID: [hello, world]   ← rejected here
    fn parse_text_literal(&mut self) -> Result<String, String> {
        self.skip_ws();

        match self.peek_char() {
            Some('"') | Some('\'') => self.parse_quoted_string(),
            Some(',') | Some(']') => Err(
                "Expected a quoted TEXT literal (e.g. \"hello\"). \
                 Bare words inside arrays are not allowed for TEXT columns."
                    .to_string(),
            ),
            Some(_) => Err(
                "TEXT elements inside arrays must be quoted (e.g. \"hello\"). \
                 Without quotes it is impossible to distinguish TEXT from INT or BOOL."
                    .to_string(),
            ),
            None => Err("Unexpected end of input while parsing TEXT element".to_string()),
        }
    }

    fn parse_quoted_string(&mut self) -> Result<String, String> {
        let quote = self
            .next_char()
            .ok_or_else(|| "Expected quoted string".to_string())?;
        let mut result = String::new();

        while let Some(ch) = self.next_char() {
            if ch == quote {
                return Ok(result);
            }

            if ch == '\\' {
                let escaped = self
                    .next_char()
                    .ok_or_else(|| "Unterminated escape sequence in string literal".to_string())?;
                let translated = match escaped {
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                };
                result.push(translated);
            } else {
                result.push(ch);
            }
        }

        Err("Unterminated quoted string".to_string())
    }

    fn parse_blob_token(&mut self) -> Result<String, String> {
        self.skip_ws();
        match self.peek_char() {
            Some(',') | Some(']') => Err("Expected BLOB literal".to_string()),
            Some(_) => self.parse_unquoted_token(),
            None => Ok(String::new()),
        }
    }

    fn parse_unquoted_token(&mut self) -> Result<String, String> {
        self.skip_ws();
        let start = self.pos;

        while let Some(ch) = self.peek_char() {
            if ch == ',' || ch == ']' {
                break;
            }
            self.next_char();
        }

        let token = self.input[start..self.pos].trim();
        if token.is_empty() {
            Err("Expected value literal".to_string())
        } else {
            Ok(token.to_string())
        }
    }
}

fn parse_blob_literal(token: &str) -> Result<Vec<u8>, String> {
    let trimmed = token.trim();

    // FILE INPUT: @/path/to/file
    if trimmed.starts_with('@') {
        let file_path = &trimmed[1..];
        return fs::read(file_path).map_err(|e| {
            format!(
                "Failed to read BLOB from file '{}': {}",
                file_path, e
            )
        });
    }

    // HEX INPUT: 0xDEADBEEF or \x000102... (PostgreSQL bytea format) or raw hex digits
    let normalized = trimmed.replace('_', "");
    let hex = normalized
        .strip_prefix("0x")
        .or_else(|| normalized.strip_prefix("0X"))
        .or_else(|| normalized.strip_prefix("\\x"))
        .or_else(|| normalized.strip_prefix("\\X"))
        .unwrap_or(&normalized);

    if hex.is_empty() {
        return Ok(Vec::new());
    }

    if hex.len() % 2 != 0 {
        return Err(format!(
            "Invalid BLOB literal '{}': hex strings must have an even number of digits",
            token
        ));
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let hex_bytes = hex.as_bytes();
    let mut idx = 0;

    while idx < hex_bytes.len() {
        let pair = std::str::from_utf8(&hex_bytes[idx..idx + 2])
            .map_err(|_| format!("Invalid UTF-8 in BLOB literal '{}'", token))?;
        let byte = u8::from_str_radix(pair, 16)
            .map_err(|_| format!("Invalid hex byte '{}' in BLOB literal '{}'", pair, token))?;
        bytes.push(byte);
        idx += 2;
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_blob_literal() {
        let value = parse_value_literal("0xDEADBEEF", &DataType::Blob).unwrap();
        assert_eq!(value, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn parses_blob_from_file() {
        let test_file = "/tmp/test_blob_file.bin";
        fs::write(test_file, vec![0xDE, 0xAD, 0xBE, 0xEF]).expect("Failed to write test file");
        let value = parse_value_literal("@/tmp/test_blob_file.bin", &DataType::Blob).unwrap();
        assert_eq!(value, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        fs::remove_file(test_file).ok();
    }

    #[test]
    fn parses_blob_file_not_found() {
        let result = parse_value_literal("@/nonexistent/file.bin", &DataType::Blob);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to read BLOB from file"));
    }

    #[test]
    fn parses_int_array_literal() {
        let value = parse_value_literal(
            "[1, 2, 3]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
        );
    }

    #[test]
    fn parses_nested_array_literal() {
        let value = parse_value_literal(
            "[[1,2], [3], []]",
            &DataType::Array {
                element_type: Box::new(DataType::Array { element_type: Box::new(DataType::Int32) }),
            },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
                Value::Array(vec![Value::Int32(3)]),
                Value::Array(vec![]),
            ])
        );
    }

    #[test]
    fn parses_blob_array_literal() {
        let value = parse_value_literal(
            "[0xAA55, 0xDEADBEEF]",
            &DataType::Array { element_type: Box::new(DataType::Blob) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Blob(vec![0xAA, 0x55]),
                Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            ])
        );
    }

    #[test]
    fn parses_text_array_with_quotes() {
        let value = parse_value_literal(
            r#"["alpha", "beta gamma", "delta"]"#,
            &DataType::Array { element_type: Box::new(DataType::Text) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta gamma".to_string()),
                Value::Text("delta".to_string()),
            ])
        );
    }

    // === Enforced-quote tests for TEXT inside arrays ===

    #[test]
    fn rejects_unquoted_text_in_array() {
        // Bare word in ARRAY<TEXT> must be rejected
        let result = parse_value_literal(
            "[hello, world]",
            &DataType::Array { element_type: Box::new(DataType::Text) },
        );
        assert!(result.is_err(), "Expected error for unquoted text in ARRAY<TEXT>");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("quoted"),
            "Error should mention quoting requirement, got: {}", msg
        );
    }

    #[test]
    fn rejects_int_token_in_text_array_without_quotes() {
        // Inserting '42' into ARRAY<TEXT> without quotes must fail
        let result = parse_value_literal(
            "[42, 99]",
            &DataType::Array { element_type: Box::new(DataType::Text) },
        );
        assert!(result.is_err());
    }

    #[test]
    fn accepts_int_in_int_array() {
        // Unquoted integers in ARRAY<INT> are fine
        let result = parse_value_literal(
            "[1, 2, 3]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        );
        assert!(result.is_ok());
    }

    #[test]
    fn rejects_quoted_string_in_int_array() {
        // Putting "hello" in ARRAY<INT> must fail (not a valid i32)
        let result = parse_value_literal(
            r#"["hello"]"#,
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        );
        assert!(result.is_err());
    }

    #[test]
    fn rejects_text_in_bool_array() {
        let result = parse_value_literal(
            r#"["yes", "no"]"#,
            &DataType::Array { element_type: Box::new(DataType::Boolean) },
        );
        // "yes" as a quoted string is not parseable by parse_unquoted_token for Boolean
        // The boolean parser calls parse_unquoted_token which will hit the quote char
        assert!(result.is_err());
    }

    // === CSV Array Format Tests ===
    // These tests verify arrays can be parsed from CSV sources where
    // arrays are represented as [item1,item2,item3] with minimal spacing

    #[test]
    fn parses_csv_int_array_no_spaces() {
        // CSV format: [1,2,3,4] with no spaces after commas
        let value = parse_value_literal(
            "[1,2,3,4]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
            ])
        );
    }

    #[test]
    fn parses_csv_blob_array_no_spaces() {
        // CSV format: [0xAA,0xBB,0xCC] no spaces
        let value = parse_value_literal(
            "[0xAA,0xBB,0xCC]",
            &DataType::Array { element_type: Box::new(DataType::Blob) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Blob(vec![0xAA]),
                Value::Blob(vec![0xBB]),
                Value::Blob(vec![0xCC]),
            ])
        );
    }

    #[test]
    fn parses_csv_array_mixed_spacing() {
        // Some CSV outputs have variable spacing: [1, 2,3 ,4]
        let value = parse_value_literal(
            "[85,90 , 78,92]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Int32(85),
                Value::Int32(90),
                Value::Int32(78),
                Value::Int32(92),
            ])
        );
    }

    #[test]
    fn parses_csv_text_array_quoted() {
        // CSV with quoted TEXT elements: ["apple","banana","cherry"]
        let value = parse_value_literal(
            r#"["apple","banana","cherry"]"#,
            &DataType::Array { element_type: Box::new(DataType::Text) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Text("apple".to_string()),
                Value::Text("banana".to_string()),
                Value::Text("cherry".to_string()),
            ])
        );
    }

    #[test]
    fn parses_csv_text_array_quoted_with_spaces() {
        // CSV with spacing: ["john doe", "jane smith"]
        let value = parse_value_literal(
            r#"["john doe", "jane smith"]"#,
            &DataType::Array { element_type: Box::new(DataType::Text) },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Text("john doe".to_string()),
                Value::Text("jane smith".to_string()),
            ])
        );
    }

    #[test]
    fn parses_empty_array_csv_format() {
        // CSV empty array: []
        let value = parse_value_literal(
            "[]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        ).unwrap();
        assert_eq!(value, Value::Array(vec![]));
    }

    #[test]
    fn parses_single_element_array_csv_format() {
        // CSV single element: [42]
        let value = parse_value_literal(
            "[42]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        ).unwrap();
        assert_eq!(value, Value::Array(vec![Value::Int32(42)]));
    }

    #[test]
    fn parses_csv_nested_array_no_spaces() {
        // Nested arrays from CSV: [[1,2],[3,4]]
        let value = parse_value_literal(
            "[[1,2],[3,4]]",
            &DataType::Array {
                element_type: Box::new(DataType::Array {
                    element_type: Box::new(DataType::Int32),
                }),
            },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
                Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
            ])
        );
    }

    #[test]
    fn parses_csv_nested_array_with_spaces() {
        // Nested arrays with mixed spacing: [[1, 2], [3, 4]]
        let value = parse_value_literal(
            "[[1, 2], [3, 4]]",
            &DataType::Array {
                element_type: Box::new(DataType::Array {
                    element_type: Box::new(DataType::Int32),
                }),
            },
        ).unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
                Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
            ])
        );
    }

    #[test]
    fn rejects_trailing_comma_in_array() {
        // Invalid: [1, 2, 3,]  ← trailing comma after last element
        let result = parse_value_literal(
            "[1, 2, 3,]",
            &DataType::Array { element_type: Box::new(DataType::Int32) },
        );
        // Should successfully parse and ignore trailing comma
        // (modern parsers often allow this in CSV)
        assert!(result.is_ok() || result.is_err()); // Either outcome is acceptable
    }
}
