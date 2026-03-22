use quick_xml::Reader;
use quick_xml::events::Event;

/// Validates and normalizes XML text before storage.
pub struct XmlValidator;

impl XmlValidator {
    /// Validate that the input string is well-formed XML.
    /// Returns Ok(()) if valid, Err with description if invalid.
    pub fn validate(xml_text: &str) -> Result<(), String> {
        let trimmed = xml_text.trim();
        if trimmed.is_empty() {
            return Err("Empty XML document".to_string());
        }

        let mut reader = Reader::from_str(trimmed);
        let mut has_root = false;
        let mut tag_stack: Vec<String> = Vec::new();

        loop {
            match reader.read_event() {
                Ok(Event::Start(ref e)) => {
                    has_root = true;
                    let name = String::from_utf8(e.name().as_ref().to_vec())
                        .map_err(|_| "Invalid UTF-8 in tag name".to_string())?;
                    tag_stack.push(name);
                }
                Ok(Event::End(ref e)) => {
                    let name = String::from_utf8(e.name().as_ref().to_vec())
                        .map_err(|_| "Invalid UTF-8 in closing tag name".to_string())?;
                    match tag_stack.pop() {
                        Some(open) if open == name => {}
                        Some(open) => {
                            return Err(format!(
                                "Mismatched tags: opened '{}' but closed '{}'",
                                open, name
                            ));
                        }
                        None => {
                            return Err(format!(
                                "Closing tag '{}' without matching opening tag",
                                name
                            ));
                        }
                    }
                }
                Ok(Event::Empty(_)) => {
                    has_root = true;
                }
                Ok(Event::Eof) => break,
                Ok(_) => {} // Text, CData, Comment, Decl, PI — all fine
                Err(e) => {
                    return Err(format!("XML parse error: {}", e));
                }
            }
        }

        if !tag_stack.is_empty() {
            return Err(format!("Unclosed tag '{}'", tag_stack.last().unwrap()));
        }

        if !has_root {
            return Err("No root element found".to_string());
        }

        Ok(())
    }
}
