use storage_manager::executor::xml_utils::XmlValidator;

#[test]
fn test_valid_simple_element() {
    assert!(XmlValidator::validate("<root/>").is_ok());
}

#[test]
fn test_valid_element_with_content() {
    assert!(XmlValidator::validate("<root>hello</root>").is_ok());
}

#[test]
fn test_valid_nested_elements() {
    assert!(XmlValidator::validate("<a><b><c/></b></a>").is_ok());
}

#[test]
fn test_valid_with_attributes() {
    assert!(XmlValidator::validate(r#"<person name="John" age="30"/>"#).is_ok());
}

#[test]
fn test_valid_with_declaration() {
    assert!(XmlValidator::validate(r#"<?xml version="1.0"?><root/>"#).is_ok());
}

#[test]
fn test_valid_with_comment() {
    assert!(XmlValidator::validate("<root><!-- comment --><child/></root>").is_ok());
}

#[test]
fn test_valid_with_cdata() {
    assert!(XmlValidator::validate("<root><![CDATA[some <data>]]></root>").is_ok());
}

#[test]
fn test_valid_mixed_content() {
    let xml = r#"<doc><title>Hello</title><body attr="val">World</body></doc>"#;
    assert!(XmlValidator::validate(xml).is_ok());
}

#[test]
fn test_invalid_empty() {
    assert!(XmlValidator::validate("").is_err());
}

#[test]
fn test_invalid_unclosed_tag() {
    assert!(XmlValidator::validate("<root>").is_err());
}

#[test]
fn test_invalid_no_root_element() {
    assert!(XmlValidator::validate("just text").is_err());
}
