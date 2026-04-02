use storage_manager::catalog::types::{Column, UdtDefinition};
use storage_manager::executor::payload_utils::octet_length;

#[test]
fn test_json_octet_length() {
    let len = octet_length("JSON", r#"{"name":"alice"}"#, None).unwrap();
    assert_eq!(len, r#"{"name":"alice"}"#.as_bytes().len());
}

#[test]
fn test_json_octet_length_invalid() {
    assert!(octet_length("JSON", "{bad", None).is_err());
}

#[test]
fn test_jsonb_octet_length() {
    // JSONB binary is larger than raw text due to tag bytes and length prefixes
    let len = octet_length("JSONB", r#"{"a":1}"#, None).unwrap();
    assert!(len > 0);
}

#[test]
fn test_xml_octet_length() {
    let xml = "<root><child>hello</child></root>";
    let len = octet_length("XML", xml, None).unwrap();
    assert_eq!(len, xml.as_bytes().len());
}

#[test]
fn test_xml_octet_length_invalid() {
    assert!(octet_length("XML", "<unclosed>", None).is_err());
}

#[test]
fn test_udt_octet_length() {
    let def = UdtDefinition {
        fields: vec![
            Column { name: "id".into(), data_type: "INT".into() },
            Column { name: "name".into(), data_type: "TEXT".into() },
            Column { name: "active".into(), data_type: "BOOLEAN".into() },
        ],
    };
    // INT=4, TEXT=10, BOOLEAN=1 → 15 bytes
    let len = octet_length("UDT:person", "42, alice, true", Some(&def)).unwrap();
    assert_eq!(len, 15);
}

#[test]
fn test_udt_octet_length_missing_def() {
    assert!(octet_length("UDT:missing", "1, x", None).is_err());
}

#[test]
fn test_unsupported_type() {
    assert!(octet_length("FLOAT", "3.14", None).is_err());
}
