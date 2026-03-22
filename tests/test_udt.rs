use storage_manager::catalog::types::{Column, UdtDefinition};
use storage_manager::executor::udt::UdtSerializer;

fn address_def() -> UdtDefinition {
    UdtDefinition {
        fields: vec![
            Column {
                name: "street".to_string(),
                data_type: "TEXT".to_string(),
            },
            Column {
                name: "city".to_string(),
                data_type: "TEXT".to_string(),
            },
            Column {
                name: "zip".to_string(),
                data_type: "INT".to_string(),
            },
        ],
    }
}

#[test]
fn test_serialize_deserialize_roundtrip() {
    let def = address_def();
    let values = vec!["Main St", "Springfield", "62704"];
    let bytes = UdtSerializer::serialize(&def, &values).unwrap();
    let result = UdtSerializer::deserialize(&def, &bytes).unwrap();
    assert_eq!(result[0], "Main St");
    assert_eq!(result[1], "Springfiel"); // TEXT truncated to 10 bytes
    assert_eq!(result[2], "62704");
}

#[test]
fn test_display_string() {
    let def = address_def();
    let values = vec!["Main St", "Boston", "02101"];
    let bytes = UdtSerializer::serialize(&def, &values).unwrap();
    let display = UdtSerializer::to_display_string(&def, &bytes).unwrap();
    assert_eq!(display, "(street=Main St, city=Boston, zip=2101)");
}

#[test]
fn test_field_count_mismatch() {
    let def = address_def();
    let values = vec!["Main St", "Boston"];
    assert!(UdtSerializer::serialize(&def, &values).is_err());
}

#[test]
fn test_invalid_int() {
    let def = address_def();
    let values = vec!["Main St", "Boston", "not_a_number"];
    assert!(UdtSerializer::serialize(&def, &values).is_err());
}

#[test]
fn test_boolean_field() {
    let def = UdtDefinition {
        fields: vec![
            Column {
                name: "active".to_string(),
                data_type: "BOOLEAN".to_string(),
            },
            Column {
                name: "count".to_string(),
                data_type: "INT".to_string(),
            },
        ],
    };
    let values = vec!["true", "42"];
    let bytes = UdtSerializer::serialize(&def, &values).unwrap();
    let result = UdtSerializer::deserialize(&def, &bytes).unwrap();
    assert_eq!(result[0], "true");
    assert_eq!(result[1], "42");
}
