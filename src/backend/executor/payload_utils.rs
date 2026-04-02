//! Utility functions for variable-length type payloads (JSON, JSONB, XML, UDT).

use crate::catalog::types::UdtDefinition;
use crate::executor::jsonb::JsonbSerializer;
use crate::executor::udt::UdtSerializer;

/// Returns the stored byte length of a variable-length column value.
///
/// - **JSON / XML**: validated, then measured as raw UTF-8 byte length.
/// - **JSONB**: parsed and binary-encoded, then measured.
/// - **UDT**: serialized according to its definition, then measured.
///
/// The returned size does **not** include the 4-byte length prefix that the
/// buffer manager prepends on disk — it is the payload size only.
pub fn octet_length(
    data_type: &str,
    value: &str,
    udt_def: Option<&UdtDefinition>,
) -> Result<usize, String> {
    match data_type {
        "JSON" => {
            crate::executor::json_utils::validate_json(value)?;
            Ok(value.as_bytes().len())
        }
        "JSONB" => {
            let parsed = JsonbSerializer::parse(value)?;
            Ok(JsonbSerializer::to_binary(&parsed).len())
        }
        "XML" => {
            crate::executor::xml_utils::XmlValidator::validate(value)?;
            Ok(value.as_bytes().len())
        }
        dt if dt.starts_with("UDT:") => {
            let def = udt_def.ok_or_else(|| {
                format!("UDT definition required for type '{}'", dt)
            })?;
            let fields: Vec<&str> = value.split(',').map(|s| s.trim()).collect();
            let serialized = UdtSerializer::serialize(def, &fields)?;
            Ok(serialized.len())
        }
        other => Err(format!("octet_length: unsupported type '{}'", other)),
    }
}
