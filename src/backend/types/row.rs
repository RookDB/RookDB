use crate::types::comparison::value_type_name;
use crate::types::datatype::DataType;
use crate::types::null_bitmap::NullBitmap;
use crate::types::value::DataValue;

pub fn serialize_nullable_row(
    schema: &[DataType],
    values: &[Option<&str>],
) -> Result<Vec<u8>, String> {
    if schema.len() != values.len() {
        return Err(format!(
            "Schema/value length mismatch: schema={}, values={}",
            schema.len(),
            values.len()
        ));
    }

    let mut bitmap = NullBitmap::new(schema.len());
    let bitmap_len = schema.len().div_ceil(8);
    let mut row = vec![0u8; bitmap_len];

    for (i, (ty, maybe_value)) in schema.iter().zip(values.iter()).enumerate() {
        match maybe_value {
            Some(raw) => {
                let encoded = DataValue::parse_and_encode(ty, raw)?;
                row.extend_from_slice(&encoded);
            }
            None => bitmap.set_null(i),
        }
    }

    row[..bitmap_len].copy_from_slice(bitmap.as_bytes());
    Ok(row)
}

pub fn deserialize_nullable_row(
    schema: &[DataType],
    row_bytes: &[u8],
) -> Result<Vec<Option<DataValue>>, String> {
    let bitmap_len = schema.len().div_ceil(8);
    if row_bytes.len() < bitmap_len {
        return Err("Row shorter than NULL bitmap".to_string());
    }

    let bitmap = NullBitmap::from_bytes(schema.len(), &row_bytes[..bitmap_len])?;
    let mut cursor = bitmap_len;
    let mut out = Vec::with_capacity(schema.len());

    for (i, ty) in schema.iter().enumerate() {
        if bitmap.is_null(i) {
            out.push(None);
            continue;
        }

        let remaining = &row_bytes[cursor..];
        let encoded_len = ty.encoded_len(remaining)?;
        let value = DataValue::from_bytes(ty, &remaining[..encoded_len])?;
        cursor += encoded_len;
        out.push(Some(value));
    }

    if cursor != row_bytes.len() {
        return Err(format!(
            "Row has {} trailing byte(s) after decode",
            row_bytes.len() - cursor
        ));
    }

    Ok(out)
}

fn data_value_matches_type(ty: &DataType, value: &DataValue) -> bool {
    matches!(
        (ty, value),
        (DataType::SmallInt, DataValue::SmallInt(_))
            | (DataType::Int, DataValue::Int(_))
            | (DataType::BigInt, DataValue::BigInt(_))
            | (DataType::Real, DataValue::Real(_))
            | (DataType::DoublePrecision, DataValue::DoublePrecision(_))
            | (DataType::Bool, DataValue::Bool(_))
            | (DataType::Varchar(_), DataValue::Varchar(_))
            | (DataType::Date, DataValue::Date(_))
            | (DataType::Bit(_), DataValue::Bit(_))
    )
}

fn serialize_nullable_typed_row(
    schema: &[DataType],
    values: &[Option<DataValue>],
) -> Result<Vec<u8>, String> {
    if schema.len() != values.len() {
        return Err(format!(
            "Schema/value length mismatch: schema={}, values={}",
            schema.len(),
            values.len()
        ));
    }

    let bitmap_len = schema.len().div_ceil(8);
    let mut bitmap = NullBitmap::new(schema.len());
    let mut out = vec![0u8; bitmap_len];

    for (i, (ty, maybe_value)) in schema.iter().zip(values.iter()).enumerate() {
        match maybe_value {
            Some(value) => {
                if !data_value_matches_type(ty, value) {
                    return Err(format!(
                        "Type mismatch at column {}: expected {}, got {}",
                        i,
                        ty,
                        value_type_name(value)
                    ));
                }
                out.extend_from_slice(&value.to_bytes());
            }
            None => bitmap.set_null(i),
        }
    }

    out[..bitmap_len].copy_from_slice(bitmap.as_bytes());
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct Row {
    schema: Vec<DataType>,
    null_bitmap: NullBitmap,
    data: Vec<u8>,
}

impl Row {
    pub fn new(schema: Vec<DataType>) -> Self {
        let mut null_bitmap = NullBitmap::new(schema.len());
        for i in 0..schema.len() {
            null_bitmap.set_null(i);
        }
        Self {
            schema,
            null_bitmap,
            data: Vec::new(),
        }
    }

    fn to_values(&self) -> Result<Vec<Option<DataValue>>, String> {
        deserialize_nullable_row(&self.schema, &self.serialize())
    }

    fn rebuild_from_values(&mut self, values: &[Option<DataValue>]) -> Result<(), String> {
        let row_bytes = serialize_nullable_typed_row(&self.schema, values)?;
        let bitmap_len = self.schema.len().div_ceil(8);
        self.null_bitmap = NullBitmap::from_bytes(self.schema.len(), &row_bytes[..bitmap_len])?;
        self.data = row_bytes[bitmap_len..].to_vec();
        Ok(())
    }

    pub fn set_value(&mut self, column_index: usize, value: &DataValue) -> Result<(), String> {
        if column_index >= self.schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }

        if !data_value_matches_type(&self.schema[column_index], value) {
            return Err(format!(
                "Type mismatch at column {}: expected {}, got {}",
                column_index,
                self.schema[column_index],
                value_type_name(value)
            ));
        }

        let mut values = self.to_values()?;
        values[column_index] = Some(value.clone());
        self.rebuild_from_values(&values)
    }

    pub fn set_null(&mut self, column_index: usize) -> Result<(), String> {
        if column_index >= self.schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }

        let mut values = self.to_values()?;
        values[column_index] = None;
        self.rebuild_from_values(&values)
    }

    pub fn get_value(&self, column_index: usize) -> Result<Option<DataValue>, String> {
        if column_index >= self.schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }
        let values = self.to_values()?;
        Ok(values[column_index].clone())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let bitmap_len = self.schema.len().div_ceil(8);
        let mut out = Vec::with_capacity(bitmap_len + self.data.len());
        out.extend_from_slice(self.null_bitmap.as_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    pub fn deserialize(schema: &[DataType], bytes: &[u8]) -> Result<Self, String> {
        let bitmap_len = schema.len().div_ceil(8);
        if bytes.len() < bitmap_len {
            return Err("Row shorter than NULL bitmap".to_string());
        }

        let _ = deserialize_nullable_row(schema, bytes)?;

        let null_bitmap = NullBitmap::from_bytes(schema.len(), &bytes[..bitmap_len])?;
        let data = bytes[bitmap_len..].to_vec();
        Ok(Self {
            schema: schema.to_vec(),
            null_bitmap,
            data,
        })
    }
}
