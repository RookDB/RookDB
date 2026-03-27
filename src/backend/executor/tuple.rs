use crate::backend::executor::value::Value;
pub struct Tuple{
    pub values: Vec<Value>,
    pub is_null_bitmap: Vec<u8>,
}