use crate::backend::executor::Value;
pub struct Tuple{
    pub values: Vec<Value>,
    pub is_null_bitmap: Vec<u8>,
}