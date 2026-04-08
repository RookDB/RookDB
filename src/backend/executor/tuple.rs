use crate::backend::executor::value::Value;
#[derive(Debug)]
pub struct Tuple{
    pub values: Vec<Value>,
    pub is_null_bitmap: Vec<u8>,
}