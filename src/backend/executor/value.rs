#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value{
    Int(i32),
    Text(String),
    Null,
}