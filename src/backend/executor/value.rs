use ordered_float::OrderedFloat;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value{
    Int(i32),
    Text(String),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    Null,
}