use crate::backend::executor::Tuple;
pub trait Executor{
    fn next(&mut self)->Option<Tuple>;
}