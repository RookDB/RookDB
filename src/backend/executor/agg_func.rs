#[derive(Clone, Debug, PartialEq)]
pub enum AggFunc{
    CountStar,
    Count,
    Min,
    Max,
}

pub struct AggReq{
    pub agg_type: AggFunc,
    pub col_index: Option<usize>,
}