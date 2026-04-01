#[derive(Clone, Debug, PartialEq)]
pub enum AggFunc{
    CountStar,
    Count,
    Min,
    Max,
    Sum,
    Avg,
    CountDistinct,
    SumDistinct,
    Variance,
    StdDev,
    BoolAnd,
    BoolOr,
}

pub struct AggReq{
    pub agg_type: AggFunc,
    pub col_index: Option<usize>,
}