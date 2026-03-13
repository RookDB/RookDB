use crate::backend::executor::{Tuple,Value};
use crate::backend::executor::agg_func::{AggFunc,AggReq};


pub enum AggValueState{
    Count(i64),
    Min(Option<Value>),
    Max(Option<Value>),
}

pub struct AggregationState{
    pub results: Vec<AggValueState>,
}

impl AggregationState{
    pub fn new(reqs: &[AggReq])->Self{
        let mut results=Vec::new();
        for agg in reqs{
            match agg.agg_type {
                AggFunc::CountStar | AggFunc::Count => {
                    results.push(AggValueState::Count(0_i64));
                }
                AggFunc::Min =>{
                    results.push(AggValueState::Min(None));
                }
                AggFunc::Max =>{
                    results.push(AggValueState::Max(None));
                }
            }
        }
        return Self{results};
    }
    
    pub fn update(&mut self, tuple:&Tuple, reqs:&[AggReq]){
        for i in 0..reqs.len() {
            let req = &reqs[i];
            let state = &mut self.results[i];
        
            match req.agg_type {
                AggFunc::CountStar => {
                    if let AggValueState::Count(count)=state{
                        *count += 1;
                    }
                }
                AggFunc::Count => {
                    if let Some(col_idx) = req.col_index { 
                        if tuple.values[col_idx] != Value::Null {
                            if let AggValueState::Count(count) = state {
                                *count += 1;
                            }
                        }
                    }
                }
                AggFunc::Min=>{
                    if let Some(col_idx) = req.col_index { 
                        let tuple_val=&tuple.values[col_idx];
                        if(*tuple_val!=Value::Null){
                            if let AggValueState::Min(min_value) = state {
                                // Check if we already have a minimum
                                if let Some(curr_min) = min_value {
                                    // 2. Manually compare matching types
                                    let should_update = match (curr_min, tuple_val) {
                                        (Value::Int(curr), Value::Int(new)) => new < curr,
                                        (Value::Text(curr), Value::Text(new)) => new < curr,
                                        _ => false, // Ignore type mismatches
                                    };
                                    
                                    if should_update {
                                        *min_value = Some(tuple_val.clone());
                                    }
                                } else {
                                    // It was None, so initialize it
                                    *min_value = Some(tuple_val.clone());
                                }
                            }
                        }
                    }
                }
                AggFunc::Max=>{
                    if let Some(col_idx) = req.col_index { 
                        let tuple_val=&tuple.values[col_idx];
                        if(*tuple_val!=Value::Null){
                            if let AggValueState::Max(max_value) = state {
                                // Check if we already have a minimum
                                if let Some(curr_max) = max_value {
                                    // 2. Manually compare matching types
                                    let should_update = match (curr_max, tuple_val) {
                                        (Value::Int(curr), Value::Int(new)) => new > curr,
                                        (Value::Text(curr), Value::Text(new)) => new > curr,
                                        _ => false, // Ignore type mismatches
                                    };
                                    
                                    if should_update {
                                        *max_value = Some(tuple_val.clone());
                                    }
                                } else {
                                    // It was None, so initialize it
                                    *max_value = Some(tuple_val.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}