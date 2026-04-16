use crate::backend::executor::tuple::Tuple;
use crate::backend::executor::iterator::{Executor, ExecutorError};
use crate::backend::executor::value::Value;
use crate::backend::executor::agg_func::{AggFunc,AggReq};
use crate::backend::executor::expr::{Expr, evaluate};

use std::collections::HashSet;
use std::collections::HashMap;


pub enum AggValueState{
    Count(i64),
    Min(Option<Value>),
    Max(Option<Value>),
    Sum(Option<Value>),
    Avg{sum: Option<Value>, count: usize},
    CountDistinct(HashSet<Value>),
    SumDistinct(HashSet<Value>),
    Variance{count:usize, mean:f64, m2:f64},
    StdDev{count:usize, mean:f64, m2:f64},
    BoolAnd(Option<bool>),
    BoolOr(Option<bool>),
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
                AggFunc::Sum => {
                    results.push(AggValueState::Sum(None));
                }
                AggFunc::Avg => {
                    results.push(AggValueState::Avg{sum:None, count:0});
                }
                AggFunc::CountDistinct=>{
                    results.push(AggValueState::CountDistinct(HashSet::new()));
                }
                AggFunc::SumDistinct=>{
                    results.push(AggValueState::SumDistinct(HashSet::new()));
                }
                AggFunc::Variance=>{
                    results.push(AggValueState::Variance{count:0, mean:0.0, m2:0.0})
                }
                AggFunc::StdDev=>{
                    results.push(AggValueState::StdDev{count:0, mean:0.0, m2:0.0})
                }
                AggFunc::BoolAnd=>{
                    results.push(AggValueState::BoolAnd(None));
                }
                AggFunc::BoolOr=>{
                    results.push(AggValueState::BoolOr(None));
                }
            }
        }
        return Self{results};
    }
    
    pub fn update(&mut self, tuple:&Tuple, reqs:&[AggReq]) -> Result<(), ExecutorError> {
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
                        if *tuple_val!=Value::Null {
                            if let AggValueState::Min(min_value) = state {
                                // Check if we already have a minimum
                                if let Some(curr_min) = min_value {
                                    // 2. Manually compare matching types
                                    let should_update = match (curr_min, tuple_val) {
                                        (Value::Int(curr), Value::Int(new)) => new < curr,
                                        (Value::Text(curr), Value::Text(new)) => new < curr,
                                        _ => return Err(ExecutorError::TypeMismatch(format!("Unsupported type mapping: {:?}", tuple_val))),
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
                        if *tuple_val!=Value::Null {
                            if let AggValueState::Max(max_value) = state {
                                // Check if we already have a minimum
                                if let Some(curr_max) = max_value {
                                    // 2. Manually compare matching types
                                    let should_update = match (curr_max, tuple_val) {
                                        (Value::Int(curr), Value::Int(new)) => new > curr,
                                        (Value::Text(curr), Value::Text(new)) => new > curr,
                                        _ => return Err(ExecutorError::TypeMismatch(format!("Unsupported type mapping: {:?}", tuple_val))),
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
                AggFunc::Sum=>{
                    if let Some(col_idx)=req.col_index {
                        let tuple_val=&tuple.values[col_idx];
                        if *tuple_val!=Value::Null {
                            if let AggValueState::Sum(current_val) = state {
                                if let Some(curr) = current_val {
                                    // Unpack the primitives to use checked_add
                                    match (&*curr, tuple_val) {
                                        (Value::Int(c), Value::Int(t)) => {
                                            match c.checked_add(*t) {
                                                Some(sum) => *curr = Value::Int(sum),
                                                None => {
                                                    // ON OVERFLOW: Push a Null to signify the error in results 
                                                    *current_val = Some(Value::Null); 
                                                }
                                            }
                                        },
                                        (Value::Null, _) => {}, // Keep poisoned
                                        _ => return Err(ExecutorError::TypeMismatch(format!("Cannot aggregate mathematical types on: {:?}", tuple_val))),
                                    }
                                } else {
                                    // It was None, so initialize it with the first value
                                    *current_val = Some(tuple_val.clone());
                                }
                            }
                        }
                    }
                }
                AggFunc::Avg=>{
                    if let Some(col_idx)=req.col_index {
                        let tuple_val=&tuple.values[col_idx];
                        if *tuple_val!=Value::Null {
                            if let AggValueState::Avg{sum, count}=state{
                                *count+=1;
                                
                                if let Some(curr)=sum {
                                    match(&*curr,tuple_val){
                                        (Value::Int(c), Value::Int(t))=>{
                                            match c.checked_add(*t){
                                                Some(sum)=>*curr=Value::Int(sum),
                                                None=>{
                                                    *sum=Some(Value::Null);
                                                }
                                            }
                                        },
                                        (Value::Null, _) => {}, // Keep poisoned
                                        _ => return Err(ExecutorError::TypeMismatch(format!("Cannot aggregate mathematical types on: {:?}", tuple_val))),

                                    }
                                } else {
                                    *sum=Some(tuple_val.clone());
                                }
                            } 
                        }
                    }
                }
                AggFunc::CountDistinct => {
                    if let Some(col_idx) = req.col_index {
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::CountDistinct(set) = state {
                                set.insert(tuple_val.clone());
                            }
                        }
                    }
                }
                AggFunc::SumDistinct => {
                    if let Some(col_idx) = req.col_index {
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::SumDistinct(set) = state {
                                set.insert(tuple_val.clone());
                            }
                        }
                    }
                }
                AggFunc::Variance=>{
                    if let Some(col_idx)=req.col_index{
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::Variance{count,mean,m2}=state{
                                if let Value::Int(v) = tuple_val {
                                    let v_f64 = *v as f64;
                                    *count+=1;
                                    let delta=v_f64 - *mean;
                                    *mean+=delta / *count as f64;
                                    let delta2=v_f64 - *mean;
                                    *m2+=delta * delta2;
                                } else if let Value::Float(f) = tuple_val {
                                    let v_f64 = f.into_inner();
                                    *count+=1;
                                    let delta=v_f64 - *mean;
                                    *mean+=delta / *count as f64;
                                    let delta2=v_f64 - *mean;
                                    *m2+=delta * delta2;
                                } else {
                                    return Err(ExecutorError::TypeMismatch(format!("Cannot calculate variance/stddev on type: {:?}", tuple_val)));
                                }
                            }
                        }
                    }
                }
                AggFunc::StdDev=>{
                    if let Some(col_idx)=req.col_index{
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::StdDev{count,mean,m2}=state{
                                if let Value::Int(v) = tuple_val {
                                    let v_f64 = *v as f64;
                                    *count+=1;
                                    let delta=v_f64 - *mean;
                                    *mean+=delta / *count as f64;
                                    let delta2=v_f64 - *mean;
                                    *m2+=delta * delta2;
                                } else if let Value::Float(f) = tuple_val {
                                    let v_f64 = f.into_inner();
                                    *count+=1;
                                    let delta=v_f64 - *mean;
                                    *mean+=delta / *count as f64;
                                    let delta2=v_f64 - *mean;
                                    *m2+=delta * delta2;
                                } else {
                                    return Err(ExecutorError::TypeMismatch(format!("Cannot calculate variance/stddev on type: {:?}", tuple_val)));
                                }
                            }
                        }
                    }
                }
                AggFunc::BoolAnd=>{
                    if let Some(col_idx)=req.col_index{
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::BoolAnd(bool_val)=state{
                                if let Value::Boolean(new_b) = tuple_val {
                                    if let Some(curr_bool)=bool_val{
                                        *bool_val=Some(*curr_bool & *new_b);
                                    } else {
                                        //Initialize if none
                                        *bool_val=Some(*new_b);
                                    }
                                } else {
                                    return Err(ExecutorError::TypeMismatch(format!("Expected boolean for bool aggregator, got: {:?}", tuple_val)));
                                }
                            }
                        }
                    }
                }
                AggFunc::BoolOr=>{
                    if let Some(col_idx)=req.col_index{
                        let tuple_val = &tuple.values[col_idx];
                        if *tuple_val != Value::Null {
                            if let AggValueState::BoolOr(bool_val)=state{
                                if let Value::Boolean(new_b) = tuple_val {
                                    if let Some(curr_bool)=bool_val{
                                        *bool_val=Some(*curr_bool | *new_b);
                                    } else {
                                        //Initialize if none
                                        *bool_val=Some(*new_b);
                                    }
                                } else {
                                    return Err(ExecutorError::TypeMismatch(format!("Expected boolean for bool aggregator, got: {:?}", tuple_val)));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}


pub struct HashAggregator {
    pub child: Box<dyn Executor>,
    pub reqs: Vec<AggReq>,
    pub group_by_cols: Vec<usize>,
    pub states: HashMap<Vec<Value>, AggregationState>,
    pub built: bool,
    pub iter: Option<std::collections::hash_map::IntoIter<Vec<Value>, AggregationState>>,
    pub having: Option<Expr>,
}

impl HashAggregator {
    pub fn new(child: Box<dyn Executor>, reqs: Vec<AggReq>, group_by_cols: Vec<usize>, having: Option<Expr>) -> Self {
        Self {
            child,
            reqs,
            group_by_cols,
            states: HashMap::new(),
            built: false,
            iter: None,
            having,
        }
    }
}

impl Executor for HashAggregator {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError> {
        if !self.built {
            // 1. Drain the child iterator to build the state
            while let Some(tuple) = self.child.next()? {
                let mut hash_key=Vec::new();
                for col_idx in &self.group_by_cols {
                    let tuple_val = &tuple.values[*col_idx];
                    hash_key.push(tuple_val.clone());
                }

                let agg_state = self.states.entry(hash_key).or_insert_with(|| {
                    AggregationState::new(&self.reqs)
                });

                agg_state.update(&tuple, &self.reqs)?;
            }
            self.built = true;

            // 1.5 Handle Empty Table Aggregations (Scalar Aggregates)
            // If there are no group_by columns and the table was entirely empty,
            // SQL standards mandate that we still return exactly 1 row containing default values (like COUNT=0, SUM=NULL).
            if self.group_by_cols.is_empty() && self.states.is_empty() {
                self.states.insert(Vec::new(), AggregationState::new(&self.reqs));
            }

            // 2. Convert the internal AggValueState into output Values
            if self.iter.is_none() {
                let hm_iter = std::mem::replace(&mut self.states, HashMap::new()).into_iter();
                self.iter = Some(hm_iter);
            }
        }

        if let Some(ref mut iter) = self.iter {
            while let Some((group_key, agg_state)) = iter.next() {
                let mut final_values = group_key; // Start with the group-by columns
                for res in agg_state.results {
                    match res {
                        AggValueState::Count(c) => {
                            final_values.push(Value::Int(c as i32)); 
                        }
                        AggValueState::Min(opt_val) | AggValueState::Max(opt_val) => {
                            if let Some(val) = opt_val {
                                final_values.push(val);
                            } else {
                                final_values.push(Value::Null);
                            }
                        }
                        AggValueState::Sum(opt_val)=>{
                            if let Some(val)=opt_val {
                                final_values.push(val);
                            } else {
                                final_values.push(Value::Null);
                            }
                        }
                        AggValueState::Avg{sum,count}=>{
                            if count == 0{
                                final_values.push(Value::Null);
                            } else if let Some(Value::Int(s))=sum{
                                let div_result=s / count as i32;
                                final_values.push(Value::Int(div_result));
                            } else {
                                final_values.push(Value::Null);
                            }
                        }
                        AggValueState::CountDistinct(set)=>{
                            final_values.push(Value::Int(set.len() as i32));
                        }
                        AggValueState::SumDistinct(set)=>{
                            let mut sum_val = Some(0_i32);
                            for val in set {
                                if let Some(current_sum) = sum_val {
                                    match val {
                                        Value::Int(v) => sum_val = current_sum.checked_add(v),
                                        Value::Null => {},
                                        _ => return Err(ExecutorError::TypeMismatch(format!("Cannot compute SumDistinct on non-integer type: {:?}", val))),
                                    }
                                }
                            }
                            
                            if let Some(final_sum) = sum_val {
                                final_values.push(Value::Int(final_sum));
                            } else {
                                final_values.push(Value::Null); // Overflow case
                            }
                        }
                        AggValueState::Variance{count,mean: _,m2}=>{
                            if count<2 {
                                final_values.push(Value::Null);
                            } else {
                                final_values.push(Value::Float(ordered_float::OrderedFloat(m2 / (count-1) as f64)));
                            }
                        }
                        AggValueState::StdDev{count,mean: _,m2}=>{
                            if count<2 {
                                final_values.push(Value::Null);
                            } else {
                                final_values.push(Value::Float(ordered_float::OrderedFloat((m2 / (count-1) as f64).sqrt())));
                            }
                        }
                        AggValueState::BoolAnd(opt_bool) | AggValueState::BoolOr(opt_bool) => {
                            if let Some(b) = opt_bool {
                                final_values.push(Value::Boolean(b));
                            } else {
                                final_values.push(Value::Null);
                            }
                        }
                    }
                }

                // 3. Dummy null bitmap
                let bitmap_len = (final_values.len() + 7) / 8;
                let is_null_bitmap = vec![0; bitmap_len];

                let output_tuple = Tuple {
                    values: final_values,
                    is_null_bitmap,
                };

                // 4. Apply HAVING clause if present
                if let Some(ref expr) = self.having {
                    let result = evaluate(expr, &output_tuple)?;
                    if let Value::Boolean(false) | Value::Null = result {
                        continue; // Skip this tuple
                    }
                }

                return Ok(Some(output_tuple));
            }
        }
        Ok(None)
    }
}

pub fn execute_aggregation(child: Box<dyn Executor>, reqs: Vec<AggReq>,group_by_cols: Vec<usize>, having: Option<Expr>) -> Result<Vec<Tuple>, ExecutorError> {

    let mut aggregator = HashAggregator::new(child, reqs, group_by_cols, having);
    let mut output_table = Vec::new();
    
    while let Some(tuple) = aggregator.next()? {
        output_table.push(tuple);
    }
    
    Ok(output_table)
}