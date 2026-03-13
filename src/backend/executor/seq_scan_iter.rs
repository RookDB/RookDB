use std::sync::Arc;

use crate::backend::executor::{Tuple,Executor,Value};
use crate::backend::catalog::types::Column;
use crate::backend::buffer_manager::BufferManager;
use crate::backend::page::{ITEM_ID_SIZE,PAGE_HEADER_SIZE};

pub struct SeqScan{
    table_name: String,
    buffer_pool: Arc<BufferManager>,
    current_page_id: u32,
    current_slot_idx: u32,
    total_pages: u32,
    schema: Vec<Column>,
}

impl Executor for SeqScan{
    fn next(&mut self) -> Option<Tuple>{
        //Explicitly start from page_id=1 to skip Header page.
        if self.current_page_id>=self.total_pages{
            return None;
        }

        let curr_page=&self.buffer_pool.pages[self.current_page_id as usize];
        
        let lower = u32::from_le_bytes(curr_page.data[0..4].try_into().unwrap());
        //let upper = u32::from_le_bytes(curr_page.data[4..8].try_into().unwrap());

        let num_items = (lower-PAGE_HEADER_SIZE)/ITEM_ID_SIZE;

        if self.current_slot_idx>=num_items{
            self.current_page_id+=1;
            self.current_slot_idx=0;
            return self.next();
        }

        
        let base=(PAGE_HEADER_SIZE+self.current_slot_idx*ITEM_ID_SIZE) as usize;
        let offset = u32::from_le_bytes(curr_page.data[base..base + 4].try_into().unwrap());
        let length = u32::from_le_bytes(curr_page.data[base + 4..base + 8].try_into().unwrap());
        let tuple_data = &curr_page.data[offset as usize..(offset + length) as usize];

        let bitmap_len=(self.schema.len()+7)/8;
        let bitmap=tuple_data[0..bitmap_len];

        let mut cursor=bitmap_len as usize;
        let mut values=Vec::new();

        let num_columns=self.schema.len();
        for j in 0..num_columns{
            let byte_idx=j/8;
            let bit_idx=j%8;
            let is_null=bitmap[byte_idx]&(1<<bit_idx);

            if(is_null!=0){
                values.push(Value::Null);
            } else {
                match self.schema[j as usize].data_type.as_str() {
                    "INT" => {
                        if cursor + 4 <= tuple_data.len() {
                            let val = i32::from_le_bytes(
                                tuple_data[cursor..cursor + 4].try_into().unwrap(),
                            );
                            values.push(Value::Int(val));
                            cursor += 4;
                        } else {
                            // FIX: Keep columns aligned if data is truncated
                            values.push(Value::Null); 
                        }
                    }
                    "TEXT" => {
                        if cursor + 10 <= tuple_data.len() {
                            let text_bytes = &tuple_data[cursor..cursor + 10];
                            let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                            values.push(Value::Text(text));
                            cursor += 10;
                        } else {
                            // FIX: Keep columns aligned if data is truncated
                            values.push(Value::Null); 
                        }
                    }
                    _ => {
                        // Fallback for unknown types to maintain alignment
                        values.push(Value::Null); 
                    }
                }
            }
            
        }
        self.current_slot_idx+=1;
        return Some(Tuple{values,is_null_bitmap: bitmap.to_vec()});
    }
}
