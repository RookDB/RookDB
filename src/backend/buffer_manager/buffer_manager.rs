use crate::catalog::types::Catalog;
use crate::disk::{read_page, write_page};
use crate::executor::tuple_codec::encode_tuple;
use crate::executor::value::Value;
use crate::page::{ITEM_ID_SIZE, PAGE_SIZE, Page, init_page, page_free_space};

use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom};

pub struct BufferManager {
    pub pages: Vec<Page>,
}

impl BufferManager {
    pub fn new() -> Self {
        let mut pages = Vec::new();
        let mut header = Page::new();
        init_page(&mut header);
        pages.push(header);
        println!("Buffer Manager initialized with header page only.");
        Self { pages }
    }

    pub fn allocate_page(&mut self) {
        let mut page = Page::new();
        init_page(&mut page);
        self.pages.push(page);
    }

    pub fn load_table_from_disk(&mut self, db_name: &str, table_name: &str) -> io::Result<()> {
        let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::open(&table_path)?;
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let total_pages = (file_size as usize) / PAGE_SIZE;

        println!(
            "Loading table '{}' ({} bytes, {} pages)...",
            table_name, file_size, total_pages
        );

        self.pages.clear();

        let mut header_page = Page::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_page.data)?;
        self.pages.push(header_page);

        for page_num in 1..total_pages {
            let mut page = Page::new();
            match read_page(&mut file, &mut page, page_num as u32) {
                Ok(_) => self.pages.push(page),
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        println!(
            "Loaded {} pages (1 header + {} data).",
            self.pages.len(),
            self.pages.len().saturating_sub(1)
        );
        Ok(())
    }

    /// Load CSV into in-memory pages using the new TupleHeader encoding.
    pub fn load_csv_into_pages(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<usize> {
        let db = catalog.databases.get(db_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
        })?;
        let table = db.tables.get(table_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
        })?;
        let schema = &table.columns;

        if schema.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Table has no columns"));
        }

        let csv_file = File::open(csv_path)?;
        let reader = BufReader::new(csv_file);
        let mut lines = reader.lines();
        if let Some(Ok(_)) = lines.next() {} // skip header

        let mut inserted_rows = 0usize;
        let mut current_page_index = self.pages.len() - 1;

        if self.pages.len() == 1 {
            self.allocate_page();
        }

        for (i, line) in lines.enumerate() {
            let row_str = line?;
            if row_str.trim().is_empty() {
                continue;
            }

            let raw_vals: Vec<&str> = row_str.split(',').map(|v| v.trim()).collect();
            if raw_vals.len() != schema.len() {
                println!(
                    "Skipping row {}: expected {} columns, got {}",
                    i + 1,
                    schema.len(),
                    raw_vals.len()
                );
                continue;
            }

            // Parse each field into a typed Value
            let values: Vec<Value> = raw_vals
                .iter()
                .zip(schema.iter())
                .map(|(raw, col)| {
                    if raw.is_empty() || raw.to_uppercase() == "NULL" {
                        return Value::Null;
                    }
                    match col.data_type.to_uppercase().as_str() {
                        "INT" | "INTEGER" => raw.parse::<i64>().map(Value::Int).unwrap_or(Value::Null),
                        "FLOAT" | "REAL" | "DOUBLE" => raw.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
                        "BOOL" | "BOOLEAN" => match raw.to_lowercase().as_str() {
                            "true" | "1" | "yes" => Value::Bool(true),
                            "false" | "0" | "no" => Value::Bool(false),
                            _ => Value::Null,
                        },
                        "DATE" => raw.parse::<i32>().map(Value::Date).unwrap_or(Value::Null),
                        "TIMESTAMP" => raw.parse::<i64>().map(Value::Timestamp).unwrap_or(Value::Null),
                        _ => Value::Text(raw.to_string()),
                    }
                })
                .collect();

            let tuple_bytes = encode_tuple(&values, schema);
            let tuple_len = tuple_bytes.len() as u32;
            let required = tuple_len + ITEM_ID_SIZE;

            loop {
                if current_page_index >= self.pages.len() {
                    self.allocate_page();
                }

                let page = &mut self.pages[current_page_index];
                let free = page_free_space(page)?;

                if free < required {
                    current_page_index += 1;
                    continue;
                }

                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

                let start = upper - tuple_len;
                page.data[start as usize..upper as usize].copy_from_slice(&tuple_bytes);

                let item_id_pos = lower as usize;
                page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
                page.data[item_id_pos + 4..item_id_pos + 8].copy_from_slice(&tuple_len.to_le_bytes());

                lower += ITEM_ID_SIZE;
                upper = start;

                page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                page.data[4..8].copy_from_slice(&upper.to_le_bytes());

                inserted_rows += 1;
                break;
            }
        }

        let used_pages = self.pages.len();
        self.pages[0].data[0..4].copy_from_slice(&(used_pages as u32).to_le_bytes());

        println!("Loaded {} rows into {} data pages.", inserted_rows, used_pages - 1);
        Ok(used_pages)
    }

    pub fn flush_to_disk(&mut self, db_name: &str, table_name: &str, used_pages: usize) -> io::Result<()> {
        let path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::options().write(true).open(&path)?;
        for (i, page) in self.pages.iter_mut().take(used_pages).enumerate() {
            write_page(&mut file, page, i as u32)?;
        }
        Ok(())
    }

    pub fn load_csv_to_buffer(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<()> {
        let used = self.load_csv_into_pages(catalog, db_name, table_name, csv_path)?;
        self.flush_to_disk(db_name, table_name, used)?;
        Ok(())
    }
}
