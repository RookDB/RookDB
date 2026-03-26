// Creates tuples in the format the storage layer expects.
// Layout: [Header | NULL Bitmap | Offset Array | Field Data]

use rand::Rng;
use std::fs::File;
use std::io::{self, Write};

// Generate random test tuples and save them to disk.
// Creates tuples with our standard schema (id, amount, name, date).
// Saves binary format to tuple_storage.bin and readable format to tuple_rows.txt.
pub fn generate_and_store_random_tuples(count: usize) -> io::Result<()> {
    let mut binary_file = File::create("tuple_storage.bin")?;
    let mut text_file = File::create("tuple_rows.txt")?;

    let mut rng = rand::thread_rng();
    let names = vec!["Alice", "Bob", "Charlie", "Diana", "Emma", "Frank"];
    let dates = vec![
        "2024-01-15",
        "2024-02-20",
        "2024-03-10",
        "2024-04-05",
        "2024-05-25",
        "2024-06-15",
        "2024-07-30",
        "2024-08-12",
        "2024-09-18",
        "2024-10-22",
        "2024-11-08",
        "2024-12-31",
    ];

    writeln!(text_file, "Generated {} random tuples:", count)?;
    writeln!(text_file, "Schema: (id INT, amount FLOAT, name STRING, date DATE)\n")?;

    for i in 0..count {
        // Generate random values
        let id: i32 = rng.gen_range(1..=1000);
        let amount: f64 = rng.gen_range(0.0..1000.0);
        let name = names[rng.gen_range(0..names.len())];
        let date = dates[rng.gen_range(0..dates.len())];

        // Occasionally make values NULL (10% chance for each field except id)
        let id_val = Some(id.to_le_bytes().to_vec());
        let amount_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(amount.to_le_bytes().to_vec())
        };
        let name_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(name.as_bytes().to_vec())
        };
        let date_val = if rng.gen_range(0.0..1.0) < 0.1 {
            None
        } else {
            Some(date.as_bytes().to_vec())
        };

        // Build tuple
        let tuple = build_tuple(vec![
            id_val,
            amount_val.clone(),
            name_val.clone(),
            date_val.clone(),
        ]);

        // Write to binary file with length prefix
        let tuple_len = tuple.len() as u32;
        binary_file.write_all(&tuple_len.to_le_bytes())?;
        binary_file.write_all(&tuple)?;

        // Write to text file
        let amount_str = if amount_val.is_none() {
            "NULL".to_string()
        } else {
            format!("{:.2}", amount)
        };
        let name_str = if name_val.is_none() {
            "NULL".to_string()
        } else {
            name.to_string()
        };
        let date_str = if date_val.is_none() {
            "NULL".to_string()
        } else {
            date.to_string()
        };

        writeln!(
            text_file,
            "Tuple {}: id={}, amount={}, name={}, date={}",
            i + 1,
            id,
            amount_str,
            name_str,
            date_str
        )?;
    }

    Ok(())
}


/// Generates test tuples based on schema type.
pub fn generate_test_tuples(schema_type: &str) -> Vec<Vec<u8>> {
    match schema_type {
        "INT" => generate_int_tuples(),
        "INT_NULL" => generate_int_tuples_with_null(),
        "FLOAT" => generate_float_tuples(),
        "FLOAT_NULL" => generate_float_tuples_with_null(),
        "DATE" => generate_date_tuples(),
        "STRING" => generate_string_tuples(),
        "MULTI_COLUMN" => generate_multi_column_tuples(),
        _ => vec![],
    }
}

// Build a tuple in the storage format.
// Header is 8 bytes: [0-3] length, [4] version, [5] flags, [6-7] column count
// Offsets are relative to where field data starts.
// We include a sentinel offset at the end to mark where data ends.
fn build_tuple(columns: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    let num_columns = columns.len();
    let null_bitmap_size = (num_columns + 7) / 8;
    let offset_array_size = (num_columns + 1) * 4; // +1 for sentinel
    
    let header_size = 8;
    let null_bitmap_start = header_size;
    let offset_array_start = null_bitmap_start + null_bitmap_size;
    let field_data_start = offset_array_start + offset_array_size;

    // Build NULL bitmap
    let mut null_bitmap = vec![0u8; null_bitmap_size];
    for (i, col) in columns.iter().enumerate() {
        if col.is_none() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            null_bitmap[byte_idx] |= 1 << bit_idx;
        }
    }

    // Build field data and offset array (offsets are relative to field_data_start)
    let mut field_data = Vec::new();
    let mut offsets = Vec::new();

    for col in columns.iter() {
        // Track where this field starts (relative to field data section)
        let relative_offset = field_data.len();
        offsets.push(relative_offset as u32);
        
        if let Some(data) = col {
            field_data.extend_from_slice(data);
        }
    }
    
    // Add one final offset to mark where all the data ends
    offsets.push(field_data.len() as u32);

    // Calculate total length
    let total_length = field_data_start + field_data.len();

    // Now assemble the tuple
    let mut tuple = Vec::new();

    // Write the 8-byte header
    tuple.extend_from_slice(&(total_length as u32).to_le_bytes()); // total length
    tuple.push(1u8);                                                // version = 1
    tuple.push(0u8);                                                // flags = 0
    tuple.extend_from_slice(&(num_columns as u16).to_le_bytes());  // how many columns

    // NULL bitmap
    tuple.extend_from_slice(&null_bitmap);

    // Offset array
    for offset in offsets {
        tuple.extend_from_slice(&offset.to_le_bytes());
    }

    // Field data
    tuple.extend_from_slice(&field_data);

    tuple
}

// Generate some simple INT tuples for testing
fn generate_int_tuples() -> Vec<Vec<u8>> {
    vec![5i32, 10, 15, 20, 30]
        .into_iter()
        .map(|val| build_tuple(vec![Some(val.to_le_bytes().to_vec())]))
        .collect()
}

// INT tuples with some NULLs mixed in
fn generate_int_tuples_with_null() -> Vec<Vec<u8>> {
    vec![
        build_tuple(vec![Some(5i32.to_le_bytes().to_vec())]),
        build_tuple(vec![None]),
        build_tuple(vec![Some(15i32.to_le_bytes().to_vec())]),
        build_tuple(vec![Some(20i32.to_le_bytes().to_vec())]),
        build_tuple(vec![None]),
    ]
}

// Some FLOAT values for testing
fn generate_float_tuples() -> Vec<Vec<u8>> {
    vec![5.5f64, 10.2, 15.8, 20.1]
        .into_iter()
        .map(|val| build_tuple(vec![Some(val.to_le_bytes().to_vec())]))
        .collect()
}

// FLOAT values with NULLs
fn generate_float_tuples_with_null() -> Vec<Vec<u8>> {
    vec![
        build_tuple(vec![Some(5.5f64.to_le_bytes().to_vec())]),
        build_tuple(vec![None]),
        build_tuple(vec![Some(15.8f64.to_le_bytes().to_vec())]),
    ]
}

// Some DATE tuples
fn generate_date_tuples() -> Vec<Vec<u8>> {
    vec!["2024-01-10", "2024-02-15", "2024-03-20"]
        .into_iter()
        .map(|date| build_tuple(vec![Some(date.as_bytes().to_vec())]))
        .collect()
}

// Some STRING tuples with names
fn generate_string_tuples() -> Vec<Vec<u8>> {
    vec!["Alice", "Bob", "Charlie"]
        .into_iter()
        .map(|name| build_tuple(vec![Some(name.as_bytes().to_vec())]))
        .collect()
}

// Tuples with multiple columns for testing more complex predicates
// Has id (INT) and amount (FLOAT)
fn generate_multi_column_tuples() -> Vec<Vec<u8>> {
    vec![
        (5i32, 10.5f64),
        (10, 20.2),
        (15, 15.8),
        (20, 30.1),
        (30, 5.5),
    ]
    .into_iter()
    .map(|(id, amount)| {
        build_tuple(vec![
            Some(id.to_le_bytes().to_vec()),
            Some(amount.to_le_bytes().to_vec()),
        ])
    })
    .collect()
}

// Get readable versions of the values for display
pub fn get_display_values(schema_type: &str) -> Vec<String> {
    match schema_type {
        "INT" => vec!["5", "10", "15", "20", "30"]
            .into_iter()
            .map(String::from)
            .collect(),
        "INT_NULL" => vec!["5", "NULL", "15", "20", "NULL"]
            .into_iter()
            .map(String::from)
            .collect(),
        "FLOAT" => vec!["5.5", "10.2", "15.8", "20.1"]
            .into_iter()
            .map(String::from)
            .collect(),
        "FLOAT_NULL" => vec!["5.5", "NULL", "15.8"]
            .into_iter()
            .map(String::from)
            .collect(),
        "DATE" => vec!["2024-01-10", "2024-02-15", "2024-03-20"]
            .into_iter()
            .map(String::from)
            .collect(),
        "STRING" => vec!["Alice", "Bob", "Charlie"]
            .into_iter()
            .map(String::from)
            .collect(),
        "MULTI_COLUMN" => vec![
            "(5, 10.5)",
            "(10, 20.2)",
            "(15, 15.8)",
            "(20, 30.1)",
            "(30, 5.5)",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        _ => vec![],
    }
}
