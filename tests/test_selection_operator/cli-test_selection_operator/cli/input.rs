// Simple utilities for reading user input

use std::io::{self, Write};

// Show a prompt and read what the user types
pub fn read_input(prompt: &str) -> io::Result<String> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}
