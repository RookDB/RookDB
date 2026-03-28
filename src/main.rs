//! Starts the program and runs the frontend menu loop.

mod frontend;

fn main() -> std::io::Result<()> {
    frontend::menu_test_buffer::run()
    // frontend::menu::run()
}
