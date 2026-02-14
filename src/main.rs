//! Starts the program and runs the frontend menu loop.

mod frontend;

fn main() -> std::io::Result<()> {
    frontend::menu::run()
}
