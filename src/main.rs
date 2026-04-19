//! Starts the program and runs the frontend menu loop.

mod frontend;

fn main() -> std::io::Result<()> {
    env_logger::init();
    frontend::menu::run()
}
