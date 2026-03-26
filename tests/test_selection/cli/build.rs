extern crate lalrpop;

fn main() {
    // Tell LALRPOP to look in the current directory (project root) for .lalrpop files
    println!("cargo:rerun-if-changed=grammar.lalrpop");

    lalrpop::Configuration::new()
        .set_in_dir(".")
        .process()
        .unwrap();
}
