use lalrpop_util::lalrpop_mod;

lalrpop_mod!(pub grammar); // loads grammar.lalrpop

use storage_manager::backend::executor::selection::Predicate;

pub fn parse_predicate(input: &str) -> Result<Predicate, String> {
    grammar::PredicateParserParser::new()
        .parse(input)
        .map_err(|e| format!("Parse error: {:?}", e))
}
