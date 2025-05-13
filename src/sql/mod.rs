use parser::Parser;
use statement::Statement;

use crate::db::{Ctx, DatabaseError};

pub(crate) mod analyzer;
pub(crate) mod optimiser;
pub(crate) mod parser;
mod prepare;
pub mod statement;
mod tokenizer;
mod tokens;

pub(crate) fn pipeline(input: &str, db: &mut impl Ctx) -> Result<Statement, DatabaseError> {
    let mut statement = Parser::new(input).parse_statement()?;

    analyzer::analyze(&statement, db)?;
    optimiser::optimise(&mut statement)?;

    Ok(statement)
}
