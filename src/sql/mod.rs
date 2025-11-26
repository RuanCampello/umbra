use parser::Parser;
use statement::Statement;

use crate::db::{Ctx, DatabaseError};
pub use parser::Keyword;

pub(crate) mod analyzer;
pub(crate) mod optimiser;
pub(crate) mod parser;
pub(crate) mod query;

mod prepare;
pub mod statement;

pub(crate) fn pipeline(input: &str, db: &mut impl Ctx) -> Result<Statement, DatabaseError> {
    let statement = Parser::new(input).parse_statement()?;
    process_statement(statement, db)
}

pub(crate) fn process_statement(
    mut statement: Statement,
    db: &mut impl Ctx,
) -> Result<Statement, DatabaseError> {
    analyzer::analyze(&statement, db)?;
    optimiser::optimise(&mut statement)?;
    prepare::prepare(&mut statement, db)?;

    Ok(statement)
}
