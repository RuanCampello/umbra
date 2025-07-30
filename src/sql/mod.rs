use statement::Statement;

use crate::db::{Ctx, DatabaseError};
pub use analyzer::AnalyzerError;
pub use parser::{Keyword, Parser};
pub use statement::Column;

pub(crate) mod analyzer;
pub(crate) mod optimiser;
pub(crate) mod parser;
pub(crate) mod query;

mod prepare;
pub mod statement;

pub(crate) fn pipeline(input: &str, db: &mut impl Ctx) -> Result<Statement, DatabaseError> {
    let mut statement = Parser::new(input).parse_statement()?;

    analyzer::analyze(&statement, db)?;
    optimiser::optimise(&mut statement)?;
    prepare::prepare(&mut statement, db)?;

    Ok(statement)
}
