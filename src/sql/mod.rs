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

pub(crate) fn pipeline(input: &str, db: &mut impl Ctx) -> Result<Statement<'static>, DatabaseError> {
    let mut statement = Parser::new(input).parse_statement()?.into_owned();

    analyzer::analyze(&statement, db)?;
    
    // Create a new borrow scope for optimiser
    {
        let temp_ref = &mut statement;
        optimiser::optimise(temp_ref)?;
    }
    
    // Create a new borrow scope for prepare 
    {
        let temp_ref = &mut statement;
        prepare::prepare(temp_ref, db)?;
    }

    Ok(statement)
}
