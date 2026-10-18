//! # The SQL Pipeline
//!
//! This module orchestrates the transformation of a raw SQL string into an executable statement.
//! The pipeline flows as follows:
//!
//! 1. **Parsing** ([parser]):
//!    The `Tokenizer` converts text into tokens, which the `Parser` consumes to build an AST
//!    (Abstract Syntax Tree) defined in `statement.rs`.
//!
//! 2. **Analysis** ([analyzer]):
//!    The AST is semantically validated. We check if tables exist, resolve column names to
//!    indexes, and ensure types match (e.g., you can't add a String to a Boolean).
//!
//! 3. **Optimisation** ([optimiser]):
//!    The AST is mutated to be more efficient. This includes:
//!    - Constant folding (e.g., `1 + 1` -> `2`).
//!    - Removing redundant expressions.
//!    - *Note:* Join order and Index selection happen later in the `query` module.
//!
//! 4. **Preparation** ([prepare]):
//!    Performs runtime-specific adjustments. For example, expanding `SELECT *` into specific
//!    column names or handling `SERIAL` auto-increments.
//!
//! The entry point is [process_statement], over a statement the session parsed.

use statement::Statement;

use crate::db::{Ctx, DatabaseError};
pub use parser::Keyword;
pub use value::Value;

pub(crate) mod analyzer;
pub(crate) mod optimiser;
pub(crate) mod parser;

mod prepare;
pub mod statement;
pub mod value;

pub(crate) fn process_statement(
    mut statement: Statement,
    db: &mut impl Ctx,
) -> Result<Statement, DatabaseError> {
    analyzer::analyze(&statement, db)?;
    analyzer::unify(&mut statement, db)?;
    optimiser::simplify(&mut statement)?;
    prepare::prepare(&mut statement, db)?;

    Ok(statement)
}
