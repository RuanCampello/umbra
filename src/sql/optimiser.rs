//! That's the very last part before execution, right after [`super::analyzer`].
//!
//! Here, we try to minimise the operations of a given statement.

use crate::core::db::SqlError;
use crate::sql::statement::{Expression, Statement};

pub(crate) fn optimise(statement: &mut Statement) -> Result<(), SqlError> {
    match statement {
        Statement::Select {
            from,
            columns,
            r#where,
            ..
        } => {
            todo!()
        }
        Statement::Update { columns, .. } => {
            todo!()
        }
        Statement::Insert { columns, .. } => {
            todo!()
        }
        _ => {}
    };
    todo!()
}

pub(crate) fn simplify(expression: &mut Expression) -> Result<(), SqlError> {
    match expression {
        Expression::BinaryOperator {left, operator, right} => {
            todo!()
        }
        _ => {}
    }
    todo!()
}
