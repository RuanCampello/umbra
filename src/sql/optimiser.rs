//! That's the very last part before execution, right after [`super::analyzer`].
//!
//! Here, we try to minimise the operations of a given statement.

use crate::core::db::SqlError;
use crate::sql::statement::{BinaryOperator, Expression, Statement, UnaryOperator, Value};

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

pub(crate) fn simplify<'exp>(expression: &mut Expression) -> Result<(), SqlError<'exp>> {
    match expression {
        Expression::UnaryOperator { expr, .. } => {
            simplify(expr)?;
            if let Expression::Value(_) = expr.as_ref() {
                *expression = resolve_expression(expression)?
            }
        }
        Expression::BinaryOperator {
            left,
            operator,
            right,
            ..
        } => {
            simplify(left)?;
            simplify(right)?;

            match (left.as_mut(), operator, right.as_mut()) {
                // if both sides are literal values, evaluate the whole expression
                (Expression::Value(_), _op, Expression::Value(_)) => {
                    *expression = resolve_expression(expression)?
                }
                // simplification rules for identity operations:
                // multiplying by 1: (1 * x) → x or (x * 1) → x or (x / 1) → x
                // adding/subtracting 0: (x + 0) → x or (0 + x) → x or (x - 0) → x
                (
                    Expression::Value(Value::Number(1)),
                    BinaryOperator::Mul,
                    variable @ Expression::Identifier(_),
                )
                | (
                    variable @ Expression::Identifier(_),
                    BinaryOperator::Mul | BinaryOperator::Div,
                    Expression::Value(Value::Number(1)),
                )
                | (
                    variable @ Expression::Identifier(_),
                    BinaryOperator::Plus | BinaryOperator::Minus,
                    Expression::Value(Value::Number(0)),
                )
                | (
                    Expression::Value(Value::Number(0)),
                    BinaryOperator::Plus,
                    variable @ Expression::Identifier(_),
                ) => {
                    *expression = std::mem::replace(variable, Expression::Wildcard);
                }
                // simplify expression patterns where 0 is subtracted from a variable (0 - x)
                (
                    Expression::Value(Value::Number(0)),
                    BinaryOperator::Minus,
                    Expression::Identifier(_),
                ) => match std::mem::replace(expression, Expression::Wildcard) {
                    Expression::BinaryOperator { right, .. } => {
                        *expression = Expression::UnaryOperator {
                            operator: UnaryOperator::Minus,
                            expr: right,
                        }
                    }
                    _ => unreachable!(),
                },
                // handle nested addition patterns of the form: (variable + literal1) + literal2
                // transforms (x + 5) + 3 → x + (5 + 3) → x + 8
                (
                    Expression::BinaryOperator {
                        left: variable,
                        operator: BinaryOperator::Plus,
                        right: center_value,
                    },
                    BinaryOperator::Plus,
                    right_value @ Expression::Value(_),
                ) if matches!(center_value.as_ref(), Expression::Value(_)) => {
                    std::mem::swap(variable.as_mut(), right_value);
                    *left.as_mut() = resolve_expression(left)?;
                    std::mem::swap(left, right);
                }
                // transforms 5 + x → x + 5
                // this reordering enables potential constant folding in later optimisations
                (
                    literal @ Expression::Value(_),
                    BinaryOperator::Plus,
                    variable @ Expression::Identifier(_),
                ) => {
                    std::mem::swap(variable, literal);
                }
                // simplify multiplication/division with zero: 0 * x, x * 0, or 0 / x → 0
                (
                    zero @ Expression::Value(Value::Number(0)),
                    BinaryOperator::Mul | BinaryOperator::Div,
                    Expression::Identifier(_),
                )
                | (
                    Expression::Identifier(_),
                    BinaryOperator::Mul,
                    zero @ Expression::Value(Value::Number(0)),
                ) => {
                    *expression = std::mem::replace(zero, Expression::Wildcard);
                }
                _ => {}
            };
        }

        //FIXME: that's going to be a pain in the ass
        Expression::Nested(nested) => {
            simplify(nested.as_mut())?;
            *expression = std::mem::replace(nested.as_mut(), Expression::Wildcard);
        }
        _ => {}
    }

    Ok(())
}

fn resolve_expression<'exp>(expression: &mut Expression) -> Result<Expression, SqlError<'exp>> {
    todo!()
}
