//! That's the very last part before execution, right after [`super::analyzer`].
//! Here, we try to minimise the operations of a given statement.

#![allow(unused)]

use crate::db::SqlError;
use crate::sql::statement::{BinaryOperator, Expression, Statement, UnaryOperator, Value};
use crate::vm::expression::resolve_only_expression;

use super::statement::{Delete, Insert, Select, Update};

mod bloom;
mod cost;
mod statistics;

/// Tries to simplify the given statement applying heuristic-based rules.
pub(crate) fn simplify(statement: &mut Statement) -> Result<(), SqlError> {
    match statement {
        Statement::Select(Select {
            columns,
            r#where,
            order_by,
            ..
        }) => {
            simplify_iter(columns.iter_mut())?;
            simplify_where(r#where)?;
            simplify_iter(order_by.iter_mut().map(|order| &mut order.expr))?;
        }
        Statement::Update(Update {
            columns, r#where, ..
        }) => {
            simplify_where(r#where)?;
            simplify_iter(columns.iter_mut().map(|col| &mut col.value))?;
        }
        Statement::Delete(Delete { r#where, .. }) => simplify_where(r#where)?,
        Statement::Explain(inner) => simplify(inner)?,
        Statement::Insert(Insert { values, .. }) => {
            let expr_iter = values.iter_mut().flat_map(|row| row.iter_mut());
            simplify_iter(expr_iter)?;
        }
        _ => {}
    };

    Ok(())
}

pub(in crate::sql) fn simplify_recursively(expression: &mut Expression) -> Result<(), SqlError> {
    match expression {
        Expression::UnaryOperation { expr, .. } => {
            simplify_recursively(expr)?;
            if let Expression::Value(_) = expr.as_ref() {
                *expression = resolve_expression(expression)?
            }
        }
        Expression::BinaryOperation {
            left,
            operator,
            right,
            ..
        } => {
            simplify_recursively(left)?;
            simplify_recursively(right)?;

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
                    Expression::BinaryOperation { right, .. } => {
                        *expression = Expression::UnaryOperation {
                            operator: UnaryOperator::Minus,
                            expr: right,
                        }
                    }
                    _ => unreachable!(),
                },
                // handle nested addition patterns of the form: (variable + literal1) + literal2
                // transforms (x + 5) + 3 → x + (5 + 3) → x + 8
                (
                    Expression::BinaryOperation {
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
            simplify_recursively(nested.as_mut())?;
            *expression = std::mem::replace(nested.as_mut(), Expression::Wildcard);
        }
        _ => {}
    }

    Ok(())
}

fn simplify_iter<'exp>(
    mut expression: impl Iterator<Item = &'exp mut Expression>,
) -> Result<(), SqlError> {
    expression.try_for_each(simplify_recursively)
}

fn simplify_where(r#where: &mut Option<Expression>) -> Result<(), SqlError> {
    r#where.as_mut().map(simplify_recursively).unwrap_or(Ok(()))
}

fn resolve_expression(expression: &Expression) -> Result<Expression, SqlError> {
    resolve_only_expression(expression).map(Expression::Value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DatabaseError;
    use crate::sql::parser::*;

    struct Optimiser<'op> {
        input: &'op str,
        optimised: &'op str,
    }

    type OptimiserResult = Result<(), DatabaseError>;

    impl<'op> Optimiser<'op> {
        fn assert(&self) -> OptimiserResult {
            let mut statement = Parser::new(self.input).parse_statement()?;
            simplify(&mut statement)?;

            assert_eq!(Parser::new(self.optimised).parse_statement()?, statement);
            Ok(())
        }

        fn assert_expression(&self) -> OptimiserResult {
            let mut expression = Parser::new(self.input).parse_expr(None)?;
            simplify_recursively(&mut expression)?;

            assert_eq!(Parser::new(self.optimised).parse_expr(None)?, expression);
            Ok(())
        }
    }

    #[test]
    fn test_simplify_add() -> OptimiserResult {
        Optimiser {
            optimised: "x + 13",
            input: "x + 4 + 7 + 2",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "x + 7",
            input: "2 + 4 + 1 + x",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "x + 24",
            input: "4 + 2 + x + 6 + 12",
        }
        .assert_expression()
    }

    #[test]
    fn test_simplify_mul() -> OptimiserResult {
        Optimiser {
            optimised: "x",
            input: "x * (3 - 2)",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "x",
            input: "(2 - 1) * x",
        }
        .assert_expression()
    }

    #[test]
    fn test_simplify_div() -> OptimiserResult {
        Optimiser {
            optimised: "x",
            input: "x / (3 - 2)",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "x",
            input: "x / (10 - 9)",
        }
        .assert_expression()
    }

    #[test]
    fn test_simplify_zero_op() -> OptimiserResult {
        Optimiser {
            optimised: "y",
            input: "(10 - 10) + y",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "y",
            input: "y - ((6 - 4) - 2)",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "z",
            input: "z + z * (2 - 2)",
        }
        .assert_expression()?;

        Optimiser {
            optimised: "-x",
            input: "(2-2) - x",
        }
        .assert_expression()
    }

    #[test]
    fn test_cant_simplify() -> OptimiserResult {
        Optimiser {
            optimised: "x * 2 + 6",
            input: "x * 2 + 6",
        }
        .assert_expression()
    }

    #[test]
    fn test_optimise_select() -> OptimiserResult {
        Optimiser {
            input: "SELECT x * 1, 2 + (2 + 2), y FROM some_table WHERE x < 5 -(-5) ORDER BY x + (y * (9-8));",
            optimised: "SELECT x, 6, y FROM some_table WHERE x < 10 ORDER BY x + y;",
        }.assert()
    }

    #[test]
    fn test_optimise_insert() -> OptimiserResult {
        Optimiser {
            input: "INSERT INTO some_table (a,b,c) VALUES (2+2, 2*(2*10), -(-5)-5);",
            optimised: "INSERT INTO some_table (a,b,c) VALUES (4, 40, 0);",
        }
        .assert()
    }

    #[test]
    fn test_optimise_delete() -> OptimiserResult {
        Optimiser {
            input: "DELETE FROM t WHERE x >= y * (2 - 2) AND x != (10+10);",
            optimised: "DELETE FROM t WHERE x >= 0 AND x != 20;",
        }
        .assert()
    }
}
