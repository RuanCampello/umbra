use crate::core::date::DateParseError;
use crate::db::{Schema, SqlError};
use crate::sql::statement::{BinaryOperator, Expression, Type, UnaryOperator, Value};
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub(crate) enum VmType {
    Bool,
    String,
    Number,
    Date,
}

#[derive(Debug, PartialEq)]
pub(crate) enum VmError {
    DivisionByZero(i128, i128),
}

#[derive(Debug, PartialEq)]
pub(crate) enum TypeError {
    CannotApplyUnary {
        operator: UnaryOperator,
        value: Value,
    },
    CannotApplyBinary {
        left: Expression,
        operator: BinaryOperator,
        right: Expression,
    },
    ExpectedType {
        expected: VmType,
        found: Expression,
    },
    InvalidDate(DateParseError),
}

pub(crate) fn resolve_expression<'exp>(
    val: &[Value],
    schema: &Schema,
    expression: &'exp Expression,
) -> Result<Value, SqlError> {
    match expression {
        Expression::Value(value) => Ok(value.clone()),
        Expression::Identifier(ident) => match schema.index_of(ident) {
            Some(idx) => Ok(val[idx].clone()),
            None => Err(SqlError::InvalidColumn(ident.clone())),
        },
        Expression::UnaryOperation { operator, expr } => {
            match resolve_expression(val, schema, expr)? {
                Value::Number(mut num) => {
                    if let UnaryOperator::Minus = operator {
                        num = -num;
                    }

                    Ok(Value::Number(num))
                }

                value => Err(SqlError::Type(TypeError::CannotApplyUnary {
                    operator: *operator,
                    value,
                })),
            }
        }
        Expression::BinaryOperation {
            operator,
            left,
            right,
        } => {
            let left_val = resolve_expression(val, schema, left)?;
            let right_val = resolve_expression(val, schema, right)?;

            let miss_type = || {
                let err = TypeError::CannotApplyBinary {
                    left: left.as_ref().clone(),
                    operator: *operator,
                    right: right.as_ref().clone(),
                };
                SqlError::Type(err)
            };

            if std::mem::discriminant(&left_val) != std::mem::discriminant(&right_val) {
                return Err(miss_type());
            }

            Ok(match operator {
                BinaryOperator::Eq => Value::Boolean(left == right),
                BinaryOperator::Neq => Value::Boolean(left != right),
                BinaryOperator::Lt => Value::Boolean(left < right),
                BinaryOperator::LtEq => Value::Boolean(left <= right),
                BinaryOperator::Gt => Value::Boolean(left > right),
                BinaryOperator::GtEq => Value::Boolean(left >= right),
                logical @ (BinaryOperator::And | BinaryOperator::Or) => {
                    let (a, b) = match (&left_val, &right_val) {
                        (Value::Boolean(a), Value::Boolean(b)) => (a, b),
                        _ => {
                            let miss_type = miss_type();
                            return Err(miss_type);
                        }
                    };
                    match logical {
                        BinaryOperator::And => Value::Boolean(*a && *b),
                        BinaryOperator::Or => Value::Boolean(*a || *b),
                        _ => unreachable!(),
                    }
                }
                arithmetic @ (BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Mul
                | BinaryOperator::Div) => {
                    let (n1, n2) = match (&left_val, &right_val) {
                        (Value::Number(n1), Value::Number(n2)) => (n1, n2),
                        _ => {
                            let miss_type = miss_type();
                            return Err(miss_type);
                        }
                    };

                    if arithmetic == &BinaryOperator::Div && *n2 == 0 {
                        return Err(VmError::DivisionByZero(*n1, *n2).into());
                    }

                    let computed = match arithmetic {
                        BinaryOperator::Plus => n1 + n2,
                        BinaryOperator::Minus => n1 - n2,
                        BinaryOperator::Mul => n1 * n2,
                        BinaryOperator::Div => n1 / n2,
                        _ => unreachable!("unhandled arithmetic operator: {arithmetic:#?}"),
                    };
                    Value::Number(computed)
                }
            })
        }
        Expression::Nested(expr) => resolve_expression(val, schema, expr),
        Expression::Wildcard => unreachable!("Wildcards should have been resolved by now"),
    }
}

pub(crate) fn resolve_only_expression(expr: &Expression) -> Result<Value, SqlError> {
    resolve_expression(&[], &Schema::empty(), expr)
}

pub(crate) fn evaluate_where(
    schema: &Schema,
    tuple: &Vec<Value>,
    expr: &Expression,
) -> Result<bool, SqlError> {
    match resolve_expression(tuple, schema, expr)? {
        Value::Boolean(boolean) => Ok(boolean),

        other => Err(SqlError::Type(TypeError::ExpectedType {
            expected: VmType::Bool,
            found: Expression::Value(other),
        })),
    }
}

impl From<&Type> for VmType {
    fn from(value: &Type) -> Self {
        match value {
            Type::Boolean => VmType::Bool,
            Type::Varchar(_) => VmType::String,
            Type::Time | Type::Date | Type::DateTime => VmType::Date,
            _ => VmType::Number,
        }
    }
}

impl Display for TypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeError::CannotApplyBinary {
                operator,
                right,
                left,
            } => {
                write!(
                    f,
                    "Cannot apply binary operator {operator:#?} to {left:#?} and {right:#?}"
                )
            }
            TypeError::CannotApplyUnary { operator, value } => {
                write!(f, "Cannot apply unary operator {operator:#?} to {value:#?}")
            }
            TypeError::ExpectedType { expected, found } => {
                write!(f, "Expected {expected:#?} but found {found:#?}")
            }
            TypeError::InvalidDate(err) => err.fmt(f),
        }
    }
}
