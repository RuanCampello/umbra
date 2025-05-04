use crate::core::date::DateParseError;
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
pub(crate) enum TypeError<'exp> {
    CannotApplyUnary {
        operator: UnaryOperator,
        value: Value,
    },
    CannotApplyBinary {
        left: &'exp Expression,
        operator: &'exp BinaryOperator,
        right: &'exp Expression,
    },
    ExpectedType {
        expected: VmType,
        found: &'exp Expression,
    },
    InvalidDate(DateParseError),
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

impl<'exp> Display for TypeError<'exp> {
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
