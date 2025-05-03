use crate::core::date::DateParseError;
use crate::sql::statement::{BinaryOperator, Expression, Type, UnaryOperator, Value};

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
        left:&'exp Expression,
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
