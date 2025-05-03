use crate::sql::statement::{BinaryOperator, Expression, Type, UnaryOperator, Value};

#[derive(Debug, PartialEq)]
pub(crate) enum VmType {
    Bool,
    String,
    Number,
    Timestamp,
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
}

impl From<&Type> for VmType {
    fn from(value: &Type) -> Self {
        match value {
            Type::Boolean => VmType::Bool,
            Type::Varchar(_) => VmType::String,
            Type::Time | Type::Date | Type::DateTime => VmType::Timestamp,
            _ => VmType::Number,
        }
    }
}
