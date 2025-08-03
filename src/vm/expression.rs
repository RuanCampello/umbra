use super::{functions, math};
use crate::core::date::DateParseError;
use crate::core::uuid::UuidError;
use crate::db::{Schema, SqlError};
use crate::sql::statement::{
    BinaryOperator, Expression, Function, Temporal, Type, UnaryOperator, Value,
};
use std::fmt::{Display, Formatter};
use std::mem;
use std::ops::Neg;

#[derive(Debug, Clone, Copy)]
pub enum VmType {
    Bool,
    String,
    Number,
    Float,
    Date,
}

#[derive(Debug, PartialEq)]
pub enum VmError {
    DivisionByZero,
    NegativeNumSqrt,
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
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
    ExpectedOneOfTypes {
        expected: Vec<VmType>,
    },
    InvalidEnumVariant {
        label: String,
        expected: Vec<String>,
        found: String,
    },
    InvalidDate(DateParseError),
    UuidError(UuidError),
}

trait ValueExtractor<T> {
    fn extract(value: Value, argument: &Expression) -> Result<T, SqlError>;
}

macro_rules! impl_value_extractor {
    ($($value_variant:ident => ($type:ty, $vm_type:ident)),* $(,)?) => {
        $(
            impl ValueExtractor<$type> for Value {
                fn extract(value: Value, argument: &Expression) -> Result<$type, SqlError> {
                    match value {
                        Value::$value_variant(x) => Ok(x),
                        _ => Err(SqlError::Type(TypeError::ExpectedType {
                            expected: VmType::$vm_type,
                            found: argument.clone()
                        })),
                    }
                }
            }
        )*
    };
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
            let value = resolve_expression(val, schema, expr)?;
            Ok(match operator {
                UnaryOperator::Minus => value.neg()?.into(),
                _ => value,
            })
        }
        Expression::BinaryOperation {
            left,
            operator,
            right,
        } => {
            let left = resolve_expression(val, schema, left)?;
            let right = resolve_expression(val, schema, right)?;

            Ok(match operator {
                BinaryOperator::Eq => Value::Boolean(left == right),
                BinaryOperator::Neq => Value::Boolean(left != right),
                BinaryOperator::Lt => Value::Boolean(left < right),
                BinaryOperator::LtEq => Value::Boolean(left <= right),
                BinaryOperator::Gt => Value::Boolean(left > right),
                BinaryOperator::GtEq => Value::Boolean(left >= right),
                BinaryOperator::Like => match (left, right) {
                    (Value::String(left), Value::String(right)) => {
                        Value::Boolean(functions::like(&left, &right))
                    }
                    _ => Value::Boolean(false),
                },

                logical @ (BinaryOperator::And | BinaryOperator::Or) => match logical {
                    BinaryOperator::And => (left & right)?,
                    BinaryOperator::Or => (left | right)?,
                    _ => unreachable!(),
                },

                BinaryOperator::Plus => (left + right)?,
                BinaryOperator::Minus => (left - right)?,
                BinaryOperator::Mul => (left * right)?,
                BinaryOperator::Div => (left / right)?,
            })
        }
        Expression::Function { func, args } => match func {
            Function::Substring => {
                let string: String = get_value(val, schema, &args[0])?;

                let start = get_value::<i128>(val, schema, &args[1])
                    .ok()
                    .map(|num| num as usize);

                let count = get_value::<i128>(val, schema, &args[2])
                    .ok()
                    .map(|num| num as isize);

                Ok(Value::String(functions::substring(&string, start, count)))
            }
            Function::Ascii => {
                let string: String = get_value(val, schema, &args[0])?;

                Ok(Value::Number(functions::ascii(&string) as i128))
            }
            Function::Concat => {
                let strings: Vec<String> = args
                    .iter()
                    .map(|arg| get_value(val, schema, arg))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Value::String(functions::concat(&strings)))
            }
            Function::Position => {
                let pat: String = get_value(val, schema, &args[0])?;
                let string: String = get_value(val, schema, &args[1])?;

                Ok(Value::Number(functions::position(&string, &pat) as i128))
            }
            Function::Abs => {
                let value = resolve_expression(val, schema, &args[0])?;
                math::abs(&value)
            }
            Function::Sqrt => {
                let value = resolve_expression(val, schema, &args[0])?;
                math::sqrt(&value)
            }
            Function::Power => {
                let base = resolve_expression(val, schema, &args[0])?;
                let expoent = resolve_expression(val, schema, &args[1])?;
                math::power(&base, &expoent)
            }
            Function::Trunc => {
                let value = resolve_expression(val, schema, &args[0])?;
                let decimals = args
                    .get(1)
                    .and_then(|arg| resolve_expression(val, schema, arg).ok());

                math::trunc(&value, decimals)
            }
            Function::Sign => {
                let value = resolve_expression(val, schema, &args[0])?;
                math::sign(&value)
            }
            Function::TypeOf => {
                let type_of = match &args[0] {
                    Expression::Identifier(name) => {
                        let idx = schema
                            .index_of(name)
                            .ok_or_else(|| SqlError::InvalidColumn(name.into()))?;
                        Ok(schema.columns[idx].data_type.to_string())
                    }
                    _ => Err(SqlError::Other(
                        "Expected identifier for typeof function".into(),
                    )),
                }?;

                Ok(Value::String(type_of))
            }
            func => unimplemented!("function {func} handling is not yet implemented"),
        },
        Expression::Nested(expr) | Expression::Alias { expr, .. } => {
            resolve_expression(val, schema, expr)
        }
        Expression::Wildcard => {
            unreachable!("Wildcards should have been resolved by now")
        }
    }
}

impl_value_extractor! {
    String => (String, String),
    Number => (i128, Number),
    Float => (f64, Float),
    Boolean => (bool, Bool),
    Temporal => (Temporal, Date)
}

fn get_value<T>(val: &[Value], schema: &Schema, argument: &Expression) -> Result<T, SqlError>
where
    Value: ValueExtractor<T>,
{
    let value = resolve_expression(val, schema, argument)?;
    Value::extract(value, argument)
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
            Type::Varchar(_) | Type::Enum(_) | Type::Text => VmType::String,
            Type::Date | Type::DateTime | Type::Time => VmType::Date,
            float if float.is_float() => VmType::Float,
            number if matches!(number, Type::Uuid) || number.is_integer() || number.is_serial() => {
                VmType::Number
            }
            _ => panic!("Cannot convert type {value} to VmType"),
        }
    }
}

impl PartialEq for VmType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // we do this for coercion properties
            (VmType::Float, VmType::Number) | (VmType::Number, VmType::Float) => true,
            (VmType::String, VmType::Date) | (VmType::Date, VmType::String) => true,
            // (VmType::String, VmType::Number) | (VmType::Number, VmType::String) => true,
            _ => mem::discriminant(self) == mem::discriminant(other),
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
            TypeError::ExpectedOneOfTypes { expected } => {
                write!(f, "Expected one of ")?;

                for (idx, r#type) in expected.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{:#?}", r#type)?;
                }

                Ok(())
            }
            TypeError::InvalidEnumVariant {
                label,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Invalid enum variant for {label}. Expected one of the variants "
                )?;

                for (idx, name) in expected.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "'{name}'")?;
                }

                write!(f, " but found '{found}'")
            }
            TypeError::InvalidDate(err) => err.fmt(f),
            TypeError::UuidError(err) => err.fmt(f),
        }
    }
}

impl Display for VmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DivisionByZero => write!(f, "Division by zero"),
            Self::NegativeNumSqrt => write!(f, "Cannot take square root of a negative number"),
        }
    }
}
