use super::{functions, math};
use crate::core::date::interval::IntervalParseError;
use crate::core::date::{DateParseError, Extract, ExtractError, ExtractKind};
use crate::core::json;
use crate::core::numeric::{Numeric, NumericError};
use crate::core::uuid::UuidError;
use crate::db::{Schema, SqlError};
use crate::sql::statement::{
    BinaryOperator, Coerce, Expression, Function, PathSegment, Temporal, Type, UnaryOperator,
    Value, NUMERIC_ANY,
};
use std::fmt::{Display, Formatter};
use std::hint::unreachable_unchecked;
use std::mem;
use std::ops::Neg;

#[derive(Debug, Clone, Copy)]
pub enum VmType {
    Bool,
    String,
    Number,
    Float,
    Date,
    Interval,
    Numeric,
    Enum,
    Blob,
}

#[derive(Debug, PartialEq)]
pub enum VmError {
    DivisionByZero(i128, i128),
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
        allowed: Vec<String>,
        found: String,
    },
    InvalidDate(DateParseError),
    InvalidInterval(IntervalParseError),
    ExtractError(ExtractError),
    UuidError(UuidError),
    NumericError(NumericError),
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
        Expression::Identifier(column) => match schema.index_of(column) {
            Some(idx) => Ok(val[idx].clone()),
            None => Err(SqlError::InvalidColumn(column.clone())),
        },
        Expression::Path { head, segments } => {
            let (base_value, remaining_segments) = match segments.first() {
                Some(PathSegment::Key(first_key)) => {
                    let qualified = format!("{head}.{first_key}");
                    match schema.index_of(&qualified) {
                        Some(idx) => (val[idx].clone(), &segments[1..]),
                        None => (
                            resolve_expression(val, schema, &Expression::Identifier(head.clone()))?,
                            &segments[..],
                        ),
                    }
                }
                _ => (
                    resolve_expression(val, schema, &Expression::Identifier(head.clone()))?,
                    &segments[..],
                ),
            };

            if matches!(base_value, Value::Null) {
                return Ok(Value::Null);
            }

            if remaining_segments.is_empty() {
                return Ok(base_value);
            }

            let mut path = String::from("$");
            for seg in remaining_segments {
                match seg {
                    PathSegment::Key(key) => {
                        path.push('.');
                        path.push_str(key);
                    }
                    PathSegment::Index(idx) => {
                        path.push('[');
                        path.push_str(&idx.to_string());
                        path.push(']');
                    }
                    PathSegment::Wildcard => path.push_str(".*"),
                }
            }

            json::get_path(&base_value, &path, json::OutputFlag::ElementType).or(Ok(Value::Null))
        }
        Expression::QualifiedIdentifier { column, table } => {
            let idx = schema
                .index_of(&format!("{table}.{column}"))
                .or_else(|| schema.last_index_of(column));

            match idx {
                Some(idx) => Ok(val[idx].clone()),
                None => {
                    // Check if `table` matches a column name. If so, treat `column` as a path key on that column.
                    if let Some(idx) = schema
                        .index_of(table)
                        .or_else(|| schema.last_index_of(table))
                    {
                        let base_value = val[idx].clone();
                        if matches!(base_value, Value::Null) {
                            return Ok(Value::Null);
                        }

                        let path = format!("$.{}", column);
                        return json::get_path(&base_value, &path, json::OutputFlag::ElementType)
                            .or(Ok(Value::Null));
                    }

                    Err(SqlError::InvalidQualifiedColumn {
                        table: table.into(),
                        column: column.into(),
                    })
                }
            }
        }
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
            let left_val = resolve_expression(val, schema, left)?;
            let right_val = resolve_expression(val, schema, right)?;

            let (left, right) = try_coerce_enum(left, left_val, right, right_val, schema)?;
            let (left, right) = left.coerce_to(right);
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }

            let mismatched_types = || {
                SqlError::Type(TypeError::CannotApplyBinary {
                    left: Expression::Value(left.clone()),
                    operator: *operator,
                    right: Expression::Value(right.clone()),
                })
            };

            let is_compatible = match operator {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Mul
                | BinaryOperator::Div => left.is_compatible(&right),

                // for comparisons and other operations, our custom `PartialEq` impl
                // handles coercion and different types correctly. we don't need a strict
                // discriminant check; we let the operation itself determine compatibility.
                _ => true,
            };

            if !is_compatible {
                return Err(mismatched_types());
            }

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

                logical @ (BinaryOperator::And | BinaryOperator::Or) => {
                    let (Value::Boolean(left), Value::Boolean(right)) = (&left, &right) else {
                        return Err(mismatched_types());
                    };

                    match logical {
                        BinaryOperator::And => Value::Boolean(*left && *right),
                        BinaryOperator::Or => Value::Boolean(*left || *right),
                        _ => unsafe { unreachable_unchecked() },
                    }
                }

                arithmetic => match (&left, &right) {
                    (Value::Temporal(t), Value::Interval(i))
                    | (Value::Interval(i), Value::Temporal(t)) => match arithmetic {
                        BinaryOperator::Plus => Value::Temporal(*t + *i),
                        BinaryOperator::Minus => Value::Temporal(*t - *i),
                        _ => return Err(mismatched_types()),
                    },

                    _ => {
                        let result = match arithmetic {
                            BinaryOperator::Plus => &left + &right,
                            BinaryOperator::Minus => &left - &right,
                            BinaryOperator::Mul => &left * &right,
                            BinaryOperator::Div => {
                                // check for division by zero

                                if right.is_zero() {
                                    let left_val = match &left {
                                        Value::Number(n) => *n,
                                        Value::Float(f) => *f as i128,
                                        _ => 0,
                                    };
                                    return Err(VmError::DivisionByZero(left_val, 0).into());
                                }

                                &left / &right
                            }

                            _ => unreachable!("unhandled arithmetic operator: {arithmetic}"),
                        };

                        result.ok_or_else(mismatched_types)?
                    }
                },
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
            Function::Extract => {
                let kind = get_value::<String>(val, schema, &args[0])?;
                let kind = ExtractKind::try_from(kind).unwrap();

                let extractable_value = resolve_expression(val, schema, &args[1])?;
                let result = match extractable_value {
                    Value::Temporal(temporal) => temporal.extract(kind)?,
                    Value::Interval(interval) => interval.extract(kind)?,
                    _ => {
                        return Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
                            expected: vec![VmType::Date, VmType::Interval],
                        }))
                    }
                };

                Ok(Value::Number(result as i128))
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
            Function::Coalesce => {
                for arg in args {
                    let value = resolve_expression(val, schema, arg)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }

                Ok(Value::Null)
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
        Expression::IsNull { expr, negated } => {
            let expr = resolve_expression(val, schema, expr)?;
            let is_null = matches!(expr, Value::Null);

            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }
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
    Temporal => (Temporal, Date),
    Numeric => (Numeric, Numeric)
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
        Value::Null => Ok(false),

        other => Err(SqlError::Type(TypeError::ExpectedType {
            expected: VmType::Bool,
            found: Expression::Value(other),
        })),
    }
}

fn try_coerce_enum(
    left_expr: &Expression,
    left: Value,
    right_expr: &Expression,
    right: Value,
    schema: &Schema,
) -> Result<(Value, Value), SqlError> {
    let get_variants = |expr: &Expression| {
        let idx = match expr {
            Expression::Identifier(column) => schema.index_of(&column),
            Expression::QualifiedIdentifier { table, column } => schema
                .index_of(&format!("{table}.{column}"))
                .or(schema.last_index_of(column)),
            _ => None,
        }?;

        let col = schema.columns.get(idx)?;
        match col.data_type {
            Type::Enum(id) => schema.get_enum(id).or(col.type_def.as_ref()),
            _ => None,
        }
    };

    let string_to_enum = |s: &str, variants: &Vec<String>| -> Result<Value, SqlError> {
        let idx = variants.iter().position(|v| v == s).ok_or_else(|| {
            SqlError::Type(TypeError::InvalidEnumVariant {
                allowed: variants.clone(),
                found: s.to_string(),
            })
        })?;

        if idx > u8::MAX as usize {
            return Err(SqlError::Other(format!(
                "Enum has too many variants ({}). Maximum is 256.",
                variants.len()
            )));
        }

        Ok(Value::Enum(idx as u8))
    };

    match (&left, &right) {
        (Value::Enum(_), Value::String(s)) => match get_variants(left_expr) {
            Some(variants) => Ok((left, string_to_enum(s, variants)?)),
            None => Ok((left, right)),
        },

        (Value::String(s), Value::Enum(_)) => match get_variants(right_expr) {
            Some(variants) => Ok((string_to_enum(s, variants)?, right)),
            None => Ok((left, right)),
        },

        _ => Ok((left, right)),
    }
}

impl From<&Type> for VmType {
    fn from(value: &Type) -> Self {
        match value {
            Type::Boolean => VmType::Bool,
            Type::Varchar(_) | Type::Text => VmType::String,
            Type::Date | Type::DateTime | Type::Time => VmType::Date,
            Type::Interval => VmType::Interval,
            Type::Jsonb => VmType::Blob,
            Type::Numeric(_, _) => VmType::Numeric,
            Type::Enum(_) => VmType::Enum,
            float if float.is_float() => VmType::Float,
            number if matches!(number, Type::Uuid) || number.is_integer() || number.is_serial() => {
                VmType::Number
            }
            _ => panic!("Cannot convert type {value} to VmType"),
        }
    }
}

impl<T> From<T> for Type
where
    T: std::borrow::Borrow<VmType>,
{
    fn from(value: T) -> Self {
        let vm_type = value.borrow();
        match vm_type {
            VmType::Bool => Self::Boolean,
            VmType::String => Self::Text,
            VmType::Number => Self::BigInteger,
            VmType::Float => Self::DoublePrecision,
            VmType::Date => Self::DateTime,
            VmType::Interval => Self::Interval,
            VmType::Enum => Self::Enum(0),
            VmType::Numeric => Self::Numeric(NUMERIC_ANY, NUMERIC_ANY),
            VmType::Blob => Self::Jsonb,
        }
    }
}

impl TryFrom<&Value> for Type {
    type Error = ();

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Number(_) => Some(Type::BigInteger),
            Value::Float(_) => Some(Type::DoublePrecision),
            Value::Boolean(_) => Some(Type::Boolean),
            Value::Temporal(Temporal::Date(_)) => Some(Type::Date),
            Value::Temporal(Temporal::DateTime(_)) => Some(Type::DateTime),
            Value::Temporal(Temporal::Time(_)) => Some(Type::Time),
            Value::Interval(_) => Some(Type::Interval),
            Value::Uuid(_) => Some(Type::Uuid),
            Value::Numeric(n) => Some(Type::Numeric(n.precision(), n.scale() as usize)),
            Value::Blob(_) => Some(Type::Jsonb),
            _ => None,
        }
        .ok_or(())
    }
}

impl From<Type> for u8 {
    #[inline]
    fn from(value: Type) -> u8 {
        match value {
            Type::SmallInt => 0,
            Type::UnsignedSmallInt => 1,
            Type::Integer => 2,
            Type::UnsignedInteger => 3,
            Type::BigInteger => 4,
            Type::UnsignedBigInteger => 5,
            Type::SmallSerial => 6,
            Type::Serial => 7,
            Type::BigSerial => 8,
            Type::Boolean => 9,
            Type::Varchar(_) => 10,
            Type::Text => 11,
            Type::Real => 12,
            Type::DoublePrecision => 13,
            Type::Uuid => 14,
            Type::Numeric(_, _) => 15,
            Type::Date => 16,
            Type::Time => 17,
            Type::DateTime => 18,
            Type::Interval => 19,
            Type::Jsonb => 20,
            Type::Enum(_) => 21,
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
                write!(f, "Expected {expected:#?} but found {found:?}")
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
            TypeError::InvalidEnumVariant { allowed, found } => write!(
                f,
                "Invalid input value '{found}' for enum. Allowed: {allowed:?}"
            ),
            TypeError::ExtractError(err) => err.fmt(f),
            TypeError::InvalidDate(err) => err.fmt(f),
            TypeError::UuidError(err) => err.fmt(f),
            TypeError::InvalidInterval(err) => err.fmt(f),
            TypeError::NumericError(err) => err.fmt(f),
        }
    }
}

impl Display for VmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DivisionByZero(left, right) => write!(f, "Division by zero: {left} / {right}"),
            Self::NegativeNumSqrt => write!(f, "Cannot take square root of a negative number"),
        }
    }
}
