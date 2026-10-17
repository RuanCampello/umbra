use super::{functions, math};
use crate::core::date::interval::IntervalParseError;
use crate::core::date::{DateParseError, Extract, ExtractError, ExtractKind};
use crate::core::json;
use crate::core::numeric::{Numeric, NumericError};
use crate::core::uuid::UuidError;
use crate::db::{Schema, SqlError};
use crate::sql::statement::{
    BinaryOperator, Coerce, Expression, Function, NUMERIC_ANY, PathSegment, Temporal, Type,
    UnaryOperator, Value,
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
    CannotApplyUnary { operator: UnaryOperator, value: Value },
    CannotApplyBinary { left: Expression, operator: BinaryOperator, right: Expression },
    ExpectedType { expected: VmType, found: Expression },
    ExpectedOneOfTypes { expected: Vec<VmType> },
    InvalidEnumVariant { allowed: Vec<String>, found: String },
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

/// rewrites the column references of `expr` that resolve in `schema` into [Expression::Column]
/// positions, and the literals combined with a bound UUID, temporal, enum or NUMERIC column into
/// the value [resolve_expression] would convert them to on every row
/// references that do not resolve stay as they are, so evaluating the bound expression fails or succeeds exactly as before
pub(crate) fn bind(expr: &mut Expression, schema: &Schema) {
    match expr {
        Expression::Identifier(column) => {
            if let Some(position) = schema.index_of(column) {
                *expr = Expression::Column(position);
            }
        },
        Expression::QualifiedIdentifier { table, column } => {
            let index = format!("{table}.{column}");
            let position = schema.index_of(&index).or_else(|| schema.last_index_of(column));
            let document = schema.index_of(table).or_else(|| schema.last_index_of(table));

            match (position, document) {
                (Some(position), _) => *expr = Expression::Column(position),
                (_, Some(column_position)) => {
                    *expr = Expression::JsonPath {
                        column: column_position,
                        path: format!("$.{column}"),
                    }
                },
                _ => {},
            }
        },
        Expression::Path { head, segments } => {
            let qualified = match segments.first() {
                Some(PathSegment::Key(first)) => schema.index_of(&format!("{head}.{first}")),
                _ => None,
            };
            let (position, remaining) = match qualified {
                Some(position) => (position, &segments[1..]),
                _ => match schema.index_of(head) {
                    Some(position) => (position, &segments[..]),
                    _ => return,
                },
            };

            *expr = match remaining.is_empty() {
                true => Expression::Column(position),
                _ => Expression::JsonPath { column: position, path: json_path(remaining) },
            };
        },
        Expression::BinaryOperation { left, right, .. } => {
            bind(left, schema);
            bind(right, schema);
            fold_literal(left, right, schema);
            fold_literal(right, left, schema);
        },
        Expression::Function { func: Function::TypeOf, .. } => {},
        Expression::Function { args, .. } => args.iter_mut().for_each(|arg| bind(arg, schema)),
        Expression::UnaryOperation { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => bind(expr, schema),
        Expression::Value(_)
        | Expression::Wildcard
        | Expression::Column(_)
        | Expression::JsonPath { .. } => {},
    }
}

/// the JSON path text of `segments`, rooted at `$`
fn json_path(segments: &[PathSegment]) -> String {
    let mut path = String::from("$");
    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                path.push('.');
                path.push_str(key);
            },
            PathSegment::Index(idx) => {
                path.push('[');
                path.push_str(&idx.to_string());
                path.push(']');
            },
            PathSegment::Wildcard => path.push_str(".*"),
        }
    }
    path
}

fn fold_literal(column: &Expression, literal: &mut Expression, schema: &Schema) {
    let (Expression::Column(position), Expression::Value(value)) = (column, &*literal) else {
        return;
    };

    let column = &schema.columns[*position];
    let folded = match (column.column_type(), value) {
        (Type::Uuid, Value::String(string)) => string.parse().ok().map(Value::Uuid),
        (Type::Enum(_), Value::String(string)) => column
            .variants()
            .and_then(|variants| variants.iter().position(|variant| **variant == **string))
            .and_then(|variant| u8::try_from(variant).ok())
            .map(Value::Enum),
        (r#type @ (Type::Date | Type::Time | Type::DateTime), Value::String(string)) => {
            match (r#type, Temporal::try_from(&**string)) {
                (Type::Date, Ok(date @ Temporal::Date(_)))
                | (Type::Time, Ok(date @ Temporal::Time(_)))
                | (Type::DateTime, Ok(date @ Temporal::DateTime(_))) => Some(date),
                (Type::Date, Ok(Temporal::DateTime(at))) => Some(Temporal::Date(at.into())),
                (Type::Time, Ok(Temporal::DateTime(at))) => Some(Temporal::Time(at.into())),
                _ => None,
            }
            .map(Value::Temporal)
        },
        // writes store NUMERIC columns as `Numeric`, and every operation converts a number
        // operand exactly like this
        (Type::Numeric(..), Value::Number(n)) => Some(Value::Numeric(Numeric::from(*n))),
        (Type::Numeric(..), Value::Float(f)) => Numeric::try_from(*f).ok().map(Value::Numeric),
        _ => None,
    };

    if let Some(folded) = folded {
        *literal = Expression::Value(folded);
    }
}

pub(crate) fn resolve_expression(
    val: &[Value],
    schema: &Schema,
    expression: &Expression,
) -> Result<Value, SqlError> {
    match expression {
        Expression::Value(value) => Ok(value.clone()),
        Expression::Column(position) => Ok(val[*position].clone()),
        Expression::JsonPath { column, path } => match &val[*column] {
            Value::Null => Ok(Value::Null),
            document => {
                json::get_path(document, path, json::OutputFlag::ElementType).or(Ok(Value::Null))
            },
        },
        Expression::Identifier(column) => match schema.index_of(column) {
            Some(idx) => Ok(val[idx].clone()),
            _ => Err(SqlError::InvalidColumn(column.clone())),
        },
        Expression::Path { head, segments } => {
            let (base_value, remaining_segments) = match segments.first() {
                Some(PathSegment::Key(first_key)) => {
                    let qualified = format!("{head}.{first_key}");
                    match schema.index_of(&qualified) {
                        Some(idx) => (val[idx].clone(), &segments[1..]),
                        _ => (
                            resolve_expression(val, schema, &Expression::Identifier(head.clone()))?,
                            &segments[..],
                        ),
                    }
                },
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

            let path = json_path(remaining_segments);
            json::get_path(&base_value, &path, json::OutputFlag::ElementType).or(Ok(Value::Null))
        },
        Expression::QualifiedIdentifier { column, table } => {
            let idx = schema
                .index_of(&format!("{table}.{column}"))
                .or_else(|| schema.last_index_of(column));

            match idx {
                Some(idx) => Ok(val[idx].clone()),
                None => {
                    // Check if `table` matches a column name. If so, treat `column` as a path key on that column.
                    if let Some(idx) =
                        schema.index_of(table).or_else(|| schema.last_index_of(table))
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
                },
            }
        },
        Expression::UnaryOperation { operator, expr } => {
            let value = resolve_expression(val, schema, expr)?;
            Ok(match operator {
                UnaryOperator::Minus => value.neg()?,
                _ => value,
            })
        },
        Expression::BinaryOperation { left, operator, right } => {
            // PERFORMANCE: bound operands of one kind (or `NULL`) need no coercion, so no copies
            if let (Some(left), Some(right)) = (borrowed(val, left), borrowed(val, right)) {
                if !coerces(left, right) {
                    return apply(left, *operator, right);
                }
            }

            let left_val = resolve_expression(val, schema, left)?;
            // a decisive left operand skips the right one, as `evaluate_where` does
            match (operator, &left_val) {
                (BinaryOperator::Or, Value::Boolean(true))
                | (BinaryOperator::And, Value::Boolean(false)) => {
                    return Ok(left_val);
                },
                _ => {},
            }
            let right_val = resolve_expression(val, schema, right)?;

            let (left, right) = try_coerce_enum(left, left_val, right, right_val, schema)?;
            let (left, right) = left.coerce_to(right);

            apply(&left, *operator, &right)
        },

        Expression::Function { func, args } => match func {
            Function::Substring => {
                let (Some(string), Some(start), Some(count)) = (
                    get_value::<String>(val, schema, &args[0])?,
                    get_value::<i128>(val, schema, &args[1])?,
                    get_value::<i128>(val, schema, &args[2])?,
                ) else {
                    return Ok(Value::Null);
                };

                let (start, count) = (Some(start as usize), Some(count as isize));
                let substring = functions::substring(&string, start, count);
                Ok(Value::String(substring.into()))
            },
            Function::Extract => {
                let Some(kind) = get_value::<String>(val, schema, &args[0])? else {
                    return Ok(Value::Null);
                };
                let kind = ExtractKind::try_from(kind).unwrap();
                let extractable_value = resolve_expression(val, schema, &args[1])?;

                let result = match extractable_value {
                    Value::Null => return Ok(Value::Null),
                    Value::Temporal(temporal) => temporal.extract(kind)?,
                    Value::Interval(interval) => interval.extract(kind)?,
                    _ => {
                        let typ = TypeError::ExpectedOneOfTypes {
                            expected: vec![VmType::Date, VmType::Interval],
                        };
                        return Err(SqlError::Type(typ));
                    },
                };

                Ok(Value::Number(result as i128))
            },
            Function::Ascii => match get_value::<String>(val, schema, &args[0])? {
                Some(string) => Ok(Value::Number(functions::ascii(&string) as i128)),
                None => Ok(Value::Null),
            },
            Function::Concat => {
                let mut joined = String::new();
                for arg in args {
                    let resolved;
                    let value = match borrowed(val, arg) {
                        Some(value) => value,
                        _ => {
                            resolved = resolve_expression(val, schema, arg)?;
                            &resolved
                        },
                    };

                    match value {
                        Value::String(string) => joined.push_str(string),
                        _ => {
                            let typ = TypeError::ExpectedType {
                                expected: VmType::String,
                                found: arg.clone(),
                            };
                            return Err(SqlError::Type(typ));
                        },
                    }
                }

                Ok(Value::String(joined.into()))
            },
            Function::Position => match (
                get_value::<String>(val, schema, &args[0])?,
                get_value::<String>(val, schema, &args[1])?,
            ) {
                (Some(pat), Some(string)) => {
                    Ok(Value::Number(functions::position(&string, &pat) as i128))
                },
                _ => Ok(Value::Null),
            },
            Function::Coalesce => {
                for arg in args {
                    let value = resolve_expression(val, schema, arg)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }

                Ok(Value::Null)
            },
            Function::Abs => math::abs(&resolve_expression(val, schema, &args[0])?),
            Function::Sqrt => math::sqrt(&resolve_expression(val, schema, &args[0])?),
            Function::Power => {
                let base = resolve_expression(val, schema, &args[0])?;
                let expoent = resolve_expression(val, schema, &args[1])?;
                math::power(&base, &expoent)
            },
            Function::Trunc => {
                let value = resolve_expression(val, schema, &args[0])?;
                let decimals =
                    args.get(1).and_then(|arg| resolve_expression(val, schema, arg).ok());

                math::trunc(&value, decimals)
            },
            Function::Sign => math::sign(&resolve_expression(val, schema, &args[0])?),
            Function::TypeOf => {
                let type_of = match &args[0] {
                    Expression::Column(position) => {
                        Ok(schema.columns[*position].column_type().to_string())
                    },
                    Expression::Identifier(name) => {
                        let idx = schema
                            .index_of(name)
                            .ok_or_else(|| SqlError::InvalidColumn(name.into()))?;
                        Ok(schema.columns[idx].column_type().to_string())
                    },
                    _ => Err(SqlError::Other("Expected identifier for typeof function".into())),
                }?;

                Ok(Value::String(type_of.into()))
            },
            func => unimplemented!("function {func} handling is not yet implemented"),
        },
        Expression::IsNull { expr, negated } => {
            let expr = resolve_expression(val, schema, expr)?;
            Ok(Value::Boolean(matches!(expr, Value::Null) ^ *negated))
        },
        Expression::Nested(expr) | Expression::Alias { expr, .. } => {
            resolve_expression(val, schema, expr)
        },
        Expression::Wildcard => unreachable!("Wildcards should have been resolved by now"),
    }
}

impl_value_extractor! {
    Number => (i128, Number),
    Float => (f64, Float),
    Boolean => (bool, Bool),
    Temporal => (Temporal, Date),
    Numeric => (Numeric, Numeric)
}

impl ValueExtractor<String> for Value {
    fn extract(value: Value, argument: &Expression) -> Result<String, SqlError> {
        match value {
            Value::String(x) => Ok(x.to_string()),
            _ => Err(SqlError::Type(TypeError::ExpectedType {
                expected: VmType::String,
                found: argument.clone(),
            })),
        }
    }
}

/// `None` for `NULL`: the functions that read their arguments through it are strict
fn get_value<T>(
    val: &[Value],
    schema: &Schema,
    argument: &Expression,
) -> Result<Option<T>, SqlError>
where
    Value: ValueExtractor<T>,
{
    match resolve_expression(val, schema, argument)? {
        Value::Null => Ok(None),
        value => Value::extract(value, argument).map(Some),
    }
}

pub(crate) fn resolve_only_expression(expr: &Expression) -> Result<Value, SqlError> {
    resolve_expression(&[], &Schema::empty(), expr)
}

pub(crate) fn evaluate_where(
    schema: &Schema,
    tuple: &[Value],
    expr: &Expression,
) -> Result<bool, SqlError> {
    if let Expression::BinaryOperation { operator, left, right } = expr {
        match operator {
            BinaryOperator::And if !evaluate_where(schema, tuple, left)? => return Ok(false),
            BinaryOperator::And => return evaluate_where(schema, tuple, right),
            BinaryOperator::Or if evaluate_where(schema, tuple, left)? => return Ok(true),
            BinaryOperator::Or => return evaluate_where(schema, tuple, right),
            _ => {},
        }

        if let Some(result) = compare_bound(tuple, left, *operator, right) {
            return Ok(result);
        }
    }

    match resolve_expression(tuple, schema, expr)? {
        Value::Boolean(boolean) => Ok(boolean),
        Value::Null => Ok(false),

        other => Err(SqlError::Type(TypeError::ExpectedType {
            expected: VmType::Bool,
            found: Expression::Value(other),
        })),
    }
}

// PERFORMANCE: `column op literal` on borrowed values, when [bind] left both sides of one kind
// so [resolve_expression] would coerce nothing
// `None` defers to it
#[inline]
fn compare_bound(
    tuple: &[Value],
    left: &Expression,
    operator: BinaryOperator,
    right: &Expression,
) -> Option<bool> {
    let (left, right) = match (left, right) {
        (Expression::Column(column), Expression::Value(value)) => (&tuple[*column], value),
        (Expression::Value(value), Expression::Column(column)) => (value, &tuple[*column]),
        (Expression::Column(a), Expression::Column(b)) => (&tuple[*a], &tuple[*b]),
        _ => return None,
    };

    let numeric =
        |value: &Value| matches!(value, Value::Number(_) | Value::Float(_) | Value::Numeric(_));

    let comparable = !left.is_null()
        && !right.is_null()
        && (mem::discriminant(left) == mem::discriminant(right) || numeric(left) && numeric(right));

    if !comparable {
        return None;
    }

    match operator {
        BinaryOperator::Eq => Some(left == right),
        BinaryOperator::Neq => Some(left != right),
        BinaryOperator::Lt => Some(left < right),
        BinaryOperator::LtEq => Some(left <= right),
        BinaryOperator::Gt => Some(left > right),
        BinaryOperator::GtEq => Some(left >= right),
        BinaryOperator::Like => match (left, right) {
            (Value::String(left), Value::String(right)) => Some(functions::like(left, right)),
            _ => None,
        },
        _ => None,
    }
}

/// `left operator right` once both are coerced: three-valued logic over `NULL`, comparisons,
/// `LIKE`, logic and arithmetic
fn apply(left: &Value, operator: BinaryOperator, right: &Value) -> Result<Value, SqlError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(three_valued_logic(left, right, &operator));
    }

    let mismatched_types = || {
        SqlError::Type(TypeError::CannotApplyBinary {
            left: Expression::Value(left.clone()),
            operator,
            right: Expression::Value(right.clone()),
        })
    };

    let is_compatible = match operator {
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Mul
        | BinaryOperator::Div => left.is_compatible(right),

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
                Value::Boolean(functions::like(left, right))
            },
            _ => Value::Boolean(false),
        },

        logical @ (BinaryOperator::And | BinaryOperator::Or) => {
            let (Value::Boolean(left), Value::Boolean(right)) = (left, right) else {
                return Err(mismatched_types());
            };

            match logical {
                BinaryOperator::And => Value::Boolean(*left && *right),
                BinaryOperator::Or => Value::Boolean(*left || *right),
                _ => unsafe { unreachable_unchecked() },
            }
        },

        arithmetic => match (left, right) {
            (Value::Temporal(t), Value::Interval(i)) | (Value::Interval(i), Value::Temporal(t)) => {
                match arithmetic {
                    BinaryOperator::Plus => Value::Temporal(*t + *i),
                    BinaryOperator::Minus => Value::Temporal(*t - *i),
                    _ => return Err(mismatched_types()),
                }
            },

            _ => {
                let result = match arithmetic {
                    BinaryOperator::Plus => left + right,
                    BinaryOperator::Minus => left - right,
                    BinaryOperator::Mul => left * right,
                    BinaryOperator::Div => {
                        // check for division by zero

                        if right.is_zero() {
                            let left_val = match left {
                                Value::Number(n) => *n,
                                Value::Float(f) => *f as i128,
                                _ => 0,
                            };
                            return Err(VmError::DivisionByZero(left_val, 0).into());
                        }

                        left / right
                    },

                    _ => unreachable!("unhandled arithmetic operator: {arithmetic}"),
                };

                result.ok_or_else(mismatched_types)?
            },
        },
    })
}

/// whether [try_coerce_enum] or [Coerce::coerce_to] would change `left` or `right`
#[inline]
const fn coerces(left: &Value, right: &Value) -> bool {
    matches!(
        (left, right),
        (Value::Float(_), Value::Number(_))
            | (Value::Number(_), Value::Float(_))
            | (Value::String(_), Value::Temporal(_) | Value::Uuid(_) | Value::Enum(_))
            | (Value::Temporal(_) | Value::Uuid(_) | Value::Enum(_), Value::String(_))
    )
}

/// the value `expr` denotes without evaluation: a bound column or a literal
#[inline]
fn borrowed<'v>(tuple: &'v [Value], expr: &'v Expression) -> Option<&'v Value> {
    match expr {
        Expression::Column(position) => Some(&tuple[*position]),
        Expression::Value(value) => Some(value),
        _ => None,
    }
}

#[inline(always)]
/// uses 3VL matching to short-circuit correctly logical expressions with `Value::Null`
const fn three_valued_logic(left: &Value, right: &Value, operator: &BinaryOperator) -> Value {
    match operator {
        BinaryOperator::Or => match (left, right) {
            (Value::Boolean(true), _) | (_, Value::Boolean(true)) => Value::Boolean(true),
            _ => Value::Null,
        },
        BinaryOperator::And => match (left, right) {
            (Value::Boolean(false), _) | (_, Value::Boolean(false)) => Value::Boolean(false),
            _ => Value::Null,
        },
        _ => Value::Null,
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
            Expression::Column(position) => Some(*position),
            Expression::Identifier(column) => schema.index_of(column),
            Expression::QualifiedIdentifier { table, column } => {
                schema.index_of(&format!("{table}.{column}")).or(schema.last_index_of(column))
            },
            _ => None,
        }?;

        schema.columns.get(idx)?.variants()
    };

    let string_to_enum = |s: &str, variants: &[String]| -> Result<Value, SqlError> {
        let idx = variants.iter().position(|v| v == s).ok_or_else(|| {
            SqlError::Type(TypeError::InvalidEnumVariant {
                allowed: variants.to_vec(),
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
            },
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
            TypeError::CannotApplyBinary { operator, right, left } => {
                write!(f, "Cannot apply binary operator {operator:#?} to {left:#?} and {right:#?}")
            },
            TypeError::CannotApplyUnary { operator, value } => {
                write!(f, "Cannot apply unary operator {operator:#?} to {value:#?}")
            },
            TypeError::ExpectedType { expected, found } => {
                write!(f, "Expected {expected:#?} but found {found:?}")
            },
            TypeError::ExpectedOneOfTypes { expected } => {
                write!(f, "Expected one of ")?;

                for (idx, r#type) in expected.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }

                    write!(f, "{:#?}", r#type)?;
                }

                Ok(())
            },
            TypeError::InvalidEnumVariant { allowed, found } => {
                write!(f, "Invalid input value '{found}' for enum. Allowed: {allowed:?}")
            },
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
