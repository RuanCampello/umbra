//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

#![allow(unused)]

use crate::core::date::{DateParseError, NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::uuid::Uuid;
use crate::vm::expression::{TypeError, VmType};
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter, Write};
use std::hash::Hash;
use std::ops::Neg;
use std::str::FromStr;

/// SQL statements.
#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    Create(Create),
    Select(Select),
    Update(Update),
    Insert(Insert),
    Delete(Delete),
    Drop(Drop),
    Commit,
    StartTransaction,
    Rollback,
    Explain(Box<Self>),
}

/// The `UPDATE` assignment instruction.
#[derive(Debug, PartialEq)]
pub(crate) struct Assignment {
    pub identifier: String,
    pub value: Expression,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: Type,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Create {
    Database(String),
    Sequence {
        name: String,
        r#type: Type,
        table: String,
    },
    Table {
        name: String,
        columns: Vec<Column>,
    },
    Index {
        name: String,
        table: String,
        column: String,
        unique: bool,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) struct Select {
    pub columns: Vec<Expression>,
    pub from: String,
    pub r#where: Option<Expression>,
    pub order_by: Vec<Expression>,
    pub group_by: Vec<Expression>,
    // TODO: limit
}

#[derive(Debug, PartialEq)]
pub(crate) struct Update {
    pub table: String,
    pub columns: Vec<Assignment>,
    pub r#where: Option<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Insert {
    pub into: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Delete {
    pub from: String,
    pub r#where: Option<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Drop {
    Table(String),
    Database(String),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Expression {
    Identifier(String),
    Value(Value),
    Wildcard,
    UnaryOperation {
        operator: UnaryOperator,
        expr: Box<Self>,
    },
    BinaryOperation {
        operator: BinaryOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
    Function {
        func: Function,
        args: Vec<Self>,
    },
    Nested(Box<Self>),
}

/// Date/Time related types.
///
/// This enum wraps actual values of date/time types, such as a specific calendar date or a time of day.
/// It distinguishes between `DATE`, `TIME`, and `TIMESTAMP` at the value level.
///
/// Values of this type are stored in the `Value::Temporal` variant.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum Temporal {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

/// A runtime value stored in a table row or returned in a query.
///
/// This enum represents a *concrete value* associated with a column,
/// typically used in query results, expression evaluation, and row serialization.
///
/// For example:
/// - A row with a `VARCHAR` column might store `Value::String("hello".to_string())`.
/// - A `DATE` column would store `Value::Temporal(Temporal::Date(...))`.
///
/// `Value` is the counterpart to `Type`:  
/// - `Type` defines what *kind* of value is allowed.  
/// - `Value` holds the *actual* data.
///
/// This separation allows the system to validate values against schema definitions at runtime.
///
/// ## Equality semantics
///
/// `Value` implements [`PartialEq`] for deep equality:
/// - Two values are equal only if they are the same variant and hold exactly equal contents.
///   For example, `Value::Number(5) == Value::Number(5)` is true,  
///   but `Value::Number(5) == Value::Float(5.0)` is also true, because they can be coerced
///   losslessly from integer to float.
///
/// This enable coercing comparisons (e.g., treating `Number` and `Float` as “same kind”)
/// during query planning.
///
/// ```rust
/// use umbra::sql::statement::Value;
///
/// assert_eq!(Value::Number(5), Value::Number(5));
/// assert_ne!(Value::Number(5), Value::Float(5.1));
/// assert!(Value::Number(5).eq(&Value::Float(5.0))); // both numeric kinds and lossless coercible
/// ```
#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Number(i128),
    Float(f64),
    Boolean(bool),
    Temporal(Temporal),
    Uuid(Uuid),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Constraint {
    PrimaryKey,
    Unique,
    Default(Expression),
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Mul,
    Div,
    And,
    Or,
    Like,
}

/// SQL data types.
///
/// This enum describes the logical *type* of a column in a table schema.
/// It is used during planning, validation, and schema definition.
///
/// For example:
/// - A column defined as `VARCHAR(255)` will be represented as `Type::Varchar(255)`.
/// - A column of `DATE` will be represented as `Type::Date`.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Type {
    /// 2-byte signed integer
    SmallInt,
    /// 2-byte unsigned integer
    UnsignedSmallInt,
    /// 4-byte signed integer
    Integer,
    /// 4-byte unsigned integer
    UnsignedInteger,
    /// 8-byte signed integer
    BigInteger,
    /// 8-byte unsigned integer
    UnsignedBigInteger,
    /// Auto-incrementing 2-byte signed integer (serial) backed by a sequence.
    /// Behaves like PostgreSQL `SERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    SmallSerial,
    /// Auto-incrementing 4-byte signed integer (serial) backed by a sequence.
    /// Behaves like PostgreSQL `SERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    Serial,
    /// Auto-incrementing 8-byte signed integer (bigserial) backed by a sequence.
    /// Behaves like PostgreSQL `BIGSERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    BigSerial,
    /// Boolean type (true/false)
    Boolean,
    /// Variable length character type with a limit
    Varchar(usize),
    /// 4-byte variable-precision floating point type.
    Real,
    /// 8-byte variable-precision floating point type.
    DoublePrecision,
    /// 8-byte Universal Unique Identifier defined by [RFC 4122](https://datatracker.ietf.org/doc/html/rfc4122).
    Uuid,
    Date,
    Time,
    DateTime,
}

/// Subset of `SQL` functions.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Function {
    /// Extracts the `string` to the `length` at the `start`th character (if specified) and stop
    /// after the `count` character. Must provide at least of of `start` and `count`.
    /// ```sql
    /// SUBSTRING(string text [FROM start text][FOR count int]) -> text;
    /// ```
    Substring,
    /// Concatenates the string representation of all arguments.
    Concat,
    /// Returns the numeric representation of argument's first character.
    Ascii,
    /// Returns the first index of the specified `substring` within the given `string`. Returns
    /// zero if it's not present.
    /// ```sql
    /// POSITION(substring text IN string text) -> int;
    /// ```
    Position,
    /// Computes the number of input rows.
    Count,
    /// Computes the average (arithmetic mean) of all input values.
    Avg,
    /// Computes the sum of the input values.
    Sum,
    /// Computes the minimum of the input values.
    Min,
    /// Computes the maximum of the input values.
    Max,
    UuidV4,
}

impl Column {
    pub fn new(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![],
        }
    }

    pub fn primary_key(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![Constraint::PrimaryKey],
        }
    }

    pub fn unique(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![Constraint::Unique],
        }
    }
}

impl Type {
    pub fn is_integer_in_bounds(&self, int: &i128) -> bool {
        let bound = match self {
            Self::SmallInt => i16::MIN as i128..=i16::MAX as i128,
            Self::UnsignedSmallInt => u16::MIN as i128..=u16::MAX as i128,
            Self::SmallSerial => 1..=(i16::MAX as i128),
            Self::Integer => i32::MIN as i128..=i32::MAX as i128,
            Self::UnsignedInteger => 0..=u32::MAX as i128,
            Self::Serial => 1..=(i32::MAX as i128),
            Self::BigInteger => i64::MIN as i128..=i64::MAX as i128,
            Self::UnsignedBigInteger => 0..=u64::MAX as i128,
            Self::BigSerial => 1..=(i64::MAX as i128),
            other => panic!("bound checking must be used only for integer: {other:#?}"),
        };

        bound.contains(int)
    }

    pub const fn is_float_in_bounds(&self, float: &f64) -> bool {
        match self {
            Self::Real => *float >= f32::MIN as f64 && *float <= f32::MAX as f64,
            Self::DoublePrecision => float.is_finite(),
            other => panic!("bound checking must be used only for floats"),
        }
    }

    pub const fn is_integer(&self) -> bool {
        match self {
            Self::SmallInt
            | Self::UnsignedSmallInt
            | Self::Integer
            | Self::UnsignedInteger
            | Self::BigInteger
            | Self::UnsignedBigInteger => true, // uuid in the end is just an integer number
            _ => self.is_serial(),
        }
    }

    pub const fn is_serial(&self) -> bool {
        match self {
            Self::SmallSerial | Self::Serial | Self::BigSerial => true,
            _ => false,
        }
    }

    pub const fn is_float(&self) -> bool {
        match self {
            Self::Real | Self::DoublePrecision => true,
            _ => false,
        }
    }

    /// Returns true if is any numeric type.
    pub const fn is_number(&self) -> bool {
        matches!(self, Self::Uuid) || self.is_serial() || self.is_integer()
    }

    // Returns true if this `Type` can be auto-generated
    pub const fn can_be_autogen(&self) -> bool {
        matches!(self, Self::Uuid) || self.is_serial()
    }

    pub const fn max(&self) -> usize {
        match self {
            Self::SmallSerial => 32767usize,
            Self::Serial => 2147483647usize,
            Self::BigSerial => 9223372036854775807usize,
            _ => panic!("MAX function is meant to be used only for serial types"),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Create(create) => match create {
                Create::Sequence {
                    name,
                    r#type,
                    table,
                } => write!(f, "CREATE SEQUENCE {name} AS {type} OWNED BY {table}")?,
                Create::Table { name, columns } => {
                    write!(f, "CREATE TABLE {name} ({})", join(columns, ", "))?;
                }

                Create::Database(name) => {
                    write!(f, "CREATE DATABASE {name}")?;
                }

                Create::Index {
                    name,
                    table,
                    column,
                    unique,
                } => {
                    let unique = if *unique { "UNIQUE" } else { "" };
                    write!(f, "CREATE {unique} INDEX {name} ON {table}({column})")?;
                }
            },

            Statement::Select(Select {
                columns,
                from,
                r#where,
                order_by,
                group_by,
            }) => {
                write!(f, "SELECT {} FROM {from}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }

                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", join(order_by, ", "))?;
                }

                if !group_by.is_empty() {
                    write!(f, " GROUP BY {}", join(group_by, ", "))?;
                }
            }

            Statement::Delete(Delete { from, r#where }) => {
                write!(f, "DELETE FROM {from}")?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            }

            Statement::Update(Update {
                table,
                columns,
                r#where,
            }) => {
                write!(f, "UPDATE {table} SET {}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            }

            Statement::Insert(Insert {
                into,
                columns,
                values,
            }) => {
                let columns = match columns.is_empty() {
                    true => String::from(" "),
                    false => format!(" ({}) ", join(columns, ", ")),
                };

                let values = join(
                    &values
                        .iter()
                        .map(|row| format!("({})", join(row, ", ")))
                        .collect::<Vec<_>>(),
                    ", ",
                );

                write!(f, "INSERT INTO {into}{columns}VALUES ({})", values)?;
            }

            Statement::Drop(drop) => {
                match drop {
                    Drop::Table(name) => write!(f, "DROP TABLE {name}")?,
                    Drop::Database(name) => write!(f, "DROP DATABASE {name}")?,
                };
            }

            Statement::StartTransaction => f.write_str("BEGIN TRANSACTION")?,
            Statement::Commit => f.write_str("COMMIT")?,
            Statement::Rollback => f.write_str("ROLLBACK")?,
            Statement::Explain(statement) => write!(f, "EXPLAIN {statement}")?,
        };

        f.write_char(';')
    }
}

impl Default for Expression {
    fn default() -> Self {
        Expression::Wildcard
    }
}

impl Value {
    pub(crate) fn as_arithmetic_pair(&self, other: &Self) -> Option<(f64, f64)> {
        match (self, other) {
            (Value::Number(a), Value::Number(b)) => Some((*a as f64, *b as f64)),
            (Value::Float(a), Value::Float(b)) => Some((*a, *b)),
            (Value::Number(a), Value::Float(b)) => Some((*a as f64, *b)),
            (Value::Float(a), Value::Number(b)) => Some((*a, *b as f64)),
            _ => None,
        }
    }
}

impl Temporal {
    pub(crate) fn try_coerce(self, other: Self) -> (Self, Self) {
        match (self, other) {
            (Self::Time(_), Self::DateTime(timestamp)) => (self, Self::Time(timestamp.into())),
            (Self::DateTime(timestamp), Self::Time(_)) => (Self::Time(timestamp.into()), other),
            (Self::Date(_), Self::DateTime(timestamp)) => (self, Self::Date(timestamp.into())),
            (Self::DateTime(timestamp), Self::Date(_)) => (Self::Date(timestamp.into()), other),
            _ => (self, other),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Number(a), Value::Number(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            // we do this because we can coerce them later to do a comparison between floats and
            // integers
            (Value::Number(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Number(b)) => *a == (*b as f64),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Temporal(a), Value::Temporal(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Number(a), Value::Number(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Temporal(a), Value::Temporal(b)) => Some(a.cmp(b)),
            (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
            _ => panic!("those values are not comparable"),
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        fn encode_float(f: &f64) -> u64 {
            // normalize -0.0 and 0.0 to the same bits, and treat all nans the same.
            let mut bits = f.to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }
            if f.is_nan() {
                // all nans are hashed as the same.
                bits = 0x7ff8000000000000u64;
            } else if bits >> 63 != 0 {
                // for negatives, flip all bits for total ordering.
                bits = !bits;
            }

            bits
        }

        match self {
            Self::Float(f) => encode_float(f).hash(state),
            Value::Number(n) => n.hash(state),
            Value::String(s) => s.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Temporal(t) => t.hash(state),
            Value::Uuid(u) => u.hash(state),
        }
    }
}

impl Neg for Value {
    type Output = Result<Self, TypeError>;
    fn neg(self) -> Self::Output {
        match self {
            Value::Number(num) => Ok(Value::Number(-num)),
            Value::Float(float) => Ok(Value::Float(-float)),
            v => Err(TypeError::CannotApplyUnary {
                operator: UnaryOperator::Minus,
                value: v,
            }),
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;

        for constraint in &self.constraints {
            f.write_char(' ')?;
            f.write_str(match constraint {
                Constraint::PrimaryKey => "PRIMARY KEY",
                Constraint::Unique => "UNIQUE",
                Constraint::Default(value) => "DEFAULT {value}",
            })?;
        }

        Ok(())
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(ident) => f.write_str(ident),
            Self::Value(value) => write!(f, "{value}"),
            Self::Wildcard => f.write_char('*'),
            Self::BinaryOperation {
                operator,
                left,
                right,
            } => {
                write!(f, "{left} {operator} {right}")
            }
            Self::UnaryOperation { operator, expr } => {
                write!(f, "{operator}{expr}")
            }
            Self::Function { func, args } => write!(f, "{func}"),
            Self::Nested(expr) => write!(f, "({expr})"),
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Like => "LIKE",
        })
    }
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char(match self {
            Self::Minus => '-',
            Self::Plus => '+',
        })
    }
}

impl Display for Assignment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.identifier, self.value)
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Boolean => f.write_str("BOOLEAN"),
            Type::SmallInt => f.write_str("SMALLINT"),
            Type::UnsignedSmallInt => f.write_str("SMALLINT UNSIGNED"),
            Type::Integer => f.write_str("INT"),
            Type::UnsignedInteger => f.write_str("INT UNSIGNED"),
            Type::BigInteger => f.write_str("BIGINT"),
            Type::UnsignedBigInteger => f.write_str("BIGINT UNSIGNED"),
            Type::SmallSerial => f.write_str("SMALLSERIAL"),
            Type::Serial => f.write_str("SERIAL"),
            Type::BigSerial => f.write_str("BIGSERIAL"),
            Type::Real => f.write_str("REAL"),
            Type::DoublePrecision => f.write_str("DOUBLE PRECISION"),
            Type::Uuid => f.write_str("UUID"),
            Type::DateTime => f.write_str("TIMESTAMP"),
            Type::Time => f.write_str("TIME"),
            Type::Date => f.write_str("DATE"),
            Type::Varchar(max) => write!(f, "VARCHAR({max})"),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(string) => write!(f, "\"{string}\""),
            Value::Number(number) => write!(f, "{number}"),
            Value::Float(float) => write!(f, "{float}"),
            Value::Boolean(bool) => f.write_str(if *bool { "TRUE" } else { "FALSE" }),
            Value::Temporal(temporal) => write!(f, "{temporal}"),
            Value::Uuid(uuid) => write!(f, "{uuid}"),
        }
    }
}

impl Function {
    /// Returns respectvly the minimum and the maximum (if there's any) of this function arguments.
    pub const fn size_of_args(&self) -> Option<(usize, usize)> {
        const UNARY: Option<(usize, usize)> = Some((1, 1));

        match self {
            Self::Substring => Some((2, 3)),
            Self::Concat => Some((1, usize::MAX)),
            Self::Position => Some((2, 2)),
            func if func.is_unary() => UNARY,
            _ => None,
        }
    }

    /// Returns the `VmType` that this function returns.
    pub const fn return_type(&self) -> VmType {
        match self {
            Self::Substring | Self::Concat => VmType::String,
            Self::Avg | Self::Min | Self::Max | Self::Sum => VmType::Float,
            Self::UuidV4 | Self::Ascii | Self::Position | Self::Count => VmType::Number,
        }
    }

    pub const fn is_aggr(&self) -> bool {
        matches!(
            self,
            Self::Count | Self::Sum | Self::Avg | Self::Min | Self::Max
        )
    }

    pub(in crate::sql) const fn is_unary(&self) -> bool {
        matches!(self, Self::Ascii) || self.is_aggr()
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Substring => f.write_str("SUBSTRING"),
            Self::Concat => f.write_str("CONCAT"),
            Self::Ascii => f.write_str("ASCII"),
            Self::Position => f.write_str("POSITION"),
            Self::Count => f.write_str("COUNT"),
            Self::Max => f.write_str("MAX"),
            Self::Min => f.write_str("MIN"),
            Self::Sum => f.write_str("SUM"),
            Self::Avg => f.write_str("AVG"),
            Self::UuidV4 => f.write_str("u4()"),
        }
    }
}

impl Display for Temporal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DateTime(datetime) => Display::fmt(datetime, f),
            Self::Date(date) => Display::fmt(date, f),
            Self::Time(time) => Display::fmt(time, f),
        }
    }
}

impl From<Temporal> for Value {
    fn from(value: Temporal) -> Self {
        Self::Temporal(value)
    }
}

impl From<NaiveDate> for Temporal {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<NaiveDateTime> for Temporal {
    fn from(value: NaiveDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<NaiveTime> for Temporal {
    fn from(value: NaiveTime) -> Self {
        Self::Time(value)
    }
}

impl TryFrom<&str> for Temporal {
    type Error = DateParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Ok(dt) = NaiveDateTime::parse_str(value) {
            return Ok(Temporal::DateTime(dt));
        }

        if let Ok(date) = NaiveDate::parse_str(value) {
            return Ok(Temporal::Date(date));
        }

        if let Ok(time) = NaiveTime::parse_str(value) {
            return Ok(Temporal::Time(time));
        }

        Err(DateParseError::InvalidDateTime)
    }
}

pub(crate) fn join<'t, T: Display + 't>(
    values: impl IntoIterator<Item = &'t T>,
    separator: &str,
) -> String {
    let mut joined = String::new();
    let mut iter = values.into_iter();

    if let Some(value) = iter.next() {
        write!(joined, "{}", &value).unwrap();
    }

    for value in iter {
        joined.push_str(separator);
        write!(joined, "{value}").unwrap();
    }

    joined
}
