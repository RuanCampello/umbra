//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

#![allow(unused)]

use crate::core::date::{DateParseError, NaiveDate, NaiveDateTime, NaiveTime, Parse};
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter, Write};

/// SQL statements.
#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    Create(Create),
    Select {
        columns: Vec<Expression>,
        from: String,
        r#where: Option<Expression>,
        order_by: Vec<Expression>,
        // TODO: limit
    },
    Update {
        table: String,
        columns: Vec<Assignment>,
        r#where: Option<Expression>,
    },
    Insert {
        into: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },
    Delete {
        from: String,
        r#where: Option<Expression>,
    },
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
pub(crate) struct Column {
    pub name: String,
    pub data_type: Type,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Create {
    Database(String),
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
pub(crate) enum Drop {
    Table(String),
    Database(String),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub(crate) enum Expression {
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
    Nested(Box<Self>),
}

/// Date/Time related types.
///
/// This enum wraps actual values of date/time types, such as a specific calendar date or a time of day.
/// It distinguishes between `DATE`, `TIME`, and `TIMESTAMP` at the value level.
///
/// Values of this type are stored in the `Value::Temporal` variant.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Temporal {
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
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Value {
    String(String),
    Number(i128),
    Boolean(bool),
    Temporal(Temporal),
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Constraint {
    PrimaryKey,
    Unique,
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub(crate) enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, PartialOrd, Clone, Copy)]
pub(crate) enum BinaryOperator {
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
}

/// SQL data types.
///
/// This enum describes the logical *type* of a column in a table schema.
/// It is used during planning, validation, and schema definition.
///
/// For example:
/// - A column defined as `VARCHAR(255)` will be represented as `Type::Varchar(255)`.
/// - A column of `DATE` will be represented as `Type::Date`.
///
/// `Type` does not store actual data — it represents the expected type *declaration* in the schema.#[derive(Debug, PartialEq, Clone)]
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Type {
    Integer,
    UnsignedInteger,
    BigInteger,
    UnsignedBigInteger,
    Boolean,
    Varchar(usize),
    Date,
    Time,
    DateTime,
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

impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Create(create) => match create {
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

            Statement::Select {
                columns,
                from,
                r#where,
                order_by,
            } => {
                write!(f, "SELECT {} FROM {from}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", join(order_by, ", "))?;
                }
            }

            Statement::Delete { from, r#where } => {
                write!(f, "DELETE FROM {from}")?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            }

            Statement::Update {
                table,
                columns,
                r#where,
            } => {
                write!(f, "UPDATE {table} SET {}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            }

            Statement::Insert {
                into,
                columns,
                values,
            } => {
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

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Number(a), Value::Number(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Type {
    pub fn is_integer_in_bounds(&self, int: &i128) -> bool {
        let bound = match self {
            Self::Integer => i32::MIN as i128..=i32::MAX as i128,
            Self::UnsignedInteger => 0..=u32::MAX as i128,
            Self::BigInteger => i64::MIN as i128..=i64::MAX as i128,
            Self::UnsignedBigInteger => 0..=u64::MAX as i128,
            other => panic!("bound checking must be used only for integer: {other:#?}"),
        };

        bound.contains(int)
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
            Type::Boolean => f.write_str("BOOL"),
            Type::Integer => f.write_str("INT"),
            Type::UnsignedInteger => f.write_str("UNSIGNED INT"),
            Type::BigInteger => f.write_str("BIGINT"),
            Type::UnsignedBigInteger => f.write_str("UNSIGNED BIGINT"),
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
            Value::Boolean(bool) => f.write_str(if *bool { "TRUE" } else { "FALSE" }),
            Value::Temporal(temporal) => write!(f, "{temporal}"),
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
