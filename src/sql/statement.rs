//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Write};

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
        values: Vec<Expression>,
    },
    Delete {
        from: String,
        r#where: Option<Expression>,
    },
    Drop(Drop),
    Commit,
    Rollback,
    Explain(Box<Self>),
}

/// The `UPDATE` assignment instruction.
#[derive(Debug, PartialEq)]
pub(in crate::sql) struct Assignment {
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

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Expression {
    Identifier(String),
    Value(Value),
    Wildcard,
    UnaryOperator {
        operator: UnaryOperator,
        expr: Box<Self>,
    },
    BinaryOperator {
        operator: BinaryOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
    Nested(Box<Self>),
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Value {
    String(String),
    Number(i128),
    Boolean(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Constraint {
    PrimaryKey,
    Unique,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, Clone)]
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
            Value::DateTime(datetime) => Display::fmt(datetime, f),
            Value::Date(date) => Display::fmt(date, f),
            Value::Time(time) => Display::fmt(time, f),
        }
    }
}
