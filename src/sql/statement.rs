//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

use std::cmp::Ordering;
use std::fmt::{Display, Write};

/// SQL statements,
pub(crate) enum Statement {
    Create(String),
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
}

/// The `UPDATE` assignment instruction.
pub(crate) struct Assignment {
    pub identifier: String,
    pub value: Expression,
}

pub(crate) struct Column {
    name: String,
    data_type: Type,
    constraints: Vec<Constraint>,
}

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

pub(crate) enum Expression {
    Identifier(String),
    Value(String),
    Nested(Box<Self>),
}

#[derive(PartialEq)]
pub(crate) enum Value {
    String(String),
    Number(i128),
    Boolean(bool),
}

#[derive(Debug, PartialEq)]
enum Constraint {
    PrimaryKey,
    Unique,
}

/// SQL data types.
#[derive(Debug, PartialEq)]
pub(crate) enum Type {
    Integer,
    UnsignedInteger,
    BigInteger,
    UnsignedBigInteger,
    Boolean,
    Varchar(usize),
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
