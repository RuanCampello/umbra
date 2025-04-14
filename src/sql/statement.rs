//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

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

pub(crate) enum Expression {
    Identifier(String),
    Value(String),
    Nested(Box<Self>),
}

pub(crate) enum Value {
    String(String),
    Number(i128),
    Boolean(bool),
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
