//! Tokens definitions for SQL.
//!
//! You may consider checking out [IBM documentation](https://www.ibm.com/docs/en/db2/11.5.0?topic=elements-tokens) on that.

use std::fmt::Display;

/// SQL tokens.
pub(in crate::sql) enum Token {
    Whitespace(Whitespace),
    Identifier(String),
    Keyword(String),
    Number(String),
    Eq,
    Neq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Mul,
    Div,
    Plus,
    Minus,
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
}

pub(in crate::sql) enum Whitespace {
    Tab,
    Space,
    Newline,
}

/// A little subset of SQL keywords.
///
/// [Here](https://www.ibm.com/docs/en/informix-servers/12.10.0?topic=appendixes-keywords-sql-informix) there are much more than that, if you want to check out.
pub(in crate::sql) enum Keyword {
    Select,
    Create,
    Update,
    Delete,
    Insert,
    Into,
    Values,
    Set,
    Drop,
    Where,
    From,
    And,
    Or,
    Primary,
    Key,
    Unique,
    Table,
    Database,
    Int,
    BigInt,
    Unsigned,
    Varchar,
    Bool,
    True,
    False,
    Index,
    Order,
    By,
    On,
    Start,
    Transaction,
    Rollback,
    Commit,
    Explain,
}
